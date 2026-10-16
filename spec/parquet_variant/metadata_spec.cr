require "../spec_helper"

# Ported from `parquet-variant/src/variant/metadata.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: Rust's zero-copy `borrowed_field_id` fast path cannot be modelled
# (Crystal `String.new(Bytes)` copies), so it always returns `nil` and the tests
# that asserted the hit now assert the (safe) miss while keeping the `get_entry`
# assertions. `metadata_bytes_for` encodes the dictionary directly instead of
# going through `VariantBuilder` (not ported yet).

private alias MD = ParquetVariant::VariantMetadata
private alias Err = ParquetVariant::InvalidArgumentError

# Encodes a metadata dictionary header + size + offsets + values for `names`,
# choosing the narrowest offset width that fits.
private def encode_metadata(names : Array(String)) : Bytes
  sorted = names == names.sort && names.uniq.size == names.size
  offsets = [0]
  names.each { |name| offsets << offsets.last + name.bytesize }
  max_offset = offsets.last
  offset_size = if max_offset <= 0xFF
                  1
                elsif max_offset <= 0xFFFF
                  2
                else
                  4
                end
  header = (0x01 | (sorted ? 0x10 : 0) | ((offset_size - 1) << 6)).to_u8

  io = IO::Memory.new
  io.write_byte(header)
  write_offset(io, names.size, offset_size)
  offsets.each { |offset| write_offset(io, offset, offset_size) }
  names.each { |name| io.write(name.to_slice) }
  io.to_slice
end

private def write_offset(io : IO, value : Int32, size : Int32) : Nil
  size.times { |i| io.write_byte(((value >> (8 * i)) & 0xFF).to_u8) }
end

# Builds a metadata buffer directly, so that offsets a builder would never emit
# can be exercised. `values` is the raw dictionary value region.
private def raw_metadata(is_sorted : Bool, offsets : Array(Int32), values : Bytes) : Bytes
  io = IO::Memory.new
  io.write_byte((0x01 | (is_sorted ? 0x10 : 0)).to_u8)
  io.write_byte((offsets.size - 1).to_u8)
  offsets.each { |offset| io.write_byte(offset.to_u8) }
  io.write(values)
  io.to_slice
end

private def bytes_of(values : Array(Int32)) : Bytes
  Bytes.new(values.size) { |i| values[i].to_u8 }
end

# Every entry must resolve to its own field id through `get_entry`. The borrowed
# fast path is disabled (divergence), so `borrowed_field_id` always declines.
private def assert_lookups_agree(metadata : MD) : Nil
  metadata.len.times do |i|
    borrowed = metadata.get(i)
    metadata.borrowed_field_id(borrowed).should be_nil
    metadata.get_entry(borrowed).should eq({i.to_u32, borrowed})
    metadata.get_entry(borrowed.dup).should eq({i.to_u32, borrowed})
    metadata.borrowed_field_id(borrowed.dup).should be_nil
  end
end

describe ParquetVariant::VariantMetadata do
  # Upstream `try_new_ok_inline`.
  it "parses inline metadata" do
    bytes = bytes_of([0b0000_0001, 0x02, 0x00, 0x03, 0x06]) + "catdog".to_slice
    metadata = MD.try_new(bytes)

    metadata.len.should eq(2)
    metadata[0].should eq("cat")
    metadata[1].should eq("dog")
    metadata.get_offset(0).should eq(0)
    metadata.get_offset(1).should eq(3)
    metadata.get_offset(2).should eq(6)
    expect_raises(Err) { metadata.get_offset(3) }

    metadata.iter.to_a.should eq(["cat", "dog"])
  end

  # Upstream `try_new_missing_last_value`.
  it "rejects a truncated final value" do
    bytes = bytes_of([0b0000_0001, 0x02, 0x00, 0x01, 0x02]) + "ab".to_slice
    working = MD.try_new(bytes)
    working.len.should eq(2)
    working[0].should eq("a")
    working[1].should eq("b")

    expect_raises(Err) { MD.try_new(bytes[0, bytes.size - 1]) }
  end

  # Upstream `try_new_fails_non_monotonic`.
  it "rejects non-monotonic offsets" do
    bytes = bytes_of([0b0000_0001, 0x03, 0x00, 0x02, 0x01, 0x10]) + "catdoglamb".to_slice
    expect_raises(Err) { MD.try_new(bytes) }
  end

  # Upstream `try_new_fails_non_monotonic2`.
  it "rejects non-monotonic offsets in full validation" do
    bytes = bytes_of([0b0000_0001, 4, 0x00, 0x02, 0x01, 0x10, 13]) + "catdoglambeeL".to_slice
    expect_raises(Err) { MD.try_new(bytes) }
  end

  # Upstream `try_new_truncated_offsets_inline`.
  it "rejects truncated offsets" do
    expect_raises(Err) { MD.try_new(bytes_of([0b0000_0001, 0x02, 0x00, 0x01])) }
  end

  # Upstream `empty_string_is_valid`.
  it "allows empty dictionary entries per sortedness rules" do
    metadata = MD.try_new(bytes_of([0b0001_0001, 1, 0x00, 0x00]))
    metadata[0].should eq("")

    metadata = MD.try_new(bytes_of([0b0001_0001, 2, 0x00, 0x00, 0x02]) + "hi".to_slice)
    metadata[0].should eq("")
    metadata[1].should eq("hi")

    expect_raises(Err) do
      MD.try_new(bytes_of([0b0001_0001, 2, 0x00, 0x02, 0x02]) + "hi".to_slice)
    end

    metadata = MD.try_new(bytes_of([0b0000_0001, 2, 0x00, 0x02, 0x02]) + "hi".to_slice)
    metadata[0].should eq("hi")
    metadata[1].should eq("")
  end

  # Upstream `test_borrowed_field_id_sorted_dictionary`.
  it "resolves entries of a large sorted dictionary" do
    names = (0...64).map { |i| "field_#{i.to_s.rjust(3, '0')}" }
    metadata = MD.try_new(encode_metadata(names))
    metadata.sorted?.should be_true
    metadata.len.should eq(64)
    assert_lookups_agree(metadata)
    metadata.get_entry("field_999").should be_nil
  end

  # Upstream `test_borrowed_field_id_unsorted_dictionary`.
  it "resolves entries of an unsorted dictionary" do
    metadata = MD.try_new(encode_metadata(["zebra", "apple", "mango", "kiwi"]))
    metadata.sorted?.should be_false
    metadata.len.should eq(4)
    assert_lookups_agree(metadata)
  end

  # Upstream `test_borrowed_field_id_unsorted_dictionary_with_duplicate_entries`.
  it "resolves duplicate entries to the first id" do
    metadata = MD.try_new(bytes_of([0b0000_0001, 2, 0x00, 0x01, 0x02]) + "aa".to_slice)
    metadata.sorted?.should be_false
    metadata.get(0).should eq("a")
    metadata.get(1).should eq("a")

    ["a", "a", "a"].each do |name|
      metadata.borrowed_field_id(name).should be_nil
      metadata.get_entry(name).should eq({0_u32, "a"})
    end
  end

  # Upstream `test_borrowed_field_id_ignores_names_from_another_dictionary`.
  it "ignores names from another dictionary" do
    this = MD.try_new(encode_metadata(["alpha", "beta", "gamma"]))
    other = MD.try_new(encode_metadata(["delta", "gamma", "beta", "alpha"]))

    other.len.times do |i|
      name = other.get(i)
      unborrowed = String.new(name.to_slice)
      this.borrowed_field_id(name).should be_nil
      this.get_entry(name).should eq(this.get_entry(unborrowed))
    end

    this.get_entry(other.get(3)).should eq({0_u32, "alpha"})
    this.get_entry(other.get(0)).should be_nil
  end

  # Upstream `test_borrowed_field_id_rejects_substring_of_an_entry`.
  it "does not resolve substrings of entries" do
    metadata = MD.try_new(bytes_of([0b0001_0001, 2, 0x00, 0x01, 0x03]) + "xyy".to_slice)
    metadata.sorted?.should be_true
    metadata.get(0).should eq("x")
    metadata.get(1).should eq("yy")
    metadata.borrowed_field_id(metadata.get(1)).should be_nil

    metadata.get_entry("y").should be_nil
    metadata.get_entry("y").should be_nil
  end

  # Upstream `test_borrowed_field_id_with_empty_field_name`.
  it "handles empty field names" do
    metadata = MD.try_new(bytes_of([0b0000_0001, 2, 0x00, 0x02, 0x02]) + "hi".to_slice)
    metadata.sorted?.should be_false
    metadata.get(0).should eq("hi")
    metadata.get(1).should eq("")
    metadata.get_entry(metadata.get(0)).should eq({0_u32, "hi"})
    metadata.get_entry(metadata.get(1)).should eq({1_u32, ""})
    metadata.get_entry("").should eq({1_u32, ""})
    metadata.borrowed_field_id(metadata.get(0)).should be_nil
  end

  # Upstream `test_borrowed_field_id_with_leading_empty_field_name`.
  it "handles a leading empty field name in a sorted dictionary" do
    metadata = MD.try_new(bytes_of([0b0001_0001, 2, 0x00, 0x00, 0x02]) + "hi".to_slice)
    metadata.sorted?.should be_true
    metadata.get(0).should eq("")
    metadata.get(1).should eq("hi")
    metadata.get_entry("").should eq({0_u32, ""})
  end

  # Upstream `test_compare_sorted_dictionary_with_unsorted_dictionary`.
  it "distinguishes sorted from unsorted dictionaries" do
    m1 = MD.try_new(encode_metadata(["a", "b"]))
    m1.sorted?.should be_true

    metadata_bytes = bytes_of([0b0000_0001, 3, 0, 1, 2, 3]) + "aab".to_slice
    m2 = MD.try_new(metadata_bytes)
    m2.sorted?.should be_false

    m1.should_not eq(m2)
  end

  # Upstream `test_compare_sorted_dictionary_with_sorted_dictionary`.
  it "compares equal sorted dictionaries" do
    bytes = encode_metadata(["a", "b"])
    MD.new(bytes).should eq(MD.new(bytes))
  end

  # Upstream `test_empty_string_field_names`.
  it "iterates field names in insertion order" do
    metadata = MD.try_new(encode_metadata(["b", "a", ""]))
    metadata.sorted?.should be_false
    metadata.iter.to_a.should eq(["b", "a", ""])
  end

  # Upstream `full_validation_rejects_offsets_splitting_a_character`.
  it "rejects offsets that split a multi-byte character" do
    [false, true].each do |is_sorted|
      bytes = raw_metadata(is_sorted, [0, 1, 2], "é".to_slice)

      shallow = MD.try_new_with_shallow_validation(bytes)
      shallow.fully_validated?.should be_false
      expect_raises(Err) { shallow.get(0) }

      expect_raises(Err) { MD.try_new(bytes) }
    end
  end

  # Upstream `validated_and_unvalidated_get_agree`.
  it "agrees between validated and unvalidated access" do
    values = "aé€\u{10348}"
    offsets = [0, 1, 1, 3, 6, 10]
    bytes = raw_metadata(false, offsets, values.to_slice)

    validated = MD.try_new(bytes)
    validated.fully_validated?.should be_true
    unvalidated = MD.try_new_with_shallow_validation(bytes)
    unvalidated.fully_validated?.should be_false

    expected = ["a", "", "é", "€", "\u{10348}"]
    validated.len.should eq(expected.size)
    expected.each_with_index do |want, i|
      validated.get(i).should eq(want)
      unvalidated.get(i).should eq(want)
    end

    expect_raises(Err) { validated.get(expected.size) }
    expect_raises(Err) { unvalidated.get(expected.size) }
  end

  # Upstream `unvalidated_get_still_reports_invalid_utf8`.
  it "reports invalid utf8 on the unvalidated path" do
    bytes = raw_metadata(false, [0, 1], bytes_of([0xFF]))
    unvalidated = MD.try_new_with_shallow_validation(bytes)
    expect_raises(Err) { unvalidated.get(0) }
    expect_raises(Err) { MD.try_new(bytes) }
  end
end
