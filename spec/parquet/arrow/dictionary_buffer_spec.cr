require "../../spec_helper"

# Ported from `parquet/src/arrow/buffer/dictionary_buffer.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# potentially dictionary-encoded byte array values buffer and its arrow
# conversion.
#
# Divergences (see `plans/dictionary_buffer.cr` and `plans/parity.md`): the Rust
# enum's `matches!` checks are the `kind` getter; upstream casts the dictionary
# array to `Utf8` to inspect it, while the port reads the keys and the dictionary
# values directly (arrow-cr has no `compute::cast` yet); the flat-values path is
# inspected through the same keys/values accessors.
private alias PDict = Parquet::Arrow::Buffer::DictionaryBuffer
private alias POffset = Parquet::Arrow::Buffer::OffsetBuffer
private alias Kind = Parquet::Arrow::Buffer::DictionaryBuffer::Kind

private def mask_from(bools : Array(Bool)) : Bytes
  bytes = Bytes.new((bools.size + 7) // 8, 0_u8)
  bools.each_with_index { |value, i| bytes[i // 8] |= (1_u8 << (i % 8)) if value }
  bytes
end

private def null_buffer(mask : Bytes, len : Int32) : Arrow::NullBuffer
  Arrow::NullBuffer.new(Arrow::BooleanBuffer.from_bits(mask, 0, len))
end

private def utf8_dictionary : Arrow::DataType
  Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8)
end

# The values of a `Dictionary(K, Utf8)` array, looked up through its keys.
private def dictionary_strings(array : Arrow::ArrayBase) : Array(String?)
  dict = array.as(Arrow::DictionaryArray(Int32))
  values = dict.values.as(Arrow::GenericStringArray(Int32))
  keys = dict.keys
  Array(String?).new(array.len) do |i|
    dict.valid?(i) ? values.value(keys.value(i)) : nil
  end
end

# The decoded dictionary of a `Dictionary(K, Utf8)` array.
private def dictionary_values(array : Arrow::ArrayBase) : Array(String?)
  values = array.as(Arrow::DictionaryArray(Int32)).values.as(Arrow::GenericStringArray(Int32))
  Array(String?).new(values.len) { |i| values.value(i) }
end

# The keys of a `Dictionary(Int32, _)` array.
private def dictionary_keys(array : Arrow::ArrayBase) : Array(Int32)
  keys = array.as(Arrow::DictionaryArray(Int32)).keys
  Array(Int32).new(keys.len) { |i| keys.value(i) }
end

describe Parquet::Arrow::Buffer::DictionaryBuffer do
  # Upstream `test_dictionary_buffer`.
  it "preserves the dictionary, spills it, and rebuilds it" do
    dict_type = utf8_dictionary
    d1 = Arrow::StringArray.from_values(["hello", "world", "", "a", "b"]).as(Arrow::ArrayBase)

    buffer = PDict(Int32, Int32).with_capacity(0)

    # Read some data preserving the dictionary.
    values = [1_i32, 0_i32, 3_i32, 2_i32, 4_i32]
    buffer.as_keys(d1).not_nil!.concat(values)

    valid = [false, false, true, true, false, true, true, true]
    valid_mask = mask_from(valid)
    buffer.pad_nulls(0, values.size, valid.size, valid_mask)

    buffer.kind.should eq(Kind::Dict)

    # Read some data not preserving the dictionary: `spill_values` converts the
    # dictionary representation in place.
    values_buffer = buffer.spill_values
    read_offset = values_buffer.len
    values_buffer.try_push("bingo".to_slice, false)
    values_buffer.try_push("bongo".to_slice, false)

    valid += [false, false, true, false, true]
    null_mask = mask_from(valid)
    buffer.pad_nulls(read_offset, 2, 5, null_mask)

    buffer.kind.should eq(Kind::Values)
    buffer.len.should eq(13)

    array = buffer.into_array(null_buffer(null_mask, 13), dict_type)
    array.data_type.should eq(dict_type)

    dictionary_strings(array).should eq([
      nil,
      nil,
      "world",
      "hello",
      nil,
      "a",
      "",
      "b",
      nil,
      nil,
      "bingo",
      nil,
      "bongo",
    ])

    # Upstream `std::mem::replace(&mut buffer, DictionaryBuffer::with_capacity(0))`:
    # `into_array` consumes the buffer there, so a fresh (flat, empty) buffer is
    # used from here on.
    buffer = PDict(Int32, Int32).with_capacity(0)
    buffer.kind.should eq(Kind::Values)
    buffer.len.should eq(0)

    # Can recreate with a new dictionary as the values are empty.
    d2 = Arrow::StringArray.from_values(["bingo", ""]).as(Arrow::ArrayBase)
    buffer.as_keys(d2).not_nil!.concat([0_i32, 1_i32, 0_i32, 1_i32])

    array = buffer.into_array(nil, dict_type)
    array.data_type.should eq(dict_type)
    dictionary_strings(array).should eq(["bingo", "", "bingo", ""])

    # Cannot change the dictionary as the keys are not empty.
    d4 = Arrow::StringArray.from_values(["bananas"]).as(Arrow::ArrayBase)
    buffer.as_keys(d4).should be_nil
  end

  # Upstream `test_validates_keys`.
  it "validates dictionary keys when converting to an array" do
    dict_type = utf8_dictionary

    buffer = PDict(Int32, Int32).with_capacity(0)
    d = Arrow::StringArray.from_values(["", "f"]).as(Arrow::ArrayBase)
    buffer.as_keys(d).not_nil!.concat([0_i32, 2_i32, 0_i32])

    expect_raises(Parquet::GeneralError, "dictionary key beyond bounds of dictionary: 0..2") do
      buffer.into_array(nil, dict_type)
    end

    buffer = PDict(Int32, Int32).with_capacity(0)
    d = Arrow::StringArray.from_values([""]).as(Arrow::ArrayBase)
    buffer.as_keys(d).not_nil!.concat([0_i32, 1_i32, 0_i32])

    expect_raises(Parquet::GeneralError, "dictionary key beyond bounds of dictionary: 0..1") do
      buffer.spill_values
    end
  end

  # Upstream `test_values_path_large_utf8_type`.
  it "keeps the requested LargeUtf8 value type on the flat values path" do
    dict_type = Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.large_utf8)
    buffer = PDict(Int32, Int64).with_capacity(0)
    values = buffer.spill_values
    ["foo", "bar", "foo"].each { |value| values.try_push(value.to_slice, false) }

    array = buffer.into_array(nil, dict_type)
    dict = array.as(Arrow::DictionaryArray(Int32))

    dict.data_type.should eq(dict_type)
    dict.values.data_type.should eq(Arrow::DataType.large_utf8)
  end

  # Upstream `test_int8_key_overflow_boundary`.
  it "fills Int8 keys to capacity and rejects overflowing ones" do
    dict_type = Arrow::DataType.dictionary(Arrow::DataType.int8, Arrow::DataType.utf8)

    buffer = PDict(Int32, Int32).with_capacity(0)
    values = buffer.spill_values
    128.times { |i| values.try_push("val#{i}".to_slice, false) }

    # 128 unique strings must fit: the last index is 127 = Int8::MAX.
    array = buffer.into_array(nil, dict_type)
    array.as(Arrow::DictionaryArray(Int8)).values.as(Arrow::GenericStringArray(Int32))
      .len.should eq(128)

    buffer = PDict(Int32, Int32).with_capacity(0)
    values = buffer.spill_values
    129.times { |i| values.try_push("val#{i}".to_slice, false) }

    expect_raises(Parquet::GeneralError, "dictionary key overflow") do
      buffer.into_array(nil, dict_type)
    end
  end

  # Upstream `test_hash_collision_deduplication`.
  it "deduplicates colliding values instead of inserting them twice" do
    offset_buffer = POffset(Int32).with_capacity(3)
    offset_buffer.try_push("alpha".to_slice, false)
    offset_buffer.try_push("beta".to_slice, false)
    offset_buffer.try_push("beta".to_slice, false)

    # "alpha", "beta", "beta": all forced to share the same hash.
    hashes = [0xdeadbeef_u64, 0xdeadbeef_u64, 0xdeadbeef_u64]
    array = PDict.pack_values_from_offsets_impl(
      Int32, Arrow::DataType.int32, Arrow::DataType.utf8, offset_buffer, hashes, nil
    )

    dictionary_values(array).should eq(["alpha", "beta"])
    dictionary_keys(array).should eq([0, 1, 1])
  end

  # Upstream `test_values_path_binary_type`.
  it "keeps the requested Binary value type on the flat values path" do
    dict_type = Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.binary)
    buffer = PDict(Int32, Int32).with_capacity(0)
    values = buffer.spill_values
    values.try_push("abc".to_slice, false)
    values.try_push(Bytes[0x00_u8, 0xFF_u8], false)
    values.try_push("abc".to_slice, false)

    array = buffer.into_array(nil, dict_type)
    dict = array.as(Arrow::DictionaryArray(Int32))

    dict.data_type.should eq(dict_type)
    dict.values.data_type.should eq(Arrow::DataType.binary)
  end
end
