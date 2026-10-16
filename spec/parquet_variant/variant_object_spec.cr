require "../spec_helper"

# Ported from `parquet-variant/src/variant/object.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: tests that construct values via `VariantBuilder` are deferred
# until the value builder is ported (Phase 3.1b); the raw-bytes tests are ported
# here. Byte-range error messages use Crystal's `start...end` range notation
# instead of Rust's `..end`.

private alias MD = ParquetVariant::VariantMetadata
private alias VO = ParquetVariant::VariantObject
private alias Err = ParquetVariant::InvalidArgumentError

describe ParquetVariant::VariantObject do
  # Upstream `test_variant_object_simple`.
  it "reads a simple object" do
    metadata_bytes = Bytes[0b0001_0001, 3, 0, 6, 9, 13] + "activeagename".to_slice
    metadata = MD.try_new(metadata_bytes)

    object_value = Bytes[0x02, 3, 0, 1, 2, 0, 1, 3, 9, 0x04, 0x0C, 42, 0x15] +
                   "hello".to_slice

    variant_obj = VO.try_new(metadata, object_value)

    variant_obj.len.should eq(3)
    variant_obj.empty?.should be_false

    variant_obj.get("active").not_nil!.as_boolean.should eq(true)
    variant_obj.get("age").not_nil!.as_int8.should eq(42)
    variant_obj.get("name").not_nil!.as_string.should eq("hello")
    variant_obj.get("missing").should be_nil

    variant_obj.field_name(3).should be_nil
    variant_obj.field_name(300).should be_nil
    variant_obj.field(3).should be_nil
    variant_obj.field(300).should be_nil

    fields = variant_obj.iter
    fields.size.should eq(3)
    fields[0][0].should eq("active")
    fields[0][1].as_boolean.should eq(true)
    fields[1][0].should eq("age")
    fields[1][1].as_int8.should eq(42)
    fields[2][0].should eq("name")
    fields[2][1].as_string.should eq("hello")

    variant_obj.field_name(0).should eq("active")
    variant_obj.field(0).not_nil!.as_boolean.should eq(true)
    variant_obj.field_name(1).should eq("age")
    variant_obj.field(1).not_nil!.as_int8.should eq(42)
    variant_obj.field_name(2).should eq("name")
    variant_obj.field(2).not_nil!.as_string.should eq("hello")
  end

  # Upstream `test_variant_object_empty`.
  it "reads an empty object" do
    metadata = MD.try_new(Bytes[0x11, 0, 0])
    variant_obj = VO.try_new(metadata, Bytes[0x02, 0, 0])

    variant_obj.len.should eq(0)
    variant_obj.empty?.should be_true
    variant_obj.get("anything").should be_nil
    variant_obj.iter.size.should eq(0)
  end

  # Upstream `test_variant_object_invalid_metadata_end_offset`.
  it "rejects invalid metadata end offset" do
    metadata_bytes = Bytes[0b0001_0001, 2, 0, 3, 8] + "agename".to_slice
    expect_raises(Err, /from 12-byte buffer/) { MD.try_new(metadata_bytes) }
  end

  # Upstream `test_variant_object_invalid_end_offset`.
  it "rejects invalid object end offset" do
    metadata = MD.try_new(Bytes[0b0001_0001, 2, 0, 3, 7] + "agename".to_slice)
    object_value = Bytes[0x02, 2, 0, 1, 0, 2, 9, 0x0C, 42, 0x15] + "hello".to_slice
    expect_raises(Err, /from 15-byte buffer/) { VO.try_new(metadata, object_value) }
  end

  # Upstream `test_object_rejects_duplicate_field_names`.
  it "rejects duplicate field names" do
    metadata_bytes = Bytes[0b0000_0001, 2, 0, 1, 2] + "aa".to_slice
    metadata = MD.try_new(metadata_bytes)
    metadata.sorted?.should be_false

    value_bytes = Bytes[0b0000_0010, 2, 0, 1, 0, 1, 2, 0b0000_1100, 0b0000_1000]
    expect_raises(Err) { variant_of(metadata, value_bytes) }
  end

  # Upstream `test_variant_object_nesting_depth_limit`.
  it "limits object nesting depth" do
    metadata = Bytes[0b0000_0001, 1, 0, 1, 'a'.ord.to_u8]

    value = make_nested_objects(ParquetVariant::MAX_NESTING_DEPTH)
    ParquetVariant::Variant.try_new(metadata, value)

    value = make_nested_objects(ParquetVariant::MAX_NESTING_DEPTH + 1)
    expect_raises(Err, /nesting depth exceeds/) do
      ParquetVariant::Variant.try_new(metadata, value)
    end
  end
end

# `Variant::try_new` with a pre-built metadata dictionary.
private def variant_of(metadata : MD, value_bytes : Bytes)
  ParquetVariant::Variant.try_new_with_metadata(metadata, value_bytes)
end

# Value bytes for `depth` single-field objects nested around a null.
private def make_nested_objects(depth : Int32) : Bytes
  value = Bytes[0_u8]
  depth.times do
    outer = Bytes[0b0000_1110, 1, 0]
    outer += Bytes.new(4) { 0_u8 }
    outer += Bytes.new(4) { |i| ((value.size.to_u32 >> (8 * i)) & 0xFF).to_u8 }
    outer += value
    value = outer
  end
  value
end

private alias V = ParquetVariant::Variant
private alias VB = ParquetVariant::VariantBuilder

# Upstream `test_variant_object_with_count`.
private def object_with_count(count : Int32, expected_field_id_size : ParquetVariant::OffsetSizeBytes) : Nil
  field_names = (0...count).map(&.to_s)
  builder = VB.new.with_field_names(field_names)
  obj = builder.new_object
  count.times { |i| obj.insert(field_names[i], i) }
  obj.finish
  metadata, value = builder.finish
  variant = V.new(metadata, value)
  variant_obj = variant.as_object.not_nil!
  variant_obj.len.should eq(count)
  variant_obj.get(field_names[0]).not_nil!.should eq(V.from(0))
  variant_obj.get(field_names[count - 1]).not_nil!.should eq(V.from(count - 1))
  variant_obj.header.field_id_size.should eq(expected_field_id_size)
end

# Upstream `test_variant_object_with_large_data`.
private def object_with_large_data(
  data_size_per_field : Int32,
  expected_field_offset_size : ParquetVariant::OffsetSizeBytes,
) : Nil
  num_fields = 20
  builder = VB.new
  obj = builder.new_object
  str_val = "a" * data_size_per_field
  num_fields.times { |v| obj.insert("id_#{v}", str_val) }
  obj.finish
  metadata, value = builder.finish
  variant = V.new(metadata, value)
  variant_obj = variant.as_object.not_nil!
  variant_obj.len.should eq(num_fields)
  variant_obj.header.field_offset_size.should eq(expected_field_offset_size)
end

describe ParquetVariant::VariantObject do
  # Upstream `test_variant_object_empty_fields`.
  it "reads an object with an empty field name" do
    builder = VB.new
    builder.new_object.with_field("", 42).finish
    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    variant_obj = variant.as_object.not_nil!
    variant_obj.len.should eq(1)
    variant_obj.get("").should eq(V.from(42))
  end

  # Upstream `test_variant_object_small_sizes_255_elements`.
  it "builds an object with 255 elements (1-byte field ids)" do
    object_with_count(255, ParquetVariant::OffsetSizeBytes::One)
  end

  # Upstream `test_variant_object_257_elements`.
  it "builds an object with 257 elements (2-byte field ids)" do
    object_with_count((1 << 8) + 1, ParquetVariant::OffsetSizeBytes::Two)
  end

  # Upstream `test_variant_object_65537_elements`.
  it "builds an object with 65537 elements (3-byte field ids)" do
    object_with_count((1 << 16) + 1, ParquetVariant::OffsetSizeBytes::Three)
  end

  # Upstream `test_variant_object_child_data_0_byte_offsets_minus_one`.
  it "chooses 1-byte field offsets for small data" do
    object_with_large_data(10, ParquetVariant::OffsetSizeBytes::One)
  end

  # Upstream `test_variant_object_256_bytes_child_data_3_byte_offsets`.
  it "chooses 2-byte field offsets for ~256 bytes per field" do
    object_with_large_data(256 + 1, ParquetVariant::OffsetSizeBytes::Two)
  end

  # Upstream `test_variant_object_16777216_bytes_child_data_4_byte_offsets`.
  it "chooses 3-byte field offsets for ~64 KiB per field" do
    object_with_large_data(65_536 + 1, ParquetVariant::OffsetSizeBytes::Three)
  end

  # Upstream `test_variant_object_65535_bytes_child_data_2_byte_offsets`.
  it "chooses 4-byte field offsets for ~16 MiB per field" do
    object_with_large_data(16_777_216 + 1, ParquetVariant::OffsetSizeBytes::Four)
  end

  # Upstream `test_objects_with_same_fields_are_equal`.
  it "treats objects with the same fields as equal" do
    b = VB.new
    o = b.new_object
    o.insert("b", nil)
    o.insert("c", nil)
    o.insert("a", nil)
    o.finish
    m, value = b.finish
    V.try_new(m, value).should eq(V.try_new(m, value))
  end

  # Upstream `test_same_objects_with_different_builder_are_equal`.
  it "treats objects from different builders as equal" do
    b1 = VB.new
    o1 = b1.new_object
    o1.insert("a", nil)
    o1.insert("b", false)
    o1.finish
    m1, v1 = b1.finish
    variant1 = V.try_new(m1, v1)

    b2 = VB.new
    o2 = b2.new_object
    o2.insert("a", nil)
    o2.insert("b", false)
    o2.finish
    m2, v2 = b2.finish
    variant2 = V.try_new(m2, v2)

    variant1.should eq(variant2)
  end

  # Upstream `test_objects_with_different_values_are_not_equal`.
  it "distinguishes objects with different values" do
    b1 = VB.new
    o1 = b1.new_object
    o1.insert("a", nil)
    o1.insert("b", 4.3)
    o1.finish
    m1, v1 = b1.finish
    variant1 = V.try_new(m1, v1)

    b2 = VB.new
    o2 = b2.new_object
    o2.insert("a", nil)
    inner = o2.new_object("b")
    inner.insert("a", 3.3)
    inner.finish
    o2.finish
    m2, v2 = b2.finish
    variant2 = V.try_new(m2, v2)

    variant1.metadata.should eq(variant2.metadata)
    variant1.should_not eq(variant2)
  end

  # Upstream `test_objects_with_different_field_names_are_not_equal`.
  it "distinguishes objects with different field names" do
    b1 = VB.new
    o1 = b1.new_object
    o1.insert("a", nil)
    o1.insert("b", 4.3)
    o1.finish
    m1, v1 = b1.finish
    variant1 = V.try_new(m1, v1)

    b2 = VB.new
    o2 = b2.new_object
    o2.insert("aardvark", nil)
    o2.insert("barracuda", 3.3)
    o2.finish
    m2, v2 = b2.finish
    variant2 = V.try_new(m2, v2)

    variant1.should_not eq(variant2)
  end

  # Upstream `test_objects_with_different_insertion_order_are_equal`.
  it "treats objects with different insertion order as equal" do
    b1 = VB.new
    o1 = b1.new_object
    o1.insert("b", false)
    o1.insert("a", nil)
    o1.finish
    m1, v1 = b1.finish
    variant1 = V.try_new(m1, v1)
    variant1.metadata.sorted?.should be_false

    b2 = VB.new.with_field_names(["b", "a"])
    o2 = b2.new_object
    o2.insert("a", nil)
    o2.insert("b", false)
    o2.finish
    m2, v2 = b2.finish
    variant2 = V.try_new(m2, v2)
    variant2.metadata.sorted?.should be_false

    variant1.should eq(variant2)
  end

  # Upstream `test_objects_with_differing_metadata_are_equal`.
  it "treats objects with differing metadata as equal" do
    b1 = VB.new
    o1 = b1.new_object
    o1.insert("a", nil)
    o1.insert("b", 4.3)
    o1.finish
    meta1, value1 = b1.finish
    variant1 = V.try_new(meta1, value1)
    variant1.metadata.sorted?.should be_true

    b2 = VB.new.with_field_names(["d", "c", "b", "a"])
    o2 = b2.new_object
    o2.insert("b", 4.3)
    o2.insert("a", nil)
    o2.finish
    meta2, value2 = b2.finish
    variant2 = V.try_new(meta2, value2)
    variant2.metadata.sorted?.should be_false

    variant1.metadata.should_not eq(variant2.metadata)
    variant1.should eq(variant2)
  end

  # Upstream `test_compare_object_with_unsorted_dictionary_vs_sorted_dictionary`.
  it "compares an unsorted dictionary against a sorted one" do
    b = VB.new
    o = b.new_object
    o.insert("a", false)
    o.insert("b", false)
    o.finish
    _, value = b.finish
    m = MD.try_new(Bytes[0b0001_0001, 2, 0, 1, 2] + "ab".to_slice)
    variant1 = V.try_new_with_metadata(m, value)

    metadata_bytes = Bytes[0b0000_0001, 3, 0, 1, 2, 3] + "aba".to_slice
    m2 = MD.try_new(metadata_bytes)
    m2.sorted?.should be_false
    variant2 = V.new_with_metadata(m2, value)

    variant1.should eq(variant2)
  end
end
