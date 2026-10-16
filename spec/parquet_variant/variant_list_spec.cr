require "../spec_helper"

# Ported from `parquet-variant/src/variant/list.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: the tests that construct values via `VariantBuilder` (large-list
# offset-width and equality tests) are deferred until the value builder is
# ported (Phase 3.1b); the raw-bytes tests are ported here.

private alias MD = ParquetVariant::VariantMetadata
private alias VL = ParquetVariant::VariantList
private alias Err = ParquetVariant::InvalidArgumentError

private def empty_metadata : MD
  MD.try_new(Bytes[0x01, 0, 0])
end

describe ParquetVariant::VariantList do
  # Upstream `test_variant_list_simple`.
  it "reads a simple list" do
    list_value = Bytes[0x03, 3, 0, 2, 3, 6, 0x0C, 42, 0x04, 0x09] + "hi".to_slice
    variant_list = VL.try_new(empty_metadata, list_value)

    variant_list.len.should eq(3)
    variant_list.empty?.should be_false

    variant_list.get(0).not_nil!.as_int8.should eq(42)
    variant_list.get(1).not_nil!.as_boolean.should eq(true)
    variant_list.get(2).not_nil!.as_string.should eq("hi")
    variant_list.get(3).should be_nil

    values = variant_list.iter
    values.size.should eq(3)
    values[0].as_int8.should eq(42)
    values[1].as_boolean.should eq(true)
    values[2].as_string.should eq("hi")
  end

  # Upstream `test_variant_list_empty`.
  it "reads an empty list" do
    variant_list = VL.try_new(empty_metadata, Bytes[0x03, 0, 0])

    variant_list.len.should eq(0)
    variant_list.empty?.should be_true
    variant_list.get(0).should be_nil
    variant_list.iter.size.should eq(0)
  end

  # Upstream `test_variant_list_large`.
  it "reads a large list with 2-byte offsets" do
    list_bytes = Bytes[
      0x17, 2, 0, 0, 0,
      0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
      0x00, 0x08,
    ]
    variant_list = VL.try_new(empty_metadata, list_bytes)

    variant_list.len.should eq(2)
    variant_list.empty?.should be_false
    variant_list.get(0).not_nil!.as_null.should be_true
    variant_list.get(1).not_nil!.as_boolean.should eq(false)
  end

  # Upstream `test_variant_list_nesting_depth_limit`.
  it "limits list nesting depth" do
    metadata = Bytes[0x01, 0, 0]

    value = make_nested_lists(ParquetVariant::MAX_NESTING_DEPTH)
    ParquetVariant::Variant.try_new(metadata, value)

    value = make_nested_lists(ParquetVariant::MAX_NESTING_DEPTH + 1)
    expect_raises(Err, /nesting depth exceeds/) do
      ParquetVariant::Variant.try_new(metadata, value)
    end
  end
end

# Value bytes for `depth` single-element lists nested around a null.
private def make_nested_lists(depth : Int32) : Bytes
  value = Bytes[0_u8]
  depth.times do
    outer = Bytes[0x0F, 1]
    outer += Bytes.new(4) { 0_u8 }
    outer += Bytes.new(4) { |i| ((value.size.to_u32 >> (8 * i)) & 0xFF).to_u8 }
    outer += value
    value = outer
  end
  value
end

private alias V = ParquetVariant::Variant
private alias VB = ParquetVariant::VariantBuilder

# Builds a list of `i32` values and returns `{metadata, value}`.
private def make_listi32(range : Range(Int32, Int32)) : Tuple(Bytes, Bytes)
  builder = VB.new
  list_builder = builder.new_list
  range.each { |v| list_builder.append_value(v) }
  list_builder.finish
  builder.finish
end

# Builds a list of `i64` values and returns `{metadata, value}`.
private def make_listi64(range : Range(Int64, Int64)) : Tuple(Bytes, Bytes)
  builder = VB.new
  list_builder = builder.new_list
  range.each { |v| list_builder.append_value(v) }
  list_builder.finish
  builder.finish
end

# Upstream `test_large_variant_list_with_child_length`.
private def large_variant_list_with_child_length(
  list_size : Int32,
  single_child_item_len : Int32,
  expected_num_element_size : ParquetVariant::OffsetSizeBytes,
  expected_offset_size_bytes : ParquetVariant::OffsetSizeBytes,
) : Nil
  builder = VB.new
  list_builder = builder.new_list

  expected_list = [] of String
  list_size.times do |i|
    random_string = (i % 256).chr.to_s * single_child_item_len
    list_builder.append_value(random_string)
    expected_list << random_string
  end
  list_builder.finish

  metadata, value = builder.finish
  variant = V.try_new(metadata, value)
  variant_list = variant.as_list.not_nil!

  variant_list.header.offset_size.should eq(expected_offset_size_bytes)
  variant_list.header.num_elements_size.should eq(expected_num_element_size)
  variant_list.len.should eq(list_size)

  list_size.times do |i|
    variant_list.get(i).not_nil!.as_string.should eq(expected_list[i])
  end
end

describe ParquetVariant::VariantList do
  # Upstream `test_large_variant_list_with_total_child_length_between_2_pow_8_and_2_pow_16`.
  it "chooses 2-byte offsets for 2^8..2^16 total child length" do
    large_variant_list_with_child_length(1, 500, ParquetVariant::OffsetSizeBytes::One,
      ParquetVariant::OffsetSizeBytes::Two)
    large_variant_list_with_child_length(255, 2, ParquetVariant::OffsetSizeBytes::One,
      ParquetVariant::OffsetSizeBytes::Two)
    large_variant_list_with_child_length(256, 2, ParquetVariant::OffsetSizeBytes::Four,
      ParquetVariant::OffsetSizeBytes::Two)
    large_variant_list_with_child_length(300, 2, ParquetVariant::OffsetSizeBytes::Four,
      ParquetVariant::OffsetSizeBytes::Two)
  end

  # Upstream `test_large_variant_list_with_total_child_length_between_2_pow_16_and_2_pow_24`.
  it "chooses 3-byte offsets for 2^16..2^24 total child length" do
    large_variant_list_with_child_length(1, 70_000, ParquetVariant::OffsetSizeBytes::One,
      ParquetVariant::OffsetSizeBytes::Three)
    large_variant_list_with_child_length(255, 275, ParquetVariant::OffsetSizeBytes::One,
      ParquetVariant::OffsetSizeBytes::Three)
    large_variant_list_with_child_length(256, 274, ParquetVariant::OffsetSizeBytes::Four,
      ParquetVariant::OffsetSizeBytes::Three)
    large_variant_list_with_child_length(300, 234, ParquetVariant::OffsetSizeBytes::Four,
      ParquetVariant::OffsetSizeBytes::Three)
  end

  # Upstream `test_large_variant_list_with_total_child_length_between_2_pow_24_and_2_pow_32`.
  it "chooses 4-byte offsets for 2^24..2^32 total child length" do
    large_variant_list_with_child_length(1, 20_000_000, ParquetVariant::OffsetSizeBytes::One,
      ParquetVariant::OffsetSizeBytes::Four)
    large_variant_list_with_child_length(255, 78_432, ParquetVariant::OffsetSizeBytes::One,
      ParquetVariant::OffsetSizeBytes::Four)
    large_variant_list_with_child_length(256, 78_125, ParquetVariant::OffsetSizeBytes::Four,
      ParquetVariant::OffsetSizeBytes::Four)
    large_variant_list_with_child_length(300, 66_667, ParquetVariant::OffsetSizeBytes::Four,
      ParquetVariant::OffsetSizeBytes::Four)
  end

  # Upstream `test_variant_list_equality`.
  it "treats lists with the same values as equal" do
    m1, v1 = make_listi32(0...10)
    m2, v2 = make_listi32(0...10)
    V.new(m1, v1).should eq(V.new(m2, v2))
  end

  # Upstream `test_variant_list_equality_different_length`.
  it "distinguishes lists of different lengths" do
    m1, v1 = make_listi32(0...10)
    m2, v2 = make_listi32(0...5)
    V.new(m1, v1).should_not eq(V.new(m2, v2))
  end

  # Upstream `test_variant_list_equality_different_values`.
  it "distinguishes lists with different values" do
    m1, v1 = make_listi32(0...10)
    m2, v2 = make_listi32(5...15)
    V.new(m1, v1).should_not eq(V.new(m2, v2))
  end

  # Upstream `test_variant_list_equality_different_types`.
  it "distinguishes lists with different element types" do
    m1, v1 = make_listi32(0...10)
    m2, v2 = make_listi64(0_i64...10_i64)
    V.new(m1, v1).should_not eq(V.new(m2, v2))
  end

  # Upstream `test_variant_list_equality_slices`.
  it "compares sub-lists within an object" do
    builder = VB.new
    object_builder = builder.new_object

    m1, v1 = make_listi32(0...10)
    object_builder.insert("list1", V.new(m1, v1))
    m2, v2 = make_listi32(0...10)
    object_builder.insert("list2", V.new(m2, v2))
    m3, v3 = make_listi32(10...20)
    object_builder.insert("list3", V.new(m3, v3))
    object_builder.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    object = variant.as_object.not_nil!
    object.get("list1").not_nil!.should eq(object.get("list2").not_nil!)
    object.get("list1").not_nil!.should_not eq(object.get("list3").not_nil!)
  end
end
