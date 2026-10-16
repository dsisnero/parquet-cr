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
