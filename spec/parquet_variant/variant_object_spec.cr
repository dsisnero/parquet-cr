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
