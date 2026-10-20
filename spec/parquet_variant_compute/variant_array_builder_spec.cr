require "../spec_helper"

# Ported from `parquet-variant-compute/src/variant_array_builder.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `VariantArrayBuilder` for unshredded arrays.
#
# Deferred: `VariantValueArrayBuilder` (only used by unshredding, see
# `plans/parity.md`).
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant

describe ParquetVariantCompute::VariantArrayBuilder do
  it "keeps the metadata and value fields non-nullable (upstream test_variant_array_builder_non_nullable)" do
    builder = PVC::VariantArrayBuilder.new(10)
    builder.extend([nil, Variant.from(42_i32)])
    array = builder.build

    array.len.should eq(2)
    array.is_null(0).should be_true
    array.is_null(1).should be_false
    array.value(1).should eq(Variant.from(42_i32))

    # The metadata and value fields of non-shredded variants must not be null.
    array.metadata_column.nulls.should be_nil
    array.value_column.nulls.should be_nil

    data_type = array.data_type
    data_type.kind.struct?.should be_true
    data_type.fields.not_nil!.each { |field| field.nullable?.should be_false }
  end

  it "appends variants, objects and lists (upstream test_variant_array_builder)" do
    builder = PVC::VariantArrayBuilder.new(10)
    builder.append_null
    builder.append_variant(Variant.from(42_i32))
    builder.new_object.with_field("foo", "bar").finish
    builder.new_list
      .with_value(Variant.from(1_i32))
      .with_value(Variant.from(2_i32))
      .finish
    array = builder.build

    array.len.should eq(4)
    array.is_null(0).should be_true
    array.is_null(1).should be_false
    array.value(1).should eq(Variant.from(42_i32))

    array.is_null(2).should be_false
    object = array.value(2).as_object.not_nil!
    object.get("foo").should eq(Variant.from("bar"))

    array.is_null(3).should be_false
    list = array.value(3).as_list.not_nil!
    list.len.should eq(2)
  end

  it "appends runs of nulls (upstream test_variant_array_builder_append_nulls)" do
    builder = PVC::VariantArrayBuilder.new(6)
    builder.append_variant(Variant.from(1_i32))
    builder.append_nulls(0)
    builder.append_nulls(3)
    builder.append_variant(Variant.from(2_i32))
    array = builder.build

    array.len.should eq(5)
    array.value(0).should eq(Variant.from(1_i32))
    array.is_null(1).should be_true
    array.is_null(2).should be_true
    array.is_null(3).should be_true
    array.value(4).should eq(Variant.from(2_i32))
  end

  it "extends from optional variants (upstream test_extend_variant_array_builder)" do
    builder = PVC::VariantArrayBuilder.new(3)
    builder.extend([nil, Variant::Null.new, Variant.from("norm")])
    array = builder.build

    array.len.should eq(3)
    array.is_null(0).should be_true
    array.value(1).should eq(Variant::Null.new)
    array.value(2).should eq(Variant.from("norm"))
  end
end
