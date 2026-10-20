require "../spec_helper"

# Ported from `parquet-variant-compute/src/variant_array.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the unshredded
# `VariantArray` surface.
#
# Deferred (documented in `plans/parity.md`): shredded variants — the
# `typed_value` interpretation, `ShreddedVariantFieldArray`, the shredding
# schema and its canonicalization — so the upstream tests that build a
# `typed_value` column are not ported yet.
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant

# Upstream `make_binary_view_array`: one non-null "test" row.
private def binary_view_array(values : Array(Bytes?)) : Arrow::BinaryViewArray
  builder = Arrow::BinaryViewBuilder.new
  values.each { |value| value ? builder.append_value(value) : builder.append_null }
  builder.finish
end

private def test_bytes : Bytes
  "test".to_slice
end

# Builds a `STRUCT<metadata, value>` array (upstream constructs the same shape
# with `StructArray::new`).
private def variant_struct(
  metadata : Arrow::ArrayBase,
  value : Arrow::ArrayBase,
  metadata_nullable : Bool = true,
  value_nullable : Bool = true,
  nulls : Arrow::NullBuffer? = nil,
) : Arrow::StructArray
  fields = Arrow::Fields.new([
    Arrow::Field.new("metadata", metadata.data_type, metadata_nullable),
    Arrow::Field.new("value", value.data_type, value_nullable),
  ])
  Arrow::StructArray.try_new(fields, [metadata, value] of Arrow::ArrayBase, nulls)
end

private def int32_array(values : Array(Int32)) : Arrow::PrimitiveArray(Int32)
  Arrow::PrimitiveArray(Int32).from_values(values)
end

describe ParquetVariantCompute::VariantArray do
  it "rejects an array that is not a struct (upstream invalid_not_a_struct_array)" do
    array = binary_view_array([test_bytes])

    error = expect_raises(Parquet::ArrowError) { PVC::VariantArray.try_new(array) }
    error.message.not_nil!.should contain "Invalid VariantArray: requires StructArray as input"
  end

  it "rejects a struct without a metadata field (upstream invalid_missing_metadata)" do
    fields = Arrow::Fields.new([Arrow::Field.new("value", Arrow::DataType.binary_view, true)])
    array = Arrow::StructArray.try_new(fields, [binary_view_array([test_bytes])] of Arrow::ArrayBase)

    error = expect_raises(Parquet::ArrowError) { PVC::VariantArray.try_new(array) }
    error.message.not_nil!.should contain "Invalid VariantArray: StructArray must contain a 'metadata' field"
  end

  it "rejects a non-binary metadata field (upstream invalid_metadata_field_type)" do
    array = variant_struct(int32_array([1]), binary_view_array([test_bytes]))

    error = expect_raises(Parquet::ArrowError) { PVC::VariantArray.try_new(array) }
    error.message.not_nil!.should contain(
      "VariantArray 'metadata' field must be Binary, LargeBinary, BinaryView, or a " \
      "Dictionary or RunEndEncoded array of one of those types, got Int32"
    )
  end

  it "rejects a non-binary value field (upstream invalid_value_field_type)" do
    array = variant_struct(binary_view_array([test_bytes]), int32_array([1]))

    error = expect_raises(Parquet::ArrowError) { PVC::VariantArray.try_new(array) }
    error.message.not_nil!.should contain(
      "VariantArray 'value' field must be Binary, LargeBinary, or BinaryView, got Int32"
    )
  end

  it "accepts an all-null value column as unshredded (upstream all_null_value_column_is_valid_and_unshredded)" do
    metadata = binary_view_array([test_bytes, test_bytes, test_bytes])
    value = binary_view_array([nil, nil, nil])
    array = variant_struct(metadata, value, metadata_nullable: false)

    variant_array = PVC::VariantArray.try_new(array)
    variant_array.typed_value_column.should be_nil

    # The rows are valid but have neither value nor typed_value; the spec
    # requires readers to return `Variant::Null` in this case.
    variant_array.len.times do |i|
      variant_array.is_valid(i).should be_true
      variant_array.value(i).should eq(Variant::Null.new)
    end
  end

  it "reports the index on out-of-bounds access (upstream test_try_value_out_of_bounds)" do
    builder = PVC::VariantArrayBuilder.new(2)
    builder.append_variant(Variant.from(1_i8))
    builder.append_variant(Variant::Null.new)
    array = builder.build

    array.try_value(0).should eq(Variant.from(1_i8))
    array.try_value(1).should eq(Variant::Null.new)

    error = expect_raises(Parquet::ArrowError) { array.try_value(2) }
    error.message.not_nil!.should contain "Index 2 out of bounds for VariantArray of length 2"
  end

  it "iterates values and nulls (upstream test_variant_array_iterable)" do
    builder = PVC::VariantArrayBuilder.new(6)
    builder.append_null
    builder.append_variant(Variant.from(1_i8))
    builder.append_variant(Variant::Null.new)
    builder.append_variant(Variant.from(2_i32))
    builder.append_variant(Variant.from(3_i64))
    builder.append_null

    array = builder.build
    array.iter.to_a.should eq([
      nil,
      Variant.from(1_i8),
      Variant::Null.new,
      Variant.from(2_i32),
      Variant.from(3_i64),
      nil,
    ] of Variant?)
  end

  it "iterates from both ends (upstream test_variant_array_iter_double_ended)" do
    builder = PVC::VariantArrayBuilder.new(5)
    builder.append_variant(Variant.from(0_i32))
    builder.append_null
    builder.append_variant(Variant.from(2_i32))
    builder.append_null
    builder.append_variant(Variant.from(4_i32))
    array = builder.build

    iter = array.iter
    iter.next.should eq(Variant.from(0_i32))
    iter.next.should be_nil
    iter.next_back.should eq(Variant.from(4_i32))
    iter.next_back.should be_nil
    iter.next_back.should eq(Variant.from(2_i32))
    iter.next.should be_a(Iterator::Stop)
    iter.next_back.should be_a(Iterator::Stop)
  end

  it "iterates in reverse (upstream test_variant_array_iter_reverse)" do
    builder = PVC::VariantArrayBuilder.new(5)
    builder.append_variant(Variant.from("a"))
    builder.append_null
    builder.append_variant(Variant.from("aaa"))
    builder.append_null
    builder.append_variant(Variant.from("aaaaa"))
    array = builder.build

    # Crystal iterators are not reversible in place; materialize and reverse.
    array.iter.to_a.reverse.should eq([
      Variant.from("aaaaa"),
      nil,
      Variant.from("aaa"),
      nil,
      Variant.from("a"),
    ] of Variant?)
  end

  it "iterates an empty array (upstream test_variant_array_iter_empty)" do
    array = PVC::VariantArrayBuilder.new(0).build
    iter = array.iter
    iter.next.should be_a(Iterator::Stop)
    iter.next_back.should be_a(Iterator::Stop)
  end

  it "builds from optional variants (upstream test_from_variant_opts_into_variant_array)" do
    array = PVC::VariantArray.from_iter(
      [nil, Variant::Null.new, Variant::BooleanFalse.new, nil] of Variant?
    )

    array.len.should eq(4)
    array.is_null(0).should be_true
    array.is_null(1).should be_false
    array.value(1).should eq(Variant::Null.new)
    array.is_null(2).should be_false
    array.value(2).should eq(Variant::BooleanFalse.new)
    array.is_null(3).should be_true
  end

  it "builds from variants (upstream test_from_variants_into_variant_array)" do
    array = PVC::VariantArray.from_iter([
      Variant::Null.new,
      Variant::BooleanFalse.new,
      Variant.from("norm"),
    ] of Variant)

    array.len.should eq(3)
    array.is_null(0).should be_false
    array.value(0).should eq(Variant::Null.new)
    array.is_null(1).should be_false
    array.value(1).should eq(Variant::BooleanFalse.new)
    array.is_null(2).should be_false
    array.value(2).should eq(Variant.from("norm"))
  end

  it "compares arrays logically (upstream test_variant_equality)" do
    values = [nil, Variant::BooleanFalse.new, Variant::Null.new, nil] of Variant?
    array = PVC::VariantArray.from_iter(values)

    PVC::VariantArray.from_iter(values).should eq(array)

    PVC::VariantArray.from_iter(values.reverse).should_not eq(array)

    array.slice(0, 1).should_not eq(array)
  end

  it "exposes the underlying struct array and its columns" do
    builder = PVC::VariantArrayBuilder.new(1)
    builder.append_variant(Variant.from(7_i8))
    array = builder.build

    array.data_type.kind.struct?.should be_true
    array.metadata_column.len.should eq(1)
    array.value_column.len.should eq(1)
    array.inner.len.should eq(1)
    array.nulls.should be_nil

    field = array.field("var")
    field.name.should eq("var")
    field.metadata.get(Arrow::Extension::EXTENSION_TYPE_NAME_KEY)
      .should eq(PVC::VariantType::NAME)
  end
end
