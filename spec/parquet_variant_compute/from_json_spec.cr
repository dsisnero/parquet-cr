require "../spec_helper"

# Ported from `parquet-variant-compute/src/from_json.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): `json_to_variant` for the
# three string array types.
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant

# The input and expectations shared by the three upstream tests.
private JSON_VALUES = ["1", nil, %({"a": 32}), "null", nil]

private def expected_object_variant : Variant
  builder = ParquetVariant::VariantBuilder.new
  object = builder.new_object
  object.insert("a", Variant.from(32_i8))
  object.finish
  metadata, value = builder.finish
  Variant.new(metadata, value)
end

private def check_json_to_variant(array : Arrow::ArrayBase, variant_array : PVC::VariantArray) : Nil
  metadata_array = variant_array.metadata_column
  value_array = variant_array.value_column

  variant_array.is_null(0).should be_false
  variant_array.value(0).should eq(Variant.from(1_i8))

  variant_array.is_null(1).should be_true

  variant_array.is_null(2).should be_false
  variant_array.value(2).should eq(expected_object_variant)

  # A variant NULL, not a null row.
  variant_array.is_null(3).should be_false
  variant_array.value(3).should eq(Variant::Null.new)

  variant_array.is_null(4).should be_true

  # The metadata and value subfields are never null.
  metadata_array.nulls.should be_nil
  value_array.nulls.should be_nil
  array.len.should eq 5
end

describe ParquetVariantCompute do
  it "parses UTF-8 JSON strings (upstream test_json_to_variant)" do
    builder = Arrow::StringBuilder.new
    JSON_VALUES.each { |value| value ? builder.append_value(value) : builder.append_null }
    input = builder.finish

    check_json_to_variant(input, PVC.json_to_variant(input))
  end

  it "parses large UTF-8 JSON strings (upstream test_json_to_variant_large_string)" do
    builder = Arrow::GenericStringBuilder(Int64).new
    JSON_VALUES.each { |value| value ? builder.append_value(value) : builder.append_null }
    input = builder.finish

    check_json_to_variant(input, PVC.json_to_variant(input))
  end

  it "parses UTF-8 view JSON strings (upstream test_json_to_variant_string_view)" do
    builder = Arrow::StringViewBuilder.new
    JSON_VALUES.each { |value| value ? builder.append_value(value) : builder.append_null }
    input = builder.finish

    check_json_to_variant(input, PVC.json_to_variant(input))
  end

  it "rejects non-string arrays" do
    input = Arrow::PrimitiveArray(Int32).from_values([1, 2])
    error = expect_raises(Parquet::ArrowError) { PVC.json_to_variant(input) }
    error.message.not_nil!.should contain(
      "Expected reference to StringArray, LargeStringArray, or StringViewArray as input"
    )
  end
end
