require "../spec_helper"

# Ported from `parquet-variant-compute/src/to_json.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): `variant_to_json`.
private alias PVC = ParquetVariantCompute

private def valid?(array : Arrow::ArrayBase, index : Int32) : Bool
  nulls = array.nulls
  nulls.nil? || nulls.valid?(index)
end

describe ParquetVariantCompute do
  it "writes variants back to JSON strings (upstream test_variant_to_json)" do
    metadata_builder = Arrow::BinaryBuilder.new
    value_builder = Arrow::BinaryBuilder.new

    # Row 0: [1, 0, 0], [12, 0]
    metadata_builder.append_value(Bytes[1, 0, 0])
    value_builder.append_value(Bytes[12, 0])

    # Row 1: null
    metadata_builder.append_null
    value_builder.append_null

    # Row 2: [1, 1, 0, 1, 97], [2, 1, 0, 0, 2, 12, 32]
    metadata_builder.append_value(Bytes[1, 1, 0, 1, 97])
    value_builder.append_value(Bytes[2, 1, 0, 0, 2, 12, 32])

    # Row 3: [1, 0, 0], [0]
    metadata_builder.append_value(Bytes[1, 0, 0])
    value_builder.append_value(Bytes[0])

    # Row 4: null
    metadata_builder.append_null
    value_builder.append_null

    metadata_array = metadata_builder.finish
    value_array = value_builder.finish

    validity = Arrow::BooleanBufferBuilder.new(value_array.len)
    value_array.len.times do |i|
      validity.append(valid?(value_array, i) && valid?(metadata_array, i))
    end
    null_buffer = Arrow::NullBuffer.new(validity.finish)

    fields = Arrow::Fields.new([
      Arrow::Field.new("metadata", Arrow::DataType.binary, true),
      Arrow::Field.new("value", Arrow::DataType.binary, true),
    ])
    input = Arrow::StructArray.try_new(
      fields, [metadata_array, value_array] of Arrow::ArrayBase, null_buffer
    )

    result = PVC.variant_to_json(input)

    result.len.should eq(5)
    (0...result.len).map { |i| valid?(result, i) ? result.value(i) : nil }
      .should eq(["0", nil, %({"a":32}), "null", nil])
  end
end
