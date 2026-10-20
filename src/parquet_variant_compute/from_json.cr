require "arrow-cr"
require "../parquet/errors"
require "../parquet_variant_json"
require "./variant_array_builder"

# Ported from `parquet-variant-compute/src/from_json.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `json_to_variant` parses a batch of JSON strings into a `VariantArray`
# represented as `STRUCT<metadata: BINARY, value: BINARY>`, preserving nulls.
#
# Divergence: upstream's error is `ArrowError::CastError`; the port raises
# `Parquet::ArrowError` with the same message body.
module ParquetVariantCompute
  # Parses a batch of JSON strings into a batch of variants, preserving nulls.
  #
  # Supports `StringArray`, `LargeStringArray` and `StringViewArray` inputs; any
  # other type raises.
  def self.json_to_variant(input : ::Arrow::ArrayBase) : VariantArray
    builder = VariantArrayBuilder.new(input.len)

    # Downcast through the supported string array types, as upstream does.
    if strings = input.as?(::Arrow::StringArray)
      strings.len.times do |i|
        if null_at?(strings, i)
          builder.append_null
        else
          builder.append_json(strings.value(i))
        end
      end
    elsif large = input.as?(::Arrow::GenericStringArray(Int64))
      large.len.times do |i|
        if null_at?(large, i)
          builder.append_null
        else
          builder.append_json(large.value(i))
        end
      end
    elsif views = input.as?(::Arrow::StringViewArray)
      views.len.times do |i|
        if null_at?(views, i)
          builder.append_null
        else
          builder.append_json(views.value(i))
        end
      end
    else
      raise Parquet::ArrowError.new(
        "Cast error: Expected reference to StringArray, LargeStringArray, or " \
        "StringViewArray as input"
      )
    end

    builder.build
  end

  # Whether the row is null in the array's own null bitmap.
  private def self.null_at?(array : ::Arrow::ArrayBase, index : Int32) : Bool
    nulls = array.nulls
    nulls.nil? ? false : !nulls.valid?(index)
  end
end
