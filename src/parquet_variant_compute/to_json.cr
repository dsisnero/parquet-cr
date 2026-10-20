require "arrow-cr"
require "../parquet_variant_json"
require "./variant_array"

# Ported from `parquet-variant-compute/src/to_json.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `variant_to_json` converts a `VariantArray` to a `StringArray` of JSON
# documents, preserving nulls.
#
# Divergence: upstream builds the `StringArray` from a scratch buffer with raw
# offsets; the port formats each value with `StringBuilder`.
module ParquetVariantCompute
  # Converts a variant array (or the struct array backing one) to JSON strings.
  def self.variant_to_json(input : ::Arrow::ArrayBase) : ::Arrow::StringArray
    array = VariantArray.try_new(input)
    builder = ::Arrow::StringBuilder.new

    array.len.times do |i|
      if array.is_null(i)
        builder.append_null
      else
        io = IO::Memory.new
        array.try_value(i).to_json(io)
        builder.append_value(io.to_s)
      end
    end

    builder.finish
  end
end
