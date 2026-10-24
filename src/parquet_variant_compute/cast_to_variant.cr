require "./arrow_to_variant"
require "./variant_array_builder"

# Ported from parquet-variant-compute/src/cast_to_variant.rs (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
module ParquetVariantCompute
  # Converts typed Arrow arrays to binary variants, raising on conversion errors.
  def self.cast_to_variant(input : ::Arrow::ArrayBase) : VariantArray
    cast_to_variant_with_options(input, ::Arrow::CastOptions.new(safe: false))
  end

  # SQL null input remains SQL null; failed safe conversions are Variant::Null.
  def self.cast_to_variant_with_options(input : ::Arrow::ArrayBase, options : ::Arrow::CastOptions) : VariantArray
    builder = VariantArrayBuilder.new(input.len)
    if input.null_count == input.len
      builder.append_nulls(input.len)
    else
      rows = ArrowToVariant.make_row_builder(input.data_type, input, options)
      input.len.times { |index| rows.append_row(builder, index) }
    end
    builder.build
  end
end
