require "arrow-cr"
require "../parquet_variant"
require "./variant_array"

# Ported from `VariantValueArrayBuilder` and its builder state/wrapper in
# `parquet-variant-compute/src/variant_array_builder.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `VariantValueArrayBuilder` builds only the `value` column of a variant array,
# reusing an existing metadata column. It is used by unshredding, data
# transformation and filtering.
#
# Divergences: the final `BinaryViewArray` is built with
# `Arrow::BinaryViewBuilder` (the null buffer is applied while appending)
# instead of upstream's raw-buffer `binary_view_array_from_buffers` plus
# `BinaryViewArray::try_new`; `usize` counts are `Int32`; `build` raises instead
# of returning a `Result`.
module ParquetVariantCompute
  # Builder-specific state that records the per-row offsets and nulls of a
  # `VariantValueArrayBuilder` (upstream `ValueArrayBuilderState`).
  class ValueArrayBuilderState < ParquetVariant::BuilderSpecificState
    def initialize(
      @value_offsets : Array(Int32),
      @nulls : ::Arrow::NullBufferBuilder,
    )
    end

    # Records the finished row: the end offset of its value bytes and a valid
    # (non-null) row.
    def finish(
      metadata_builder : ParquetVariant::VariantMetadataBuilder,
      value_builder : ParquetVariant::ValueBuilder,
    ) : Nil
      @value_offsets << value_builder.offset
      @nulls.append_non_null
      nil
    end
  end

  # Builds only the `value` column of a variant array (upstream
  # `VariantValueArrayBuilder`).
  #
  # The resulting `BinaryViewArray` can be combined with an existing metadata
  # column to form a complete `VariantArray`.
  class VariantValueArrayBuilder
    # The underlying value builder.
    getter value_builder : ParquetVariant::ValueBuilder

    @value_offsets : Array(Int32)
    @nulls : ::Arrow::NullBufferBuilder

    def initialize(row_capacity : Int32 = 0)
      @value_builder = ParquetVariant::ValueBuilder.new
      @value_offsets = [] of Int32
      @nulls = ::Arrow::NullBufferBuilder.new(row_capacity)
    end

    # Builds the value array.
    #
    # A physical null row is only valid for the `value` column of a shredded
    # variant (which is nullable); the `value` column of an unshredded variant is
    # non-nullable, and callers append `Variant::Null` instead.
    def build : ::Arrow::BinaryViewArray
      buffer = @value_builder.into_inner
      nulls = @nulls.finish

      builder = ::Arrow::BinaryViewBuilder.new
      previous = 0
      @value_offsets.each_with_index do |stop, index|
        if nulls.nil? || nulls.valid?(index)
          builder.append_value(buffer[previous, stop - previous])
        else
          builder.append_null
        end
        previous = stop
      end
      builder.finish
    end

    # Appends a null row.
    def append_null : Nil
      @value_offsets << @value_builder.offset
      @nulls.append_null
      nil
    end

    # Appends `value`, using its own metadata to resolve field names.
    def append_value(value : ParquetVariant::Variant) : Nil
      builder_ext(value.metadata).append_value(value)
      nil
    end

    # Creates a builder-specific parent state over `metadata_builder`.
    def parent_state(
      metadata_builder : ParquetVariant::VariantMetadataBuilder,
    ) : ParquetVariant::ParentState
      state = ValueArrayBuilderState.new(@value_offsets, @nulls)
      ParquetVariant::ParentState.new(
        @value_builder,
        metadata_builder,
        state,
        @value_builder.offset,
        metadata_builder.num_field_names,
      )
    end

    # A thin `VariantBuilderExt` wrapper that hides the per-row metadata builder
    # (upstream `builder_ext`).
    def builder_ext(metadata : ParquetVariant::VariantMetadata) : VariantValueArrayBuilderExt
      VariantValueArrayBuilderExt.new(
        ParquetVariant::ReadOnlyMetadataBuilder.new(metadata), self
      )
    end
  end

  # A `VariantBuilderExt` wrapper over a `VariantValueArrayBuilder` that hides
  # the short-lived per-row `ReadOnlyMetadataBuilder` (upstream
  # `VariantValueArrayBuilderExt`).
  class VariantValueArrayBuilderExt
    include ParquetVariant::VariantBuilderExt

    def initialize(
      @metadata_builder : ParquetVariant::ReadOnlyMetadataBuilder,
      @value_builder : VariantValueArrayBuilder,
    )
    end

    # Appends a physical null row (valid only for a shredded `value` column).
    def append_null : Nil
      @value_builder.append_null
    end

    def append_value(value : ParquetVariant::VariantValue) : Nil
      state = @value_builder.parent_state(@metadata_builder)
      ParquetVariant::ValueBuilder.append_variant_bytes(
        state, ParquetVariant::ValueConversion.to_variant(value)
      )
    end

    def try_new_list : ParquetVariant::ListBuilder
      ParquetVariant::ListBuilder.new(@value_builder.parent_state(@metadata_builder), false)
    end

    def try_new_object : ParquetVariant::ObjectBuilder
      ParquetVariant::ObjectBuilder.new(@value_builder.parent_state(@metadata_builder), false)
    end
  end
end
