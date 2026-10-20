require "arrow-cr"
require "../parquet_variant"
require "../parquet_variant_json"
require "./variant_array"

# Ported from `parquet-variant-compute/src/variant_array_builder.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `VariantArrayBuilder` builds an unshredded `VariantArray` row by row. It
# implements `VariantBuilderExt`, so nested objects and lists are appended the
# same way as with a single-value `VariantBuilder`, and `JsonToVariant`, so
# `append_json` works too.
#
# Deferred: `VariantValueArrayBuilder` (only needed by unshredding) and
# shredding support.
#
# Divergences: the final `metadata`/`value` `BinaryViewArray`s are built with
# `Arrow::BinaryViewBuilder` from the per-row metadata/value slices instead of
# upstream's raw-buffer `binary_view_array_from_buffers`; the values are
# identical, only buffer sharing differs. `usize` counts are `Int32`, and
# `Extend<Option<Variant>>` is `#extend`.
module ParquetVariantCompute
  # Builder-specific state that tracks the per-row offsets and nulls of a
  # `VariantArrayBuilder` (upstream `ArrayBuilderState`).
  class ArrayBuilderState < ParquetVariant::BuilderSpecificState
    def initialize(
      @metadata_offsets : Array(Int32),
      @value_offsets : Array(Int32),
      @nulls : ::Arrow::NullBufferBuilder,
    )
    end

    # Records the finished row: the end offset of its metadata and value bytes,
    # and a valid (non-null) struct row.
    def finish(
      metadata_builder : ParquetVariant::VariantMetadataBuilder,
      value_builder : ParquetVariant::ValueBuilder,
    ) : Nil
      @metadata_offsets << metadata_builder.finish
      @value_offsets << value_builder.offset
      @nulls.append_non_null
      nil
    end
  end

  # A builder for `VariantArray` (upstream `VariantArrayBuilder`).
  #
  # The builder always creates an unshredded `VariantArray` using
  # `BinaryViewArray` for both the metadata and value fields.
  class VariantArrayBuilder
    include ParquetVariant::VariantBuilderExt
    include ParquetVariantJson::JsonToVariant

    @nulls : ::Arrow::NullBufferBuilder
    @metadata_builder : ParquetVariant::WritableMetadataBuilder
    @metadata_offsets : Array(Int32)
    @value_builder : ParquetVariant::ValueBuilder
    @value_offsets : Array(Int32)
    @fields : ::Arrow::Fields

    def initialize(row_capacity : Int32 = 0)
      # The subfields are expected to be non-nullable according to the variant
      # spec.
      @fields = ::Arrow::Fields.new([
        ::Arrow::Field.new("metadata", ::Arrow::DataType.binary_view, false),
        ::Arrow::Field.new("value", ::Arrow::DataType.binary_view, false),
      ])
      @nulls = ::Arrow::NullBufferBuilder.new(row_capacity)
      @metadata_builder = ParquetVariant::WritableMetadataBuilder.new
      @metadata_offsets = [] of Int32
      @value_builder = ParquetVariant::ValueBuilder.new
      @value_offsets = [] of Int32
    end

    # Builds the final array.
    def build : VariantArray
      metadata_buffer = @metadata_builder.into_inner
      value_buffer = @value_builder.into_inner

      metadata_array = views_from_buffer(metadata_buffer, @metadata_offsets)
      value_array = views_from_buffer(value_buffer, @value_offsets)

      inner = ::Arrow::StructArray.try_new(
        @fields,
        [metadata_array, value_array] of ::Arrow::ArrayBase,
        @nulls.finish,
      )
      VariantArray.try_new(inner)
    end

    # Appends a null row.
    def append_null : Nil
      @nulls.append_null
      # The subfields are non-nullable, so a null row just repeats the current
      # offsets.
      @metadata_offsets << @metadata_builder.offset
      @value_offsets << @value_builder.offset
      nil
    end

    # Appends `n` null rows.
    def append_nulls(n : Int32) : Nil
      @nulls.append_n_nulls(n)
      metadata_offset = @metadata_builder.offset
      value_offset = @value_builder.offset
      n.times do
        @metadata_offsets << metadata_offset
        @value_offsets << value_offset
      end
      nil
    end

    # Appends `variant` as the next row.
    def append_variant(variant : ParquetVariant::Variant) : Nil
      ParquetVariant::ValueBuilder.append_variant(parent_state, variant)
      nil
    end

    # Appends values, using `nil` for a null row
    # (upstream `Extend<Option<Variant>>`).
    def extend(values : Enumerable) : Nil
      values.each do |value|
        case value
        when Nil                     then append_null
        when ParquetVariant::Variant then append_variant(value)
        else
          append_value(value)
        end
      end
      nil
    end

    # `VariantBuilderExt`: appends a value. Note that a null variant value is
    # `Variant::Null`, not a null row.
    def append_value(value : ParquetVariant::VariantValue) : Nil
      append_variant(ParquetVariant::ValueConversion.to_variant(value))
      nil
    end

    # `VariantBuilderExt`: creates a nested list builder.
    def try_new_list : ParquetVariant::ListBuilder
      ParquetVariant::ListBuilder.new(parent_state, false)
    end

    # `VariantBuilderExt`: creates a nested object builder.
    def try_new_object : ParquetVariant::ObjectBuilder
      ParquetVariant::ObjectBuilder.new(parent_state, false)
    end

    # The parent state shared by the builders appending to this array.
    private def parent_state : ParquetVariant::ParentState
      state = ArrayBuilderState.new(@metadata_offsets, @value_offsets, @nulls)
      ParquetVariant::ParentState.new(
        @value_builder,
        @metadata_builder,
        state,
        @value_builder.offset,
        @metadata_builder.num_field_names,
      )
    end

    # Builds a `BinaryViewArray` from the per-row end offsets into `buffer`.
    private def views_from_buffer(
      buffer : Bytes,
      offsets : Array(Int32),
    ) : ::Arrow::BinaryViewArray
      builder = ::Arrow::BinaryViewBuilder.new
      previous = 0
      offsets.each do |stop|
        builder.append_value(buffer[previous, stop - previous])
        previous = stop
      end
      builder.finish
    end
  end
end
