require "arrow-cr"
require "../parquet_variant"
require "./variant_array"
require "./variant_value_array_builder"

# Ported from `parquet-variant-compute/src/unshred_variant.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `unshred_variant` removes all (nested) `typed_value` columns from a
# `VariantArray` by converting them back to the binary variant encoding and
# merging the result into the `value` column.
#
# Divergences: the Rust enum of row builders is a sealed class hierarchy;
# `usize` counts are `Int32`; errors raise (upstream returns `Result`); the
# primitive/timestamp/decimal conversions reuse
# `VariantArray.typed_value_to_variant` (so failures raise the "Cast failed at
# index N ..." message instead of the per-type messages), and the
# `Time64`/`Timestamp`/`FixedSizeBinary` "not a valid variant shredding type"
# errors render the full data type instead of `Time64({unit})` etc.
module ParquetVariantCompute
  # Removes all (nested) `typed_value` columns from `array` by converting them
  # back to binary variant and merging the values into the `value` column.
  #
  # The result has no `typed_value` column and annotates `value` as non-nullable
  # as the spec requires.
  def self.unshred_variant(array : VariantArray) : VariantArray
    nulls = array.nulls
    metadata = array.metadata_column
    value_column = array.value_column
    typed_value_column = array.typed_value_column

    # Already unshredded: no data movement is needed, but the output must
    # annotate `value` as non-nullable. Inputs whose value nulls are not all
    # masked by the parent null buffer (spec-invalid "missing" rows) cannot be
    # re-annotated and fall through to the row loop, whose top-level sink
    # materializes `Variant::Null`.
    if typed_value_column.nil?
      return array if value_field_non_nullable?(array)
      if value_nulls_are_masked?(value_column, nulls)
        return VariantArray.from_parts_unshredded(metadata, value_column, nulls)
      end
    end

    # `nil`/`nil` at top level is technically invalid, but the shredding spec
    # requires `Variant::Null` when a required value is missing.
    row_builder = UnshredRowBuilder.try_new(array.inner) ||
                  UnshredRowBuilder::NullBuilder.new

    value_builder = VariantValueArrayBuilder.new(array.len)
    array.len.times do |i|
      if array.is_null(i)
        value_builder.append_null
      else
        metadata_bytes = VariantArray.binary_array_value(metadata, i)
        unless metadata_bytes
          raise Parquet::ArrowError.new(
            "Invalid argument error: metadata field must be a binary-like array"
          )
        end
        row_metadata = ParquetVariant::VariantMetadata.try_new(metadata_bytes)
        row_sink = TopLevelRowSink.new(value_builder.builder_ext(row_metadata))
        row_builder.append_row(row_sink, row_metadata, i)
      end
    end

    VariantArray.from_parts_unshredded(metadata, value_builder.build, nulls)
  end

  # Whether the `value` field is annotated non-nullable.
  private def self.value_field_non_nullable?(array : VariantArray) : Bool
    !array.inner.field_by_name("value").try(&.nullable?)
  end

  # Whether every null in `value` is masked by a parent null, i.e. the column
  # may be annotated non-nullable.
  private def self.value_nulls_are_masked?(
    value : ::Arrow::ArrayBase,
    parent_nulls : ::Arrow::NullBuffer?,
  ) : Bool
    value_nulls = value.nulls
    return true if value_nulls.nil? || value_nulls.null_count == 0
    return false unless parent_nulls

    parent_nulls.contains(value_nulls)
  end

  # Wraps the sink every top-level row is appended into (upstream
  # `TopLevelRowSink`).
  #
  # Row builders signal a missing value (value and typed_value both null) by
  # calling `append_null`; at the top level that must become `Variant::Null`
  # because the non-nullable output `value` column cannot hold a physical null.
  # Nested builders created via `try_new_object`/`try_new_list` are returned
  # unwrapped, so nested missing values keep their own semantics.
  class TopLevelRowSink
    include ParquetVariant::VariantBuilderExt

    def initialize(@inner : VariantValueArrayBuilderExt)
    end

    def append_null : Nil
      @inner.append_value(ParquetVariant::Variant::Null.new)
    end

    def append_value(value : ParquetVariant::VariantValue) : Nil
      @inner.append_value(value)
    end

    def try_new_list : ParquetVariant::ListBuilder
      @inner.try_new_list
    end

    def try_new_object : ParquetVariant::ObjectBuilder
      @inner.try_new_object
    end
  end

  # Converts one shredded row back to unshredded form (upstream
  # `UnshredVariantRowBuilder`).
  abstract class UnshredRowBuilder
    # The optional `value` column.
    @value : ::Arrow::ArrayBase?
    # The `typed_value` column.
    @typed_value : ::Arrow::ArrayBase

    def initialize(@value : ::Arrow::ArrayBase?, @typed_value : ::Arrow::ArrayBase)
    end

    # Appends the row at `index`.
    abstract def append_row(
      builder : ParquetVariant::VariantBuilderExt,
      metadata : ParquetVariant::VariantMetadata,
      index : Int32,
    ) : Nil

    # Creates the row builder for a shredded `(value, typed_value)` struct, or
    # `nil` when both columns are absent (upstream `try_new_opt`).
    #
    # ameba:disable Metrics/CyclomaticComplexity
    def self.try_new(inner : ::Arrow::StructArray) : UnshredRowBuilder?
      value = inner.column_by_name("value")
      if value
        VariantArray.validate_binary_array(value, "value")
      end

      typed_value = inner.column_by_name("typed_value")
      unless typed_value
        # Copy the value across directly, if present.
        return value ? ValueOnlyRowBuilder.new(value) : nil
      end

      data_type = typed_value.data_type
      case data_type.kind
      when .int8?, .int16?, .int32?, .int64?, .float32?, .float64?, .boolean?,
           .utf8?, .large_utf8?, .utf8_view?, .binary?, .large_binary?, .binary_view?,
           .date32?
        return PrimitiveRowBuilder.new(value, typed_value)
      when .decimal32?
        return PrimitiveRowBuilder.new(value, typed_value) if valid_decimal?(data_type, 4)
      when .decimal64?
        return PrimitiveRowBuilder.new(value, typed_value) if valid_decimal?(data_type, 8)
      when .decimal128?
        return PrimitiveRowBuilder.new(value, typed_value) if valid_decimal?(data_type, 16)
      when .decimal256?
        # falls through to the invalid-type error below
      when .time64?
        if data_type.time_unit.try(&.microsecond?)
          return PrimitiveRowBuilder.new(value, typed_value)
        end
      when .timestamp?
        unit = data_type.time_unit
        if unit == ::Arrow::TimeUnit::Microsecond || unit == ::Arrow::TimeUnit::Nanosecond
          return PrimitiveRowBuilder.new(value, typed_value)
        end
      when .fixed_size_binary?
        if data_type.byte_width == 16
          return PrimitiveRowBuilder.new(value, typed_value)
        end
      when .struct?
        return StructRowBuilder.try_new(value, typed_value.as(::Arrow::StructArray))
      when .list?
        return ListRowBuilder::List.new(value, typed_value.as(::Arrow::ListArray))
      when .large_list?
        return ListRowBuilder::LargeList.new(value, typed_value.as(::Arrow::LargeListArray))
      when .list_view?
        return ListRowBuilder::ListView.new(value, typed_value.as(::Arrow::ListViewArray))
      when .large_list_view?
        return ListRowBuilder::LargeListView.new(
          value, typed_value.as(::Arrow::LargeListViewArray)
        )
      when .fixed_size_list?
        return ListRowBuilder::FixedSizeList.new(
          value, typed_value.as(::Arrow::FixedSizeListArray)
        )
      end

      if data_type.kind.decimal32? || data_type.kind.decimal64? ||
         data_type.kind.decimal128? || data_type.kind.decimal256?
        raise Parquet::ArrowError.new(
          "Invalid argument error: #{data_type} is not a valid variant shredding type"
        )
      end
      if data_type.kind.time64? || data_type.kind.timestamp? ||
         data_type.kind.fixed_size_binary?
        raise Parquet::ArrowError.new(
          "Invalid argument error: #{data_type} is not a valid variant shredding type"
        )
      end

      raise nyi_err!("Unshredding not yet supported for type: #{data_type}")
    end

    # Whether `data_type` has a valid precision/scale for the given variant
    # decimal width (4, 8 or 16 bytes).
    private def self.valid_decimal?(data_type : ::Arrow::DataType, width : Int32) : Bool
      precision = data_type.precision || 0_u8
      scale = data_type.scale || 0_i8
      case width
      when 4 then ParquetVariant::VariantDecimal4.is_valid_precision_and_scale(precision, scale)
      when 8 then ParquetVariant::VariantDecimal8.is_valid_precision_and_scale(precision, scale)
      else        ParquetVariant::VariantDecimal16.is_valid_precision_and_scale(precision, scale)
      end
    end

    # Appends the unshredded row when `typed_value` is null, returning whether it
    # did (upstream `handle_unshredded_case!`, which returns early in that case).
    protected def append_unshredded_row(
      builder : ParquetVariant::VariantBuilderExt,
      metadata : ParquetVariant::VariantMetadata,
      index : Int32,
    ) : Bool
      return false if typed_value_valid?(index)

      value = unshredded_value(metadata, index)
      value ? builder.append_value(value) : builder.append_null
      true
    end

    # Raises when a non-struct `typed_value` is set next to a `value`: only a
    # partially shredded object may carry both.
    protected def ensure_no_value_conflict(value : ParquetVariant::Variant?) : Nil
      return unless value

      raise Parquet::ArrowError.new(
        "Invalid argument error: Invalid shredded variant: both value and typed_value are non-null"
      )
    end

    # Decodes the row's `value` bytes, or `nil` when `value` is absent or null.
    private def unshredded_value(
      metadata : ParquetVariant::VariantMetadata,
      index : Int32,
    ) : ParquetVariant::Variant?
      value_column = @value
      return nil unless value_column

      nulls = value_column.nulls
      return nil if nulls && !nulls.valid?(index)

      bytes = VariantArray.binary_array_value(value_column, index)
      unless bytes
        raise Parquet::ArrowError.new(
          "Invalid argument error: value field must be a binary-like array, " \
          "instead got #{value_column.data_type}"
        )
      end
      ParquetVariant::Variant.new_with_metadata(metadata, bytes)
    end

    private def typed_value_valid?(index : Int32) : Bool
      nulls = @typed_value.nulls
      nulls.nil? || nulls.valid?(index)
    end

    # Row builder for an array with neither `typed_value` nor `value`: every row
    # is `Variant::Null` (upstream `NullUnshredVariantBuilder`).
    class NullBuilder < UnshredRowBuilder
      def initialize
        super(nil, ::Arrow.new_null_array(::Arrow::DataType.null, 0))
      end

      def append_row(
        builder : ParquetVariant::VariantBuilderExt,
        metadata : ParquetVariant::VariantMetadata,
        index : Int32,
      ) : Nil
        builder.append_value(ParquetVariant::Variant::Null.new)
      end
    end

    # Row builder for an array that only has a `value` column (upstream
    # `ValueOnlyUnshredVariantBuilder`).
    class ValueOnlyRowBuilder < UnshredRowBuilder
      def initialize(value : ::Arrow::ArrayBase)
        super(value, value)
      end

      def append_row(
        builder : ParquetVariant::VariantBuilderExt,
        metadata : ParquetVariant::VariantMetadata,
        index : Int32,
      ) : Nil
        value_column = @value
        return builder.append_null unless value_column

        nulls = value_column.nulls
        if nulls && !nulls.valid?(index)
          builder.append_null
          return
        end

        bytes = VariantArray.binary_array_value(value_column, index)
        unless bytes
          raise Parquet::ArrowError.new(
            "Invalid argument error: value field must be a binary-like array"
          )
        end
        builder.append_value(ParquetVariant::Variant.new_with_metadata(metadata, bytes))
      end
    end

    # Row builder for primitive typed values (upstream
    # `UnshredPrimitiveRowBuilder`, `TimestampUnshredRowBuilder` and
    # `DecimalUnshredRowBuilder`).
    #
    # Divergence: the conversion is `VariantArray.typed_value_to_variant`, so all
    # supported primitive types (including decimals, timestamps and UUIDs) are
    # handled by one builder.
    class PrimitiveRowBuilder < UnshredRowBuilder
      def append_row(
        builder : ParquetVariant::VariantBuilderExt,
        metadata : ParquetVariant::VariantMetadata,
        index : Int32,
      ) : Nil
        return if append_unshredded_row(builder, metadata, index)

        ensure_no_value_conflict(unshredded_value(metadata, index))
        builder.append_value(VariantArray.typed_value_to_variant(@typed_value, index))
      end
    end

    # Row builder for shredded objects (upstream `StructUnshredVariantBuilder`).
    class StructRowBuilder < UnshredRowBuilder
      @field_builders : Array(Tuple(String, UnshredRowBuilder?))

      def initialize(
        value : ::Arrow::ArrayBase?,
        typed_value : ::Arrow::ArrayBase,
        @field_builders : Array(Tuple(String, UnshredRowBuilder?)),
      )
        super(value, typed_value)
      end

      # Creates the nested builders for the shredded object's fields.
      def self.try_new(
        value : ::Arrow::ArrayBase?,
        typed_value : ::Arrow::StructArray,
      ) : StructRowBuilder
        fields = typed_value.data_type.fields
        unless fields
          raise Parquet::ArrowError.new(
            "Invalid argument error: Invalid shredded variant object field: expected Struct, " \
            "got #{typed_value.data_type}"
          )
        end

        field_builders = [] of Tuple(String, UnshredRowBuilder?)
        fields.each_with_index do |field, i|
          column = typed_value.column(i)
          struct_column = column.as?(::Arrow::StructArray)
          unless struct_column
            raise Parquet::ArrowError.new(
              "Invalid argument error: Invalid shredded variant object field: expected Struct, " \
              "got #{column.data_type}"
            )
          end
          field_builders << {field.name, UnshredRowBuilder.try_new(struct_column)}
        end

        new(value, typed_value, field_builders)
      end

      def append_row(
        builder : ParquetVariant::VariantBuilderExt,
        metadata : ParquetVariant::VariantMetadata,
        index : Int32,
      ) : Nil
        value = unshredded_value(metadata, index)
        unless typed_value_valid?(index)
          value ? builder.append_value(value) : builder.append_null
          return
        end

        # typed_value is non-null here; value may or may not be (partial
        # shredding), so both are allowed.
        object_builder = builder.try_new_object

        @field_builders.each do |(field_name, field_builder)|
          next unless field_builder

          field_sink = ParquetVariant::ObjectFieldBuilder.new(field_name, object_builder)
          field_builder.append_row(field_sink, metadata, index)
        end

        # Process any unshredded fields (partial shredding).
        if value
          object = value.as_object
          unless object
            raise Parquet::ArrowError.new(
              "Invalid argument error: Expected object in value field for partially shredded struct"
            )
          end

          object.iter_try.each do |(field_name, field_value)|
            if @field_builders.any? { |(name, _)| name == field_name }
              raise Parquet::ArrowError.new(
                "Invalid argument error: Field '#{field_name}' appears in both typed_value and value"
              )
            end
            object_builder.insert_bytes(field_name, field_value)
          end
        end

        object_builder.finish
      end
    end

    # Row builder for shredded lists (upstream
    # `ListUnshredVariantBuilder`).
    abstract class ListRowBuilder < UnshredRowBuilder
      @element_builder : UnshredRowBuilder

      def initialize(
        value : ::Arrow::ArrayBase?,
        typed_value : ::Arrow::ArrayBase,
        @element_builder : UnshredRowBuilder,
      )
        super(value, typed_value)
      end

      # Creates the element row builder from the list's element struct.
      protected def self.element_builder(values : ::Arrow::ArrayBase) : UnshredRowBuilder
        element_struct = values.as?(::Arrow::StructArray)
        unless element_struct
          raise Parquet::ArrowError.new(
            "Invalid argument error: Invalid shredded variant array element: expected Struct, " \
            "got #{values.data_type}"
          )
        end

        # A nil/nil element is technically invalid, but the shredding spec
        # requires `Variant::Null` when a required value is missing.
        UnshredRowBuilder.try_new(element_struct) || UnshredRowBuilder::NullBuilder.new
      end

      # The list's flattened element array.
      abstract def values : ::Arrow::ArrayBase

      # The element indices of the list at `index`.
      abstract def element_range(index : Int32) : Range(Int32, Int32)

      def append_row(
        builder : ParquetVariant::VariantBuilderExt,
        metadata : ParquetVariant::VariantMetadata,
        index : Int32,
      ) : Nil
        return if append_unshredded_row(builder, metadata, index)

        ensure_no_value_conflict(unshredded_value(metadata, index))

        list_builder = builder.try_new_list
        element_range(index).each do |element_index|
          @element_builder.append_row(list_builder, metadata, element_index)
        end
        list_builder.finish
      end

      # `ListArray` rows.
      class List < ListRowBuilder
        @list : ::Arrow::ListArray

        def initialize(value : ::Arrow::ArrayBase?, typed_value : ::Arrow::ListArray)
          super(value, typed_value, ListRowBuilder.element_builder(typed_value.values))
          @list = typed_value
        end

        def values : ::Arrow::ArrayBase
          @list.values
        end

        def element_range(index : Int32) : Range(Int32, Int32)
          offsets = @list.value_offsets
          offsets[index].to_i32...offsets[index + 1].to_i32
        end
      end

      # `LargeListArray` rows.
      class LargeList < ListRowBuilder
        @large_list : ::Arrow::LargeListArray

        def initialize(value : ::Arrow::ArrayBase?, typed_value : ::Arrow::LargeListArray)
          super(value, typed_value, ListRowBuilder.element_builder(typed_value.values))
          @large_list = typed_value
        end

        def values : ::Arrow::ArrayBase
          @large_list.values
        end

        def element_range(index : Int32) : Range(Int32, Int32)
          offsets = @large_list.value_offsets
          offsets[index].to_i32...offsets[index + 1].to_i32
        end
      end

      # `ListViewArray` rows.
      class ListView < ListRowBuilder
        @list_view : ::Arrow::ListViewArray

        def initialize(value : ::Arrow::ArrayBase?, typed_value : ::Arrow::ListViewArray)
          super(value, typed_value, ListRowBuilder.element_builder(typed_value.values))
          @list_view = typed_value
        end

        def values : ::Arrow::ArrayBase
          @list_view.values
        end

        def element_range(index : Int32) : Range(Int32, Int32)
          offset = @list_view.offsets[index].to_i32
          offset...(offset + @list_view.sizes[index].to_i32)
        end
      end

      # `LargeListViewArray` rows.
      class LargeListView < ListRowBuilder
        @large_list_view : ::Arrow::LargeListViewArray

        def initialize(value : ::Arrow::ArrayBase?, typed_value : ::Arrow::LargeListViewArray)
          super(value, typed_value, ListRowBuilder.element_builder(typed_value.values))
          @large_list_view = typed_value
        end

        def values : ::Arrow::ArrayBase
          @large_list_view.values
        end

        def element_range(index : Int32) : Range(Int32, Int32)
          offset = @large_list_view.offsets[index].to_i32
          offset...(offset + @large_list_view.sizes[index].to_i32)
        end
      end

      # `FixedSizeListArray` rows.
      class FixedSizeList < ListRowBuilder
        @fixed_size_list : ::Arrow::FixedSizeListArray

        def initialize(value : ::Arrow::ArrayBase?, typed_value : ::Arrow::FixedSizeListArray)
          super(value, typed_value, ListRowBuilder.element_builder(typed_value.values))
          @fixed_size_list = typed_value
        end

        def values : ::Arrow::ArrayBase
          @fixed_size_list.values
        end

        def element_range(index : Int32) : Range(Int32, Int32)
          size = @fixed_size_list.value_length
          start = index * size
          start...(start + size)
        end
      end
    end
  end
end
