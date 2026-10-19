require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../metrics"
require "../schema/complex"
require "./primitive_array"
require "./byte_array"
require "./byte_array_dictionary"
require "./byte_view_array"
require "./struct_array"
require "./list_array"
require "./list_view_array"
require "./map_array"
require "./fixed_size_list_array"
require "./null_array"
require "./fixed_len_byte_array"
require "./row_group_cache"
require "./cached_array_reader"

# Ported from `parquet/src/arrow/array_reader/builder.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ArrayReaderBuilder` (and `CacheOptionsBuilder`/`CacheOptions`): it walks
# a `ParquetField` tree (produced by the Parquet → Arrow schema conversion) and
# builds the matching `ArrayReader`s over the row groups, propagating the
# projection mask and the selective-padding threshold (`def_level` of the
# nearest enclosing list/map) down to the leaves. Leaves selected by the cache
# options are wrapped in a `CachedArrayReader`, which serves them from (or
# inserts them into) a shared `RowGroupCache`.
#
# Divergences:
# - no virtual columns (`RowNumber`/`RowGroupIndex`), so `ParquetFieldType` only
#   has primitive/group fields, and there is no `with_parquet_metadata` (it only
#   feeds those virtual columns);
# - not-yet-ported leaf readers raise `NYIError` instead of being built:
#   INT96 (the numeric primitive reader does not model `Int96`);
# - `usize` counts are `Int32`, and the per-field `ReaderArgs` struct is replaced
#   by three method parameters (`field`, `mask`, `padding_threshold`).
module Parquet
  module Arrow
    # Builder for `CacheOptions` (upstream `CacheOptionsBuilder`).
    class CacheOptionsBuilder
      # Projection mask to apply to the cache.
      getter projection_mask : ProjectionMask
      # Cache to use for storing row group data.
      getter cache : RowGroupCache

      def initialize(@projection_mask : ProjectionMask, @cache : RowGroupCache)
      end

      # Returns new cache options for producing (populating) the cache.
      def producer : CacheOptions
        CacheOptions.new(@projection_mask, @cache, CacheRole::Producer)
      end

      # Returns new cache options for consuming (reading) the cache.
      def consumer : CacheOptions
        CacheOptions.new(@projection_mask, @cache, CacheRole::Consumer)
      end
    end

    # Cache options containing the projection mask, cache, and role (upstream
    # `CacheOptions`).
    class CacheOptions
      getter projection_mask : ProjectionMask
      getter cache : RowGroupCache
      getter role : CacheRole

      def initialize(@projection_mask : ProjectionMask, @cache : RowGroupCache, @role : CacheRole)
      end
    end

    # Builds `ArrayReader`s from the parquet schema, a projection mask and the row
    # groups (upstream `ArrayReaderBuilder`).
    class ArrayReaderBuilder
      # Upstream `DEFAULT_BATCH_SIZE`.
      DEFAULT_BATCH_SIZE = 1024

      # Reader metrics, shared with the readers built by this builder (upstream
      # passes `&ArrowReaderMetrics` to `new`).
      getter metrics : ArrowReaderMetrics
      # Optional cache options for the array readers.
      getter cache_options : CacheOptions?

      def initialize(
        @row_groups : RowGroups,
        @batch_size : Int32 = DEFAULT_BATCH_SIZE,
        @metrics : ArrowReaderMetrics = ArrowReaderMetrics.disabled,
      )
        @cache_options = nil
      end

      # Sets the batch size hint used to pre-allocate internal buffers.
      def with_batch_size(batch_size : Int32) : self
        @batch_size = batch_size
        self
      end

      # Adds cache options to the builder.
      def with_cache_options(cache_options : CacheOptions?) : self
        @cache_options = cache_options
        self
      end

      # Creates an `ArrayReader` from the parquet schema, the projection mask and
      # the row groups.
      def build_array_reader(field : ParquetField?, mask : ProjectionMask) : ArrayReader
        reader = field ? build_reader(field, mask, nil) : nil
        reader || Arrow.make_empty_array_reader(num_rows)
      end

      # The total number of rows.
      def num_rows : Int32
        @row_groups.num_rows.to_i32
      end

      private def build_reader(field : ParquetField, mask : ProjectionMask,
                               padding_threshold : Int16?) : ArrayReader?
        case field.field_type
        when ParquetFieldType::Primitive
          reader = build_primitive_reader(field, mask, padding_threshold)
          return nil unless reader

          cache_options = @cache_options
          return reader unless cache_options

          col_idx = field.field_type.as(ParquetFieldType::Primitive).col_idx
          if cache_options.projection_mask.leaf_included?(col_idx)
            CachedArrayReader.new(
              reader, cache_options.cache, col_idx, cache_options.role, @metrics
            )
          else
            reader
          end
        else
          case field.arrow_type.kind
          when .map?
            build_map_reader(field, mask, padding_threshold)
          when .struct?
            build_struct_reader(field, mask, padding_threshold)
          when .list?, .large_list?, .list_view?, .large_list_view?
            build_list_reader(field, mask, padding_threshold)
          when .fixed_size_list?
            build_fixed_size_list_reader(field, mask, padding_threshold)
          else
            raise NYIError.new("reading group type #{field.arrow_type} not implemented")
          end
        end
      end

      # Builds the reader for a map field (`List<Struct<key, value>>`).
      private def build_map_reader(field : ParquetField, mask : ProjectionMask,
                                   padding_threshold : Int16?) : ArrayReader?
        children = field.children || raise general_err!("map field must have children")
        unless children.size == 2
          raise general_err!("map field must have two children, got #{children.size}")
        end

        # The key/value readers are children of the struct inside the list, so
        # their padding threshold is the map's (= list's) def level.
        key_reader = build_reader(children[0], mask, field.def_level)
        value_reader = build_reader(children[1], mask, field.def_level)

        if key_reader && value_reader
          data_type = map_data_type(field, key_reader.data_type, value_reader.data_type)

          MapArrayReader.new(
            key_reader, value_reader, data_type,
            field.def_level, field.rep_level, field.nullable, padding_threshold
          )
        elsif key_reader.nil? && value_reader.nil?
          nil
        else
          raise general_err!("partial projection of MapArray is not supported")
        end
      end

      # Rebuilds the map data type with the key/value types actually produced by
      # the child readers (handles projection-driven type changes).
      private def map_data_type(field : ParquetField, key_type : ::Arrow::DataType,
                                value_type : ::Arrow::DataType) : ::Arrow::DataType
        map_field = field.arrow_type.field || raise general_err!("map field must have an entries field")
        struct_fields = map_field.data_type.fields || raise general_err!("map entries must be a struct")
        unless struct_fields.len == 2
          raise general_err!("map entries must have two fields, got #{struct_fields.len}")
        end

        key_field = struct_fields[0]
        value_field = struct_fields[1]
        entries = ::Arrow::Field.new(
          map_field.name,
          ::Arrow::DataType.struct(::Arrow::Fields.new([
            ::Arrow::Field.new(key_field.name, key_type, key_field.nullable?, key_field.metadata),
            ::Arrow::Field.new(value_field.name, value_type, value_field.nullable?, value_field.metadata),
          ])),
          map_field.nullable?,
          map_field.metadata
        )
        ::Arrow::DataType.map(entries, field.arrow_type.is_sorted || false)
      end

      # Builds the reader for a list/list-view field.
      private def build_list_reader(field : ParquetField, mask : ProjectionMask,
                                    padding_threshold : Int16?) : ArrayReader?
        children = field.children || raise general_err!("list field must have children")
        unless children.size == 1
          raise general_err!("list field must have one child, got #{children.size}")
        end

        # The child is padded at this list's def level; the threshold received
        # from the enclosing list/map becomes this list's parent threshold.
        item_reader = build_reader(children[0], mask, field.def_level) || return nil
        item_field = field.arrow_type.field || raise general_err!("list field must have an item field")
        item = ::Arrow::Field.new(
          item_field.name, item_reader.data_type, item_field.nullable?, item_field.metadata
        )

        def_level = field.def_level
        rep_level = field.rep_level

        case field.arrow_type.kind
        when .list?
          ListArrayReader(Int32).new(
            item_reader, ::Arrow::DataType.list(item), def_level, rep_level,
            field.nullable, padding_threshold
          )
        when .large_list?
          ListArrayReader(Int64).new(
            item_reader, ::Arrow::DataType.large_list(item), def_level, rep_level,
            field.nullable, padding_threshold
          )
        when .list_view?
          ListViewArrayReader(Int32).new(
            item_reader, ::Arrow::DataType.list_view(item), def_level, rep_level,
            field.nullable, padding_threshold
          )
        when .large_list_view?
          ListViewArrayReader(Int64).new(
            item_reader, ::Arrow::DataType.large_list_view(item), def_level, rep_level,
            field.nullable, padding_threshold
          )
        else
          raise NYIError.new("reading list type #{field.arrow_type} not implemented")
        end
      end

      # Builds the reader for a fixed-size list field.
      private def build_fixed_size_list_reader(field : ParquetField, mask : ProjectionMask,
                                               padding_threshold : Int16?) : ArrayReader?
        children = field.children || raise general_err!("fixed size list field must have children")
        unless children.size == 1
          raise general_err!("fixed size list must have one child, got #{children.size}")
        end

        item_reader = build_reader(children[0], mask, field.def_level) || return nil
        item_field = field.arrow_type.field || raise general_err!("fixed size list must have an item field")
        size = field.arrow_type.size || raise general_err!("fixed size list must have a size")
        item = ::Arrow::Field.new(
          item_field.name, item_reader.data_type, item_field.nullable?, item_field.metadata
        )

        FixedSizeListArrayReader.new(
          item_reader, size, ::Arrow::DataType.fixed_size_list(item, size),
          field.def_level, field.rep_level, field.nullable, padding_threshold
        )
      end

      # Builds the reader for a primitive (leaf) field.
      # ameba:disable Metrics/CyclomaticComplexity
      private def build_primitive_reader(field : ParquetField, mask : ProjectionMask,
                                         padding_threshold : Int16?) : ArrayReader?
        primitive = field.field_type
        return nil unless primitive.is_a?(ParquetFieldType::Primitive)

        col_idx = primitive.col_idx
        primitive_type = primitive.primitive_type
        return nil unless mask.leaf_included?(col_idx)

        physical_type = primitive_type.get_physical_type

        # The column path is only required by the descriptor; the readers do not
        # use it (upstream does the same), because the arrow mapping can collapse
        # several parquet levels into one arrow field.
        column_desc = Schema::ColumnDescriptor.new(
          primitive_type, field.def_level, field.rep_level, Schema::ColumnPath.from([] of String)
        )

        page_iterator = @row_groups.column_chunks(col_idx)
        arrow_type = field.arrow_type

        # `LogicalType::Unknown` maps to `DataType::Null` (an entirely null
        # column), which is read by the null array reader. Upstream notes the
        # physical type is irrelevant there (it can only be annotated on a
        # column of nulls), so the reader is always instantiated for INT32.
        if arrow_type.kind.null?
          return NullArrayReader.new(page_iterator, column_desc, @batch_size, padding_threshold)
        end

        case physical_type
        when Parquet::Type::BOOLEAN
          PrimitiveArrayReader(Bool).new(
            page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
          )
        when Parquet::Type::INT32
          PrimitiveArrayReader(Int32).new(
            page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
          )
        when Parquet::Type::INT64
          PrimitiveArrayReader(Int64).new(
            page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
          )
        when Parquet::Type::FLOAT
          PrimitiveArrayReader(Float32).new(
            page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
          )
        when Parquet::Type::DOUBLE
          PrimitiveArrayReader(Float64).new(
            page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
          )
        when Parquet::Type::INT96
          raise NYIError.new("INT96 array readers are not ported (no Int96 support)")
        when Parquet::Type::BYTE_ARRAY
          case arrow_type.kind
          when .dictionary?
            Arrow.make_byte_array_dictionary_reader(
              page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
            )
          when .utf8_view?, .binary_view?
            Arrow.make_byte_view_array_reader(
              page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
            )
          else
            Arrow.make_byte_array_reader(
              page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
            )
          end
        else
          if arrow_type.kind.dictionary?
            Arrow.make_byte_array_dictionary_reader(
              page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
            )
          else
            Arrow.make_fixed_len_byte_array_reader(
              page_iterator, column_desc, arrow_type, @batch_size, padding_threshold
            )
          end
        end
      end

      # Builds the reader for a struct field.
      private def build_struct_reader(field : ParquetField, mask : ProjectionMask,
                                      padding_threshold : Int16?) : ArrayReader?
        arrow_fields = field.arrow_type.fields || raise general_err!("struct field must have fields")
        children = field.children || raise general_err!("struct field must have children")
        unless arrow_fields.len == children.size
          raise general_err!(
            "struct field has #{arrow_fields.len} arrow fields but #{children.size} parquet children"
          )
        end

        fields = [] of ::Arrow::Field
        readers = [] of ArrayReader

        arrow_fields.to_a.each_with_index do |arrow_field, idx|
          reader = build_reader(children[idx], mask, padding_threshold)
          next unless reader

          # Rebuild the field with the reader's data type (projection can change
          # the type), copying to avoid arrow-cr's in-place `with_*` helpers.
          fields << ::Arrow::Field.new(
            arrow_field.name, reader.data_type, arrow_field.nullable?, arrow_field.metadata
          )
          readers << reader
        end

        return nil if readers.empty?

        StructArrayReader.new(
          ::Arrow::DataType.struct(::Arrow::Fields.new(fields)),
          readers,
          field.def_level,
          field.rep_level,
          field.nullable,
          padding_threshold
        )
      end
    end
  end
end
