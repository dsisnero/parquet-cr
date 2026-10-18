require "arrow-cr"
require "../../errors"
require "../array_reader"
require "./list_array"
require "./struct_array"

# Ported from `parquet/src/arrow/array_reader/map_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `MapArrayReader`: a map column is read as a `ListArrayReader` over a
# two-field `StructArrayReader` (the key/value entries), and the resulting list
# array is reinterpreted as a `MapArray`.
#
# Divergences: upstream's `assert!`/`unreachable!` panics become `GeneralError`s
# (same messages); `ArrayData::into_builder` +
# `build_unchecked` is done by rebuilding the data from the list array's parts
# with `ArrayData.builder(map_data_type)`; `usize` counts are `Int32`.
module Parquet
  module Arrow
    # An `ArrayReader` that builds `MapArray`s from a key reader and a value
    # reader (upstream `MapArrayReader`).
    class MapArrayReader < ArrayReader
      @reader : ListArrayReader(Int32)

      # `def_level`/`rep_level`/`nullable` come from the map's `ParquetField`;
      # `parent_threshold` is set when the map is itself nested in a list.
      def initialize(
        key_reader : ArrayReader,
        value_reader : ArrayReader,
        @data_type : ::Arrow::DataType,
        def_level : Int16,
        rep_level : Int16,
        nullable : Bool,
        parent_threshold : Int16?,
      )
        # The entries struct exists exactly when the key does (keys are always
        # required in maps), so the struct's def level is the key's max def level.
        struct_def_level = key_reader.max_def_level
        struct_rep_level = rep_level + 1

        unless @data_type.kind.map?
          raise general_err!("expected map type")
        end

        element = @data_type.field || raise general_err!("expected map type")

        unless element.data_type.kind.struct? && element.data_type.fields.try(&.len) == 2
          raise general_err!("expected struct with two fields")
        end

        # Parquet cannot represent nullability at this level, so a nullable
        # entries field indicates a schema inconsistency / inference bug.
        if element.nullable?
          raise general_err!("map struct cannot be nullable")
        end

        struct_reader = StructArrayReader.new(
          element.data_type,
          [key_reader, value_reader] of ArrayReader,
          struct_def_level,
          struct_rep_level,
          false,
          def_level
        )

        @reader = ListArrayReader(Int32).new(
          struct_reader,
          ::Arrow::DataType.list(element),
          def_level,
          rep_level,
          nullable,
          parent_threshold
        )
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        @reader.read_records(batch_size)
      end

      def skip_records(num_records : Int32) : Int32
        @reader.skip_records(num_records)
      end

      def def_levels : Array(Int16)?
        @reader.def_levels
      end

      def rep_levels : Array(Int16)?
        @reader.rep_levels
      end

      def max_def_level : Int16
        @reader.max_def_level
      end

      def consume_batch : ::Arrow::ArrayBase
        # A `MapArray` is a `ListArray` with a `StructArray` child, so only the
        # data type changes (the list reader already produced valid list data).
        array = @reader.consume_batch
        data = array.to_data

        builder = ::Arrow::ArrayData.builder(@data_type)
          .len(data.len)
          .add_buffer(data.buffers[0])
          .add_child_data(data.child_data[0])
          .nulls(data.nulls)

        ::Arrow::MapArray.from(builder.build_unchecked).as(::Arrow::ArrayBase)
      end
    end
  end
end
