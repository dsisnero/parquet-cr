require "arrow-cr"
require "../../errors"
require "../array_reader"
require "./list_array"

# Ported from `parquet/src/arrow/array_reader/list_view_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ListViewArrayReader<OffsetSize>`: it wraps a `ListArrayReader` over the
# corresponding `List`/`LargeList` data type and converts the produced list array
# into a `ListView`/`LargeListView` array (the offsets are reused, the sizes are
# `offsets[i + 1] - offsets[i]`).
#
# Divergences: upstream's `unreachable!()` for a non-list-view data type becomes
# a `GeneralError`; upstream's `From<GenericListArray>` conversion is done here
# because arrow-cr's list-view array only exposes `try_new`/`from(ArrayData)`
# (`OffsetBuffer#inner` supplies the offsets); `usize` counts are `Int32`.
module Parquet
  module Arrow
    # An `ArrayReader` that builds `ListView`/`LargeListView` arrays from an item
    # reader (upstream `ListViewArrayReader<OffsetSize>`).
    class ListViewArrayReader(O) < ArrayReader
      @inner : ListArrayReader(O)

      def initialize(
        item_reader : ArrayReader,
        @data_type : ::Arrow::DataType,
        def_level : Int16,
        rep_level : Int16,
        nullable : Bool,
        parent_threshold : Int16?,
      )
        # The underlying list reader works on the corresponding List type.
        list_data_type =
          case @data_type.kind
          when .list_view?
            field = @data_type.field || raise general_err!("expected list view type")
            ::Arrow::DataType.list(field)
          when .large_list_view?
            field = @data_type.field || raise general_err!("expected list view type")
            ::Arrow::DataType.large_list(field)
          else
            raise general_err!("expected list view type, got #{@data_type}")
          end

        @inner = ListArrayReader(O).new(
          item_reader, list_data_type, def_level, rep_level, nullable, parent_threshold
        )
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        @inner.read_records(batch_size)
      end

      def skip_records(num_records : Int32) : Int32
        @inner.skip_records(num_records)
      end

      def def_levels : Array(Int16)?
        @inner.def_levels
      end

      def rep_levels : Array(Int16)?
        @inner.rep_levels
      end

      def max_def_level : Int16
        @inner.max_def_level
      end

      def consume_batch : ::Arrow::ArrayBase
        array = @inner.consume_batch
        field = @data_type.field || raise general_err!("expected list view type")

        # An empty batch yields an empty list-view array (upstream
        # `new_empty_array`).
        return ::Arrow::GenericListViewArray(O).new_null(field, 0) if array.len == 0

        list = array.as(::Arrow::GenericListArray(O))
        offset_values = list.offsets.inner.to_a

        # A list view stores one offset and one size per value (a list stores
        # `len + 1` offsets), so the trailing list offset is dropped and the
        # sizes are the differences between consecutive list offsets.
        view_offsets = ::Arrow::ScalarBuffer(O).from(Array(O).new(list.len) { |idx| offset_values[idx] })
        sizes = ::Arrow::ScalarBuffer(O).from(
          Array(O).new(list.len) { |idx| offset_values[idx + 1] - offset_values[idx] }
        )

        view = ::Arrow::GenericListViewArray(O).try_new(
          field, view_offsets, sizes, list.values, list.nulls
        )

        unless view.data_type == @data_type
          raise general_err!(
            "Converted array type does not match expected type: expected #{@data_type}, got #{view.data_type}"
          )
        end

        view
      end
    end
  end
end
