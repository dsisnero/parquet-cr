require "arrow-cr"
require "../../errors"
require "../array_reader"

# Ported from `parquet/src/arrow/array_reader/list_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ListArrayReader<OffsetSize>` for `List` (`O = Int32`) / `LargeList`
# (`O = Int64`). The item reader must already be configured with a padding
# threshold of this list's definition level, so it produces item-level padded
# arrays; `consume_batch` then reconstructs the offsets directly from the item
# reader's definition/repetition levels (no `MutableArrayData`).
#
# Divergences: `usize` counts are `Int32`, so the upstream `OffsetSize::from_usize`
# overflow check (offsets above `Int32::MAX`/`Int64::MAX`) cannot trigger and is
# omitted; the unchecked `ArrayDataBuilder::build_unchecked` maps to arrow-cr's
# positional `GenericListArray#initialize(data_type, offsets, values, nulls)`;
# `new_empty_array` for an empty batch maps to a zero-length list over the
# consumed (empty) child array.
module Parquet
  module Arrow
    # An `ArrayReader` that builds nested lists from an item reader
    # (upstream `ListArrayReader<OffsetSize>`).
    class ListArrayReader(O) < ArrayReader
      # `def_level` is the level at which this list is non-null, `rep_level` the
      # repetition level of a new value in this list, and `parent_threshold` is
      # set when this list is itself a child of another list (entries with
      # `def < threshold` belong to a null/empty parent list and are excluded).
      def initialize(
        @item_reader : ArrayReader,
        @data_type : ::Arrow::DataType,
        @def_level : Int16,
        @rep_level : Int16,
        @nullable : Bool,
        @parent_threshold : Int16?,
      )
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        @item_reader.read_records(batch_size)
      end

      def skip_records(num_records : Int32) : Int32
        @item_reader.skip_records(num_records)
      end

      def def_levels : Array(Int16)?
        @item_reader.def_levels
      end

      def rep_levels : Array(Int16)?
        @item_reader.rep_levels
      end

      def max_def_level : Int16
        @def_level
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def consume_batch : ::Arrow::ArrayBase
        child_array = @item_reader.consume_batch
        def_levels = @item_reader.def_levels || raise general_err!("item_reader def levels are None.")
        rep_levels = @item_reader.rep_levels || raise general_err!("item_reader rep levels are None.")

        return empty_list(child_array) if def_levels.empty?

        unless rep_levels[0] == 0
          raise general_err!("first repetition level of batch must be 0")
        end

        # Definition levels identify whether each list boundary contributes a
        # child slot. For a nullable list:
        #
        #   d >= def_level     : list is present with a child item
        #   d == def_level - 1 : list is present but empty
        #   d <= def_level - 2 : list is null
        #
        # `r <= rep_level` (not `==`) keeps only direct children: the first
        # element of each list has `r < rep_level` (it coincides with an outer
        # boundary), while continuation entries of a nested child's own list have
        # `r > rep_level` and are excluded.
        list_offsets = [] of O
        validity = @nullable ? ::Arrow::BooleanBufferBuilder.new(def_levels.size) : nil
        cur_offset = 0
        threshold = @parent_threshold
        def_level = @def_level
        rep_level = @rep_level

        def_levels.each_with_index do |def_level_value, index|
          rep_level_value = rep_levels[index]
          next if threshold && def_level_value < threshold

          if rep_level_value < rep_level
            list_offsets << O.new(cur_offset)
            validity.try(&.append(def_level_value + 1 >= def_level))
          end

          cur_offset += 1 if def_level_value >= def_level && rep_level_value <= rep_level
        end

        list_offsets << O.new(cur_offset)

        unless cur_offset == child_array.len
          raise general_err!(
            "Failed to reconstruct list from level data: expected #{cur_offset} child values but got #{child_array.len}"
          )
        end

        nulls =
          if builder = validity
            unless builder.len == list_offsets.size - 1
              raise general_err!(
                "Failed to decode level data for list array: expected #{list_offsets.size - 1} validity bits but got #{builder.len}"
              )
            end
            ::Arrow::NullBuffer.new(builder.finish)
          else
            nil
          end

        offsets = ::Arrow::OffsetBuffer(O).new(::Arrow::ScalarBuffer(O).from(list_offsets))
        ::Arrow::GenericListArray(O).new(@data_type, offsets, child_array, nulls)
      end

      # A zero-length list over the consumed (empty) child array.
      private def empty_list(child_array : ::Arrow::ArrayBase) : ::Arrow::ArrayBase
        offsets = ::Arrow::OffsetBuffer(O).new(::Arrow::ScalarBuffer(O).from([O.new(0)]))
        ::Arrow::GenericListArray(O).new(@data_type, offsets, child_array, nil)
      end
    end
  end
end
