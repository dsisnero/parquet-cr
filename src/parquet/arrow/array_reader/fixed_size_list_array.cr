require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../record_reader/definition_levels"

# Ported from `parquet/src/arrow/array_reader/fixed_size_list_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `FixedSizeListArrayReader`: the item reader (already padded at this
# list's definition level) yields a compact child array with only the items of
# non-null lists, and `consume_batch` inserts `fixed_size` null entries for each
# null list (via `MutableArrayData`), then wraps the child in a
# `FixedSizeListArray` with a level-derived null bitmap.
#
# Divergences: `usize` counts are `Int32`; `MutableArrayData::try_extend` /
# `try_extend_nulls` raise on error in Crystal (upstream returns `Result`), and
# the unchecked `ArrayDataBuilder::build_unchecked` maps to
# `ArrayData.builder(...).nulls(...)` + `build_unchecked`.
module Parquet
  module Arrow
    # An `ArrayReader` that builds `FixedSizeListArray`s
    # (upstream `FixedSizeListArrayReader`).
    class FixedSizeListArrayReader < ArrayReader
      # `def_level`/`rep_level` are this list's own levels; `nullable` controls
      # the null bitmap, and `parent_threshold` is set when this reader is the
      # child of another list (entries with `def < threshold` are excluded).
      def initialize(
        @item_reader : ArrayReader,
        @fixed_size : Int32,
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

        def_levels = self.def_levels || raise general_err!("item_reader def levels are None")
        rep_levels = self.rep_levels || raise general_err!("item_reader rep levels are None")

        return empty_list if def_levels.empty?

        unless rep_levels[0] == 0
          raise general_err!("first repetition level of batch must be 0")
        end

        # Single pass: collect the definition level at each list boundary and
        # check that every non-null row has exactly `fixed_size` items.
        boundary_defs = [] of Int16
        current_row_len = nil.as(Int32?)
        threshold = @parent_threshold

        def_levels.each_with_index do |def_level_value, index|
          rep_level_value = rep_levels[index]
          next if threshold && def_level_value < threshold

          if rep_level_value < @rep_level
            if row_len = current_row_len
              current_row_len = nil
              unless row_len == @fixed_size
                raise general_err!(
                  "Encountered misaligned row with length #{row_len} (expected length #{@fixed_size})"
                )
              end
            end

            boundary_defs << def_level_value
            current_row_len = def_level_value >= @def_level ? 1 : nil
          elsif rep_level_value == @rep_level
            if row_len = current_row_len
              current_row_len = row_len + 1
            end
          elsif def_level_value < @def_level
            raise general_err!("Encountered repetition level too large for definition level")
          end
        end

        if row_len = current_row_len
          unless row_len == @fixed_size
            raise general_err!(
              "Encountered misaligned row with length #{row_len} (expected length #{@fixed_size})"
            )
          end
        end

        list_len = boundary_defs.size
        items_count = boundary_defs.count { |def_level_value| def_level_value >= @def_level }

        unless child_array.len == items_count * @fixed_size
          raise general_err!(
            "Fixed-size list child mismatch: expected #{items_count * @fixed_size} entries but got #{child_array.len}"
          )
        end

        child_data =
          if items_count == list_len
            # Every list is non-null: the compact child is already correct.
            child_array.to_data
          else
            data = child_array.to_data
            builder = ::Arrow::MutableArrayData.new([data] of ::Arrow::ArrayData, true,
              list_len * @fixed_size)
            src_idx = 0
            boundary_defs.each do |def_level_value|
              if def_level_value >= @def_level
                builder.try_extend(0, src_idx, src_idx + @fixed_size)
                src_idx += @fixed_size
              else
                builder.try_extend_nulls(@fixed_size)
              end
            end
            builder.freeze
          end

        list_builder = ::Arrow::ArrayData.builder(@data_type)
          .len(list_len)
          .add_child_data(child_data)

        if @nullable
          validity = ::Arrow::BooleanBufferBuilder.new(list_len)
          RecordReader.build_filtered_validity_bitmap(
            boundary_defs, nil, nil, @def_level - 1, validity
          )
          list_builder = list_builder.nulls(::Arrow::NullBuffer.new(validity.finish))
        end

        ::Arrow::FixedSizeListArray.from(list_builder.build_unchecked)
      end

      # An empty batch yields an empty fixed-size list array (upstream
      # `new_empty_array`).
      private def empty_list : ::Arrow::ArrayBase
        field = @data_type.field || raise general_err!("expected fixed size list type")
        values = ::Arrow.make_array(::Arrow::ArrayData.new_empty(field.data_type))
        ::Arrow::FixedSizeListArray.try_new_with_length(field, @fixed_size, values, nil, 0)
      end
    end
  end
end
