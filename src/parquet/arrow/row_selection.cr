require "../errors"

# Ported from `parquet/src/arrow/arrow_reader/selection/selector.rs` and the
# selector-backed parts of `selection/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `RowSelector`, the RLE-backed `RowSelection` and the selector
# transforms (`split_off`, `offset`, `limit`). A `RowSelection` describes the
# rows of a parquet file to read before any column data is loaded, so skips can
# be pushed down into the array readers.
#
# Divergences: only the selector (RLE) backing is modelled — the bitmap backing
# (`RowSelection::from_boolean_buffer`/`from_filters`/`as_mask`,
# `MaskRunIter`, and `selection/boolean.rs` + `selection/algebra.rs`) is not
# ported, so every selection is selector-backed; `usize` counts are `Int32`;
# `intersection`/`union`/`and_then`/`scan_ranges`/`expand_to_batch_boundaries`
# are deferred with the read plan.
module Parquet
  module Arrow
    # A run-length encoded selection of rows (upstream `RowSelector`).
    struct RowSelector
      # The number of rows in this run.
      getter row_count : Int32

      def initialize(@row_count : Int32, @skip : Bool)
      end

      # Selects `row_count` rows.
      def self.select(row_count : Int32) : RowSelector
        new(row_count, false)
      end

      # Skips `row_count` rows.
      def self.skip(row_count : Int32) : RowSelector
        new(row_count, true)
      end

      # Whether this run skips rows.
      def skip? : Bool
        @skip
      end

      def ==(other : RowSelector) : Bool
        @row_count == other.row_count && @skip == other.skip?
      end

      def to_s(io : IO) : Nil
        io << (@skip ? "skip(" : "select(") << @row_count << ')'
      end
    end

    # The selector transforms of `selection/selector.rs` (upstream
    # `split_off_selectors`/`offset_selectors`/`limit_selectors`).
    module RowSelectorTransforms
      # Splits `selectors` at the first `row_count` rows, returning
      # `{head, tail}`.
      def self.split_off(selectors : Array(RowSelector), row_count : Int32) : Tuple(Array(RowSelector), Array(RowSelector))
        total_count = 0
        split_idx = nil.as(Int32?)

        selectors.each_with_index do |selector, idx|
          total_count += selector.row_count
          if total_count > row_count
            split_idx = idx
            break
          end
        end

        idx = split_idx
        return {selectors.dup, [] of RowSelector} unless idx

        head = selectors[0, idx]
        tail = selectors[idx, selectors.size - idx].dup

        # `head` keeps the part of the straddling selector that fits.
        next_selector = tail.first
        overflow = total_count - row_count
        if next_selector.row_count != overflow
          head << RowSelector.new(next_selector.row_count - overflow, next_selector.skip?)
        end
        tail[0] = RowSelector.new(overflow, next_selector.skip?)

        {head, tail}
      end

      # Skips the first `offset` selected rows of `selectors`.
      def self.offset(selectors : Array(RowSelector), offset : Int32) : Array(RowSelector)
        return selectors.dup if offset == 0

        selected_count = 0
        skipped_count = 0
        split_idx = nil.as(Int32?)

        selectors.each_with_index do |selector, idx|
          if selector.skip?
            skipped_count += selector.row_count
          else
            selected_count += selector.row_count
            if selected_count > offset
              split_idx = idx
              break
            end
          end
        end

        idx = split_idx
        return [] of RowSelector unless idx

        result = [] of RowSelector
        result << RowSelector.skip(skipped_count + offset)
        result << RowSelector.select(selected_count - offset)
        result.concat(selectors[(idx + 1), selectors.size - idx - 1])
        result
      end

      # Keeps only the first `limit` selected rows of `selectors`.
      def self.limit(selectors : Array(RowSelector), limit : Int32) : Array(RowSelector)
        return [] of RowSelector if limit == 0

        remaining = limit
        result = [] of RowSelector

        selectors.each do |selector|
          if selector.skip?
            result << selector
          else
            if selector.row_count >= remaining
              result << RowSelector.select(remaining)
              break
            end
            remaining -= selector.row_count
            result << selector
          end
        end

        result
      end
    end

    # A collection of `RowSelector`s describing which rows to read (upstream
    # `RowSelection`).
    class RowSelection
      include Enumerable(RowSelector)

      @selectors : Array(RowSelector)

      # Creates a selection, dropping empty selectors and combining adjacent
      # ones (upstream `FromIterator<RowSelector>`).
      def initialize(selectors : Enumerable(RowSelector))
        @selectors = normalize(selectors)
      end

      # An empty selection.
      def self.empty : RowSelection
        new([] of RowSelector)
      end

      # Creates a selection from consecutive ranges of selected rows.
      def self.from_consecutive_ranges(ranges : Enumerable(Range(Int32, Int32)), total_rows : Int32) : RowSelection
        selectors = [] of RowSelector
        last_end = 0

        ranges.each do |range|
          len = range.end - range.begin
          next if len == 0

          if range.begin == last_end
            if last = selectors.last?
              selectors[selectors.size - 1] =
                RowSelector.new(last.row_count + len, last.skip?)
            else
              selectors << RowSelector.select(len)
            end
          elsif range.begin > last_end
            selectors << RowSelector.skip(range.begin - last_end)
            selectors << RowSelector.select(len)
          else
            raise general_err!("out of order")
          end

          last_end = range.end
        end

        if last_end != total_rows
          selectors << RowSelector.skip(total_rows - last_end)
        end

        new(selectors)
      end

      # Iterates the selectors.
      def each(& : RowSelector ->) : Nil
        @selectors.each { |selector| yield selector }
      end

      # The number of selected rows.
      def row_count : Int32
        @selectors.reject(&.skip?).sum(0, &.row_count)
      end

      # The number of skipped rows.
      def skipped_row_count : Int32
        @selectors.select(&.skip?).sum(0, &.row_count)
      end

      # The total number of rows spanned, selected and skipped.
      def total_row_count : Int32
        @selectors.sum(0, &.row_count)
      end

      # Whether any row is selected.
      def selects_any? : Bool
        @selectors.any? { |selector| !selector.skip? }
      end

      # Skips the first `offset` selected rows (upstream `RowSelection::offset`).
      def offset(offset : Int32) : RowSelection
        return self if offset == 0

        RowSelection.new(RowSelectorTransforms.offset(@selectors, offset))
      end

      # Keeps only the first `limit` selected rows (upstream
      # `RowSelection::limit`).
      def limit(limit : Int32) : RowSelection
        RowSelection.new(RowSelectorTransforms.limit(@selectors, limit))
      end

      # Removes trailing skips (upstream `RowSelection::trim`).
      def trim : RowSelection
        selectors = @selectors.dup
        while selectors.last?.try(&.skip?)
          selectors.pop
        end
        RowSelection.new(selectors)
      end

      # Splits off the first `row_count` rows, leaving the rest in `self`
      # (upstream `RowSelection::split_off`).
      def split_off(row_count : Int32) : RowSelection
        head, tail = RowSelectorTransforms.split_off(@selectors, row_count)
        @selectors = tail
        RowSelection.new(head)
      end

      def ==(other : RowSelection) : Bool
        @selectors == other.to_a
      end

      def to_s(io : IO) : Nil
        io << "RowSelection("
        @selectors.each_with_index do |selector, idx|
          io << ", " if idx > 0
          selector.to_s(io)
        end
        io << ')'
      end

      private def normalize(selectors : Enumerable(RowSelector)) : Array(RowSelector)
        result = [] of RowSelector
        selectors.each do |selector|
          next if selector.row_count == 0

          if last = result.last?
            if last.skip? == selector.skip?
              result[result.size - 1] = RowSelector.new(
                last.row_count + selector.row_count, last.skip?
              )
              next
            end
          end
          result << selector
        end
        result
      end
    end
  end
end
