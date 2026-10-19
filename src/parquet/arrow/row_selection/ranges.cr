require "../../errors"
require "../../file/page_index/offset_index"

# Ported from `parquet/src/arrow/arrow_reader/selection/ranges.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Maps the `RowSelector` runs of a selection onto ranges: the byte ranges of the
# data pages that must be fetched (`RowSelection#scan_ranges`) and the expansion
# of a selection to batch boundaries (`RowSelection#expand_to_batch_boundaries`).
# Both are shared by the selector and mask backings, which stream their runs
# from a slice and a `MaskRunIter` respectively.
#
# Divergences: `usize` row counts are `Int32` (row offsets are tracked as
# `Int64` to match `PageLocation#first_row_index`), and the `IntoIterator`
# parameters are `Enumerable`.
module Parquet
  module Arrow
    # Upstream `selection/ranges.rs`.
    module RowSelectionRanges
      # Byte ranges of the data pages containing at least one selected row.
      def self.scan_ranges(
        selectors : Enumerable(RowSelector),
        page_locations : Array(::Parquet::File::PageIndex::PageLocation),
      ) : Array(Range(UInt64, UInt64))
        ranges = [] of Range(UInt64, UInt64)
        row_offset = 0_i64

        all_selectors = selectors.to_a
        selector_index = 0
        current_selector = selector_index < all_selectors.size ? all_selectors[selector_index] : nil
        page_index = 0
        current_page = page_index < page_locations.size ? page_locations[page_index] : nil
        current_page_included = false

        while (selector = current_selector) && (page = current_page)
          if !selector.skip? && !current_page_included
            start = page.offset.to_u64
            ranges << (start...(start + page.compressed_page_size.to_u64))
            current_page_included = true
          end

          next_page = page_index + 1 < page_locations.size ? page_locations[page_index + 1] : nil
          if next_page
            if row_offset + selector.row_count > next_page.first_row_index
              remaining_in_page = (next_page.first_row_index - row_offset).to_i32
              # Keep the (shrunk) selector for the next page: upstream mutates
              # the current selector in place.
              current_selector = RowSelector.new(selector.row_count - remaining_in_page, selector.skip?)
              row_offset += remaining_in_page
              page_index += 1
              current_page = page_locations[page_index]
              current_page_included = false
            else
              if row_offset + selector.row_count == next_page.first_row_index
                page_index += 1
                current_page = page_locations[page_index]
                current_page_included = false
              end
              row_offset += selector.row_count
              selector_index += 1
              current_selector = selector_index < all_selectors.size ? all_selectors[selector_index] : nil
            end
          else
            if !selector.skip? && !current_page_included
              start = page.offset.to_u64
              ranges << (start...(start + page.compressed_page_size.to_u64))
            end
            selector_index += 1
            current_selector = selector_index < all_selectors.size ? all_selectors[selector_index] : nil
          end
        end

        ranges
      end

      # Grows each selected run to the batch boundaries containing it, merging
      # the runs that overlap as a result.
      def self.expand_to_batch_boundaries(
        selectors : Enumerable(RowSelector),
        batch_size : Int32,
        total_rows : Int32,
      ) : RowSelection
        expanded = [] of Range(Int32, Int32)
        row_offset = 0

        selectors.each do |selector|
          unless selector.skip?
            start = row_offset
            end_ = row_offset + selector.row_count

            # Expand start and end to batch boundaries.
            expanded_start = (start // batch_size) * batch_size
            expanded_end = Math.min(
              ((end_ + batch_size - 1) // batch_size) * batch_size,
              total_rows,
            )
            expanded << (expanded_start...expanded_end)
          end
          row_offset += selector.row_count
        end

        # Merge overlapping or consecutive ranges (they are already ordered).
        merged = [] of Range(Int32, Int32)
        expanded.each do |range|
          if last = merged.last?
            if range.begin <= last.end
              merged[merged.size - 1] = last.begin...Math.max(last.end, range.end)
            else
              merged << range
            end
          else
            merged << range
          end
        end

        RowSelection.from_consecutive_ranges(merged, total_rows)
      end
    end
  end
end
