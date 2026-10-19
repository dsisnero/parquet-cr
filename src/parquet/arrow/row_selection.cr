require "arrow-cr"
require "../errors"
require "./row_selection/algebra"

# Ported from `parquet/src/arrow/arrow_reader/selection/` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): `selector.rs` (the
# run-length `RowSelector` and its transforms), `boolean.rs` (the bitmap-backed
# `MaskSelection` and its transforms), `algebra.rs` (`and_then`/`intersection`/
# `union`, see `row_selection/algebra.cr`) and the dispatching parts of `mod.rs`.
#
# A `RowSelection` describes the rows of a parquet file to read before any
# column data is loaded, so skips can be pushed down into the array readers. It
# has two backings, exactly like upstream:
#
# * the RLE backing, an `Array(RowSelector)` (built by the normalizing
#   constructors, `from_consecutive_ranges` and `from_filters`);
# * the bitmap backing, a `MaskSelection` over an `::Arrow::BooleanBuffer` (built
#   by `from_boolean_buffer`, which keeps the mask as-is).
#
# The operations common to both backings (`and_then`, `intersection`, `union`,
# `split_off`, `trim`, `offset`, `limit`, equality, concatenation) dispatch on
# the backing, as upstream does.
#
# Divergences: `usize` counts are `Int32`; the cached `OnceLock`s are plain
# nilable instance variables (Crystal has no interior mutability requirement for
# a single-threaded cache); `RowSelection#iter` is `#each` (`Enumerable`), and
# `into_selectors`/`into_inner` leave the receiver usable; `scan_ranges`/
# `row_ranges_for_selected_pages`/`expand_to_batch_boundaries`/
# `auto_selection_strategy` are deferred with page pruning and the mask cursor;
# the mask algebra is bit-at-a-time (the upstream word kernels are an
# optimization only); `RowSelection` is not `Send`/`Sync`-constrained.
module Parquet
  module Arrow
    # A run-length encoded selection of rows (upstream `RowSelector`).
    struct RowSelector
      # The number of rows in this run.
      #
      # Writable because the algebra merges runs in place, as upstream's
      # `peek_mut` does.
      property row_count : Int32

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

    # Bitmap-backed `RowSelection` storage (upstream `selection/boolean.rs`
    # `MaskSelection`).
    #
    # `selectors` is only populated when a caller asks for the RLE form (via
    # `#selectors`/`#each`/`#into_selectors`); the mask stays authoritative.
    # `count` caches the popcount, and `RowSelection#split_off` propagates it to
    # both halves so repeated `row_count` calls do not rescan the bitmap.
    class MaskSelection
      # The selection bitmap: one bit per row.
      getter mask : ::Arrow::BooleanBuffer

      @selectors : Array(RowSelector)?
      @count : Int32?

      def initialize(@mask : ::Arrow::BooleanBuffer, @count : Int32? = nil)
        @selectors = nil
      end

      # Creates a mask selection whose selected-row count is already known.
      def self.with_count(mask : ::Arrow::BooleanBuffer, count : Int32) : MaskSelection
        raise general_err!("count exceeds the mask length") if count > mask.len

        new(mask, count)
      end

      # Number of selected rows, computed once and cached.
      def count : Int32
        @count ||= @mask.count_set_bits
      end

      # The cached selected-row count, if it has been computed.
      def cached_count : Int32?
        @count
      end

      # The backing bitmap.
      def into_mask : ::Arrow::BooleanBuffer
        @mask
      end

      # The RLE form, materialized once and cached.
      def selectors : Array(RowSelector)
        @selectors ||= SelectionMasks.to_selectors(@mask)
      end

      # The RLE form, reusing the cache when it is populated.
      def borrowed_selectors : Array(RowSelector)
        @selectors || SelectionMasks.to_selectors(@mask)
      end

      # The RLE form, taking the cache if it was populated.
      def into_selectors : Array(RowSelector)
        selectors
      end

      # Whether the RLE cache has been materialized (upstream's private
      # `selectors` `OnceLock`).
      def selectors_cached? : Bool
        !@selectors.nil?
      end

      # Upstream `Clone`: drops the selector cache but keeps the count cache.
      def dup : MaskSelection
        MaskSelection.new(@mask, @count)
      end
    end

    # Streaming RLE view of an `::Arrow::BooleanBuffer`, yielding `RowSelector`s
    # without allocating the selector vector (upstream `MaskRunIter`).
    class MaskRunIter
      include Iterator(RowSelector)

      def initialize(mask : ::Arrow::BooleanBuffer)
        @slices = mask.set_slices
        @cursor = 0
        @total = mask.len
        @pending = nil.as(RowSelector?)
        @finished = false
      end

      def next
        if pending = @pending
          @pending = nil
          return pending
        end
        return Iterator::Stop::INSTANCE if @finished

        slice = @slices.next
        if slice.is_a?(Tuple)
          start, end_ = slice
          selected = RowSelector.select(end_ - start)
          if start > @cursor
            skip = RowSelector.skip(start - @cursor)
            @pending = selected
            @cursor = end_
            skip
          else
            @cursor = end_
            selected
          end
        else
          @finished = true
          if @cursor < @total
            skip = RowSelector.skip(@total - @cursor)
            @cursor = @total
            skip
          else
            Iterator::Stop::INSTANCE
          end
        end
      end
    end

    # The mask primitives of `selection/boolean.rs`: `mask_to_selectors`,
    # `mask_has_at_least_runs`, `split_off_mask`, `trim_mask`, `offset_mask`,
    # `limit_mask` and `boolean_mask_from_selectors`.
    module SelectionMasks
      # Materializes a `BooleanBuffer` into its RLE form.
      def self.to_selectors(mask : ::Arrow::BooleanBuffer) : Array(RowSelector)
        total_rows = mask.len
        return [] of RowSelector if total_rows == 0

        selectors = [] of RowSelector
        last_end = 0
        mask.set_slices.each do |(start, end_)|
          if start > last_end
            selectors << RowSelector.skip(start - last_end)
          end
          selectors << RowSelector.select(end_ - start)
          last_end = end_
        end
        selectors << RowSelector.skip(total_rows - last_end) if last_end != total_rows
        selectors
      end

      # Whether `mask` contains at least `min_runs` alternating set/unset runs.
      #
      # Stops as soon as the requested number of runs is found, avoiding a full
      # scan when callers only need to know whether a boundary has been crossed.
      def self.has_at_least_runs?(mask : ::Arrow::BooleanBuffer, min_runs : Int32) : Bool
        return true if min_runs == 0

        total_rows = mask.len
        return false if total_rows == 0

        run_count = 0
        last_end = 0
        mask.set_slices.each do |(start, end_)|
          run_count += (start > last_end ? 1 : 0) + 1
          return true if run_count >= min_runs
          last_end = end_
        end

        run_count + (last_end < total_rows ? 1 : 0) >= min_runs
      end

      # Splits a mask into `(head, tail)` at `row_count`, preserving an empty
      # mask tail when the split point is past the end.
      def self.split_off(mask : ::Arrow::BooleanBuffer, row_count : Int32) : Tuple(::Arrow::BooleanBuffer, ::Arrow::BooleanBuffer)
        total = mask.len
        return {mask, ::Arrow::BooleanBuffer.new_unset(0)} if row_count >= total

        {mask.slice(0, row_count), mask.slice(row_count, total - row_count)}
      end

      # Trims trailing unset bits from a mask-backed selection, returning `nil`
      # when there is nothing to trim.
      def self.trim(mask : ::Arrow::BooleanBuffer) : ::Arrow::BooleanBuffer?
        len = mask.len
        # Fast path: a set final bit means there is nothing to trim.
        return nil if len == 0 || mask.value(len - 1)

        last_set = nil.as(Int32?)
        index = len - 1
        while index >= 0
          if mask.value(index)
            last_set = index
            break
          end
          index -= 1
        end
        new_len = last_set ? last_set + 1 : 0
        mask.slice(0, new_len)
      end

      # Skips the first `offset` selected rows of a mask-backed selection.
      # `popcount` is the caller's (possibly cached) set-bit count of `mask`.
      def self.offset(mask : ::Arrow::BooleanBuffer, offset : Int32, popcount : Int32) : ::Arrow::BooleanBuffer
        return ::Arrow::BooleanBuffer.new_unset(0) if offset >= popcount

        # Position one past the `offset`-th set bit, i.e. the index of the first
        # selected row to keep.
        pos = mask.find_nth_set_bit_position(0, offset)
        builder = ::Arrow::BooleanBufferBuilder.new(mask.len)
        builder.append_n(pos, false)
        builder.append_buffer(mask.slice(pos, mask.len - pos))
        builder.finish
      end

      # Keeps only the first `limit` selected rows of a mask-backed selection.
      def self.limit(mask : ::Arrow::BooleanBuffer, limit : Int32) : ::Arrow::BooleanBuffer
        # `find_nth_set_bit_position` returns `mask.len` when there are fewer
        # than `limit` set bits, so the slice naturally degrades to the
        # original mask in that case.
        cut = mask.find_nth_set_bit_position(0, limit)
        mask.slice(0, cut)
      end

      # Builds a bitmap from a selector sequence (upstream
      # `boolean_mask_from_selectors`).
      def self.from_selectors(selectors : Array(RowSelector)) : ::Arrow::BooleanBuffer
        total_rows = selectors.sum(0, &.row_count)
        builder = ::Arrow::BooleanBufferBuilder.new(total_rows)
        selectors.each { |selector| builder.append_n(selector.row_count, !selector.skip?) }
        builder.finish
      end
    end

    # Internal storage for `RowSelection` (upstream `RowSelectionInner`).
    abstract class RowSelectionInner
      # RLE-backed storage.
      class Selectors < RowSelectionInner
        # The (invariant-preserving) selector runs.
        getter selectors : Array(RowSelector)

        def initialize(@selectors : Array(RowSelector))
        end
      end

      # Bitmap-backed storage.
      class Mask < RowSelectionInner
        # The mask and its caches.
        getter mask : MaskSelection

        def initialize(@mask : MaskSelection)
        end
      end
    end

    # A selection of rows to read from a parquet file (upstream `RowSelection`).
    #
    # Depending on the pattern of rows to be selected, a `RowSelection` has
    # either a bitmap or an RLE (`RowSelector`) based implementation.
    # An RLE-backed selection maintains the following invariants (they do not
    # apply to the bitmap-backed implementation):
    #
    # * it contains no `RowSelector` of 0 rows;
    # * consecutive `RowSelector`s alternate skipping or selecting rows.
    class RowSelection
      include Enumerable(RowSelector)

      # Internal storage (upstream `RowSelectionInner`).
      getter inner : RowSelectionInner

      # Creates a selection, dropping empty selectors and combining adjacent
      # ones (upstream `FromIterator<RowSelector>`).
      def initialize(selectors : Enumerable(RowSelector))
        @inner = RowSelectionInner::Selectors.new(RowSelection.normalize(selectors))
      end

      # Wraps prepared storage (upstream's private constructors).
      def initialize(@inner : RowSelectionInner)
      end

      # An empty selection.
      def self.empty : RowSelection
        new([] of RowSelector)
      end

      # Wraps selectors without validating/normalizing them (upstream
      # `RowSelection::from_selectors`, which is not `pub`).
      def self.from_selectors(selectors : Array(RowSelector)) : RowSelection
        new(RowSelectionInner::Selectors.new(selectors))
      end

      # Wraps a bitmap (upstream `RowSelection::from_mask_selection`).
      def self.from_mask_selection(mask : MaskSelection) : RowSelection
        new(RowSelectionInner::Mask.new(mask))
      end

      # Creates a selection from a packed `::Arrow::BooleanBuffer`
      # (upstream `RowSelection::from_boolean_buffer`).
      #
      # Each set bit selects a row, each unset bit skips one. Unlike
      # `from_filters`, the bitmap is kept as-is rather than eagerly
      # run-length-encoded; `#each` materializes and caches the RLE form on
      # first use, while `MaskRunIter` streams it directly from the bitmap.
      def self.from_boolean_buffer(mask : ::Arrow::BooleanBuffer) : RowSelection
        from_mask_selection(MaskSelection.new(mask))
      end

      # Upstream `From<BooleanBuffer> for RowSelection`.
      def self.from(mask : ::Arrow::BooleanBuffer) : RowSelection
        from_boolean_buffer(mask)
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

        from_selectors(selectors)
      end

      # Creates a selection from a slice of `Arrow::BooleanArray` filters
      # (upstream `RowSelection::from_filters`).
      #
      # Rows selected by any filter are kept. Raises if a filter contains nulls.
      def self.from_filters(filters : Array(::Arrow::BooleanArray)) : RowSelection
        next_offset = 0
        total_rows = filters.sum(0, &.len)
        ranges = [] of Range(Int32, Int32)

        filters.each do |filter|
          offset = next_offset
          next_offset += filter.len
          raise general_err!("filter contains nulls") unless filter.null_count == 0

          ::Arrow::SlicesIterator.new(filter).each do |(start, end_)|
            ranges << Range.new(start + offset, end_ + offset)
          end
        end

        from_consecutive_ranges(ranges, total_rows)
      end

      # Concatenates selections in iterator order (upstream
      # `FromIterator<RowSelection> for RowSelection`).
      #
      # When every input is mask-backed the result stays mask-backed (the
      # bitmaps are appended); otherwise it falls back to flattening through the
      # per-`RowSelector` form.
      def self.concat(selections : Enumerable(RowSelection)) : RowSelection
        items = selections.to_a
        masks = items.compact_map(&.as_mask)
        if masks.size == items.size
          builder = ::Arrow::BooleanBufferBuilder.new(masks.sum(0, &.len))
          masks.each { |mask| builder.append_buffer(mask) }
          return from_boolean_buffer(builder.finish)
        end

        new(items.flat_map(&.into_selectors_vec))
      end

      # Returns the underlying bitmap if this selection is bitmap-backed.
      def as_mask : ::Arrow::BooleanBuffer?
        inner = @inner
        inner.is_a?(RowSelectionInner::Mask) ? inner.mask.mask : nil
      end

      # Whether this selection is bitmap-backed (upstream pattern-matches on
      # `RowSelectionInner`).
      def mask_backed? : Bool
        @inner.is_a?(RowSelectionInner::Mask)
      end

      # Iterates the selectors (upstream `RowSelection::iter`).
      #
      # Bitmap-backed selections materialize a selector cache on first call;
      # use `#as_mask` + `MaskRunIter` to stream without allocating.
      def each(& : RowSelector ->) : Nil
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          inner.mask.selectors.each { |selector| yield selector }
        else
          inner.as(RowSelectionInner::Selectors).selectors.each { |selector| yield selector }
        end
      end

      # The number of selected rows.
      def row_count : Int32
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          inner.mask.count
        else
          inner.as(RowSelectionInner::Selectors).selectors.sum(0) do |selector|
            selector.skip? ? 0 : selector.row_count
          end
        end
      end

      # The total number of rows spanned, selected and skipped.
      def total_row_count : Int32
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          inner.mask.mask.len
        else
          inner.as(RowSelectionInner::Selectors).selectors.sum(0, &.row_count)
        end
      end

      # The number of skipped rows.
      def skipped_row_count : Int32
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          mask = inner.mask.mask
          mask.len - inner.mask.count
        else
          inner.as(RowSelectionInner::Selectors).selectors.sum(0) do |selector|
            selector.skip? ? selector.row_count : 0
          end
        end
      end

      # Whether any row is selected.
      def selects_any? : Bool
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          if count = inner.mask.cached_count
            count > 0
          else
            !inner.mask.mask.set_indices.next.is_a?(Iterator::Stop)
          end
        else
          inner.as(RowSelectionInner::Selectors).selectors.any? { |selector| !selector.skip? }
        end
      end

      # Skips the first `offset` selected rows (upstream `RowSelection::offset`).
      def offset(offset : Int32) : RowSelection
        return self if offset == 0

        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          count = inner.mask.count
          buffer = SelectionMasks.offset(inner.mask.mask, offset, count)
          RowSelection.from_mask_selection(MaskSelection.with_count(buffer, Math.max(count - offset, 0)))
        else
          RowSelection.from_selectors(RowSelectorTransforms.offset(inner.as(RowSelectionInner::Selectors).selectors, offset))
        end
      end

      # Keeps only the first `limit` selected rows (upstream
      # `RowSelection::limit`).
      def limit(limit : Int32) : RowSelection
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          cached = inner.mask.cached_count
          buffer = SelectionMasks.limit(inner.mask.mask, limit)
          if count = cached
            RowSelection.from_mask_selection(MaskSelection.with_count(buffer, Math.min(count, limit)))
          else
            RowSelection.from_boolean_buffer(buffer)
          end
        else
          RowSelection.from_selectors(RowSelectorTransforms.limit(inner.as(RowSelectionInner::Selectors).selectors, limit))
        end
      end

      # Removes trailing skips (upstream `RowSelection::trim`).
      def trim : RowSelection
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          mask = inner.mask
          if trimmed = SelectionMasks.trim(mask.mask)
            if count = mask.cached_count
              RowSelection.from_mask_selection(MaskSelection.with_count(trimmed, count))
            else
              RowSelection.from_boolean_buffer(trimmed)
            end
          else
            self
          end
        else
          selectors = inner.as(RowSelectionInner::Selectors).selectors.dup
          while selectors.last?.try(&.skip?)
            selectors.pop
          end
          RowSelection.from_selectors(selectors)
        end
      end

      # Splits off the first `row_count` rows, leaving the rest in `self`
      # (upstream `RowSelection::split_off`).
      def split_off(row_count : Int32) : RowSelection
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          mask = inner.mask
          total = mask.cached_count
          head, tail = SelectionMasks.split_off(mask.mask, row_count)
          head, tail = if count = total
                         head_count = tail.len == 0 ? count : head.count_set_bits
                         {
                           MaskSelection.with_count(head, head_count),
                           MaskSelection.with_count(tail, count - head_count),
                         }
                       else
                         {MaskSelection.new(head), MaskSelection.new(tail)}
                       end
          @inner = RowSelectionInner::Mask.new(tail)
          RowSelection.from_mask_selection(head)
        else
          head, tail = RowSelectorTransforms.split_off(inner.as(RowSelectionInner::Selectors).selectors, row_count)
          @inner = RowSelectionInner::Selectors.new(tail)
          RowSelection.from_selectors(head)
        end
      end

      # Applies `other` to the rows selected by `self` (upstream
      # `RowSelection::and_then`).
      #
      # Raises if `other` does not have a length equal to the number of rows
      # selected by `self`.
      def and_then(other : RowSelection) : RowSelection
        RowSelectionAlgebra.and_then(self, other)
      end

      # The rows selected by both selections (upstream
      # `RowSelection::intersection`).
      def intersection(other : RowSelection) : RowSelection
        RowSelectionAlgebra.intersection(self, other)
      end

      # The rows selected by either selection (upstream `RowSelection::union`).
      def union(other : RowSelection) : RowSelection
        RowSelectionAlgebra.union(self, other)
      end

      # Copies the selection, dropping a bitmap-backed selector cache
      # (upstream `Clone`).
      def dup : RowSelection
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          RowSelection.from_mask_selection(inner.mask.dup)
        else
          RowSelection.from_selectors(inner.as(RowSelectionInner::Selectors).selectors.dup)
        end
      end

      # Upstream `From<RowSelection> for Vec<RowSelector>`.
      def into_selectors_vec : Array(RowSelector)
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          inner.mask.into_selectors
        else
          inner.as(RowSelectionInner::Selectors).selectors
        end
      end

      def ==(other : RowSelection) : Bool
        a = @inner
        b = other.inner

        if a.is_a?(RowSelectionInner::Selectors)
          if b.is_a?(RowSelectionInner::Selectors)
            a.selectors == b.selectors
          else
            selectors_equal_mask?(a.selectors, b.as(RowSelectionInner::Mask).mask.mask)
          end
        elsif b.is_a?(RowSelectionInner::Selectors)
          selectors_equal_mask?(b.selectors, a.as(RowSelectionInner::Mask).mask.mask)
        else
          a.as(RowSelectionInner::Mask).mask.mask == b.as(RowSelectionInner::Mask).mask.mask
        end
      end

      def to_s(io : IO) : Nil
        inner = @inner
        if inner.is_a?(RowSelectionInner::Mask)
          io << "RowSelection(mask_len=" << inner.mask.mask.len << ')'
          return
        end

        io << "RowSelection("
        inner.as(RowSelectionInner::Selectors).selectors.each_with_index do |selector, idx|
          io << ", " if idx > 0
          selector.to_s(io)
        end
        io << ')'
      end

      # Upstream `FromIterator<RowSelector>`: drops empty selectors and
      # combines adjacent ones. (Upstream inlines this in the `FromIterator`
      # impl; it is a class method here so `RowSelection.new` can reach it.)
      def self.normalize(selectors : Enumerable(RowSelector)) : Array(RowSelector)
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

      # Upstream's mixed-backing `PartialEq`: the selector runs must line up
      # exactly with the mask's set slices.
      private def selectors_equal_mask?(selectors : Array(RowSelector), mask : ::Arrow::BooleanBuffer) : Bool
        return false unless selectors.sum(0, &.row_count) == mask.len

        slices = mask.set_slices.to_a
        slice_idx = 0
        cursor = 0

        selectors.each do |selector|
          end_ = cursor + selector.row_count
          if selector.skip?
            if slice_idx < slices.size && slices[slice_idx][0] < end_
              return false
            end
          else
            slice = slice_idx < slices.size ? slices[slice_idx] : nil
            return false unless slice && slice[0] == cursor && slice[1] == end_

            slice_idx += 1
          end
          cursor = end_
        end

        slice_idx == slices.size
      end
    end
  end
end
