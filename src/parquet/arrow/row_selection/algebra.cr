require "arrow-cr"
require "../../errors"

# Ported from `parquet/src/arrow/arrow_reader/selection/algebra.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The set algebra backing `RowSelection#and_then`, `#intersection` and
# `#union`. Each operation has two implementations, picked by the backing of
# its operands: a merge of the `RowSelector` runs, or a bitwise variant over
# `::Arrow::BooleanBuffer` masks.
#
# Divergences: the run iterators are index cursors over mutable copies (Crystal
# has no `Peekable<Copied<_>>`); the mask kernels are bit-at-a-time instead of
# `u64` word operations, and the unequal-length mask combination always
# produces a zero-offset `BooleanBuffer` (upstream carries the longer mask's
# sub-byte offset); upstream's `panic!`s are `GeneralError`s with the same
# messages.
module Parquet
  module Arrow
    # Upstream `selection/algebra.rs`.
    module RowSelectionAlgebra
      # Applies `second` to the rows selected by `first` (upstream
      # `RowSelection::and_then`).
      def self.and_then(first : RowSelection, second : RowSelection) : RowSelection
        f = first.inner
        s = second.inner

        if f.is_a?(RowSelectionInner::Mask)
          RowSelection.from_boolean_buffer(and_then_mask(f.mask.mask, second))
        elsif s.is_a?(RowSelectionInner::Selectors)
          and_then_row_selections(
            f.as(RowSelectionInner::Selectors).selectors,
            s.selectors,
          )
        else
          and_then_selectors_with_mask(
            f.as(RowSelectionInner::Selectors).selectors,
            s.as(RowSelectionInner::Mask).mask.mask,
          )
        end
      end

      # The rows selected by both `first` and `second` (upstream
      # `RowSelection::intersection`).
      def self.intersection(first : RowSelection, second : RowSelection) : RowSelection
        f = first.inner
        s = second.inner
        f_selectors = f.is_a?(RowSelectionInner::Selectors) ? f.selectors : nil
        s_selectors = s.is_a?(RowSelectionInner::Selectors) ? s.selectors : nil

        if f_selectors && s_selectors
          intersect_row_selections(f_selectors, s_selectors)
        elsif f_selectors
          intersect_row_selections(f_selectors, s.as(RowSelectionInner::Mask).mask.borrowed_selectors)
        elsif s_selectors
          intersect_row_selections(f.as(RowSelectionInner::Mask).mask.borrowed_selectors, s_selectors)
        else
          f_mask = f.as(RowSelectionInner::Mask).mask.mask
          s_mask = s.as(RowSelectionInner::Mask).mask.mask
          RowSelection.from_boolean_buffer(combine_masks(f_mask, s_mask) { |a, b| a && b })
        end
      end

      # The rows selected by either `first` or `second` (upstream
      # `RowSelection::union`).
      def self.union(first : RowSelection, second : RowSelection) : RowSelection
        f = first.inner
        s = second.inner
        f_selectors = f.is_a?(RowSelectionInner::Selectors) ? f.selectors : nil
        s_selectors = s.is_a?(RowSelectionInner::Selectors) ? s.selectors : nil

        if f_selectors && s_selectors
          union_row_selections(f_selectors, s_selectors)
        elsif f_selectors
          union_row_selections(f_selectors, s.as(RowSelectionInner::Mask).mask.borrowed_selectors)
        elsif s_selectors
          union_row_selections(f.as(RowSelectionInner::Mask).mask.borrowed_selectors, s_selectors)
        else
          f_mask = f.as(RowSelectionInner::Mask).mask.mask
          s_mask = s.as(RowSelectionInner::Mask).mask.mask
          RowSelection.from_boolean_buffer(combine_masks(f_mask, s_mask) { |a, b| a || b })
        end
      end

      # Applies `second` to the rows selected by the RLE-backed `first`.
      def self.and_then_row_selections(first : Array(RowSelector), second : Array(RowSelector)) : RowSelection
        selectors = [] of RowSelector
        and_then_iter(selectors, first.dup, second.dup)
        RowSelection.from_selectors(selectors)
      end

      # Applies the mask `second` to the rows selected by the RLE-backed
      # `first`. The mask is streamed as `RowSelector` runs, so it is never
      # materialized as a bitmap-to-selector cache.
      def self.and_then_selectors_with_mask(first : Array(RowSelector), second : ::Arrow::BooleanBuffer) : RowSelection
        and_then_row_selections(first, MaskRunIter.new(second).to_a)
      end

      # Upstream `and_then_iter`: merges the selected rows of `first` through
      # `second`, mutating both run lists as it consumes them.
      private def self.and_then_iter(selectors : Array(RowSelector), first : Array(RowSelector), second : Array(RowSelector)) : Nil
        to_skip = 0
        i = 0
        j = 0

        while j < second.size
          raise general_err!("selection exceeds the number of selected rows") if i >= first.size

          b = second[j]
          a = first[i]

          if b.row_count == 0
            j += 1
            next
          end

          if a.row_count == 0
            i += 1
            next
          end

          if a.skip?
            # Records were skipped when producing `second`
            to_skip += a.row_count
            i += 1
            next
          end

          skip = b.skip?
          to_process = Math.min(a.row_count, b.row_count)

          a.row_count -= to_process
          b.row_count -= to_process
          first[i] = a
          second[j] = b

          if skip
            to_skip += to_process
          else
            if to_skip != 0
              selectors << RowSelector.skip(to_skip)
              to_skip = 0
            end
            selectors << RowSelector.select(to_process)
          end
        end

        (i...first.size).each do |idx|
          value = first[idx]
          next if value.row_count == 0

          unless value.skip?
            raise general_err!("selection contains less than the number of selected rows")
          end
          to_skip += value.row_count
        end

        selectors << RowSelector.skip(to_skip) if to_skip != 0
      end

      # Combines two lists of `RowSelector`s and returns their intersection.
      # For example:
      #
      #   self:      NNYYYYNNYYNYN
      #   other:     NYNNNNNNY
      #
      #   returned:  NNNNNNNNYYNYN
      def self.intersect_row_selections(left : Array(RowSelector), right : Array(RowSelector)) : RowSelection
        l = left.dup
        r = right.dup
        selectors = [] of RowSelector
        i = 0
        j = 0

        loop do
          while i < l.size && l[i].row_count == 0
            i += 1
          end
          while j < r.size && r[j].row_count == 0
            j += 1
          end

          if i >= l.size && j >= r.size
            break
          elsif i >= l.size
            selectors << r[j]
            j += 1
          elsif j >= r.size
            selectors << l[i]
            i += 1
          else
            a = l[i]
            b = r[j]

            if !a.skip? && !b.skip?
              # Keep both ranges
              if a.row_count < b.row_count
                b.row_count -= a.row_count
                r[j] = b
                selectors << a
                i += 1
              else
                a.row_count -= b.row_count
                l[i] = a
                selectors << b
                j += 1
              end
            else
              # Skip at least one
              if a.row_count < b.row_count
                skip = a.row_count
                b.row_count -= skip
                r[j] = b
                i += 1
              else
                skip = b.row_count
                a.row_count -= skip
                l[i] = a
                j += 1
              end
              selectors << RowSelector.skip(skip)
            end
          end
        end

        # Upstream ends with `iter.collect()`, i.e. `FromIterator<RowSelector>`,
        # which normalizes the runs.
        RowSelection.new(selectors)
      end

      # Combines two lists of `RowSelector`s and returns their union.
      # For example:
      #
      #   self:      NNYYYYNNYYNYN
      #   other:     NYNNNNNNY
      #
      #   returned:  NYYYYYNNYYNYN
      def self.union_row_selections(left : Array(RowSelector), right : Array(RowSelector)) : RowSelection
        l = left.dup
        r = right.dup
        selectors = [] of RowSelector
        i = 0
        j = 0

        loop do
          while i < l.size && l[i].row_count == 0
            i += 1
          end
          while j < r.size && r[j].row_count == 0
            j += 1
          end

          if i >= l.size && j >= r.size
            break
          elsif i >= l.size
            selectors << r[j]
            j += 1
          elsif j >= r.size
            selectors << l[i]
            i += 1
          else
            a = l[i]
            b = r[j]

            if a.skip?
              if b.skip?
                # Skip both ranges
                if a.row_count < b.row_count
                  skip = a.row_count
                  b.row_count -= skip
                  r[j] = b
                  i += 1
                else
                  skip = b.row_count
                  a.row_count -= skip
                  l[i] = a
                  j += 1
                end
                selectors << RowSelector.skip(skip)
              else
                # Keep rows from `right`
                if a.row_count < b.row_count
                  l_row_count = a.row_count
                  b.row_count -= l_row_count
                  r[j] = b
                  i += 1
                  selectors << RowSelector.select(l_row_count)
                else
                  a.row_count -= b.row_count
                  l[i] = a
                  j += 1
                  selectors << b
                end
              end
            else
              if b.skip?
                # Keep rows from `left`
                if a.row_count < b.row_count
                  b.row_count -= a.row_count
                  r[j] = b
                  i += 1
                  selectors << a
                else
                  r_row_count = b.row_count
                  a.row_count -= r_row_count
                  l[i] = a
                  j += 1
                  selectors << RowSelector.select(r_row_count)
                end
              else
                # Keep at least one
                if a.row_count < b.row_count
                  b.row_count -= a.row_count
                  r[j] = b
                  i += 1
                  selectors << a
                else
                  a.row_count -= b.row_count
                  l[i] = a
                  j += 1
                  selectors << b
                end
              end
            end
          end
        end

        # Upstream ends with `iter.collect()`, i.e. `FromIterator<RowSelector>`,
        # which normalizes the runs.
        RowSelection.new(selectors)
      end

      # Applies `other` to the selected rows of `mask`, preserving the original
      # row domain.
      private def self.and_then_mask(mask : ::Arrow::BooleanBuffer, other : RowSelection) : ::Arrow::BooleanBuffer
        inner = other.inner
        if inner.is_a?(RowSelectionInner::Mask)
          and_then_masks(mask, inner.mask.mask)
        else
          and_then_mask_from_selectors(mask, inner.as(RowSelectionInner::Selectors).selectors)
        end
      end

      private def self.and_then_mask_from_selectors(mask : ::Arrow::BooleanBuffer, other : Array(RowSelector)) : ::Arrow::BooleanBuffer
        builder = ::Arrow::BooleanBufferBuilder.new(mask.len)
        other_idx = 0
        current = other_idx < other.size ? other[other_idx] : nil
        cursor = 0

        # Iterate only over the set positions in `mask`; the gaps of unset bits
        # are filled in bulk with `append_n` instead of bit-by-bit.
        mask.set_indices.each do |set_idx|
          if set_idx > cursor
            builder.append_n(set_idx - cursor, false)
          end
          cursor = set_idx + 1

          while current && current.row_count == 0
            other_idx += 1
            current = other_idx < other.size ? other[other_idx] : nil
          end
          selector = current || raise general_err!("selection contains less than the number of selected rows")
          selected = !selector.skip?
          selector.row_count -= 1
          current = selector
          builder.append(selected)
        end
        if cursor < mask.len
          builder.append_n(mask.len - cursor, false)
        end

        leftover = current ? current.row_count != 0 : false
        remaining = other_idx + 1 < other.size ? other[(other_idx + 1)..] : [] of RowSelector
        if leftover || remaining.any? { |selector| selector.row_count != 0 }
          raise general_err!("selection exceeds the number of selected rows")
        end

        builder.finish
      end

      private def self.and_then_masks(mask : ::Arrow::BooleanBuffer, other : ::Arrow::BooleanBuffer) : ::Arrow::BooleanBuffer
        selected_count = mask.count_set_bits
        case other.len <=> selected_count
        when -1
          raise general_err!("selection contains less than the number of selected rows")
        when 1
          raise general_err!("selection exceeds the number of selected rows")
        end

        other_true_count = other.count_set_bits
        return ::Arrow::BooleanBuffer.new_unset(mask.len) if other_true_count == 0
        return mask if other_true_count == selected_count

        builder = ::Arrow::BooleanBufferBuilder.new(mask.len)
        outer_set_indices = mask.set_indices.to_a
        next_selected_ordinal = 0
        cursor = 0

        other.set_indices.each do |selected_ordinal|
          skip = selected_ordinal - next_selected_ordinal
          set_idx = outer_set_indices[next_selected_ordinal + skip]? ||
                    raise general_err!("validated other length matches selected row count")
          if set_idx > cursor
            builder.append_n(set_idx - cursor, false)
          end
          builder.append(true)
          cursor = set_idx + 1
          next_selected_ordinal = selected_ordinal + 1
        end

        if cursor < mask.len
          builder.append_n(mask.len - cursor, false)
        end

        builder.finish
      end

      # Bitwise combination of two masks. The longer side's tail passes through
      # unchanged.
      private def self.combine_masks(l : ::Arrow::BooleanBuffer, r : ::Arrow::BooleanBuffer, & : Bool, Bool -> Bool) : ::Arrow::BooleanBuffer
        if l.len == r.len
          return ::Arrow::BooleanBuffer.from_bitwise_binary_op(
            l.values, l.offset, r.values, r.offset, l.len,
          ) { |a, b| yield a, b }
        end

        longer, shorter = l.len > r.len ? {l, r} : {r, l}
        builder = ::Arrow::BooleanBufferBuilder.new(longer.len)
        index = 0
        while index < shorter.len
          builder.append(yield(longer.value(index), shorter.value(index)))
          index += 1
        end
        while index < longer.len
          builder.append(longer.value(index))
          index += 1
        end
        builder.finish
      end
    end
  end
end
