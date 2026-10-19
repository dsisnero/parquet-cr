require "arrow-cr"
require "../errors"
require "./row_selection"
# `with_predicate` drives a nested `ParquetRecordBatchReader` over the current
# selection, so this file and `arrow_reader.cr` require each other (Crystal
# resolves the cycle; upstream keeps both in `arrow_reader/mod.rs`).
require "./arrow_reader"

# Ported from `parquet/src/arrow/arrow_reader/read_plan.rs` and the selector
# cursor of `arrow_reader/selection/cursor.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ReadPlanBuilder`, `LimitedReadPlanBuilder`, `ReadPlan`,
# `PredicateOptions` and the `RowSelectionCursor` (the "all rows" and
# selector-queue cursors). The plan decides which rows a
# `ParquetRecordBatchReader` reads and skipped runs are pushed down into the
# array readers via `skip_records`; `with_predicate`/`with_predicate_options`
# evaluate an `ArrowPredicate` and narrow the selection to the matching rows.
#
# Both cursor strategies are modelled: the selector queue and the bitmap cursor
# (`RowSelectionPolicy`/`RowSelectionStrategy`/`MaskCursor`/`LoadedRowRanges`,
# resolved per plan from the selection's density). Divergences: `usize` counts
# are `Int32`; `RowSelectionPolicy` is a struct carrying a `Kind` plus the `Auto`
# threshold (upstream payload enum, with `SELECTORS`/`MASK` constants and
# `default` = auto 32); `RowSelectionStrategy` lives in `row_selection.cr`
# (upstream `selection/cursor.rs`); `ReadPlanBuilder#dup` is a shallow copy (the
# selection is not mutated by `build`); `LoadedRowRanges#ranges` is a public
# getter (upstream `#[cfg(test)]`); the `MaskCursor` constructors' and `#next_chunk`'s debug
# assertions are `GeneralError`s; `BooleanArray#take_n_true` lives here because arrow-cr
# does not provide it.
module Parquet
  module Arrow
    # Policy for picking a strategy to materialize a `RowSelection` during
    # execution (upstream `selection/cursor.rs` `RowSelectionPolicy`).
    #
    # Divergence: upstream's payload enum is a struct carrying a `Kind` plus the
    # `Auto` threshold; the unit variants are the constants `SELECTORS`/`MASK`
    # and `auto(threshold)` builds the payload variant (like `Compression`).
    struct RowSelectionPolicy
      enum Kind
        Selectors
        Mask
        Auto
      end

      getter kind : Kind
      # Average selector length below which masks are preferred (`Auto` only).
      getter threshold : Int32

      def initialize(@kind : Kind, @threshold : Int32 = 0)
      end

      # Use a queue of `RowSelector` values.
      SELECTORS = new(Kind::Selectors)

      # Use a boolean mask to materialize the selection.
      MASK = new(Kind::Mask)

      # Chooses between mask and selectors based on selector density.
      def self.auto(threshold : Int32) : RowSelectionPolicy
        new(Kind::Auto, threshold)
      end

      # Upstream `Default`: auto with a threshold of 32.
      def self.default : RowSelectionPolicy
        auto(32)
      end

      def selectors? : Bool
        @kind.selectors?
      end

      def mask? : Bool
        @kind.mask?
      end

      def auto? : Bool
        @kind.auto?
      end
    end

    # Result of computing the next chunk to read when using a `MaskCursor`
    # (upstream `MaskChunk`).
    struct MaskChunk
      # Number of leading rows to skip before reaching selected rows.
      getter initial_skip : Int32
      # Total rows covered by this chunk (selected + skipped).
      getter chunk_rows : Int32
      # Rows actually selected within the chunk.
      getter selected_rows : Int32
      # Starting offset within the mask where the chunk begins.
      getter mask_start : Int32

      def initialize(@initial_skip : Int32, @chunk_rows : Int32, @selected_rows : Int32, @mask_start : Int32)
      end
    end

    # Row ranges whose backing pages are loaded for every projected column
    # (upstream `LoadedRowRanges`).
    #
    # Page pruning produces sparse column data; a `MaskCursor` then limits each
    # decoded chunk to rows whose pages are loaded for every projected leaf.
    class LoadedRowRanges
      # The loaded ranges, in row order.
      getter ranges : Array(Range(Int32, Int32))

      def initialize(@ranges : Array(Range(Int32, Int32)))
      end

      # Builds the loaded ranges from a selection's selected runs.
      def self.from_selection(selection : RowSelection) : LoadedRowRanges
        position = 0
        ranges = [] of Range(Int32, Int32)
        selection.into_selectors_vec.each do |selector|
          start = position
          position += selector.row_count
          ranges << (start...position) unless selector.skip?
        end
        new(ranges)
      end

      # The end of the loaded range containing `row`, if any.
      def end_containing(row : Int32) : Int32?
        @ranges.each do |range|
          return range.end if range.begin <= row && row < range.end
          return nil if range.begin > row
        end
        nil
      end
    end

    # Options for `ReadPlanBuilder#with_predicate_options` (upstream
    # `PredicateOptions`).
    class PredicateOptions
      # The reader producing the rows to evaluate.
      getter array_reader : ArrayReader
      # The predicate to evaluate.
      getter predicate : ArrowPredicate
      # The match-count limit, if any (upstream `limit`).
      getter limit : Int32?
      # The row count `array_reader` would yield if iterated to completion.
      getter total_rows : Int32

      # Creates options for evaluating `predicate` against the rows produced by
      # `array_reader`.
      #
      # By default there is no match-count limit; the predicate is evaluated
      # over every row the reader yields. Use `#with_limit` to enable early
      # termination.
      def initialize(@array_reader : ArrayReader, @predicate : ArrowPredicate)
        @limit = nil
        @total_rows = 0
      end

      # Stops scanning `array_reader` once `limit` matches have accumulated.
      #
      # `limit` counts predicate matches, not output rows — callers applying an
      # offset must pass `offset + limit`.
      #
      # `total_rows` is the row count `array_reader` would yield if iterated to
      # completion. It is used to pad un-evaluated trailing rows as "not
      # selected" so the returned `RowSelection` covers the full row group.
      #
      # Only valid for the *last* predicate in a filter chain: intermediate
      # predicates' match counts do not map 1:1 to output rows.
      def with_limit(limit : Int32, total_rows : Int32) : self
        @limit = limit
        @total_rows = total_rows
        self
      end
    end

    # A cursor over the rows a plan still has to read (upstream
    # `RowSelectionCursor`).
    abstract class RowSelectionCursor
      # A cursor that reads every remaining row.
      def self.new_all : RowSelectionCursor
        AllCursor.new
      end

      # A cursor over an explicit selector queue.
      def self.new_selectors(selectors : Array(RowSelector)) : RowSelectionCursor
        SelectorsCursor.new(selectors)
      end

      # Creates a `MaskCursor` from an existing set of selectors (upstream
      # `new_mask_from_selectors`).
      def self.new_mask_from_selectors(
        selectors : Array(RowSelector),
        loaded_row_ranges : LoadedRowRanges? = nil,
      ) : RowSelectionCursor
        if (last = selectors.last?) && last.skip?
          raise general_err!("Mask selectors must not end with a skip")
        end

        MaskCursor.new(SelectionMasks.from_selectors(selectors), loaded_row_ranges)
      end

      # Creates a `MaskCursor` backed by an existing bitmask (upstream
      # `new_mask_from_buffer`).
      def self.new_mask_from_buffer(
        mask : ::Arrow::BooleanBuffer,
        loaded_row_ranges : LoadedRowRanges? = nil,
      ) : RowSelectionCursor
        if mask.len > 0 && !mask.value(mask.len - 1)
          raise general_err!("Mask selections must not end with a skip")
        end

        MaskCursor.new(mask, loaded_row_ranges)
      end

      # Whether no rows remain.
      abstract def empty? : Bool

      # A cursor that reads all rows (upstream `RowSelectionCursor::All`).
      class AllCursor < RowSelectionCursor
        def empty? : Bool
          false
        end
      end

      # A bitmap cursor, best for dense selections (upstream `MaskCursor`).
      #
      # When `loaded_row_ranges` is set (page pruning), each decoded chunk is
      # limited to rows whose pages are loaded for every projected leaf, and
      # every returned chunk ends at a selected row.
      class MaskCursor < RowSelectionCursor
        getter mask : ::Arrow::BooleanBuffer
        # Current absolute offset into the selection.
        getter position : Int32
        getter loaded_row_ranges : LoadedRowRanges?

        def initialize(@mask : ::Arrow::BooleanBuffer, @loaded_row_ranges : LoadedRowRanges? = nil)
          @position = 0
        end

        def empty? : Bool
          @position >= @mask.len
        end

        # Advances through the mask, producing the next chunk summary (upstream
        # `next_mask_chunk`).
        def next_mask_chunk(batch_size : Int32) : MaskChunk?
          return nil if empty?

          next_mask_chunk_non_empty(batch_size)
        end

        # Returns the next non-empty mask chunk without crossing an unloaded row
        # range (upstream `next_chunk`).
        #
        # The read plan removes trailing skips before constructing the cursor,
        # so this is only called for a non-empty mask with a selected row left.
        def next_chunk(batch_size : Int32) : MaskChunk
          raise general_err!("batch size must be positive") if batch_size <= 0

          unless loaded_row_ranges
            return next_mask_chunk_non_empty(batch_size)
          end

          start_position = @position
          cursor = start_position
          while cursor < @mask.len && !@mask.value(cursor)
            cursor += 1
          end

          if cursor == @mask.len
            raise general_err!(
              "Internal Error: Mask cursor reached the end without finding a selected row; " \
              "ReadPlan must remove trailing skips"
            )
          end

          loaded_range_end = loaded_row_ranges.try(&.end_containing(cursor))
          unless loaded_range_end
            raise general_err!("Internal Error: selected row #{cursor} has no loaded page range")
          end

          mask_start = cursor
          selected_rows = 0
          chunk_end = cursor
          while cursor < loaded_range_end && cursor < @mask.len && selected_rows < batch_size
            if @mask.value(cursor)
              selected_rows += 1
              chunk_end = cursor + 1
            end
            cursor += 1
          end

          @position = chunk_end
          MaskChunk.new(
            initial_skip: mask_start - start_position,
            chunk_rows: chunk_end - mask_start,
            selected_rows: selected_rows,
            mask_start: mask_start,
          )
        end

        # Materializes the boolean values for a mask-backed chunk (upstream
        # `mask_values_for`).
        def mask_values_for(chunk : MaskChunk) : ::Arrow::BooleanArray
          if chunk.mask_start + chunk.chunk_rows > @mask.len
            raise general_err!("Internal Error: MaskChunk exceeds mask length")
          end

          ::Arrow::BooleanArray.new(@mask.slice(chunk.mask_start, chunk.chunk_rows))
        end

        private def next_mask_chunk_non_empty(batch_size : Int32) : MaskChunk
          start_position = @position
          cursor = start_position
          initial_skip = 0

          while cursor < @mask.len && !@mask.value(cursor)
            initial_skip += 1
            cursor += 1
          end
          if cursor == @mask.len
            raise general_err!("ReadPlan must remove trailing skips from Mask selections")
          end

          mask_start = cursor
          chunk_rows = 0
          selected_rows = 0

          # Advance until enough rows have been selected to satisfy the batch
          # size, or until the mask is exhausted. This mirrors the legacy
          # selector-queue iteration.
          while cursor < @mask.len && selected_rows < batch_size
            chunk_rows += 1
            selected_rows += 1 if @mask.value(cursor)
            cursor += 1
          end

          @position = cursor
          MaskChunk.new(
            initial_skip: initial_skip,
            chunk_rows: chunk_rows,
            selected_rows: selected_rows,
            mask_start: mask_start,
          )
        end
      end

      # A selector-queue cursor, best for sparse selections (upstream
      # `SelectorsCursor`).
      class SelectorsCursor < RowSelectionCursor
        @selectors : Array(RowSelector)
        # The current absolute offset into the selection (upstream `position`).
        getter position : Int32

        def initialize(selectors : Array(RowSelector))
          @selectors = selectors.dup
          @position = 0
        end

        def empty? : Bool
          @selectors.empty?
        end

        # Returns the next selector (it must not be empty).
        def next_selector : RowSelector
          selector = @selectors.shift
          @position += selector.row_count
          selector
        end

        # Returns a selector to the front, rewinding the position.
        def return_selector(selector : RowSelector) : Nil
          @position -= selector.row_count
          @position = 0 if @position < 0
          @selectors.unshift(selector)
          nil
        end
      end
    end

    # A plan reading specific rows of a row group (upstream `ReadPlan`).
    class ReadPlan
      # The number of rows to read in each output batch.
      getter batch_size : Int32
      # The row selection cursor.
      getter cursor : RowSelectionCursor

      def initialize(@batch_size : Int32, @cursor : RowSelectionCursor)
      end
    end

    # A builder for `ReadPlan` (upstream `ReadPlanBuilder`).
    class ReadPlanBuilder
      getter batch_size : Int32
      getter selection : RowSelection?
      getter row_selection_policy : RowSelectionPolicy

      def initialize(
        @batch_size : Int32,
        @selection : RowSelection? = nil,
        @row_selection_policy : RowSelectionPolicy = RowSelectionPolicy.default,
        @loaded_row_ranges : LoadedRowRanges? = nil,
      )
      end

      # Sets the current selection.
      def with_selection(selection : RowSelection?) : self
        @selection = selection
        self
      end

      # Configures the policy used when materializing the selection (upstream
      # `with_row_selection_policy`).
      def with_row_selection_policy(policy : RowSelectionPolicy) : self
        @row_selection_policy = policy
        self
      end

      # Sets the row ranges whose pages are loaded for every projected column
      # (upstream `with_loaded_row_ranges`).
      def with_loaded_row_ranges(ranges : LoadedRowRanges?) : self
        @loaded_row_ranges = ranges
        self
      end

      # Resolves the cursor strategy for this plan (upstream
      # `resolve_selection_strategy`).
      #
      # Guarantees to return either `Selectors` or `Mask`, never Auto.
      def resolve_selection_strategy : RowSelectionStrategy
        policy = @row_selection_policy
        if policy.selectors?
          RowSelectionStrategy::Selectors
        elsif policy.mask?
          RowSelectionStrategy::Mask
        else
          selection = @selection
          return RowSelectionStrategy::Selectors unless selection

          selection.auto_selection_strategy(policy.threshold)
        end
      end

      # Whether the plan selects any rows (true when there is no selection).
      def selects_any? : Bool
        selection = @selection
        selection ? selection.selects_any? : true
      end

      # The number of selected rows, or `nil` when every row is selected.
      def num_rows_selected : Int32?
        @selection.try(&.row_count)
      end

      # Returns a builder that can apply an offset and a limit over
      # `row_count` rows (upstream `ReadPlanBuilder::limited`).
      def limited(row_count : Int32) : LimitedReadPlanBuilder
        LimitedReadPlanBuilder.new(self, row_count)
      end

      # A shallow copy (upstream `#[derive(Clone)]`); `build` does not mutate
      # the selection, so the copy can share it.
      def dup : ReadPlanBuilder
        ReadPlanBuilder.new(@batch_size, @selection, @row_selection_policy, @loaded_row_ranges)
      end

      # Evaluates an `ArrowPredicate`, updating this plan's selection
      # (upstream `ReadPlanBuilder::with_predicate`).
      #
      # If the current selection is non-nil, the resulting `RowSelection` will
      # be the conjunction of the existing selection and the rows selected by
      # `predicate`.
      #
      # Note: pre-existing selections may come from evaluating a previous
      # predicate or from an explicit `RowSelection` the reader was given in
      # addition to one or more predicates.
      def with_predicate(array_reader : ArrayReader, predicate : ArrowPredicate) : self
        with_predicate_options(PredicateOptions.new(array_reader, predicate))
      end

      # Evaluates an `ArrowPredicate` with the given options, updating this
      # plan's selection (upstream `ReadPlanBuilder::with_predicate_options`).
      #
      # Like `#with_predicate`, but allows additional options such as a
      # match-count limit for early termination (see
      # `PredicateOptions#with_limit`).
      def with_predicate_options(options : PredicateOptions) : self
        array_reader = options.array_reader
        predicate = options.predicate
        limit = options.limit
        total_rows = options.total_rows

        # Target length for the concatenated filter output:
        # - prior selection ⇒ the reader yields that many rows; `and_then`
        #   below requires the filter output to match;
        # - no prior selection ⇒ the reader yields `total_rows`. We only need to
        #   pad when `limit` may short-circuit the loop; otherwise iteration
        #   naturally exhausts.
        expected_rows = if selection = @selection
                          selection.row_count
                        else
                          limit ? total_rows : nil
                        end

        reader = ParquetRecordBatchReader.new(array_reader, dup.build)
        filters = [] of ::Arrow::BooleanArray
        processed_rows = 0
        matched_rows = 0

        reader.each do |batch|
          input_rows = batch.num_rows
          filter = predicate.evaluate(batch)
          # Since this is a user-supplied predicate, check here to catch bugs
          # quickly.
          if filter.len != input_rows
            raise arrow_err!("ArrowPredicate predicate returned #{filter.len} rows, expected #{input_rows}")
          end
          # `RowSelection.from_filters` expects non-null filters: convert NULL
          # predicate results to `false` so they are not selected.
          filter = ::Arrow.prep_null_mask_filter(filter) if filter.null_count > 0

          processed_rows += input_rows

          if (lim = limit) && lim - matched_rows <= filter.len
            truncated = take_n_true(filter, lim - matched_rows)
            matched_rows += truncated.true_count
            filters << truncated
            break if matched_rows >= lim
          else
            matched_rows += filter.true_count
            filters << filter
          end
        end

        # Pad the tail so the filters cover `expected_rows` total. This keeps
        # the invariant that the resulting `RowSelection` spans every row the
        # reader would have produced — rows past the early break are marked
        # "not selected". When no limit is set the loop always exhausts and no
        # padding is needed.
        if (expected = expected_rows) && processed_rows < expected
          filters << ::Arrow::BooleanArray.new(
            ::Arrow::BooleanBuffer.new_unset(expected - processed_rows), nil
          )
        end

        # If the predicate selected all rows, applying it is a no-op. With no
        # prior selection this keeps the selection empty (nil), enabling
        # coalesced page fetches; with a prior selection it avoids rebuilding
        # the same selection.
        all_selected = filters.all? { |filter| filter.true_count == filter.len }
        return self if all_selected

        raw = if (selection = @selection) && selection.as_mask
                RowSelection.from_boolean_buffer(filters_to_boolean_buffer(filters))
              else
                RowSelection.from_filters(filters)
              end
        @selection = if selection = @selection
                       selection.and_then(raw)
                     else
                       raw
                     end
        self
      end

      # Creates the final plan (upstream `ReadPlanBuilder::build`).
      def build : ReadPlan
        selection = self.selection
        if !selects_any?
          selection = RowSelection.empty
        end

        strategy = resolve_selection_strategy
        cursor =
          if selection
            build_cursor(selection.trim, strategy, @loaded_row_ranges)
          else
            RowSelectionCursor.new_all
          end

        ReadPlan.new(@batch_size, cursor)
      end

      # Lowers a selection to the cursor form requested by the resolved
      # strategy (upstream `build_cursor`).
      private def build_cursor(
        selection : RowSelection,
        strategy : RowSelectionStrategy,
        loaded_row_ranges : LoadedRowRanges?,
      ) : RowSelectionCursor
        inner = selection.inner
        case strategy
        when RowSelectionStrategy::Mask
          if inner.is_a?(RowSelectionInner::Mask)
            RowSelectionCursor.new_mask_from_buffer(inner.mask.mask, loaded_row_ranges)
          else
            RowSelectionCursor.new_mask_from_selectors(
              inner.as(RowSelectionInner::Selectors).selectors, loaded_row_ranges
            )
          end
        else
          if inner.is_a?(RowSelectionInner::Mask)
            RowSelectionCursor.new_selectors(inner.mask.into_selectors)
          else
            RowSelectionCursor.new_selectors(inner.as(RowSelectionInner::Selectors).selectors)
          end
        end
      end

      # Concatenates the filter bitmaps (upstream `filters_to_boolean_buffer`).
      private def filters_to_boolean_buffer(filters : Array(::Arrow::BooleanArray)) : ::Arrow::BooleanBuffer
        builder = ::Arrow::BooleanBufferBuilder.new(filters.sum(0, &.len))
        filters.each do |filter|
          raise general_err!("filter contains nulls") unless filter.null_count == 0

          builder.append_buffer(filter.values)
        end
        builder.finish
      end

      # Keeps only the first `n` non-null `true` positions, clearing the rest;
      # the null buffer is preserved (upstream `BooleanArray::take_n_true`,
      # which arrow-cr does not provide).
      private def take_n_true(filter : ::Arrow::BooleanArray, n : Int32) : ::Arrow::BooleanArray
        nulls = filter.nulls
        end_bit = nil.as(Int32?)
        count = 0
        filter.values.set_indices.each do |index|
          next if nulls && !nulls.valid?(index)

          if count == n
            end_bit = index
            break
          end
          count += 1
        end
        return filter unless end_bit

        builder = ::Arrow::BooleanBufferBuilder.new(filter.len)
        index = 0
        while index < end_bit
          builder.append(filter.value(index))
          index += 1
        end
        builder.append_n(filter.len - end_bit, false)
        ::Arrow::BooleanArray.new(builder.finish, nulls)
      end
    end

    # Applies an offset and a limit to a `ReadPlanBuilder` (upstream
    # `LimitedReadPlanBuilder`).
    class LimitedReadPlanBuilder
      def initialize(@inner : ReadPlanBuilder, @row_count : Int32, @offset : Int32? = nil,
                     @limit : Int32? = nil)
      end

      # Sets the offset to apply.
      def with_offset(offset : Int32?) : self
        @offset = offset
        self
      end

      # Sets the limit to apply.
      def with_limit(limit : Int32?) : self
        @limit = limit
        self
      end

      # Applies the offset and limit, returning the updated builder.
      def build_limited : ReadPlanBuilder
        inner = @inner

        # If the selection is empty, truncate.
        inner = inner.with_selection(RowSelection.empty) unless inner.selects_any?

        if offset = @offset
          remaining = @row_count - offset
          current = inner.selection
          updated =
            if remaining < 0
              RowSelection.empty
            elsif selection = current
              selection.offset(offset)
            else
              RowSelection.new([
                RowSelector.skip(offset),
                RowSelector.select(remaining),
              ])
            end
          inner = inner.with_selection(updated)
        end

        if limit = @limit
          current = inner.selection
          updated =
            if selection = current
              selection.limit(limit)
            else
              RowSelection.new([RowSelector.select(Math.min(limit, @row_count))])
            end
          inner = inner.with_selection(updated)
        end

        inner
      end
    end
  end
end
