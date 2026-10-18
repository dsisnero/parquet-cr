require "../errors"
require "./row_selection"

# Ported from `parquet/src/arrow/arrow_reader/read_plan.rs` and the selector
# cursor of `arrow_reader/selection/cursor.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ReadPlanBuilder`, `LimitedReadPlanBuilder`, `ReadPlan` and the
# `RowSelectionCursor` (the "all rows" and selector-queue cursors). The plan
# decides which rows a `ParquetRecordBatchReader` reads and skipped runs are
# pushed down into the array readers via `skip_records`.
#
# Divergences: only the selector backing exists, so there is no
# `RowSelectionPolicy`/mask cursor and no loaded-row-ranges (page pruning);
# row predicates (`with_predicate`/`PredicateOptions`) are not ported with
# `RowFilter`. `usize` counts are `Int32`.
module Parquet
  module Arrow
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

      # Whether no rows remain.
      abstract def empty? : Bool

      # A cursor that reads all rows (upstream `RowSelectionCursor::All`).
      class AllCursor < RowSelectionCursor
        def empty? : Bool
          false
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

      def initialize(@batch_size : Int32, @selection : RowSelection? = nil)
      end

      # Sets the current selection.
      def with_selection(selection : RowSelection?) : self
        @selection = selection
        self
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

      # Creates the final plan (upstream `ReadPlanBuilder::build`).
      def build : ReadPlan
        selection = self.selection
        if !selects_any?
          selection = RowSelection.empty
        end

        cursor =
          if selection
            RowSelectionCursor.new_selectors(selection.trim.to_a)
          else
            RowSelectionCursor.new_all
          end

        ReadPlan.new(@batch_size, cursor)
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
