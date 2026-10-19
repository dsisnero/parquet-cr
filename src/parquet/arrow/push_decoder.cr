require "../decode_result"
require "../util/push_buffers"
require "./array_reader"
require "./array_reader/builder"
require "./arrow_reader"
require "./in_memory_row_group"
require "./metrics"
require "./mod"
require "./read_plan"

# Ported from `parquet/src/arrow/push_decoder/{mod,remaining,reader_builder/*}.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# A push-based decoder: instead of reading from a file, the caller pushes the
# byte ranges the decoder asks for ([`DecodeResult::NeedsData`]) and decodes the
# batches the decoder produces ([`DecodeResult::Data`]). The decoder tracks which
# row groups and rows remain, applies offset/limit budgets across row groups, and
# reads each row group from an `InMemoryRowGroup` built from the pushed bytes.
#
# The structures mirror upstream: `RowGroupFrontier` owns the cross-row-group
# state (queued row groups, selections and the offset/limit budget) and hands
# one `NextRowGroup` at a time to `RowGroupReaderBuilder`, which drives the
# per-row-group decode state machine (`Start` → `StartData` → `WaitingOnData` →
# `Finished`) and asks for the byte ranges it needs through `DataRequest`.
# `RemainingRowGroups` glues the two together; `ParquetPushDecoder` adds the
# reader/state machine on top and exposes the adaptive-scan API
# (`try_next_reader`, `peek_next_row_group`, `into_builder`).
#
# Deferred in this slice (see `plans/parity.md`): row filters in the push path
# (`RowFilter`/predicate cache) and the generic `ArrowReaderBuilder<I>` refactor
# upstream shares with the sync/async builders (this builder carries the same
# options directly).
module Parquet
  module Arrow
    # Running offset/limit budget shared across row groups (upstream
    # `RowBudget`).
    struct RowBudget
      # The offset still to be skipped before the next readable row group.
      getter offset : Int32?
      # The number of output rows still permitted.
      getter limit : Int32?

      def initialize(@offset : Int32? = nil, @limit : Int32? = nil)
      end

      # Whether the output limit has been reached.
      def exhausted? : Bool
        @limit == 0
      end

      # How many selected rows remain after applying this budget.
      def rows_after(rows_before : Int32) : Int32
        rows_after_offset = rows_before - (@offset || 0)
        rows_after_offset = 0 if rows_after_offset < 0
        if limit = @limit
          Math.min(rows_after_offset, limit)
        else
          rows_after_offset
        end
      end

      # The number of selected rows needed before applying the offset
      # (upstream `selected_row_limit`).
      def selected_row_limit : Int32?
        if limit = @limit
          limit + (@offset || 0)
        end
      end

      # Advances the budget past one row group.
      #
      # `rows_before` is the number of rows selected before applying the budget
      # and `rows_after` the number retained for output from this row group.
      def advance(rows_before : Int32, rows_after : Int32) : RowBudget
        offset = @offset
        limit = @limit
        if current = offset
          reduction = rows_before - rows_after
          offset = current > reduction ? current - reduction : 0
        end
        if rows_after != 0 && (current = limit)
          limit = current - rows_after
        end
        RowBudget.new(offset, limit)
      end

      # Applies this budget to `plan_builder` (upstream `apply_to_plan`).
      def apply_to_plan(plan_builder : ReadPlanBuilder, row_count : Int32) : BudgetedReadPlan
        rows_before = plan_builder.num_rows_selected || row_count
        limited = plan_builder.limited(row_count)
        limited = limited.with_offset(@offset) if @offset
        limited = limited.with_limit(@limit) if @limit
        plan_builder = limited.build_limited
        rows_after = rows_after(rows_before)
        BudgetedReadPlan.new(
          plan_builder,
          rows_before,
          rows_after,
          advance(rows_before, rows_after),
        )
      end
    end

    # The result of applying a `RowBudget` to a row group's read plan (upstream
    # `BudgetedReadPlan`).
    struct BudgetedReadPlan
      # Read plan after applying this row group's share of the offset/limit
      # budget.
      getter plan_builder : ReadPlanBuilder
      # Rows selected by the row selection (and predicates) before applying this
      # row group's offset/limit budget.
      getter rows_before_budget : Int32
      # Rows that remain to be read after applying this row group's budget.
      getter rows_after_budget : Int32
      # Budget remaining for later row groups.
      getter remaining_budget : RowBudget

      def initialize(
        @plan_builder : ReadPlanBuilder,
        @rows_before_budget : Int32,
        @rows_after_budget : Int32,
        @remaining_budget : RowBudget,
      )
      end
    end

    # The row group currently being planned, its read plan and its budget
    # (upstream `RowGroupInfo`).
    class RowGroupInfo
      getter row_group_idx : Int32
      getter row_count : Int32
      property plan_builder : ReadPlanBuilder
      getter budget : RowBudget

      def initialize(
        @row_group_idx : Int32,
        @row_count : Int32,
        @plan_builder : ReadPlanBuilder,
        @budget : RowBudget,
      )
      end
    end

    # Work item handed from `RowGroupFrontier` to `RowGroupReaderBuilder`
    # (upstream `NextRowGroup`).
    class NextRowGroup
      getter row_group_idx : Int32
      getter row_count : Int32
      # This row group's selection, or nil when all rows are selected.
      getter selection : RowSelection?
      # Budget snapshot to apply while decoding this row group.
      getter budget : RowBudget

      def initialize(
        @row_group_idx : Int32,
        @row_count : Int32,
        @selection : RowSelection?,
        @budget : RowBudget,
      )
      end
    end

    # Tracks the byte ranges needed to build an `InMemoryRowGroup`, together
    # with any column chunks already fetched by earlier stages (upstream
    # `DataRequest`).
    class DataRequest
      # Any previously read column chunk data.
      getter column_chunks : Array(ColumnChunkData?)
      # The ranges of data that are needed next.
      getter ranges : Array(Range(UInt64, UInt64))
      # Optional page start offsets for each requested range; used to build the
      # relevant `InMemoryRowGroup`.
      getter page_start_offsets : Array(Array(UInt64))?

      def initialize(
        @column_chunks : Array(ColumnChunkData?),
        @ranges : Array(Range(UInt64, UInt64)),
        @page_start_offsets : Array(Array(UInt64))?,
      )
      end

      # The ranges still needed to satisfy this request (empty when all ranges
      # are buffered).
      def needed_ranges(buffers : Util::PushBuffers) : Array(Range(UInt64, UInt64))
        @ranges.reject { |range| buffers.has_range?(range) }
      end

      # Creates a new `InMemoryRowGroup` filled with the buffered data, and
      # clears the explicitly requested ranges (upstream
      # `try_into_in_memory_row_group`).
      #
      # Assumes that all needed data is present in the buffers.
      def try_into_in_memory_row_group(
        row_group_idx : Int32,
        row_count : Int32,
        metadata : File::Metadata::ParquetMetaData,
        projection : ProjectionMask,
        buffers : Util::PushBuffers,
      ) : InMemoryRowGroup
        chunks = @ranges.map do |range|
          length = (range.end - range.begin).to_i32
          begin
            buffers.get_bytes(range.begin, length)
          rescue error : NeedMoreDataRangeError
            raise general_err!(
              "Internal Error missing data for range #{range} in buffers: #{error.message}"
            )
          end
        end

        page_index = DataRequestBuilder.page_index_for(metadata, row_group_idx)
        row_group = InMemoryRowGroup.new(
          row_count,
          @column_chunks,
          page_index,
          row_group_idx,
          metadata,
        )
        row_group.fill_column_chunks(projection, @page_start_offsets, chunks)
        buffers.clear_ranges(@ranges)
        row_group
      end
    end

    # Builder for `DataRequest` (upstream `DataRequestBuilder`).
    class DataRequestBuilder
      def initialize(
        @row_group_idx : Int32,
        @row_count : Int32,
        @batch_size : Int32,
        @metadata : File::Metadata::ParquetMetaData,
        @projection : ProjectionMask,
      )
        @selection = nil.as(RowSelection?)
        @column_chunks = nil.as(Array(ColumnChunkData?)?)
      end

      # Sets an optional row selection to apply.
      def with_selection(selection : RowSelection?) : self
        @selection = selection
        self
      end

      # Provides any previously read column chunks.
      def with_column_chunks(column_chunks : Array(ColumnChunkData?)?) : self
        @column_chunks = column_chunks
        self
      end

      # The row group's page index, when offset indexes are available (upstream
      # inlines this check in `build`/`try_into_in_memory_row_group`).
      def self.page_index_for(
        metadata : File::Metadata::ParquetMetaData,
        row_group_idx : Int32,
      ) : File::Metadata::RowGroupPageIndex?
        page_index = metadata.page_index
        if page_index && page_index.offset_indexes?
          metadata.page_index_for_row_group(row_group_idx)
        end
      end

      # Builds the request (upstream `build`).
      def build : DataRequest
        row_group_metadata = @metadata.row_group(@row_group_idx)
        column_chunks = @column_chunks ||
                        Array(ColumnChunkData?).new(row_group_metadata.columns.size, nil)

        row_group = InMemoryRowGroup.new(
          @row_count,
          column_chunks,
          DataRequestBuilder.page_index_for(@metadata, @row_group_idx),
          @row_group_idx,
          @metadata,
        )
        fetch = row_group.fetch_ranges(@projection, @selection, @batch_size)

        DataRequest.new(row_group.column_chunks, fetch.ranges, fetch.page_start_offsets)
      end
    end

    # The per-row-group decoder state (upstream `RowGroupDecoderState`).
    abstract class RowGroupDecoderState
      # Start: planning the row group, before any data is requested.
      class Start < RowGroupDecoderState
        getter row_group_info : RowGroupInfo

        def initialize(@row_group_info : RowGroupInfo)
        end
      end

      # Know what data to actually read.
      class StartData < RowGroupDecoderState
        getter row_group_info : RowGroupInfo
        # Any previously read column chunk data.
        getter column_chunks : Array(ColumnChunkData?)?

        def initialize(@row_group_info : RowGroupInfo, @column_chunks : Array(ColumnChunkData?)?)
        end
      end

      # Needs data to proceed with reading the output.
      class WaitingOnData < RowGroupDecoderState
        getter row_group_info : RowGroupInfo
        getter data_request : DataRequest

        def initialize(@row_group_info : RowGroupInfo, @data_request : DataRequest)
        end
      end

      # Finished (or not yet started) reading this group.
      class Finished < RowGroupDecoderState
      end
    end

    # The result of one `RowGroupReaderBuilder#try_build` step (upstream
    # `RowGroupBuildResult`).
    #
    # Divergence: the upstream enum is a sealed class hierarchy here.
    abstract class RowGroupBuildResult
      # The active row group is complete without producing a reader.
      class Finished < RowGroupBuildResult
        # Budget remaining after applying this row group's selection.
        getter remaining_budget : RowBudget

        def initialize(@remaining_budget : RowBudget)
        end
      end

      # More bytes are needed before the active row group can make progress.
      class NeedsData < RowGroupBuildResult
        getter ranges : Array(Range(UInt64, UInt64))

        def initialize(@ranges : Array(Range(UInt64, UInt64)))
        end
      end

      # The active row group produced a reader.
      class Data < RowGroupBuildResult
        getter batch_reader : ParquetRecordBatchReader
        # Budget remaining after applying this row group's selection.
        getter remaining_budget : RowBudget

        def initialize(@batch_reader : ParquetRecordBatchReader, @remaining_budget : RowBudget)
        end
      end
    end

    # One `try_transition` result: the next state and, when processing should
    # stop, the result to return (upstream `NextState`).
    class RowGroupNextState
      getter next_state : RowGroupDecoderState
      getter result : RowGroupBuildResult?

      def initialize(@next_state : RowGroupDecoderState, @result : RowGroupBuildResult? = nil)
      end
    end

    # The builder-configurable parts of a `RowGroupReaderBuilder`, recovered by
    # `#into_parts` (upstream `RowGroupReaderBuilderParts`).
    class RowGroupReaderBuilderParts
      getter batch_size : Int32
      getter projection : ProjectionMask
      getter fields : ParquetField?
      getter metrics : ArrowReaderMetrics
      getter row_selection_policy : RowSelectionPolicy
      # Bytes already pushed into the decoder, carried across a rebuild so they
      # are not re-requested.
      getter buffers : Util::PushBuffers

      def initialize(
        @batch_size : Int32,
        @projection : ProjectionMask,
        @fields : ParquetField?,
        @metrics : ArrowReaderMetrics,
        @row_selection_policy : RowSelectionPolicy,
        @buffers : Util::PushBuffers,
      )
      end
    end

    # Builder for `ParquetRecordBatchReader` for a single row group (upstream
    # `RowGroupReaderBuilder`).
    #
    # This drives the main state machine for decoding each row group: it
    # determines what data is needed, and assembles the batch reader when all
    # data is available.
    class RowGroupReaderBuilder
      @state : RowGroupDecoderState?

      def initialize(
        @batch_size : Int32,
        @projection : ProjectionMask,
        @metadata : File::Metadata::ParquetMetaData,
        @fields : ParquetField?,
        @metrics : ArrowReaderMetrics,
        @row_selection_policy : RowSelectionPolicy,
        @buffers : Util::PushBuffers,
      )
        @state = RowGroupDecoderState::Finished.new
      end

      # Pushes new data buffers that can be used to satisfy pending requests.
      def push_data(ranges : Array(Range(UInt64, UInt64)), buffers : Array(Bytes)) : Nil
        @buffers.push_ranges(ranges, buffers)
      end

      # The total number of bytes buffered so far.
      def buffered_bytes : UInt64
        @buffers.buffered_bytes
      end

      # Clears any staged ranges currently buffered for future decode work.
      def clear_all_ranges : Nil
        @buffers.clear_all_ranges
      end

      # Whether the inner state is `Finished` — the only state in which it is
      # safe to decompose the builder via `#into_parts`.
      def finished? : Bool
        @state.is_a?(RowGroupDecoderState::Finished)
      end

      # Decomposes the builder into its configurable parts (upstream
      # `into_parts`). The runtime decode state is discarded.
      def into_parts : RowGroupReaderBuilderParts
        RowGroupReaderBuilderParts.new(
          @batch_size,
          @projection,
          @fields,
          @metrics,
          @row_selection_policy,
          @buffers,
        )
      end

      # Whether this builder is currently decoding a row group.
      def has_active_row_group? : Bool
        !@state.is_a?(RowGroupDecoderState::Finished)
      end

      # Sets up this reader to read the next row group (upstream
      # `next_row_group`).
      def next_row_group(
        row_group_idx : Int32,
        row_count : Int32,
        selection : RowSelection?,
        budget : RowBudget,
      ) : Nil
        state = take_state
        unless state.is_a?(RowGroupDecoderState::Finished)
          @state = state
          raise general_err!(
            "Internal Error: next_row_group called while still reading a row group. " \
            "Expected Finished state"
          )
        end

        plan_builder = ReadPlanBuilder.new(@batch_size)
          .with_selection(selection)
          .with_row_selection_policy(@row_selection_policy)
        @state = RowGroupDecoderState::Start.new(
          RowGroupInfo.new(row_group_idx, row_count, plan_builder, budget)
        )
        nil
      end

      # Tries to build the next `ParquetRecordBatchReader` for the active row
      # group (upstream `try_build`).
      def try_build : RowGroupBuildResult
        loop do
          current_state = take_state
          next_state = try_transition(current_state)
          @state = next_state.next_state
          if result = next_state.result
            return result
          end
        end
      end

      private def take_state : RowGroupDecoderState
        state = @state
        unless state
          raise general_err!("Internal Error: RowGroupReader in invalid state")
        end
        @state = nil
        state
      end

      # Current state -> next state + optional output (upstream
      # `try_transition`).
      private def try_transition(current_state : RowGroupDecoderState) : RowGroupNextState
        case current_state
        when RowGroupDecoderState::Start
          info = current_state.row_group_info
          RowGroupNextState.new(RowGroupDecoderState::StartData.new(info, nil))
        when RowGroupDecoderState::StartData
          info = current_state.row_group_info
          budgeted = info.budget.apply_to_plan(info.plan_builder, info.row_count)

          if budgeted.rows_before_budget == 0
            # Ruled out the entire row group.
            return RowGroupNextState.new(
              RowGroupDecoderState::Finished.new,
              RowGroupBuildResult::Finished.new(budgeted.remaining_budget),
            )
          end

          if budgeted.rows_after_budget == 0
            # No rows left after applying the limit/offset budget.
            return RowGroupNextState.new(
              RowGroupDecoderState::Finished.new,
              RowGroupBuildResult::Finished.new(budgeted.remaining_budget),
            )
          end

          info.plan_builder = budgeted.plan_builder
            .with_row_selection_policy(@row_selection_policy)

          data_request = DataRequestBuilder.new(
            info.row_group_idx,
            info.row_count,
            @batch_size,
            @metadata,
            @projection,
          )
            .with_selection(info.plan_builder.selection)
            .with_column_chunks(current_state.column_chunks)
            .build

          # The budget consumed by this row group must not be re-applied when the
          # data arrives (upstream rebuilds `RowGroupInfo` with
          # `remaining_budget` before handing it to `WaitingOnData`).
          next_info = RowGroupInfo.new(
            info.row_group_idx,
            info.row_count,
            info.plan_builder,
            budgeted.remaining_budget,
          )

          RowGroupNextState.new(RowGroupDecoderState::WaitingOnData.new(next_info, data_request))
        when RowGroupDecoderState::WaitingOnData
          info = current_state.row_group_info
          needed_ranges = current_state.data_request.needed_ranges(@buffers)
          unless needed_ranges.empty?
            # Still need data.
            return RowGroupNextState.new(
              current_state,
              RowGroupBuildResult::NeedsData.new(needed_ranges),
            )
          end

          row_group = current_state.data_request.try_into_in_memory_row_group(
            info.row_group_idx, info.row_count, @metadata, @projection, @buffers
          )
          plan = info.plan_builder.build
          array_reader = ArrayReaderBuilder.new(row_group, @batch_size, @metrics)
            .build_array_reader(@fields, @projection)
          reader = ParquetRecordBatchReader.new(array_reader, plan)
          RowGroupNextState.new(
            RowGroupDecoderState::Finished.new,
            RowGroupBuildResult::Data.new(reader, info.budget),
          )
        else
          raise general_err!("Internal Error: try_build called without an active row group")
        end
      end
    end

    # Row groups and selections that have not yet been handed to the row-group
    # reader (upstream `QueuedRowGroups`).
    #
    # Divergence: upstream's `Global(vec)`/`PerRowGroup(vec)` enum is a
    # `Kind`-tagged class.
    class QueuedRowGroups
      enum Kind
        # One selection cursor spans all queued row groups.
        Global
        # Selections are already relative to their respective row groups.
        PerRowGroup
      end

      getter kind : Kind
      @row_groups : Array(Int32)
      @selection : RowSelection?
      @selections : Array(RowGroupSelection)

      def initialize(
        @kind : Kind,
        @row_groups : Array(Int32) = [] of Int32,
        @selection : RowSelection? = nil,
        @selections : Array(RowGroupSelection) = [] of RowGroupSelection,
      )
      end

      # Validates and queues a row-group plan (upstream `QueuedRowGroups::try_new`).
      def self.try_new(
        metadata : File::Metadata::ParquetMetaData,
        plan : RowGroupPlan,
      ) : QueuedRowGroups
        if plan.per_row_group?
          plan.selections.each do |row_group|
            row_count = metadata.row_group_num_rows(row_group.row_group_index)
            if selection = row_group.selection
              selection_rows = selection.total_row_count
              if selection_rows > row_count
                raise general_err!(
                  "Row selection for row group #{row_group.row_group_index} contains " \
                  "#{selection_rows} rows, but the row group has #{row_count}"
                )
              end
            end
          end
          new(Kind::PerRowGroup, selections: plan.selections.dup)
        elsif plan.conflicting?
          raise RowGroupPlan.conflict_error
        else
          row_groups = plan.row_groups || (0...metadata.num_row_groups).to_a
          new(
            Kind::Global,
            row_groups: row_groups.dup,
            selection: plan.selection.try(&.dup),
          )
        end
      end

      # Converts the remaining queue back into a builder configuration
      # (upstream `into_plan`).
      def into_plan : RowGroupPlan
        if @kind.global?
          RowGroupPlan.global(@row_groups.dup, @selection)
        else
          RowGroupPlan.per_row_group(@selections.dup)
        end
      end

      # A copy used for non-mutating peeking (upstream clones the frontier and
      # with it the queued row groups).
      def dup : QueuedRowGroups
        QueuedRowGroups.new(
          @kind,
          @row_groups.dup,
          @selection.try(&.dup),
          @selections.dup,
        )
      end

      # The index of the next queued row group, if any.
      def front : Int32?
        if @kind.global?
          @row_groups.first?
        else
          @selections.first?.try(&.row_group_index)
        end
      end

      # Number of queued row groups.
      def size : Int32
        @kind.global? ? @row_groups.size : @selections.size
      end

      def empty? : Bool
        size == 0
      end

      def clear : Nil
        if @kind.global?
          @row_groups.clear
          @selection = nil
        else
          @selections.clear
        end
        nil
      end

      # Whether a shared global selection has no selected rows left.
      def global_selection_exhausted? : Bool
        return false unless @kind.global?

        selection = @selection
        !selection.nil? && selection.row_count == 0
      end

      # Removes the front row group and returns its local selection.
      def pop_front_selection(row_count : Int32) : RowSelection?
        if @kind.global?
          @row_groups.shift
          @selection.try(&.split_off(row_count))
        else
          row_group = @selections.shift
          row_group.selection
        end
      end
    end

    # Cross-row-group scan state: queued work plus the offset/limit budget
    # (upstream `RowGroupFrontier`).
    class RowGroupFrontier
      getter metadata : File::Metadata::ParquetMetaData
      getter queued : QueuedRowGroups
      property budget : RowBudget
      # If predicates are present, row groups with selected rows must be read so
      # the predicate can decide whether they are actually needed.
      getter? has_predicates : Bool

      def initialize(
        @metadata : File::Metadata::ParquetMetaData,
        row_group_plan : RowGroupPlan,
        @budget : RowBudget,
        @has_predicates : Bool = false,
      )
        @queued = QueuedRowGroups.try_new(@metadata, row_group_plan)
      end

      # Creates a frontier over already-queued row groups (used by `#dup`).
      #
      # Divergence: upstream's second constructor is private; Crystal cannot
      # call a private constructor with an explicit receiver, so this one is
      # `protected` (callable from other instances of this type only).
      protected def initialize(
        @metadata : File::Metadata::ParquetMetaData,
        @queued : QueuedRowGroups,
        @budget : RowBudget,
        @has_predicates : Bool,
      )
      end

      # A copy of this frontier, used for non-mutating peeking.
      def dup : RowGroupFrontier
        RowGroupFrontier.new(@metadata, @queued.dup, @budget, @has_predicates)
      end

      # Updates the budget after a row group finished (upstream
      # `update_budget_after_row_group`).
      def update_budget_after_row_group(budget : RowBudget) : Nil
        @budget = budget
      end

      # The row-group index `#next_readable_row_group` would hand out, without
      # mutating any state (upstream `peek_next_row_group`).
      #
      # Runs the real advance logic on a throwaway copy so peek can never drift
      # from the read path.
      def peek_next_row_group : Int32?
        dup.next_readable_row_group.try(&.row_group_idx)
      end

      # Clears the remaining queued row groups.
      def clear_remaining : Nil
        @queued.clear
      end

      # Whether `next_row_group` should be read or skipped under the remaining
      # budget (upstream `plan_selected_row_group`).
      #
      # Returns the row group to read, or nil after advancing the budget past a
      # skipped row group.
      private def plan_selected_row_group(
        next_row_group : NextRowGroup,
        selected_rows : Int32,
      ) : NextRowGroup?
        # With predicates, the row group must be read so the predicate can
        # decide whether any of its rows are actually needed.
        return next_row_group if @has_predicates

        rows_after_budget = @budget.rows_after(selected_rows)
        return next_row_group if rows_after_budget != 0

        @budget = @budget.advance(selected_rows, rows_after_budget)
        nil
      end

      # Advances queued row groups until one should be handed to the builder
      # (upstream `next_readable_row_group`).
      def next_readable_row_group : NextRowGroup?
        loop do
          row_group_idx = @queued.front
          return nil unless row_group_idx

          # A global selection can be exhausted before its row-group queue.
          # Per-row-group selections have no shared cursor to exhaust; empty
          # local selections are discarded by the `selected_rows == 0` path
          # below.
          if @budget.exhausted? || @queued.global_selection_exhausted?
            clear_remaining
            return nil
          end

          row_count = @metadata.row_group_num_rows(row_group_idx).to_i32
          selection = @queued.pop_front_selection(row_count)
          if selection
            selected_rows = selection.row_count
            next if selected_rows == 0

            # An all-rows selection is equivalent to no selection.
            selection = nil if selected_rows == row_count
          else
            selected_rows = row_count
          end

          next_row_group = NextRowGroup.new(row_group_idx, row_count, selection, @budget)
          if planned = plan_selected_row_group(next_row_group, selected_rows)
            return planned
          end
        end
      end
    end

    # The state recovered from a `RemainingRowGroups` by `#into_parts`,
    # describing the row groups not yet decoded (upstream
    # `RemainingRowGroupsParts`).
    class RemainingRowGroupsParts
      # The arrow schema of the decoded output.
      getter schema : ::Arrow::Schema
      # The parquet file metadata.
      getter metadata : File::Metadata::ParquetMetaData
      # Row groups and selections not yet handed to the reader builder.
      getter row_group_plan : RowGroupPlan
      # Offset still to be skipped before the next readable row group.
      getter offset : Int32?
      # Output rows still permitted across the remaining row groups.
      getter limit : Int32?
      # Builder-configurable parts of the inner row-group reader builder.
      getter reader_builder : RowGroupReaderBuilderParts

      def initialize(
        @schema : ::Arrow::Schema,
        @metadata : File::Metadata::ParquetMetaData,
        @row_group_plan : RowGroupPlan,
        @offset : Int32?,
        @limit : Int32?,
        @reader_builder : RowGroupReaderBuilderParts,
      )
      end
    end

    # The row groups and rows still to be decoded (upstream
    # `RemainingRowGroups`).
    class RemainingRowGroups
      getter frontier : RowGroupFrontier
      getter row_group_reader_builder : RowGroupReaderBuilder
      # The arrow schema of the decoded output; carried only so `#into_parts`
      # can hand it to a rebuilt builder.
      getter schema : ::Arrow::Schema

      def initialize(
        @schema : ::Arrow::Schema,
        metadata : File::Metadata::ParquetMetaData,
        row_group_plan : RowGroupPlan,
        budget : RowBudget,
        has_predicates : Bool,
        @row_group_reader_builder : RowGroupReaderBuilder,
      )
        @frontier = RowGroupFrontier.new(metadata, row_group_plan, budget, has_predicates)
      end

      # Decomposes into `RemainingRowGroupsParts` (upstream `into_parts`).
      #
      # Must be called at a row-group boundary. The inner reader builder's
      # runtime decode state is discarded; its buffered bytes are carried
      # through.
      def into_parts : RemainingRowGroupsParts
        RemainingRowGroupsParts.new(
          @schema,
          @frontier.metadata,
          @frontier.queued.into_plan,
          @frontier.budget.offset,
          @frontier.budget.limit,
          @row_group_reader_builder.into_parts,
        )
      end

      # Pushes new data buffers that can satisfy pending requests.
      def push_data(ranges : Array(Range(UInt64, UInt64)), data : Array(Bytes)) : Nil
        @row_group_reader_builder.push_data(ranges, data)
      end

      # The total number of bytes buffered so far.
      def buffered_bytes : UInt64
        @row_group_reader_builder.buffered_bytes
      end

      # Clears any staged ranges currently buffered for future decode work.
      def clear_all_ranges : Nil
        @row_group_reader_builder.clear_all_ranges
      end

      # Whether the decoder is between row groups (no active row group state).
      def at_row_group_boundary? : Bool
        @row_group_reader_builder.finished?
      end

      # Number of row groups remaining (not including the one currently being
      # decoded).
      def row_groups_remaining : Int32
        @frontier.queued.size
      end

      # The file-level row-group index the next `#try_next_reader` call will
      # produce a reader for, after simulating the same skip logic, without
      # mutating state (upstream `peek_next_row_group`).
      def peek_next_row_group : Int32?
        return nil if @row_group_reader_builder.has_active_row_group?

        @frontier.peek_next_row_group
      end

      # Returns a `ParquetRecordBatchReader` for the next row group, or the byte
      # ranges still needed to build it (upstream `try_next_reader`).
      def try_next_reader : DecodeResult(ParquetRecordBatchReader)
        loop do
          unless @row_group_reader_builder.has_active_row_group?
            # Done with the previous row group, seek the next one.
            next_row_group = @frontier.next_readable_row_group
            return DecodeResult(ParquetRecordBatchReader).finished unless next_row_group

            @row_group_reader_builder.next_row_group(
              next_row_group.row_group_idx,
              next_row_group.row_count,
              next_row_group.selection,
              next_row_group.budget,
            )
          end

          case result = @row_group_reader_builder.try_build
          when RowGroupBuildResult::Finished
            @frontier.update_budget_after_row_group(result.remaining_budget)
            # Reader is done, proceed to the next row group.
          when RowGroupBuildResult::NeedsData
            return DecodeResult(ParquetRecordBatchReader).needs_data(result.ranges)
          when RowGroupBuildResult::Data
            @frontier.update_budget_after_row_group(result.remaining_budget)
            return DecodeResult(ParquetRecordBatchReader).data(result.batch_reader)
          else
            raise general_err!("Internal Error: invalid row group build result")
          end
        end
      end
    end

    # The push decoder's state machine (upstream `ParquetDecoderState`).
    abstract class ParquetDecoderState
      # Waiting for the data needed to decode the next row group.
      class ReadingRowGroup < ParquetDecoderState
        getter remaining : RemainingRowGroups

        def initialize(@remaining : RemainingRowGroups)
        end
      end

      # Actively decoding a row group.
      class DecodingRowGroup < ParquetDecoderState
        getter reader : ParquetRecordBatchReader
        getter remaining : RemainingRowGroups

        def initialize(@reader : ParquetRecordBatchReader, @remaining : RemainingRowGroups)
        end
      end

      # All rows have been decoded.
      class Finished < ParquetDecoderState
      end
    end

    # A push-based Parquet decoder (upstream `ParquetPushDecoder`).
    class ParquetPushDecoder
      @state : ParquetDecoderState

      def initialize(@state : ParquetDecoderState)
      end

      # Returns the next decoded batch, the byte ranges needed to proceed, or
      # `Finished` (upstream `try_decode`).
      def try_decode : DecodeResult(::Arrow::RecordBatch)
        loop do
          case state = @state
          when ParquetDecoderState::Finished
            return DecodeResult(::Arrow::RecordBatch).finished
          when ParquetDecoderState::DecodingRowGroup
            reader = state.reader
            if batch = reader.next_batch
              return DecodeResult(::Arrow::RecordBatch).data(batch)
            end
            @state = ParquetDecoderState::ReadingRowGroup.new(state.remaining)
          when ParquetDecoderState::ReadingRowGroup
            remaining = state.remaining
            result = remaining.try_next_reader
            if result.needs_data?
              return DecodeResult(::Arrow::RecordBatch).needs_data(result.ranges)
            elsif result.finished?
              @state = ParquetDecoderState::Finished.new
              return DecodeResult(::Arrow::RecordBatch).finished
            end
            @state = ParquetDecoderState::DecodingRowGroup.new(result.data, remaining)
          else
            raise general_err!("Internal Error: invalid push decoder state")
          end
        end
      end

      # Returns a reader for the next set of rows, or the byte ranges needed to
      # produce it (upstream `try_next_reader`).
      #
      # The returned `ParquetRecordBatchReader` is handed off to the caller: the
      # decoder keeps no reference to it, so it is safe to call `#into_builder`
      # while the reader is still being iterated.
      def try_next_reader : DecodeResult(ParquetRecordBatchReader)
        case state = @state
        when ParquetDecoderState::Finished
          DecodeResult(ParquetRecordBatchReader).finished
        when ParquetDecoderState::DecodingRowGroup
          # The active reader is handed off; the decoder becomes ready to plan
          # the next row group.
          @state = ParquetDecoderState::ReadingRowGroup.new(state.remaining)
          DecodeResult(ParquetRecordBatchReader).data(state.reader)
        when ParquetDecoderState::ReadingRowGroup
          result = state.remaining.try_next_reader
          if result.needs_data?
            DecodeResult(ParquetRecordBatchReader).needs_data(result.ranges)
          elsif result.finished?
            @state = ParquetDecoderState::Finished.new
            DecodeResult(ParquetRecordBatchReader).finished
          else
            @state = ParquetDecoderState::ReadingRowGroup.new(state.remaining)
            DecodeResult(ParquetRecordBatchReader).data(result.data)
          end
        else
          raise general_err!("Internal Error: invalid push decoder state")
        end
      end

      # Pushes the data for `range` (upstream `push_range`).
      def push_range(range : Range(UInt64, UInt64), data : Bytes) : Nil
        push_ranges([range], [data])
      end

      # Pushes the data for `ranges` (upstream `push_ranges`).
      #
      # It is fine to push data before it is requested.
      def push_ranges(ranges : Array(Range(UInt64, UInt64)), data : Array(Bytes)) : Nil
        remaining =
          case state = @state
          when ParquetDecoderState::ReadingRowGroup
            state.remaining
          when ParquetDecoderState::DecodingRowGroup
            state.remaining
          else
            raise general_err!("Cannot push data to a finished decoder")
          end
        remaining.push_data(ranges, data)
      end

      # How many bytes are currently buffered in the decoder?
      def buffered_bytes : UInt64
        case state = @state
        when ParquetDecoderState::ReadingRowGroup
          state.remaining.buffered_bytes
        when ParquetDecoderState::DecodingRowGroup
          state.remaining.buffered_bytes
        else
          0_u64
        end
      end

      # Clears any staged ranges currently buffered in the decoder.
      def clear_all_ranges : Nil
        case state = @state
        when ParquetDecoderState::ReadingRowGroup
          state.remaining.clear_all_ranges
        when ParquetDecoderState::DecodingRowGroup
          state.remaining.clear_all_ranges
        end
        nil
      end

      # Whether the decoder is between row groups (no active reader).
      #
      # A boundary is "between row groups": the previous row group's reader has
      # been handed off (via `#try_next_reader`) or fully drained (via
      # `#try_decode`), and the next row group has not yet been planned.
      # ameba:disable Naming/PredicateName
      def is_at_row_group_boundary : Bool
        case state = @state
        when ParquetDecoderState::ReadingRowGroup
          state.remaining.at_row_group_boundary?
        else
          false
        end
      end

      # Number of row groups remaining (not including the active one).
      def row_groups_remaining : Int32
        case state = @state
        when ParquetDecoderState::ReadingRowGroup
          state.remaining.row_groups_remaining
        when ParquetDecoderState::DecodingRowGroup
          state.remaining.row_groups_remaining
        else
          0
        end
      end

      # The row-group index that the next `#try_next_reader` call will produce a
      # reader for (upstream `peek_next_row_group`).
      #
      # Safe to call at any time, does not mutate any state, and returns nil
      # when no row groups remain or every remaining row group would be skipped.
      # Mid-row-group it reports the row group `#try_next_reader` will produce
      # *after* the active one.
      def peek_next_row_group : Int32?
        case state = @state
        when ParquetDecoderState::ReadingRowGroup
          state.remaining.peek_next_row_group
        when ParquetDecoderState::DecodingRowGroup
          state.remaining.peek_next_row_group
        else
          nil
        end
      end

      # Decomposes this decoder back into a builder for the row groups that have
      # not yet been decoded (upstream `into_builder`).
      #
      # This is the adaptive-scan API: drive the decoder with
      # `#try_next_reader`, and at any row-group boundary call `#into_builder` to
      # recover a builder, adjust it and build a fresh decoder that resumes from
      # the next row group. Already-decoded rows are not produced again, and
      # bytes already pushed are carried across the rebuild.
      def into_builder : ParquetPushDecoderBuilder
        remaining =
          case state = @state
          when ParquetDecoderState::ReadingRowGroup
            state.remaining
          when ParquetDecoderState::DecodingRowGroup
            raise general_err!(
              "into_builder called while a row group is being decoded; " \
              "check is_at_row_group_boundary() first"
            )
          else
            raise general_err!("into_builder called on a finished decoder")
          end

        unless remaining.at_row_group_boundary?
          raise general_err!(
            "into_builder called mid-row-group; check is_at_row_group_boundary() first"
          )
        end

        ParquetPushDecoderBuilder.from_parts(remaining.into_parts)
      end
    end

    # A builder for `ParquetPushDecoder` (upstream
    # `ParquetPushDecoderBuilder`).
    #
    # Divergence: upstream aliases `ArrowReaderBuilder<PushDecoderInput>`; this
    # carries the same options directly (the generic builder refactor is
    # deferred) and models `PushDecoderInput` as the `PushBuffers` field.
    class ParquetPushDecoderBuilder
      getter metadata : File::Metadata::ParquetMetaData
      getter schema : ::Arrow::Schema
      getter fields : ParquetField?

      def initialize(
        @metadata : File::Metadata::ParquetMetaData,
        @schema : ::Arrow::Schema,
        @fields : ParquetField?,
        @batch_size : Int32 = ArrayReaderBuilder::DEFAULT_BATCH_SIZE,
        @projection : ProjectionMask = ProjectionMask.all,
        @row_group_plan : RowGroupPlan = RowGroupPlan.global,
        @offset : Int32? = nil,
        @limit : Int32? = nil,
        @row_selection_policy : RowSelectionPolicy = RowSelectionPolicy.default,
        @metrics : ArrowReaderMetrics = ArrowReaderMetrics.disabled,
        @buffers : Util::PushBuffers = Util::PushBuffers.new,
      )
      end

      # Reassembles a builder from a decoder's not-yet-decoded state — the
      # inverse of `#build` (upstream `builder_from_remaining`).
      def self.from_parts(parts : RemainingRowGroupsParts) : ParquetPushDecoderBuilder
        reader_builder = parts.reader_builder
        new(
          parts.metadata,
          parts.schema,
          reader_builder.fields,
          reader_builder.batch_size,
          reader_builder.projection,
          parts.row_group_plan,
          parts.offset,
          parts.limit,
          reader_builder.row_selection_policy,
          reader_builder.metrics,
          reader_builder.buffers,
        )
      end

      # Creates a builder for the given parquet metadata (upstream
      # `try_new_decoder`).
      def self.try_new_decoder(metadata : File::Metadata::ParquetMetaData) : ParquetPushDecoderBuilder
        try_new_decoder_with_options(metadata, ArrowReaderOptions.new)
      end

      # Creates a builder with reader options such as a supplied arrow schema
      # (upstream `try_new_decoder_with_options`).
      def self.try_new_decoder_with_options(
        metadata : File::Metadata::ParquetMetaData,
        options : ArrowReaderOptions,
      ) : ParquetPushDecoderBuilder
        arrow_metadata = ArrowReaderMetadata.try_new(metadata, options)
        new_with_metadata(arrow_metadata)
      end

      # Creates a builder from pre-loaded arrow metadata (upstream
      # `new_with_metadata`).
      def self.new_with_metadata(arrow_metadata : ArrowReaderMetadata) : ParquetPushDecoderBuilder
        new(arrow_metadata.metadata, arrow_metadata.schema, arrow_metadata.fields)
      end

      # Sets the batch size.
      def with_batch_size(batch_size : Int32) : self
        @batch_size = batch_size
        self
      end

      # Only decodes the leaves selected by `mask`.
      def with_projection(projection : ProjectionMask) : self
        @projection = projection
        self
      end

      # Only decodes the given row groups, in the supplied order.
      def with_row_groups(row_groups : Enumerable(Int32)) : self
        @row_group_plan.set_row_groups(row_groups.to_a)
        self
      end

      # Selects rows within each row group, in the supplied order (upstream
      # `with_row_group_selections`).
      #
      # Row-group-local selections are in row-group coordinates and may repeat
      # row groups. They are mutually exclusive with `with_row_groups` and
      # `with_row_selection`; mixing them is an error reported by `build`.
      def with_row_group_selections(selections : Array(RowGroupSelection)) : self
        @row_group_plan.set_row_group_selections(selections)
        self
      end

      # Selects rows across the concatenated selected row groups.
      def with_row_selection(selection : RowSelection) : self
        @row_group_plan.set_row_selection(selection)
        self
      end

      # Skips the first `offset` selected rows.
      def with_offset(offset : Int32) : self
        @offset = offset
        self
      end

      # Reads at most `limit` selected rows.
      def with_limit(limit : Int32) : self
        @limit = limit
        self
      end

      # Configures the policy used to materialize the row selection.
      def with_row_selection_policy(policy : RowSelectionPolicy) : self
        @row_selection_policy = policy
        self
      end

      # Configures metrics collection during decoding.
      def with_metrics(metrics : ArrowReaderMetrics) : self
        @metrics = metrics
        self
      end

      # Provides preexisting buffers so bytes already fetched are not requested
      # again (upstream `with_buffers`).
      def with_buffers(buffers : Util::PushBuffers) : self
        @buffers = buffers
        self
      end

      # The parquet schema descriptor.
      def parquet_schema : Schema::SchemaDescriptor
        @metadata.file_metadata.schema_descr
      end

      # Builds the decoder (upstream `build`).
      def build : ParquetPushDecoder
        row_group_reader_builder = RowGroupReaderBuilder.new(
          @batch_size,
          @projection,
          @metadata,
          @fields,
          @metrics,
          @row_selection_policy,
          @buffers,
        )

        remaining = RemainingRowGroups.new(
          @schema,
          @metadata,
          @row_group_plan,
          RowBudget.new(@offset, @limit),
          false,
          row_group_reader_builder,
        )
        ParquetPushDecoder.new(ParquetDecoderState::ReadingRowGroup.new(remaining))
      end
    end
  end
end
