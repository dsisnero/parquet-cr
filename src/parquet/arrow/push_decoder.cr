require "../decode_result"
require "../util/push_buffers"
require "./array_reader"
require "./array_reader/builder"
require "./arrow_reader"
require "./filter"
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
# per-row-group decode state machine (`Start` → (`Filters` →
# `WaitingOnFilterData`)* → `StartData` → `WaitingOnData` → `Finished`) and asks
# for the byte ranges it needs through `DataRequest`. `RemainingRowGroups` glues
# the two together; `ParquetPushDecoder` adds the reader/state machine on top
# and exposes the adaptive-scan API (`try_next_reader`, `peek_next_row_group`,
# `into_builder`).
#
# Deferred in this slice (see `plans/parity.md`): the generic
# `ArrowReaderBuilder<I>` refactor upstream shares with the sync/async builders
# (this builder carries the same options directly).
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
        @cache_projection = nil.as(ProjectionMask?)
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

      # Sets the columns whose data is cached across predicates (upstream
      # `with_cache_projection`).
      #
      # The selection of the cached columns is expanded to batch boundaries in
      # `InMemoryRowGroup#fetch_ranges` so a cache miss can safely fetch a
      # complete batch from the retained sparse column data.
      def with_cache_projection(cache_projection : ProjectionMask?) : self
        @cache_projection = cache_projection
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
        fetch = row_group.fetch_ranges(@projection, @selection, @batch_size, @cache_projection)

        DataRequest.new(row_group.column_chunks, fetch.ranges, fetch.page_start_offsets)
      end
    end

    # Predicate cache configuration for one row group (upstream `CacheInfo` in
    # `push_decoder/reader_builder/filter.rs`).
    #
    # Note this is basically the same as `CacheOptionsBuilder` but it owns the
    # `ProjectionMask` and the `RowGroupCache`.
    #
    # Divergence: upstream holds the cache as `Arc<RwLock<RowGroupCache>>`; the
    # port shares the object directly (see `row_group_cache.cr`).
    class CacheInfo
      # The columns to cache in the predicate cache.
      #
      # Normally these are the columns that filters may look at, such that for a
      # filter like `(a + 10 > 5) AND (a + b = 0)` we cache `a` to avoid
      # re-reading it between evaluating `a + 10 > 5` and `a + b = 0`.
      getter cache_projection : ProjectionMask
      getter row_group_cache : RowGroupCache

      def initialize(@cache_projection : ProjectionMask, @row_group_cache : RowGroupCache)
      end

      # A `CacheOptionsBuilder` over this row group's cache.
      def builder : CacheOptionsBuilder
        CacheOptionsBuilder.new(@cache_projection, @row_group_cache)
      end
    end

    # State machine for evaluating a sequence of predicates (upstream
    # `FilterInfo`).
    #
    # The `FilterInfo` owns the `RowFilter` being evaluated and tracks the
    # current predicate. `ArrowPredicate#evaluate` takes `self` in Rust, so the
    # filter is moved out of the `RowGroupReaderBuilder` while it is evaluated
    # and put back when the chain is done (see `FilterAdvanceResult`).
    class FilterInfo
      @filter : RowFilter
      # The predicate to evaluate next, 1-based (upstream `NonZeroUsize`).
      @next_predicate : Int32
      getter cache_info : CacheInfo

      def initialize(@filter : RowFilter, @cache_info : CacheInfo)
        @next_predicate = 1
      end

      # The predicate to evaluate now.
      def current : ArrowPredicate
        @filter.predicates[@next_predicate - 1]
      end

      # Whether the current predicate is the last one in the chain.
      def last? : Bool
        @next_predicate == @filter.predicates.size
      end

      # The columns to cache for the current predicate.
      def cache_projection : ProjectionMask
        @cache_info.cache_projection
      end

      # A cache builder to save the results of predicate evaluation.
      def cache_builder : CacheOptionsBuilder
        @cache_info.builder
      end

      # Advances to the next predicate (upstream `FilterInfo::advance`).
      def advance : FilterAdvanceResult
        if @next_predicate >= @filter.predicates.size
          FilterAdvanceResult::Done.new(@filter, @cache_info)
        else
          @next_predicate += 1
          FilterAdvanceResult::Continue.new(self)
        end
      end

      # Returns the inner filter, consuming this `FilterInfo`.
      def into_filter : RowFilter
        @filter
      end
    end

    # The result of `FilterInfo#advance` (upstream `AdvanceResult`).
    abstract class FilterAdvanceResult
      # There are more predicates to evaluate.
      class Continue < FilterAdvanceResult
        getter filter_info : FilterInfo

        def initialize(@filter_info : FilterInfo)
        end
      end

      # No more predicates: the filter and its cache info are returned.
      class Done < FilterAdvanceResult
        getter filter : RowFilter
        getter cache_info : CacheInfo

        def initialize(@filter : RowFilter, @cache_info : CacheInfo)
        end
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

      # Planning filters, but the data to evaluate the current predicate has not
      # been requested yet.
      class Filters < RowGroupDecoderState
        getter row_group_info : RowGroupInfo
        # Any previously read column chunk data from prior predicates.
        getter column_chunks : Array(ColumnChunkData?)?
        getter filter_info : FilterInfo

        def initialize(
          @row_group_info : RowGroupInfo,
          @column_chunks : Array(ColumnChunkData?)?,
          @filter_info : FilterInfo,
        )
        end
      end

      # Needs data to evaluate the current filter.
      class WaitingOnFilterData < RowGroupDecoderState
        getter row_group_info : RowGroupInfo
        getter filter_info : FilterInfo
        getter data_request : DataRequest

        def initialize(
          @row_group_info : RowGroupInfo,
          @filter_info : FilterInfo,
          @data_request : DataRequest,
        )
        end
      end

      # Know what data to actually read.
      class StartData < RowGroupDecoderState
        getter row_group_info : RowGroupInfo
        # Any previously read column chunk data.
        getter column_chunks : Array(ColumnChunkData?)?
        # Any cached filter results.
        getter cache_info : CacheInfo?

        def initialize(
          @row_group_info : RowGroupInfo,
          @column_chunks : Array(ColumnChunkData?)?,
          @cache_info : CacheInfo? = nil,
        )
        end
      end

      # Needs data to proceed with reading the output.
      class WaitingOnData < RowGroupDecoderState
        getter row_group_info : RowGroupInfo
        getter data_request : DataRequest
        # Any cached filter results.
        getter cache_info : CacheInfo?

        def initialize(
          @row_group_info : RowGroupInfo,
          @data_request : DataRequest,
          @cache_info : CacheInfo? = nil,
        )
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
      getter filter : RowFilter?
      getter max_predicate_cache_size : Int32
      getter metrics : ArrowReaderMetrics
      getter row_selection_policy : RowSelectionPolicy
      # Bytes already pushed into the decoder, carried across a rebuild so they
      # are not re-requested.
      getter buffers : Util::PushBuffers

      def initialize(
        @batch_size : Int32,
        @projection : ProjectionMask,
        @fields : ParquetField?,
        @filter : RowFilter?,
        @max_predicate_cache_size : Int32,
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
      # The row filter. It is taken while its predicates are evaluated (see
      # `FilterInfo`) and put back when the predicate chain completes.
      getter filter : RowFilter?
      getter max_predicate_cache_size : Int32

      def initialize(
        @batch_size : Int32,
        @projection : ProjectionMask,
        @metadata : File::Metadata::ParquetMetaData,
        @fields : ParquetField?,
        @filter : RowFilter?,
        @metrics : ArrowReaderMetrics,
        @max_predicate_cache_size : Int32,
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
          @filter,
          @max_predicate_cache_size,
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

          # Take the filter so its predicates can be evaluated mutably; it is
          # put back when the predicate chain is done (upstream
          # `self.filter.take()`).
          filter = @filter
          @filter = nil

          if filter.nil? || filter.predicates.empty?
            # No filter (or no predicates): start trying to read data
            # immediately. An empty filter is dropped, as upstream does.
            return RowGroupNextState.new(RowGroupDecoderState::StartData.new(info, nil, nil))
          end

          # There are predicates to evaluate.
          cache_projection =
            compute_cache_projection(info.row_group_idx, filter)
          cache_info = CacheInfo.new(
            cache_projection,
            RowGroupCache.new(@batch_size, @max_predicate_cache_size),
          )
          filter_info = FilterInfo.new(filter, cache_info)
          RowGroupNextState.new(RowGroupDecoderState::Filters.new(info, nil, filter_info))
        when RowGroupDecoderState::Filters
          transition_filters(current_state)
        when RowGroupDecoderState::WaitingOnFilterData
          transition_waiting_on_filter_data(current_state)
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
            # Final projection fetch shouldn't expand selection for cache, so
            # don't call `with_cache_projection` here.
            .build

          # Page pruning leaves sparse column data behind, so record which rows
          # are loaded for the mask path (upstream
          # `prepare_selection_for_page_skipping`).
          plan_builder = Arrow.prepare_selection_for_page_skipping(
            info.plan_builder,
            @projection,
            row_group_offset_index(info.row_group_idx),
            @metadata.file_metadata.schema_descr.num_columns,
            info.row_count,
          )

          # The budget consumed by this row group must not be re-applied when the
          # data arrives (upstream rebuilds `RowGroupInfo` with
          # `remaining_budget` before handing it to `WaitingOnData`).
          next_info = RowGroupInfo.new(
            info.row_group_idx,
            info.row_count,
            plan_builder,
            budgeted.remaining_budget,
          )

          RowGroupNextState.new(
            RowGroupDecoderState::WaitingOnData.new(
              next_info, data_request, current_state.cache_info
            )
          )
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

          # If we have any cached results, connect them up.
          builder = ArrayReaderBuilder.new(row_group, @batch_size, @metrics)
          array_reader =
            if cache_info = current_state.cache_info
              cache_options = cache_info.builder.consumer
              builder
                .with_cache_options(cache_options)
                .build_array_reader(@fields, @projection)
            else
              builder.build_array_reader(@fields, @projection)
            end
          reader = ParquetRecordBatchReader.new(array_reader, plan)
          RowGroupNextState.new(
            RowGroupDecoderState::Finished.new,
            RowGroupBuildResult::Data.new(reader, info.budget),
          )
        else
          raise general_err!("Internal Error: try_build called without an active row group")
        end
      end

      # Handles the `Filters` state: requests the data to evaluate the current
      # predicate (upstream `RowGroupDecoderState::Filters`).
      private def transition_filters(
        current_state : RowGroupDecoderState::Filters,
      ) : RowGroupNextState
        info = current_state.row_group_info
        filter_info = current_state.filter_info

        # If nothing is selected, this row group is done.
        unless info.plan_builder.selects_any?
          @filter = filter_info.into_filter
          return RowGroupNextState.new(
            RowGroupDecoderState::Finished.new,
            RowGroupBuildResult::Finished.new(info.budget),
          )
        end

        # Request the data needed to evaluate the current predicate, using the
        # predicate's own projection.
        predicate = filter_info.current
        data_request = DataRequestBuilder.new(
          info.row_group_idx,
          info.row_count,
          @batch_size,
          @metadata,
          predicate.projection,
        )
          .with_selection(info.plan_builder.selection)
          # Output columns cached across predicates reuse these chunks; the
          # cache itself is not ported, so this changes no requested ranges.
          .with_cache_projection(filter_info.cache_projection)
          .with_column_chunks(current_state.column_chunks)
          .build

        RowGroupNextState.new(
          RowGroupDecoderState::WaitingOnFilterData.new(info, filter_info, data_request),
        )
      end

      # Handles the `WaitingOnFilterData` state: evaluates the current predicate
      # once its data is buffered, then advances the predicate chain (upstream
      # `RowGroupDecoderState::WaitingOnFilterData`).
      private def transition_waiting_on_filter_data(
        current_state : RowGroupDecoderState::WaitingOnFilterData,
      ) : RowGroupNextState
        info = current_state.row_group_info
        filter_info = current_state.filter_info
        data_request = current_state.data_request

        needed_ranges = data_request.needed_ranges(@buffers)
        unless needed_ranges.empty?
          # Still need data to evaluate the predicate.
          return RowGroupNextState.new(
            current_state,
            RowGroupBuildResult::NeedsData.new(needed_ranges),
          )
        end

        # All the data needed to evaluate the predicate is buffered.
        predicate = filter_info.current
        row_group = data_request.try_into_in_memory_row_group(
          info.row_group_idx, info.row_count, @metadata, predicate.projection, @buffers
        )
        cache_options = filter_info.cache_builder.producer
        array_reader = ArrayReaderBuilder.new(row_group, @batch_size, @metrics)
          .with_cache_options(cache_options)
          .build_array_reader(@fields, predicate.projection)

        # Restore the configured policy, then evaluate the predicate, optionally
        # stopping once the offset+limit of output rows is reached when this is
        # the last predicate in the chain. Auto resolution and loaded ranges are
        # projection-specific, so prepare the plan for this predicate's
        # projection first (upstream `prepare_selection_for_page_skipping`).
        info.plan_builder = info.plan_builder.with_row_selection_policy(@row_selection_policy)
        info.plan_builder = Arrow.prepare_selection_for_page_skipping(
          info.plan_builder,
          predicate.projection,
          row_group_offset_index(info.row_group_idx),
          @metadata.file_metadata.schema_descr.num_columns,
          info.row_count,
        )
        options = PredicateOptions.new(array_reader, predicate)
        if filter_info.last? && (limit = info.budget.selected_row_limit)
          options = options.with_limit(limit, info.row_count)
        end
        info.plan_builder = info.plan_builder.with_predicate_options(options)

        # Keep the column chunks for subsequent predicates and the output stage
        # (upstream `row_group.column_chunks`).
        column_chunks = row_group.column_chunks

        case advance = filter_info.advance
        when FilterAdvanceResult::Continue
          RowGroupNextState.new(
            RowGroupDecoderState::Filters.new(info, column_chunks, advance.filter_info),
          )
        when FilterAdvanceResult::Done
          @filter = advance.filter
          RowGroupNextState.new(
            RowGroupDecoderState::StartData.new(info, column_chunks, advance.cache_info)
          )
        else
          raise general_err!("Internal Error: invalid filter advance result")
        end
      end

      # Computes the columns to cache for `filter` in the given row group
      # (upstream `compute_cache_projection`).
      private def compute_cache_projection(
        row_group_idx : Int32,
        filter : RowFilter,
      ) : ProjectionMask
        metadata = @metadata.row_group(row_group_idx)
        compute_cache_projection_inner(filter) || ProjectionMask.none(metadata.columns.size)
      end

      private def compute_cache_projection_inner(filter : RowFilter) : ProjectionMask?
        # Do not compute the projection mask if the predicate cache is disabled.
        return nil if @max_predicate_cache_size == 0

        first = filter.predicates.first?
        return nil unless first

        cache_projection = ProjectionMask.new(first.projection.mask.try(&.dup))
        filter.predicates.each do |predicate|
          cache_projection.union(predicate.projection)
        end
        cache_projection.intersect(@projection)
        exclude_nested_columns_from_cache(cache_projection)
      end

      # Excludes leaves belonging to roots that span multiple parquet leaves
      # (i.e. nested columns) (upstream `exclude_nested_columns_from_cache`).
      private def exclude_nested_columns_from_cache(mask : ProjectionMask) : ProjectionMask?
        mask.without_nested_types(@metadata.file_metadata.schema_descr)
      end

      # The offset index for the given row group, if any (upstream
      # `row_group_offset_index`).
      private def row_group_offset_index(
        row_group_idx : Int32,
      ) : File::Metadata::RowGroupPageIndex?
        page_index = @metadata.page_index
        if page_index && page_index.offset_indexes?
          @metadata.page_index_for_row_group(row_group_idx)
        end
      end
    end

    # Prepares row-selection execution when page pruning produced sparse column
    # data (upstream `prepare_selection_for_page_skipping`).
    #
    # Some pages can be skipped while the `InMemoryRowGroup` is built if the
    # selection does not read them, so their data pages are never loaded. The
    # selector strategy copes with that: `skip_records` skips the page. A mask
    # chunk, however, decodes every row it covers before the mask filter is
    # applied, so a chunk cannot cross a page that was skipped.
    #
    # A simple example: the page size is 2, the mask is 100001 and the row
    # selection should read(1) skip(4) read(1); the `ColumnChunkData` holds
    # page1(10), page2(skipped), page3(01). Mask execution therefore records the
    # row ranges loaded for every projected column, so each mask chunk stays
    # within loaded data and `skip_records()` crosses the gaps. This applies both
    # to an explicit mask policy and to Auto when it resolves to mask execution.
    #
    # Divergence: upstream keeps this a private free function of
    # `push_decoder/reader_builder/mod.rs` (its unit tests live in the same
    # module); the port exposes it on `Parquet::Arrow` so the ported tests can
    # call it.
    def self.prepare_selection_for_page_skipping(
      plan_builder : ReadPlanBuilder,
      projection_mask : ProjectionMask,
      page_index : File::Metadata::RowGroupPageIndex?,
      num_columns : Int32,
      total_rows : Int32,
    ) : ReadPlanBuilder
      case plan_builder.resolve_selection_strategy
      when .mask?
        loaded = loaded_row_ranges_for_projection(
          plan_builder.selection, projection_mask, page_index, num_columns, total_rows
        )
        plan_builder
          .with_row_selection_policy(RowSelectionPolicy::MASK)
          .with_loaded_row_ranges(loaded)
      else
        plan_builder.with_row_selection_policy(RowSelectionPolicy::SELECTORS)
      end
    end

    # Computes the row ranges for which every projected column has page data
    # loaded (upstream `loaded_row_ranges_for_projection`).
    #
    # Returns `nil` when there is no selection or no page index, or when every
    # row of the selection is loaded.
    #
    # Divergence: exposed on `Parquet::Arrow` for the same reason as
    # `prepare_selection_for_page_skipping`.
    def self.loaded_row_ranges_for_projection(
      selection : RowSelection?,
      projection_mask : ProjectionMask,
      page_index : File::Metadata::RowGroupPageIndex?,
      num_columns : Int32,
      total_rows : Int32,
    ) : LoadedRowRanges?
      sel = selection
      return nil unless sel
      row_group_index = page_index
      return nil unless row_group_index

      loaded : RowSelection? = nil
      num_columns.times do |leaf_idx|
        column_metadata = row_group_index.offset_index(leaf_idx)
        next unless column_metadata

        pages = column_metadata.page_locations
        next unless projection_mask.leaf_included?(leaf_idx)
        next if pages.empty?

        column_loaded = RowSelection.from_consecutive_ranges(
          sel.row_ranges_for_selected_pages(pages, total_rows), total_rows
        )
        loaded = loaded ? loaded.intersection(column_loaded) : column_loaded
      end

      result = loaded
      return nil unless result
      return nil if result.skipped_row_count == 0

      LoadedRowRanges.from_selection(result)
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
      # Upstream `ArrowReaderBuilder`'s default predicate cache size (100MB).
      DEFAULT_MAX_PREDICATE_CACHE_SIZE = 100 * 1024 * 1024

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
        @filter : RowFilter? = nil,
        @max_predicate_cache_size : Int32 = DEFAULT_MAX_PREDICATE_CACHE_SIZE,
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
          reader_builder.filter,
          reader_builder.max_predicate_cache_size,
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

      # Applies a row filter: only rows accepted by every predicate are
      # returned (upstream `with_row_filter`).
      #
      # Predicates are evaluated row group by row group, after any row-group
      # selection and before the offset/limit budget is applied.
      def with_row_filter(filter : RowFilter) : self
        @filter = filter
        self
      end

      # Sets the maximum size of the predicate cache (upstream
      # `with_max_predicate_cache_size`).
      #
      # The cache stores the decoded arrays of the columns used by both the row
      # filter and the output projection, so they are decoded once per row
      # group; `0` disables it.
      def with_max_predicate_cache_size(max_predicate_cache_size : Int32) : self
        @max_predicate_cache_size = max_predicate_cache_size
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
        filter = @filter
        has_predicates = filter.nil? ? false : !filter.predicates.empty?

        row_group_reader_builder = RowGroupReaderBuilder.new(
          @batch_size,
          @projection,
          @metadata,
          @fields,
          @filter,
          @metrics,
          @max_predicate_cache_size,
          @row_selection_policy,
          @buffers,
        )

        remaining = RemainingRowGroups.new(
          @schema,
          @metadata,
          @row_group_plan,
          RowBudget.new(@offset, @limit),
          has_predicates,
          row_group_reader_builder,
        )
        ParquetPushDecoder.new(ParquetDecoderState::ReadingRowGroup.new(remaining))
      end
    end
  end
end
