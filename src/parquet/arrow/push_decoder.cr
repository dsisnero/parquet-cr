require "../decode_result"
require "../util/push_buffers"
require "./array_reader"
require "./array_reader/builder"
require "./arrow_reader"
require "./in_memory_row_group"
require "./metrics"
require "./mod"
require "./read_plan"

# Ported from `parquet/src/arrow/push_decoder/{mod,remaining}.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# A push-based decoder: instead of reading from a file, the caller pushes the
# byte ranges the decoder asks for ([`DecodeResult::NeedsData`]) and decodes the
# batches the decoder produces ([`DecodeResult::Data`]). The decoder tracks which
# row groups and rows remain, applies offset/limit budgets across row groups, and
# reads each row group from an `InMemoryRowGroup` built from the pushed bytes.
#
# Deferred in this slice (see `plans/parity.md`): row filters in the push path
# (`RowFilter`/predicate cache), row-group-local selections
# (`with_row_group_selections`), `peek_next_row_group`, `try_next_reader` and
# `into_builder` (adaptive scans), and the generic `ArrowReaderBuilder<I>`
# refactor upstream shares with the sync/async builders (this builder carries
# the same options directly).
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

    # The row groups and rows still to be decoded (upstream
    # `RemainingRowGroups`).
    class RemainingRowGroups
      getter metadata : File::Metadata::ParquetMetaData
      getter budget : RowBudget

      # A row group whose data request is still outstanding.
      private class PendingRowGroup
        getter row_group_idx : Int32
        getter row_count : Int32
        getter plan_builder : ReadPlanBuilder
        getter fetch : InMemoryRowGroup::FetchRanges
        getter group : InMemoryRowGroup

        def initialize(@row_group_idx, @row_count, @plan_builder, @fetch, @group)
        end
      end

      def initialize(
        @metadata : File::Metadata::ParquetMetaData,
        @row_groups : Array(Int32),
        @selection : RowSelection?,
        @budget : RowBudget,
        @batch_size : Int32,
        @projection : ProjectionMask,
        @fields : ParquetField?,
        @row_selection_policy : RowSelectionPolicy,
        @buffers : Util::PushBuffers,
        @metrics : ArrowReaderMetrics,
      )
        @pending = nil.as(PendingRowGroup?)
      end

      # Pushes new data buffers that can satisfy pending requests.
      def push_data(ranges : Array(Range(UInt64, UInt64)), data : Array(Bytes)) : Nil
        @buffers.push_ranges(ranges, data)
      end

      # The total number of bytes buffered so far.
      def buffered_bytes : UInt64
        @buffers.buffered_bytes
      end

      # Clears any staged ranges currently buffered for future decode work.
      def clear_all_ranges : Nil
        @buffers.clear_all_ranges
      end

      # Whether no row group is currently being set up (i.e. the decoder is
      # between row groups).
      def at_row_group_boundary? : Bool
        @pending.nil?
      end

      # Number of row groups remaining (not including the one currently being
      # decoded).
      def row_groups_remaining : Int32
        @row_groups.size
      end

      # Returns a `ParquetRecordBatchReader` for the next row group, or the byte
      # ranges still needed to build it (upstream `try_next_reader`).
      def try_next_reader : DecodeResult(ParquetRecordBatchReader)
        loop do
          if pending = @pending
            return DecodeResult(ParquetRecordBatchReader).needs_data(needed_ranges(pending)) unless pending_ready?(pending)

            return DecodeResult(ParquetRecordBatchReader).data(build_reader(pending))
          end

          next_row_group = next_readable_row_group
          return DecodeResult(ParquetRecordBatchReader).finished unless next_row_group

          row_group_idx, row_count, local_selection = next_row_group
          plan_builder = ReadPlanBuilder.new(@batch_size)
            .with_selection(local_selection)
            .with_row_selection_policy(@row_selection_policy)

          rows_before = plan_builder.num_rows_selected || row_count
          limited = plan_builder.limited(row_count)
          limited = limited.with_offset(@budget.offset) if @budget.offset
          limited = limited.with_limit(@budget.limit) if @budget.limit
          plan_builder = limited.build_limited
          rows_after = @budget.rows_after(rows_before)
          @budget = @budget.advance(rows_before, rows_after)

          # The row group is ruled out by the selection or the budget.
          next if rows_before == 0 || rows_after == 0

          group = in_memory_row_group(row_group_idx, row_count)
          fetch = group.fetch_ranges(@projection, plan_builder.selection, @batch_size)
          pending = PendingRowGroup.new(row_group_idx, row_count, plan_builder, fetch, group)
          @pending = pending

          return DecodeResult(ParquetRecordBatchReader).needs_data(needed_ranges(pending)) unless pending_ready?(pending)

          return DecodeResult(ParquetRecordBatchReader).data(build_reader(pending))
        end
      end

      private def needed_ranges(pending : PendingRowGroup) : Array(Range(UInt64, UInt64))
        pending.fetch.ranges.reject { |range| @buffers.has_range?(range) }
      end

      private def pending_ready?(pending : PendingRowGroup) : Bool
        needed_ranges(pending).empty?
      end

      private def build_reader(pending : PendingRowGroup) : ParquetRecordBatchReader
        group = pending.group
        fetch = pending.fetch
        chunks = fetch.ranges.map do |range|
          @buffers.get_bytes(range.begin, (range.end - range.begin).to_i32)
        end
        group.fill_column_chunks(@projection, fetch.page_start_offsets, chunks)
        @buffers.clear_ranges(fetch.ranges)
        @pending = nil

        array_reader = ArrayReaderBuilder.new(group, @batch_size, @metrics)
          .build_array_reader(@fields, @projection)
        ParquetRecordBatchReader.new(array_reader, pending.plan_builder.build)
      end

      private def in_memory_row_group(row_group_idx : Int32, row_count : Int32) : InMemoryRowGroup
        num_columns = @metadata.file_metadata.schema_descr.num_columns
        page_index = @metadata.page_index.try { @metadata.page_index_for_row_group(row_group_idx) }
        InMemoryRowGroup.new(
          row_count,
          Array(ColumnChunkData?).new(num_columns, nil),
          page_index,
          row_group_idx,
          @metadata,
        )
      end

      # Plans whether a selected row group should be read or skipped (upstream
      # `RowGroupFrontier#next_readable_row_group`).
      private def next_readable_row_group : Tuple(Int32, Int32, RowSelection?)?
        loop do
          return nil if @row_groups.empty?

          selection = @selection
          if @budget.exhausted? || (selection && selection.row_count == 0)
            @row_groups.clear
            @selection = nil
            return nil
          end

          row_group_idx = @row_groups.shift
          row_count = @metadata.row_group(row_group_idx).num_rows.to_i32
          local_selection = selection.try(&.split_off(row_count))
          selected_rows = local_selection.try(&.row_count) || row_count
          next if selected_rows == 0

          local_selection = nil if selected_rows == row_count

          # Apply the offset/limit budget: skip the row group when it contributes
          # no output rows.
          rows_after = @budget.rows_after(selected_rows)
          if rows_after == 0
            @budget = @budget.advance(selected_rows, 0)
            next
          end

          return {row_group_idx, row_count, local_selection}
        end
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
        row_groups, selection = @row_group_plan.into_global
        queued = row_groups || (0...@metadata.num_row_groups).to_a

        remaining = RemainingRowGroups.new(
          @metadata,
          queued.dup,
          selection,
          RowBudget.new(@offset, @limit),
          @batch_size,
          @projection,
          @fields,
          @row_selection_policy,
          @buffers,
          @metrics,
        )
        ParquetPushDecoder.new(ParquetDecoderState::ReadingRowGroup.new(remaining))
      end
    end
  end
end
