require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../metrics"
require "./row_group_cache"

# Ported from `parquet/src/arrow/array_reader/cached_array_reader.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `CachedArrayReader` is a transparent wrapper around an `ArrayReader` that
# avoids duplicate decoding when the same column appears in both filter
# predicates and the output projection. It uses a `RowGroupCache` shared by the
# readers of one row group in two roles:
#
# - `Producer`: during the filter phase, inserts decoded data into the cache.
# - `Consumer`: during output building, takes the data from the cache and drops
#   it once consumed.
#
# This means the memory consumption of the cache has two stages: it grows while
# the filter phase populates it, peaks when the filters are built, and decreases
# as the cached data is consumed.
#
# ```
#    ▲
#    │     ╭─╮
#    │    ╱   ╲
#    │   ╱     ╲
#    │  ╱       ╲
#    │ ╱         ╲
#    │╱           ╲
#    └─────────────╲──────► Time
#    │      │      │
#    Filter  Peak  Consume
#    Phase (Built) (Decrease)
# ```
#
# Divergences: the shared cache is a plain `RowGroupCache` rather than an
# `Arc<RwLock<RowGroupCache>>` (see `row_group_cache.cr`); upstream's
# `assert_eq!` on the skipped row count raises a `ParquetError` instead; the
# positions and caches the upstream unit tests read directly are exposed as
# getters because Crystal specs cannot reach private state.
module Parquet
  module Arrow
    # Role of the cached array reader (upstream `CacheRole`).
    enum CacheRole
      # Producer role: inserts data into the cache during the filter phase.
      Producer
      # Consumer role: removes consumed data from the cache during the output
      # building phase.
      Consumer
    end

    # A cached wrapper around an `ArrayReader` (upstream `CachedArrayReader`).
    class CachedArrayReader < ArrayReader
      # The underlying array reader.
      @inner : ArrayReader
      # Shared cache for this row group.
      @shared_cache : RowGroupCache
      # Column index for cache key generation.
      @column_idx : Int32
      # Current logical position in the data stream for this reader (for cache
      # key generation).
      getter outer_position : Int32
      # Current position in `@inner`.
      getter inner_position : Int32
      # Batch size for the cache.
      getter batch_size : Int32
      # Tracks the selections for the next `#consume_batch`.
      @selections : ::Arrow::BooleanBufferBuilder
      # Role of this reader.
      getter role : CacheRole
      # Local cache holding batches between `#read_records` and
      # `#consume_batch` so data stays available even if the shared cache
      # evicts it.
      getter local_cache : Hash(BatchID, ::Arrow::ArrayBase)
      # Statistics to report on the cache behavior.
      @metrics : ArrowReaderMetrics

      def initialize(
        @inner : ArrayReader,
        @shared_cache : RowGroupCache,
        @column_idx : Int32,
        @role : CacheRole,
        @metrics : ArrowReaderMetrics,
      )
        @outer_position = 0
        @inner_position = 0
        @batch_size = @shared_cache.batch_size
        @selections = ::Arrow::BooleanBufferBuilder.new(0)
        @local_cache = {} of BatchID => ::Arrow::ArrayBase
      end

      def data_type : ::Arrow::DataType
        @inner.data_type
      end

      private def batch_id_from_position(row_id : Int32) : BatchID
        BatchID.new(row_id // @batch_size)
      end

      # Loads the batch with the given id (its first row offset) from the inner
      # reader.
      #
      # After this call the required batch is available in `@local_cache` and
      # may also be stored in `@shared_cache`.
      #
      # For sparse mask reads, this full-batch fallback relies on the mask
      # cursor ending every chunk at a selected row. Predicate fetch expands
      # cached columns to batch boundaries, so the batch containing that row is
      # loaded.
      private def fetch_batch(batch_id : BatchID) : Int32
        first_row_offset = batch_id.val * @batch_size
        if @inner_position < first_row_offset
          to_skip = first_row_offset - @inner_position
          skipped = @inner.skip_records(to_skip)
          if skipped != to_skip
            raise general_err!(
              "Internal Error: expected to skip #{to_skip} rows, skipped #{skipped}"
            )
          end
          @inner_position += skipped
        end

        read = @inner.read_records(@batch_size)

        # If there are no remaining records (EOF), return immediately without
        # attempting to cache an empty batch. This prevents inserting
        # zero-length arrays into the cache, which can later cause errors when
        # slicing.
        return 0 if read == 0

        array = @inner.consume_batch

        # Store in both shared cache and local cache: the shared cache is used to
        # reuse results between readers, the local cache ensures the data is
        # available for our own `consume_batch` call. If the shared cache is
        # full, we continue without caching.
        @shared_cache.insert(@column_idx, batch_id, array)
        @local_cache[batch_id] = array

        @inner_position += read
        read
      end

      # Removes batches that have been completely consumed.
      #
      # This is only called for consumer role readers. It keeps the current and
      # any future batches, which might still be needed.
      private def cleanup_consumed_batches : Nil
        current_batch_id = batch_id_from_position(@outer_position)

        if current_batch_id.val > 1
          (current_batch_id.val - 1).times do |batch_id_to_remove|
            @shared_cache.remove(@column_idx, BatchID.new(batch_id_to_remove))
          end
        end
        nil
      end

      # Upstream names this parameter `num_records`; it is `batch_size` here to
      # match the `ArrayReader` interface.
      def read_records(batch_size : Int32) : Int32
        read = 0
        while read < batch_size
          batch_id = batch_id_from_position(@outer_position)

          # Check local cache first; otherwise we are a consumer and may find the
          # batch in the shared cache.
          cached = @local_cache[batch_id]?
          unless cached
            cached = @shared_cache.get(@column_idx, batch_id)
            # Store in local cache for later use in `consume_batch`
            @local_cache[batch_id] = cached if cached
          end

          if array = cached
            array_len = array.len
            if array_len + batch_id.val * @batch_size > @outer_position
              # the cached batch has some records that we can select
              available = array_len + batch_id.val * @batch_size - @outer_position
              select_cnt = Math.min(batch_size - read, available)
              read += select_cnt
              @metrics.increment_cache_reads(select_cnt)
              @outer_position += select_cnt
              @selections.append_n(select_cnt, true)
            else
              # this is the last batch and we have used all its records
              break
            end
          else
            read_from_inner = fetch_batch(batch_id)
            # Reached end-of-file, no more records to read
            break if read_from_inner == 0

            @metrics.increment_inner_reads(read_from_inner)
            select_from_this_batch =
              Math.min(batch_size - read, @inner_position - @outer_position)
            read += select_from_this_batch
            @outer_position += select_from_this_batch
            @selections.append_n(select_from_this_batch, true)

            # this is the last batch from the inner reader
            break if read_from_inner < @batch_size
          end
        end
        read
      end

      def skip_records(num_records : Int32) : Int32
        skipped = 0
        while skipped < num_records
          size = Math.min(num_records - skipped, @batch_size)
          skipped += size
          @selections.append_n(size, false)
          @outer_position += size
        end
        num_records
      end

      def consume_batch : ::Arrow::ArrayBase
        row_count = @selections.len
        return ::Arrow.new_empty_array(@inner.data_type) if row_count == 0

        start_position = @outer_position - row_count
        selection_buffer = @selections.finish

        start_batch = start_position // @batch_size
        end_batch = (start_position + row_count - 1) // @batch_size

        selected_arrays = [] of ::Arrow::ArrayBase
        (start_batch..end_batch).each do |batch_offset|
          batch_start = batch_offset * @batch_size
          batch_end = batch_start + @batch_size - 1
          batch_id = batch_id_from_position(batch_start)

          # Calculate the overlap between the start position and the batch
          overlap_start = Math.max(start_position, batch_start)
          overlap_end = Math.min(start_position + row_count - 1, batch_end)
          next if overlap_start > overlap_end

          selection_start = overlap_start - start_position
          selection_length = overlap_end - overlap_start + 1
          mask = selection_buffer.slice(selection_start, selection_length)
          next if mask.count_set_bits == 0

          mask_array = ::Arrow::BooleanArray.new(mask)
          # Read from the local cache instead of the shared cache to avoid cache
          # eviction issues
          cached = @local_cache[batch_id]? ||
                   raise general_err!(
                     "Internal Error: data must be already cached in the read_records call, this is a bug"
                   )
          cached = cached.slice(overlap_start - batch_start, selection_length)
          selected_arrays << ::Arrow.filter(cached, mask_array)
        end

        @selections = ::Arrow::BooleanBufferBuilder.new(0)

        # Only remove batches from the local buffer that are completely behind
        # the current position; keep the current and any future batches as they
        # might still be needed.
        current_batch_id = batch_id_from_position(@outer_position)
        @local_cache.reject! { |batch_id, _| batch_id.val < current_batch_id.val }

        # For consumers, clean up batches that have been completely consumed.
        # This reduces the memory usage of the shared cache.
        cleanup_consumed_batches if @role.consumer?

        case selected_arrays.size
        when 0
          ::Arrow.new_empty_array(@inner.data_type)
        when 1
          selected_arrays[0]
        else
          ::Arrow.concat(selected_arrays)
        end
      end

      # Definition levels of the data read by the last call, if any.
      #
      # Always `nil`: we don't allow a nullable parent for now.
      def def_levels : Array(Int16)?
        nil
      end

      # Repetition levels of the data read by the last call, if any.
      def rep_levels : Array(Int16)?
        nil
      end

      def max_def_level : Int16
        @inner.max_def_level
      end
    end
  end
end
