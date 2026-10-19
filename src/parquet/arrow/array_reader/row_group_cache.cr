require "arrow-cr"
require "../../errors"

# Ported from `parquet/src/arrow/array_reader/row_group_cache.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `RowGroupCache` stores decoded arrow arrays at batch granularity so the same
# column is not decoded twice when it is used both by a row filter and by the
# output projection.
#
# Divergences: upstream shares the cache between readers as
# `Arc<RwLock<RowGroupCache>>` because the arrow reader is multi-threaded;
# Crystal fibers share the object directly, so `CacheOptions` and
# `CachedArrayReader` hold the cache itself. `usize` byte/row counts are `Int32`
# and the byte accounting uses `Arrow::ArrayBase#array_memory_size` for every
# type (upstream special-cases `Utf8View`, whose buffers are shared between
# arrays; the port's `buffer_memory_size` already counts the views buffer and
# the data buffers separately, so the workaround is unnecessary).
module Parquet
  module Arrow
    # Starting row ID for this batch (upstream `BatchID`).
    #
    # The `BatchID` is used to identify batches of rows within a row group. The
    # row index in the id is relative to the rows being read from the underlying
    # column reader (which might already have a `RowSelection` applied). The
    # `BatchID` for any particular row is `row_index / batch_size`; the integer
    # division ensures that rows in the same batch share the same `BatchID`,
    # which can be calculated quickly from the row index.
    struct BatchID
      getter val : Int32

      def initialize(@val : Int32)
      end
    end

    # Cache key that uniquely identifies a batch within a row group (upstream
    # `CacheKey`).
    struct CacheKey
      # Column index in the row group.
      getter column_idx : Int32
      # Starting row ID for this batch.
      getter batch_id : BatchID

      def initialize(@column_idx : Int32, @batch_id : BatchID)
      end
    end

    # Row group cache that stores decoded arrow arrays at batch granularity
    # (upstream `RowGroupCache`).
    #
    # This cache is designed to avoid duplicate decoding when the same column
    # appears in both filter predicates and output projection.
    class RowGroupCache
      @cache : Hash(CacheKey, ::Arrow::ArrayBase)
      # Cache granularity (rows per batch).
      getter batch_size : Int32
      @max_cache_bytes : Int32
      @current_cache_size : Int32

      def initialize(@batch_size : Int32, @max_cache_bytes : Int32)
        @cache = {} of CacheKey => ::Arrow::ArrayBase
        @current_cache_size = 0
      end

      # The memory used by one cached array (upstream
      # `get_array_memory_size_for_cache`).
      def self.array_memory_size(array : ::Arrow::ArrayBase) : Int32
        array.array_memory_size
      end

      # Inserts an array into the cache for the given column and starting row
      # ID, returning `false` if it would exceed the cache size limit.
      def insert(column_idx : Int32, batch_id : BatchID, array : ::Arrow::ArrayBase) : Bool
        array_size = RowGroupCache.array_memory_size(array)

        # Check if adding this array would exceed the cache size limit
        return false if @current_cache_size + array_size > @max_cache_bytes

        # Upstream asserts the key is absent here; the cache is only ever
        # inserted into for a batch that has just been decoded, so a duplicate
        # insert would mean the caller read the same batch twice.
        @cache[CacheKey.new(column_idx, batch_id)] = array
        @current_cache_size += array_size
        true
      end

      # Retrieves a cached array for the given column and batch, or `nil` on a
      # miss.
      def get(column_idx : Int32, batch_id : BatchID) : ::Arrow::ArrayBase?
        @cache[CacheKey.new(column_idx, batch_id)]?
      end

      # Removes a cached array for the given column and batch, returning whether
      # an entry was found and removed.
      def remove(column_idx : Int32, batch_id : BatchID) : Bool
        if array = @cache.delete(CacheKey.new(column_idx, batch_id))
          @current_cache_size -= RowGroupCache.array_memory_size(array)
          true
        else
          false
        end
      end

      # The number of bytes currently cached (upstream keeps this private; it is
      # exposed for the tests).
      def current_cache_size : Int32
        @current_cache_size
      end
    end
  end
end
