require "../errors"

# Ported from `parquet/src/arrow/arrow_reader/metrics.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `ArrowReaderMetrics` collects metrics about the arrow reader. Create one via
# `ArrowReaderMetrics.enabled` and pass it to the reader builder with
# `with_metrics`; copies of the metrics share the same counters (upstream keeps
# them in an `Arc`).
#
# Note: only the predicate cache (`CachedArrayReader`) increments these
# counters, so a synchronous read leaves them at zero — exactly as upstream,
# where the synchronous reader does not use the cache either (arrow-rs#8000).
#
# Divergences: `usize` counters are `Int32` (updated atomically, as upstream);
# the `Disabled`/`Enabled(Arc<..>)` enum is a class holding an optional inner
# class, and `#dup` shares the inner counters like cloning the upstream `Arc`.
# The `increment_*` methods are public (upstream `pub(crate)`) because Crystal
# has no crate-private visibility.
module Parquet
  module Arrow
    # Collects metrics about the arrow reader (upstream `ArrowReaderMetrics`).
    class ArrowReaderMetrics
      # The actual counters, shared by every copy of the metrics (upstream
      # `ArrowReaderMetricsInner`).
      class Inner
        def initialize
          @records_read_from_inner = Atomic(Int32).new(0)
          @records_read_from_cache = Atomic(Int32).new(0)
        end

        # Total number of records read from the inner reader (uncached).
        def records_read_from_inner : Int32
          @records_read_from_inner.get
        end

        # Total number of records read from previously cached pages.
        def records_read_from_cache : Int32
          @records_read_from_cache.get
        end

        def increment_inner_reads(count : Int32) : Nil
          @records_read_from_inner.add(count)
        end

        def increment_cache_reads(count : Int32) : Nil
          @records_read_from_cache.add(count)
        end
      end

      @inner : Inner?

      def initialize(@inner : Inner? = nil)
      end

      # Creates disabled metrics (upstream `ArrowReaderMetrics::disabled`).
      def self.disabled : ArrowReaderMetrics
        new
      end

      # Creates enabled metrics with fresh counters (upstream
      # `ArrowReaderMetrics::enabled`).
      def self.enabled : ArrowReaderMetrics
        new(Inner.new)
      end

      # Whether metrics are collected.
      def enabled? : Bool
        !@inner.nil?
      end

      # Whether metrics collection is disabled (the default).
      def disabled? : Bool
        @inner.nil?
      end

      # Predicate cache: number of records read directly from the inner reader.
      #
      # This is the total number of records read from the inner reader (that is,
      # actually decoding), measuring the work that could not be avoided with
      # caching. It counts records across all columns, so reading two columns of
      # 100 records each returns 200.
      #
      # Returns nil if metrics are disabled.
      def records_read_from_inner : Int32?
        @inner.try(&.records_read_from_inner)
      end

      # Predicate cache: number of records read from the cache.
      #
      # Counts records across all columns. Returns nil if metrics are disabled.
      def records_read_from_cache : Int32?
        @inner.try(&.records_read_from_cache)
      end

      # Increments the count of records read from the inner reader.
      def increment_inner_reads(count : Int32) : Nil
        @inner.try(&.increment_inner_reads(count))
      end

      # Increments the count of records read from the cache.
      def increment_cache_reads(count : Int32) : Nil
        @inner.try(&.increment_cache_reads(count))
      end
    end
  end
end
