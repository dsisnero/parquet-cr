# Ported from `parquet/src/file/metadata/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Currently ports `LevelHistogram`. The remaining metadata types
# (`ParquetMetaData`, `FileMetaData`, `RowGroupMetaData`, `ColumnChunkMetaData`,
# builders, and thrift conversion) are ported in follow-up features (see
# `plans/parity.md`).
module Parquet
  module File
    module Metadata
      # A histogram of repetition or definition levels.
      #
      # Each histogram is a vector of length `max_level + 1`; the value at index
      # `i` is the number of values at level `i`.
      struct LevelHistogram
        getter inner : Array(Int64)

        def initialize(@inner : Array(Int64))
        end

        # Creates a new histogram of length `max_level + 1`.
        #
        # Returns `nil` when `max_level <= 0` (histograms are unnecessary).
        def self.try_new(max_level : Int16) : LevelHistogram?
          if max_level > 0
            new(Array(Int64).new(max_level.to_i32 + 1, 0_i64))
          end
        end

        def self.from(inner : Array(Int64)) : LevelHistogram
          new(inner)
        end

        # Returns a reference to the histogram's values.
        def values : Array(Int64)
          @inner
        end

        # Returns the inner vector, consuming self.
        def into_inner : Array(Int64)
          @inner
        end

        # Returns the histogram value at the given index, or `nil` if out of bounds.
        def get(index : Int) : Int64?
          @inner[index]?
        end

        # Adds the values from the other histogram to this histogram.
        def add(other : LevelHistogram) : Nil
          raise ArgumentError.new("histogram length mismatch: #{len} != #{other.len}") unless len == other.len

          other.inner.each_with_index { |value, i| @inner[i] += value }
        end

        def len : Int32
          @inner.size
        end

        def empty? : Bool
          @inner.empty?
        end

        # Sets the values of all histogram levels to 0.
        def reset : Nil
          @inner.fill(0_i64)
        end

        # Increments the count for a level value by `count`.
        def increment_by(level : Int16, count : Int64) : Nil
          @inner[level.to_i32] += count
        end

        # Updates histogram values using provided repetition levels.
        def update_from_levels(levels : Array(Int16)) : Nil
          levels.each { |level| increment_by(level, 1_i64) }
        end
      end
    end
  end
end
