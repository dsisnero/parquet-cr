# Ported from `parquet/src/util/push_buffers.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: the `ChunkReader`/`Length`/`Read` impls (used by the Arrow push
# decoder) are not ported; this provides the buffer API needed by
# `ParquetMetaDataPushDecoder`.
module Parquet
  module Util
    # Holds multiple non-contiguous, caller-provided buffers of file data.
    #
    # Ranges are not coalesced (merging adjacent ranges would require copying).
    class PushBuffers
      getter file_len : UInt64
      @ranges : Array(Range(UInt64, UInt64))
      @buffers : Array(Bytes)

      def initialize(@file_len : UInt64 = 0_u64)
        @ranges = [] of Range(UInt64, UInt64)
        @buffers = [] of Bytes
      end

      # Pushes all the ranges and buffers.
      #
      # Raises if the number of ranges differs from the number of buffers.
      def push_ranges(ranges : Array(Range(UInt64, UInt64)), buffers : Array(Bytes)) : Nil
        if ranges.size != buffers.size
          raise general_err!(
            "Number of ranges (#{ranges.size}) must match number of buffers (#{buffers.size})"
          )
        end
        ranges.each_with_index { |range, i| push_range(range, buffers[i]) }
      end

      # Pushes a new range and its associated buffer.
      #
      # Raises if the buffer length does not match the range length.
      def push_range(range : Range(UInt64, UInt64), buffer : Bytes) : Nil
        expected = range.end &- range.begin
        if expected != buffer.size.to_u64
          raise general_err!(
            "Buffer length (#{buffer.size}) does not match length (#{expected}) of range #{range.begin}..#{range.end}"
          )
        end
        @ranges << range
        @buffers << buffer
      end

      # Whether this buffer contains data for the given range.
      def has_range?(range : Range(UInt64, UInt64)) : Bool
        @ranges.any? { |existing| existing.begin <= range.begin && existing.end >= range.end }
      end

      # Returns the bytes for `[start, start + length)`, raising
      # `NeedMoreDataRangeError` when the range is not covered.
      def get_bytes(start : UInt64, length : Int32) : Bytes
        requested_end = start + length.to_u64
        @ranges.each_with_index do |range, i|
          if range.begin <= start && range.end >= requested_end
            offset = (start - range.begin).to_i32
            return @buffers[i][offset, length]
          end
        end
        raise Parquet::NeedMoreDataRangeError.new(start...requested_end)
      end

      # Clears all buffered ranges and their corresponding data.
      def clear_all_ranges : Nil
        @ranges.clear
        @buffers.clear
      end

      # The total length of all buffered ranges.
      def buffered_bytes : UInt64
        @ranges.sum(0_u64) { |range| range.end - range.begin }
      end

      # Clears the buffered ranges (and their data) that exactly match one of
      # `ranges_to_clear` (upstream `clear_ranges`).
      def clear_ranges(ranges_to_clear : Enumerable(Range(UInt64, UInt64))) : Nil
        to_clear = ranges_to_clear.to_a
        new_ranges = [] of Range(UInt64, UInt64)
        new_buffers = [] of Bytes
        @ranges.each_with_index do |range, idx|
          next if to_clear.any? { |candidate| candidate.begin == range.begin && candidate.end == range.end }

          new_ranges << range
          new_buffers << @buffers[idx]
        end
        @ranges = new_ranges
        @buffers = new_buffers
        nil
      end
    end
  end
end
