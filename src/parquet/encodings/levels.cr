require "./rle"

# Ported from `parquet/src/encodings/levels.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `LevelEncoder` (definition/repetition levels, RLE + bit-packed).
# Divergence: the Rust `LevelEncoder::Rle`/`RleV2` enum is a struct carrying a
# `v2` flag plus the shared `RleEncoder`; observer callbacks are Crystal blocks.
module Parquet
  module Encodings
    # Encoder for definition/repetition levels (Rle and BitPacked, incl. v2).
    class LevelEncoder
      def initialize(@v2 : Bool, @encoder : RleEncoder)
      end

      # Streaming level encoder for Data Page v1.
      def self.v1_streaming(max_level : Int16) : LevelEncoder
        bit_width = Util::BitUtil.num_required_bits(max_level.to_u64).to_i32
        buffer = Array(UInt8).new(4, 0_u8)
        new(false, RleEncoder.new_from_buf(bit_width, buffer))
      end

      # Streaming level encoder for Data Page v2.
      def self.v2_streaming(max_level : Int16) : LevelEncoder
        bit_width = Util::BitUtil.num_required_bits(max_level.to_u64).to_i32
        new(true, RleEncoder.new_from_buf(bit_width, [] of UInt8))
      end

      # Encodes `buffer` and calls `observer(value, count)` for each run.
      def put_with_observer(buffer : Array(Int16), &observer : Int16, Int32 ->) : Int32
        i = 0
        while i < buffer.size
          value = buffer[i]
          @encoder.put(value.to_u64)
          if @encoder.accumulating_rle?(value.to_u64)
            run_len = 0
            j = i + 1
            while j < buffer.size && buffer[j] == value
              run_len += 1
              j += 1
            end
            @encoder.extend_run(run_len) if run_len > 0
            observer.call(value, 1 + run_len)
            i = j
          else
            observer.call(value, 1)
            i += 1
          end
        end
        buffer.size
      end

      # Encodes `count` repetitions of `value`, calling `observer` once.
      def put_n_with_observer(value : Int16, count : Int32, &observer : Int16, Int32 ->) : Nil
        remaining = count
        while remaining > 0 && !@encoder.accumulating_rle?(value.to_u64)
          @encoder.put(value.to_u64)
          remaining -= 1
        end
        @encoder.extend_run(remaining) if remaining > 0
        observer.call(value, count)
      end

      # Flushes and returns the encoded buffer (v1 is length-prefixed).
      def consume : Bytes
        return @encoder.consume if @v2

        encoded_data = @encoder.consume
        encoded_len = encoded_data.size - 4
        encoded_data[0, 4].copy_from(int32_le(encoded_len.to_i32))
        encoded_data
      end

      # Flushes, yields the encoded data to `block`, then resets for reuse.
      def flush_to(&block : Bytes -> R) : R forall R
        result = if @v2
                   block.call(@encoder.flush_buffer)
                 else
                   data = @encoder.flush_buffer
                   encoded_len = (data.size - 4).to_i32
                   data[0, 4].copy_from(int32_le(encoded_len))
                   block.call(data)
                 end

        @encoder.clear
        @encoder.skip(4) unless @v2
        result
      end

      private def int32_le(value : Int32) : Bytes
        buffer = Bytes.new(4)
        IO::Memory.new(buffer).write_bytes(value, IO::ByteFormat::LittleEndian)
        buffer
      end
    end
  end
end
