require "../../errors"

# Ported from `parquet/src/arrow/buffer/bit_util.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: upstream's `arrow_buffer::bit_chunk_iterator::UnalignedBitChunk`
# SIMD/word kernels are scalar bit loops here; `iter_set_bits_rev` returns an
# `Array(Int32)` of positions (upstream returns a lazy iterator), and
# `sign_extend_be<const N>` takes the target length as a parameter.
module Parquet
  module Arrow
    # Bit-level helpers shared by the arrow reader/writer layers.
    module Buffer
      # Counts the set bits in the half-open bit range `[start, end)` of `bytes`.
      def self.count_set_bits(bytes : Bytes, start : Int32, end_ : Int32) : Int32
        count = 0
        index = start
        while index < end_
          byte = bytes[index // 8]
          count += 1 if (byte & (1_u8 << (index % 8))) != 0
          index += 1
        end
        count
      end

      # Returns the positions of the set bits in `bytes`, highest first.
      def self.iter_set_bits_rev(bytes : Bytes) : Array(Int32)
        positions = [] of Int32
        index = bytes.size * 8 - 1
        while index >= 0
          byte = bytes[index // 8]
          positions << index if (byte & (1_u8 << (index % 8))) != 0
          index -= 1
        end
        positions
      end

      # Sign-extends the big-endian integer `bytes` to `size` bytes.
      def self.sign_extend_be(bytes : Bytes, size : Int32) : Bytes
        if bytes.size > size
          raise general_err!("Array too large, expected less than #{size}")
        end

        fill = (bytes[0] & 128_u8) == 128_u8 ? 0xFF_u8 : 0x00_u8
        result = Bytes.new(size, fill)
        bytes.copy_to(result[size - bytes.size, bytes.size])
        result
      end
    end
  end
end
