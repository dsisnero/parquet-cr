require "../errors"

# Ported from `parquet/src/util/bit_pack.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the vectorised bit-packing unpack helpers (`unpack8`/`16`/`32`/`64`).
# Divergences: Rust's `unpack::<NUM_BITS>` const generic is a `num_bits`
# parameter (the scalar loop replaces `seq_macro` and the SIMD kernels); the
# fixed-size `[T; bits]` output is an `Array(T)` of the same length.
module Parquet
  module Util
    # Bit-packing helpers (`unpack8`/`16`/`32`/`64`).
    module BitPack
      # Unpacks packed `input` into `output` with a bit width of `num_bits`.
      def self.unpack8(input : Bytes, output : Array(UInt8), num_bits : Int32) : Nil
        unpack(UInt8, input, output, num_bits, 8)
      end

      # Unpacks packed `input` into `output` with a bit width of `num_bits`.
      def self.unpack16(input : Bytes, output : Array(UInt16), num_bits : Int32) : Nil
        unpack(UInt16, input, output, num_bits, 16)
      end

      # Unpacks packed `input` into `output` with a bit width of `num_bits`.
      def self.unpack32(input : Bytes, output : Array(UInt32), num_bits : Int32) : Nil
        unpack(UInt32, input, output, num_bits, 32)
      end

      # Unpacks packed `input` into `output` with a bit width of `num_bits`.
      def self.unpack64(input : Bytes, output : Array(UInt64), num_bits : Int32) : Nil
        unpack(UInt64, input, output, num_bits, 64)
      end

      private def self.unpack(
        type : T.class,
        input : Bytes,
        output : Array(T),
        num_bits : Int32,
        bits : Int32,
      ) : Nil forall T
        if num_bits == 0
          output.size.times { |i| output[i] = T.new(0) }
          return
        end

        raise general_err!("invalid num_bits #{num_bits}") if num_bits > bits

        width = bits // 8
        mask = num_bits == bits ? T::MAX : ((T.new(1) << num_bits) - 1)
        if input.size < num_bits * width
          raise general_err!("input too short: #{input.size} < #{num_bits * width}")
        end

        reader = ->(idx : Int32) { IO::ByteFormat::LittleEndian.decode(T, input[idx * width, width]) }

        output.size.times do |i|
          start_bit = i * num_bits
          end_bit = start_bit + num_bits
          start_bit_offset = start_bit % bits
          end_bit_offset = end_bit % bits
          start_byte = start_bit // bits
          end_byte = end_bit // bits
          if start_byte != end_byte && end_bit_offset != 0
            a = reader.call(start_byte) >> start_bit_offset
            b = reader.call(end_byte) << (num_bits - end_bit_offset)
            output[i] = a | (b & mask)
          else
            output[i] = (reader.call(start_byte) >> start_bit_offset) & mask
          end
        end
        nil
      end
    end
  end
end
