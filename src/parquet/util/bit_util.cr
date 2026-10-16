# Ported from `parquet/src/util/bit_util.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `BitWriter`, `BitReader`, and the small bit helpers
# (`ceil`/`trailing_bits`/`num_required_bits`/`get_bit`/`read_num_bytes`/
# `compress`). Divergences: Rust's `FromBytes`/`FromBitpacked` traits (with
# associated consts and SIMD `unpack_batch`) are replaced by explicit `T.class`
# dispatch and a scalar `get_batch` loop; `put_aligned` encodes via
# `IO::ByteFormat::LittleEndian` rather than `AsBytes`.
module Parquet
  module Util
    module BitUtil
      # Maximum byte length for a VLQ encoded integer (10 for i64).
      MAX_VLQ_BYTE_LEN = 10

      BIT_MASK = StaticArray[1_u8, 2_u8, 4_u8, 8_u8, 16_u8, 32_u8, 64_u8, 128_u8]

      # Returns the ceil of `value / divisor`.
      def self.ceil(value : T, divisor : T) : T forall T
        (value + divisor - 1) // divisor
      end

      # Returns the `num_bits` least-significant bits of `v`.
      def self.trailing_bits(v : UInt64, num_bits : Int32) : UInt64
        num_bits >= 64 ? v : v & ((1_u64 << num_bits) - 1_u64)
      end

      # Returns the minimum number of bits needed to represent `x`.
      def self.num_required_bits(x : UInt64) : UInt8
        (64 - x.leading_zeros_count).to_u8
      end

      # Returns whether bit `i` in `data` is set.
      def self.get_bit(data : Bytes, i : Int32) : Bool
        (data[i >> 3] & BIT_MASK[i & 7]) != 0
      end

      # Parallel bit extract: packs the `mask`-selected bits of `value`.
      def self.compress(value : UInt64, mask : UInt64) : UInt64
        m = mask
        result = 0_u64
        dest_bit = 1_u64
        while m != 0
          lowest = m & (~m + 1_u64)
          result |= dest_bit if (value & lowest) != 0
          dest_bit <<= 1
          m ^= lowest
        end
        result
      end

      # Reads `size` bytes from `src` as a little-endian `T`, zero-padding.
      def self.read_num_bytes(type : T.class, size : Int32, src : Bytes) : T forall T
        buffer = Bytes.new(sizeof(T))
        buffer[0, size].copy_from(src[0, size])
        IO::Memory.new(buffer).read_bytes(T, IO::ByteFormat::LittleEndian)
      end

      # Converts the low bits of `raw` to `T` (the `FromBitpacked::from_u64`
      # operation). Integer conversions reinterpret the low bits.
      def self.from_u64(type : T.class, raw : UInt64) : T forall T
        {% if T == Bool %}
          raw != 0
        {% elsif T == Int8 %}
          raw.to_u8!.unsafe_as(Int8)
        {% elsif T == Int16 %}
          raw.to_u16!.unsafe_as(Int16)
        {% elsif T == Int32 %}
          raw.to_u32!.unsafe_as(Int32)
        {% elsif T == Int64 %}
          raw.unsafe_as(Int64)
        {% elsif T == UInt8 %}
          raw.to_u8!
        {% elsif T == UInt16 %}
          raw.to_u16!
        {% elsif T == UInt32 %}
          raw.to_u32!
        {% elsif T == UInt64 %}
          raw
        {% else %}
          {% raise "BitUtil.from_u64: unsupported type #{T}" %}
        {% end %}
      end
    end

    # Writes bit packed values to an in-memory buffer.
    class BitWriter
      @buffer : Array(UInt8)
      @buffered_values : UInt64
      @bit_offset : Int32

      def initialize(initial_capacity : Int32)
        @buffer = Array(UInt8).new(initial_capacity)
        @buffered_values = 0_u64
        @bit_offset = 0
      end

      # Creates a writer appending to an existing buffer.
      def self.new_from_buf(buffer : Array(UInt8)) : BitWriter
        writer = new(buffer.size)
        writer.buffer_replace(buffer)
        writer
      end

      # Flushes buffered bits and returns the underlying buffer.
      def consume : Bytes
        flush
        buffer_slice.dup
      end

      # Flushes buffered bits and returns a view of the buffer.
      def flush_buffer : Bytes
        flush
        buffer_slice
      end

      # Clears the internal state.
      def clear : Nil
        @buffer.clear
        @buffered_values = 0_u64
        @bit_offset = 0
      end

      # Flushes buffered bits to the next byte boundary.
      def flush : Nil
        num_bytes = BitUtil.ceil(@bit_offset, 8)
        @buffer.concat(u64_le_bytes(@buffered_values)[0, num_bytes])
        @buffered_values = 0_u64
        @bit_offset = 0
      end

      # Reserves `num_bytes` zero-filled bytes, returning the start offset.
      def skip(num_bytes : Int32) : Int32
        flush
        result = @buffer.size
        @buffer.concat(Array(UInt8).new(num_bytes, 0_u8))
        result
      end

      # Reserves `num_bytes` bytes and returns a writable view of them.
      def get_next_byte_ptr(num_bytes : Int32) : Bytes
        offset = skip(num_bytes)
        buffer_slice[offset, num_bytes]
      end

      # Total bytes written, rounded up to a whole byte.
      def bytes_written : Int32
        @buffer.size + BitUtil.ceil(@bit_offset, 8)
      end

      # A view of the bytes flushed to the underlying buffer.
      def buffer : Bytes
        buffer_slice
      end

      # The current offset within the output buffer.
      def byte_offset : Int32
        @buffer.size
      end

      # Overwrites the byte at `offset`.
      def write_at(offset : Int32, value : UInt8) : Nil
        @buffer[offset] = value
      end

      # Writes the `num_bits` least-significant bits of `v`.
      def put_value(v : UInt64, num_bits : Int32) : Nil
        @buffered_values |= v << @bit_offset
        @bit_offset += num_bits
        if @bit_offset >= 64
          @buffer.concat(u64_le_bytes(@buffered_values))
          @bit_offset -= 64
          shift = num_bits - @bit_offset
          @buffered_values = (shift >= 0 && shift < 64) ? (v >> shift) : 0_u64
        end
      end

      # Writes the first `num_bytes` little-endian bytes of `value`.
      def put_aligned(value : T, num_bytes : Int32) : Nil forall T
        flush
        slice = le_bytes(value)
        @buffer.concat(slice[0, Math.min(num_bytes, slice.size)])
      end

      # Writes `value` at `offset`, overwriting existing bytes.
      def put_aligned_offset(value : T, num_bytes : Int32, offset : Int32) : Nil forall T
        slice = le_bytes(value)
        len = Math.min(num_bytes, slice.size)
        buffer_slice[offset, len].copy_from(slice[0, len])
      end

      # Writes `v` in VLQ encoding.
      def put_vlq_int(v : UInt64) : Nil
        value = v
        while (value & 0xFFFFFFFFFFFFFF80_u64) != 0
          put_aligned(((value & 0x7F) | 0x80).to_u8, 1)
          value >>= 7
        end
        put_aligned((value & 0x7F).to_u8, 1)
      end

      # Writes `v` in zigzag-VLQ encoding.
      def put_zigzag_vlq_int(v : Int64) : Nil
        u = (v.unsafe_as(UInt64) << 1) ^ (v < 0 ? UInt64::MAX : 0_u64)
        put_vlq_int(u)
      end

      # Estimated heap memory used by this writer, in bytes.
      def estimated_memory_size : Int32
        @buffer.size
      end

      protected def buffer_replace(buffer : Array(UInt8)) : Nil
        @buffer = buffer
      end

      private def buffer_slice : Bytes
        Slice.new(@buffer.to_unsafe, @buffer.size)
      end

      private def u64_le_bytes(value : UInt64) : Bytes
        buffer = Bytes.new(8)
        IO::Memory.new(buffer).write_bytes(value, IO::ByteFormat::LittleEndian)
        buffer
      end

      private def le_bytes(value : T) : Bytes forall T
        io = IO::Memory.new(sizeof(T))
        io.write_bytes(value, IO::ByteFormat::LittleEndian)
        io.to_slice.dup
      end
    end

    # Reads bit packed values from an in-memory buffer.
    class BitReader
      @buffer : Bytes
      @buffered_values : UInt64
      @byte_offset : Int32
      @bit_offset : Int32

      def initialize(@buffer : Bytes)
        @buffered_values = 0_u64
        @byte_offset = 0
        @bit_offset = 0
      end

      # Creates a reader from a byte array.
      def self.from(buffer : Array(UInt8)) : BitReader
        new(Slice.new(buffer.to_unsafe, buffer.size).dup)
      end

      # Resets this reader to read from the start of `buffer`.
      def reset(buffer : Bytes) : Nil
        @buffer = buffer
        @buffered_values = 0_u64
        @byte_offset = 0
        @bit_offset = 0
      end

      # Returns the current byte offset, rounded up to a whole byte.
      # ameba:disable Naming/AccessorMethodName
      def get_byte_offset : Int32
        @byte_offset + BitUtil.ceil(@bit_offset, 8)
      end

      # Reads a single `num_bits`-bit value as `T`.
      def get_value(type : T.class, num_bits : Int32) : T? forall T
        raw = next_bits(num_bits)
        return nil if raw.nil?

        BitUtil.from_u64(T, raw)
      end

      # Reads up to `batch.size` `num_bits`-bit values, returning the count.
      def get_batch(type : T.class, batch : Array(T), num_bits : Int32) : Int32 forall T
        get_batch(T, Slice.new(batch.to_unsafe, batch.size), num_bits)
      end

      # Reads up to `batch.size` `num_bits`-bit values into a slice.
      def get_batch(type : T.class, batch : Slice(T), num_bits : Int32) : Int32 forall T
        remaining_bits = (@buffer.size - @byte_offset) * 8 - @bit_offset
        values_to_read = Math.min(batch.size, remaining_bits // num_bits)

        i = 0
        while i < values_to_read
          batch[i] = BitUtil.from_u64(T, next_bits(num_bits).as(UInt64))
          i += 1
        end
        values_to_read
      end

      # Skips `num_values` `num_bits`-bit values, returning the count skipped.
      def skip(num_values : Int32, num_bits : Int32) : Int32
        needed_bits = num_bits * num_values
        remaining_bits = (@buffer.size - @byte_offset) * 8 - @bit_offset
        values_to_read = remaining_bits < needed_bits ? remaining_bits // num_bits : num_values

        end_bit_offset = @byte_offset * 8 + values_to_read * num_bits + @bit_offset
        @byte_offset = end_bit_offset // 8
        @bit_offset = end_bit_offset % 8
        load_buffered_values if @bit_offset != 0
        values_to_read
      end

      # Reads a `num_bytes`-sized little-endian value of type `T`.
      def get_aligned(type : T.class, num_bytes : Int32) : T? forall T
        @byte_offset = get_byte_offset
        @bit_offset = 0
        return nil if @byte_offset + num_bytes > @buffer.size

        value = BitUtil.read_num_bytes(T, num_bytes, @buffer[@byte_offset, num_bytes])
        @byte_offset += num_bytes
        value
      end

      # Reads a VLQ-encoded little-endian integer.
      # ameba:disable Naming/AccessorMethodName
      def get_vlq_int : Int64?
        @byte_offset = get_byte_offset
        @bit_offset = 0

        base = @byte_offset
        shift = 0
        v = 0_i64
        i = 0
        while base + i < @buffer.size
          byte = @buffer[base + i]
          v |= (byte & 0x7F).to_i64 << shift
          shift += 7
          if shift > BitUtil::MAX_VLQ_BYTE_LEN * 7
            raise general_err!("Num of bytes exceed MAX_VLQ_BYTE_LEN (#{BitUtil::MAX_VLQ_BYTE_LEN})")
          end
          if (byte & 0x80) == 0
            @byte_offset = base + i + 1
            return v
          end
          i += 1
        end
        nil
      end

      # Reads a zigzag-VLQ-encoded little-endian integer.
      # ameba:disable Naming/AccessorMethodName
      def get_zigzag_vlq_int : Int64?
        get_vlq_int.try do |v|
          u = v.unsafe_as(UInt64)
          (u >> 1).to_i64! ^ -((u & 1_u64).to_i64)
        end
      end

      private def next_bits(num_bits : Int32) : UInt64?
        if @byte_offset * 8 + @bit_offset + num_bits > @buffer.size * 8
          return nil
        end

        load_buffered_values if @bit_offset == 0

        v = BitUtil.trailing_bits(@buffered_values, @bit_offset + num_bits) >> @bit_offset
        @bit_offset += num_bits

        if @bit_offset >= 64
          @byte_offset += 8
          @bit_offset -= 64
          if @bit_offset != 0
            load_buffered_values
            v |= BitUtil.trailing_bits(@buffered_values, @bit_offset) << (num_bits - @bit_offset)
          end
        end
        v
      end

      private def load_buffered_values : Nil
        bytes_to_read = Math.min(@buffer.size - @byte_offset, 8)
        @buffered_values = BitUtil.read_num_bytes(UInt64, bytes_to_read, @buffer[@byte_offset, bytes_to_read])
      end
    end
  end
end
