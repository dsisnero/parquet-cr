# Ported from `parquet/src/encodings/rle.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `RleEncoder` and `RleDecoder` (the RLE/bit-packing hybrid encoding).
# Divergences: `FromBitpacked` generic dispatch becomes `T.class`; the decoder's
# batch paths read values individually rather than through the SIMD `unpack`
# routines; `is_accumulating_rle` -> `accumulating_rle?`.
module Parquet
  module Encodings
    # Maximum bit width for dictionary page indices (i32).
    MAX_RLE_DICTIONARY_BIT_WIDTH = 32_u8

    # Number of values in one bit-packed group.
    BIT_PACK_GROUP_SIZE = 8

    # Maximum groups of `BIT_PACK_GROUP_SIZE` values per bit-packed run.
    MAX_GROUPS_PER_BIT_PACKED_RUN = 1 << 6

    # An RLE/bit-packing hybrid encoder.
    class RleEncoder
      def initialize(@bit_width : Int32, @bit_writer : Util::BitWriter)
        @buffered_values = Array(UInt64).new(BIT_PACK_GROUP_SIZE, 0_u64)
        @num_buffered_values = 0
        @current_value = 0_u64
        @repeat_count = 0
        @bit_packed_count = 0
        @indicator_byte_pos = -1_i64
      end

      # Creates a new encoder with an internal buffer of `buffer_len`.
      def self.new(bit_width : Int32, buffer_len : Int32) : RleEncoder
        new(bit_width, Util::BitWriter.new(buffer_len))
      end

      # Initializes an encoder appending to an existing buffer.
      def self.new_from_buf(bit_width : Int32, buffer : Array(UInt8)) : RleEncoder
        new(bit_width, Util::BitWriter.new_from_buf(buffer))
      end

      # Maximum buffer size to encode `num_values` values with `bit_width`.
      def self.max_buffer_size(bit_width : Int32, num_values : Int32) : Int32
        num_runs = Util::BitUtil.ceil(num_values, BIT_PACK_GROUP_SIZE)
        bytes_per_run = bit_width
        bit_packed_max_size = num_runs + num_runs * bytes_per_run

        rle_len_prefix = 1
        min_rle_run_size = rle_len_prefix + Util::BitUtil.ceil(bit_width, 8)
        rle_max_size = num_runs * min_rle_run_size

        Math.max(bit_packed_max_size, rle_max_size)
      end

      # Whether the encoder is currently in RLE accumulation mode for `value`.
      def accumulating_rle?(value : UInt64) : Bool
        @repeat_count >= BIT_PACK_GROUP_SIZE && @current_value == value
      end

      # Extends the current RLE run by `count` additional repetitions.
      def extend_run(count : Int32) : Nil
        @repeat_count += count
      end

      # Encodes `value`, which must be representable with `bit_width` bits.
      def put(value : UInt64) : Nil
        if @current_value == value
          @repeat_count += 1
          return if @repeat_count > BIT_PACK_GROUP_SIZE
        else
          flush_rle_run if @repeat_count >= BIT_PACK_GROUP_SIZE
          @repeat_count = 1
          @current_value = value
        end

        @buffered_values[@num_buffered_values] = value
        @num_buffered_values += 1
        flush_buffered_values if @num_buffered_values == BIT_PACK_GROUP_SIZE
      end

      # A view of the underlying buffers.
      def buffer : Bytes
        @bit_writer.buffer
      end

      # The number of bytes written.
      def len : Int32
        @bit_writer.bytes_written
      end

      def empty? : Bool
        @bit_writer.bytes_written == 0
      end

      # Flushes and returns the underlying buffer.
      def consume : Bytes
        flush
        @bit_writer.consume
      end

      # Flushes and returns a view of the underlying buffer.
      def flush_buffer : Bytes
        flush
        @bit_writer.flush_buffer
      end

      # Clears the internal state so the encoder can be reused.
      def clear : Nil
        @bit_writer.clear
        @num_buffered_values = 0
        @current_value = 0_u64
        @repeat_count = 0
        @bit_packed_count = 0
        @indicator_byte_pos = -1
      end

      # Advances the buffer by `num_bytes` zero bytes.
      def skip(num_bytes : Int32) : Nil
        @bit_writer.skip(num_bytes)
      end

      # Flushes all remaining values.
      def flush : Nil
        if @bit_packed_count > 0 || @repeat_count > 0 || @num_buffered_values > 0
          all_repeat = @bit_packed_count == 0 &&
                       (@repeat_count == @num_buffered_values || @num_buffered_values == 0)
          if @repeat_count > 0 && all_repeat
            flush_rle_run
          else
            if @num_buffered_values > 0
              while @num_buffered_values < BIT_PACK_GROUP_SIZE
                @buffered_values[@num_buffered_values] = 0_u64
                @num_buffered_values += 1
              end
            end
            @bit_packed_count += @num_buffered_values
            flush_bit_packed_run(true)
            @repeat_count = 0
          end
        end
      end

      # Estimated memory size of this encoder.
      def estimated_memory_size : Int32
        @bit_writer.estimated_memory_size
      end

      private def flush_rle_run : Nil
        indicator_value = @repeat_count.to_u64 << 1
        @bit_writer.put_vlq_int(indicator_value)
        @bit_writer.put_aligned(@current_value, Util::BitUtil.ceil(@bit_width, 8))
        @num_buffered_values = 0
        @repeat_count = 0
      end

      private def flush_bit_packed_run(end_current_run : Bool) : Nil
        @indicator_byte_pos = @bit_writer.skip(1).to_i64 if @indicator_byte_pos < 0

        @num_buffered_values.times do |i|
          @bit_writer.put_value(@buffered_values[i], @bit_width)
        end
        @num_buffered_values = 0
        finish_bit_packed_run if end_current_run
      end

      private def finish_bit_packed_run : Nil
        num_groups = @bit_packed_count // BIT_PACK_GROUP_SIZE
        indicator_byte = ((num_groups << 1) | 1).to_u8!
        @bit_writer.put_aligned_offset(indicator_byte, 1, @indicator_byte_pos.to_i32)
        @indicator_byte_pos = -1
        @bit_packed_count = 0
      end

      private def flush_buffered_values : Nil
        if @repeat_count >= BIT_PACK_GROUP_SIZE
          @num_buffered_values = 0
          finish_bit_packed_run if @bit_packed_count > 0
          return
        end

        @bit_packed_count += @num_buffered_values
        num_groups = @bit_packed_count // BIT_PACK_GROUP_SIZE
        if num_groups + 1 >= MAX_GROUPS_PER_BIT_PACKED_RUN
          flush_bit_packed_run(true)
        else
          flush_bit_packed_run(false)
        end
        @repeat_count = 0
      end
    end

    # An RLE/bit-packing hybrid decoder.
    class RleDecoder
      def initialize(@bit_width : Int32)
        @bit_reader = nil.as(Util::BitReader?)
        @rle_left = 0_u32
        @bit_packed_left = 0_u32
        @current_value = nil.as(UInt64?)
      end

      # Sets the input data and resets the decoder state.
      # ameba:disable Naming/AccessorMethodName
      def set_data(data : Bytes) : Nil
        if reader = @bit_reader
          reader.reset(data)
        else
          @bit_reader = Util::BitReader.new(data)
        end
        reload
        nil
      end

      # Reads the next value, or `nil` when the data is exhausted.
      def get(type : T.class) : T? forall T
        while @rle_left == 0 && @bit_packed_left == 0
          return nil unless reload
        end

        if @rle_left > 0
          current = @current_value || raise general_err!("current_value should be Some")
          @rle_left -= 1
          Util::BitUtil.from_u64(T, current)
        else
          reader = @bit_reader || raise general_err!("bit_reader should be Some")
          value = reader.get_value(T, @bit_width)
          raise eof_err!("Not enough data for 'bit_packed_value'") if value.nil?

          @bit_packed_left -= 1
          value
        end
      end

      # Reads up to `buffer.size` values, returning the count read.
      def get_batch(type : T.class, buffer : Array(T)) : Int32 forall T
        values_read = 0
        while values_read < buffer.size
          if @rle_left > 0
            num_values = Math.min(buffer.size - values_read, @rle_left.to_i32)
            repeated = Util::BitUtil.from_u64(T, @current_value.as(UInt64))
            num_values.times { |k| buffer[values_read + k] = repeated }
            @rle_left -= num_values
            values_read += num_values
          elsif @bit_packed_left > 0
            num_values = Math.min(buffer.size - values_read, @bit_packed_left.to_i32)
            reader = @bit_reader || raise general_err!("bit_reader should be set")
            slice = Slice.new(buffer.to_unsafe, buffer.size)[values_read, num_values]
            num_values = reader.get_batch(T, slice, @bit_width)
            if num_values == 0
              @bit_packed_left = 0
              next
            end
            @bit_packed_left -= num_values
            values_read += num_values
          elsif !reload
            break
          end
        end
        values_read
      end

      # Skips up to `num_values` values, returning the count skipped.
      def skip(num_values : Int32) : Int32
        values_skipped = 0
        while values_skipped < num_values
          if @rle_left > 0
            count = Math.min(num_values - values_skipped, @rle_left.to_i32)
            @rle_left -= count
            values_skipped += count
          elsif @bit_packed_left > 0
            count = Math.min(num_values - values_skipped, @bit_packed_left.to_i32)
            reader = @bit_reader || raise general_err!("bit_reader should be set")
            count = reader.skip(count, @bit_width)
            if count == 0
              @bit_packed_left = 0
              next
            end
            @bit_packed_left -= count
            values_skipped += count
          elsif !reload
            break
          end
        end
        values_skipped
      end

      # Reads up to `max_values` dictionary indices, mapping through `dict`.
      def get_batch_with_dict(type : T.class, dict : Array(T), buffer : Array(T), max_values : Int32) : Int32 forall T
        values_read = 0
        while values_read < max_values
          if @rle_left > 0
            num_values = Math.min(max_values - values_read, @rle_left.to_i32)
            dict_idx = @current_value.as(UInt64).to_i32
            raise dict_oob(dict_idx, dict.size) if dict_idx >= dict.size

            dict_value = dict[dict_idx]
            num_values.times { |k| buffer[values_read + k] = dict_value }
            @rle_left -= num_values
            values_read += num_values
          elsif @bit_packed_left > 0
            reader = @bit_reader || raise general_err!("bit_reader should be set")
            idx = reader.get_value(Int32, @bit_width)
            if idx.nil?
              @bit_packed_left = 0
              break
            end
            raise dict_oob(idx, dict.size) if idx >= dict.size

            buffer[values_read] = dict[idx]
            @bit_packed_left -= 1
            values_read += 1
          elsif !reload
            break
          end
        end
        values_read
      end

      private def dict_oob(index : Int32, len : Int32) : Parquet::ParquetError
        general_err!("dictionary index out of bounds: the len is #{len} but the index is #{index}")
      end

      private def reload : Bool
        reader = @bit_reader || raise general_err!("bit_reader should be set")
        indicator_value = reader.get_vlq_int
        return false if indicator_value.nil? || indicator_value == 0

        if (indicator_value & 1) == 1
          @bit_packed_left = ((indicator_value >> 1) * BIT_PACK_GROUP_SIZE).to_u32!
        else
          @rle_left = (indicator_value >> 1).to_u32!
          value_width = Util::BitUtil.ceil(@bit_width, 8)
          @current_value = reader.get_aligned(UInt64, value_width)
          if @current_value.nil?
            raise general_err!("parquet_data_error: not enough data for RLE decoding")
          end
        end
        true
      end
    end
  end
end
