require "./plain"

# Ported from `parquet/src/encodings/encoding/mod.rs` (DeltaBitPackEncoder) and
# `parquet/src/encodings/decoding.rs` (DeltaBitPackDecoder)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# DELTA_BINARY_PACKED for INT32/INT64. Divergence: trait dispatch is
# `DeltaBitPackEncoder(T)`/`DeltaBitPackDecoder(T)` with `T.class`; the decoder
# `get` reads a mini-block's packed deltas via `BitReader#get_batch`; `skip`
# decodes into a scratch buffer (behaviourally identical to the upstream fast
# path).
module Parquet
  module Encodings
    # DELTA_BINARY_PACKED encoder for INT32/INT64.
    class DeltaBitPackEncoder(T)
      DEFAULT_NUM_MINI_BLOCKS     =  4
      MAX_PAGE_HEADER_WRITER_SIZE = 32
      DEFAULT_BIT_WRITER_SIZE     = 1024 * 1024

      @mini_block_size : Int32
      @num_mini_blocks : Int32
      @block_size : Int32
      @page_header_writer : Util::BitWriter
      @bit_writer : Util::BitWriter
      @total_values : Int32
      @first_value : Int64
      @current_value : Int64
      @values_in_block : Int32
      @deltas : Array(Int64)

      def initialize
        @mini_block_size = {% if T == Int32 %} 32 {% else %} 64 {% end %}
        @num_mini_blocks = DEFAULT_NUM_MINI_BLOCKS
        @block_size = @mini_block_size * @num_mini_blocks
        @page_header_writer = Util::BitWriter.new(MAX_PAGE_HEADER_WRITER_SIZE)
        @bit_writer = Util::BitWriter.new(DEFAULT_BIT_WRITER_SIZE)
        @total_values = 0
        @first_value = 0_i64
        @current_value = 0_i64
        @values_in_block = 0
        @deltas = Array(Int64).new(@block_size, 0_i64)
      end

      def encoding : Encoding
        Encoding::DELTA_BINARY_PACKED
      end

      def estimated_data_encoded_size : Int32
        @bit_writer.bytes_written
      end

      def estimated_memory_size : Int32
        @page_header_writer.estimated_memory_size +
          @bit_writer.estimated_memory_size + @deltas.size * 8
      end

      # Encodes `values`.
      def put(values : Array(T)) : Nil
        return if values.empty?

        idx = if @total_values == 0
                @first_value = as_i64(values[0])
                @current_value = @first_value
                1
              else
                0
              end
        @total_values += values.size

        while idx < values.size
          value = as_i64(values[idx])
          @deltas[@values_in_block] = subtract(value, @current_value)
          @current_value = value
          idx += 1
          @values_in_block += 1
          flush_block_values if @values_in_block == @block_size
        end
      end

      # Flushes and returns the encoded bytes.
      def flush_buffer : Bytes
        flush_block_values
        write_page_header

        buffer = [] of UInt8
        buffer.concat(@page_header_writer.flush_buffer)
        buffer.concat(@bit_writer.flush_buffer)

        @page_header_writer.clear
        @bit_writer.clear
        @total_values = 0
        @first_value = 0_i64
        @current_value = 0_i64
        @values_in_block = 0
        Slice.new(buffer.to_unsafe, buffer.size).dup
      end

      private def write_page_header : Nil
        @page_header_writer.put_vlq_int(@block_size.to_u64)
        @page_header_writer.put_vlq_int(@num_mini_blocks.to_u64)
        @page_header_writer.put_vlq_int(@total_values.to_u64)
        @page_header_writer.put_zigzag_vlq_int(@first_value)
      end

      private def flush_block_values : Nil
        return if @values_in_block == 0

        min_delta = Int64::MAX
        @values_in_block.times { |i| min_delta = Math.min(min_delta, @deltas[i]) }
        @bit_writer.put_zigzag_vlq_int(min_delta)

        offset = @bit_writer.skip(@num_mini_blocks)

        @num_mini_blocks.times do |i|
          n = Math.min(@mini_block_size, @values_in_block)
          if n == 0
            (i...@num_mini_blocks).each { |j| @bit_writer.write_at(offset + j, 0_u8) }
            break
          end

          max_delta = Int64::MIN
          n.times { |j| max_delta = Math.max(max_delta, @deltas[i * @mini_block_size + j]) }

          bit_width = Util::BitUtil.num_required_bits(subtract_u64(max_delta, min_delta)).to_i32
          @bit_writer.write_at(offset + i, bit_width.to_u8!)

          n.times do |j|
            packed = subtract_u64(@deltas[i * @mini_block_size + j], min_delta)
            @bit_writer.put_value(packed, bit_width)
          end
          (n...@mini_block_size).each { @bit_writer.put_value(0_u64, bit_width) }

          @values_in_block -= n
        end
      end

      private def as_i64(value : T) : Int64
        {% if T == Int32 %}
          value.to_i64
        {% else %}
          value
        {% end %}
      end

      private def subtract(left : Int64, right : Int64) : Int64
        {% if T == Int32 %}
          (left.to_i32! &- right.to_i32!).to_i64
        {% else %}
          left &- right
        {% end %}
      end

      private def subtract_u64(left : Int64, right : Int64) : UInt64
        {% if T == Int32 %}
          (left.to_i32! &- right.to_i32!).unsafe_as(UInt32).to_u64
        {% else %}
          (left &- right).unsafe_as(UInt64)
        {% end %}
      end
    end

    # DELTA_BINARY_PACKED decoder for INT32/INT64.
    class DeltaBitPackDecoder(T)
      @bit_reader : Util::BitReader
      @initialized : Bool
      @block_size : Int32
      @values_left : Int32
      @mini_blocks_per_block : Int32
      @values_per_mini_block : Int32
      @min_delta : T
      @block_end_offset : Int32
      @mini_block_idx : Int32
      @mini_block_bit_widths : Array(UInt8)
      @mini_block_remaining : Int32
      @first_value : T?
      @last_value : T

      def initialize
        @bit_reader = Util::BitReader.new(Bytes.new(0))
        @initialized = false
        @block_size = 0
        @values_left = 0
        @mini_blocks_per_block = 0
        @values_per_mini_block = 0
        @min_delta = default_value
        @block_end_offset = 0
        @mini_block_idx = 0
        @mini_block_bit_widths = [] of UInt8
        @mini_block_remaining = 0
        @first_value = nil.as(T?)
        @last_value = default_value
      end

      # Sets the input data and reads the page header.
      def set_data(data : Bytes, _num_values : Int32) : Nil
        @bit_reader = Util::BitReader.new(data)
        @initialized = true

        @block_size = (@bit_reader.get_vlq_int || raise eof_err!("Not enough data to decode 'block_size'")).to_i32!
        @mini_blocks_per_block = (@bit_reader.get_vlq_int || raise eof_err!("Not enough data to decode 'mini_blocks_per_block'")).to_i32!
        raise general_err!("cannot have zero miniblocks per block") if @mini_blocks_per_block == 0

        @values_left = (@bit_reader.get_vlq_int || raise eof_err!("Not enough data to decode 'values_left'")).to_i32!
        first_value = @bit_reader.get_zigzag_vlq_int || raise eof_err!("Not enough data to decode 'first_value'")
        @first_value = from_i64(first_value)

        raise general_err!("'block_size' must be a multiple of 128, got #{@block_size}") unless @block_size % 128 == 0
        unless @block_size % @mini_blocks_per_block == 0
          raise general_err!("'block_size' must be a multiple of 'mini_blocks_per_block' got #{@block_size} and #{@mini_blocks_per_block}")
        end

        @mini_block_idx = 0
        @values_per_mini_block = @block_size // @mini_blocks_per_block
        @mini_block_remaining = 0
        @mini_block_bit_widths.clear

        unless @values_per_mini_block % 32 == 0
          raise general_err!("'values_per_mini_block' must be a multiple of 32 got #{@values_per_mini_block}")
        end
      end

      def values_left : Int32
        @values_left
      end

      def encoding : Encoding
        Encoding::DELTA_BINARY_PACKED
      end

      # Returns the current byte offset.
      # ameba:disable Naming/AccessorMethodName
      def get_offset : Int32
        raise "Bit reader is not initialized" unless @initialized

        if @values_left == 0
          Math.max(@bit_reader.get_byte_offset, @block_end_offset)
        else
          @bit_reader.get_byte_offset
        end
      end

      # Decodes up to `buffer.size` values, returning the count decoded.
      def get(buffer : Array(T)) : Int32
        return 0 if buffer.empty?

        read = 0
        to_read = Math.min(buffer.size, @values_left)

        if value = @first_value
          @first_value = nil
          @last_value = value
          buffer[0] = value
          read += 1
          @values_left -= 1
        end

        while read != to_read
          next_mini_block if @mini_block_remaining == 0

          bit_width = @mini_block_bit_widths[@mini_block_idx].to_i32
          check_bit_width(bit_width)
          batch = Math.min(@mini_block_remaining, to_read - read)

          if bit_width == 0
            batch.times do |k|
              @last_value = @last_value &+ @min_delta
              buffer[read + k] = @last_value
            end
          else
            slice = Slice.new(buffer.to_unsafe, buffer.size)[read, batch]
            got = @bit_reader.get_batch(T, slice, bit_width)
            if got != batch
              raise general_err!("Expected to read #{batch} values from miniblock got #{got}")
            end

            batch.times do |k|
              value = buffer[read + k] &+ @min_delta &+ @last_value
              buffer[read + k] = value
              @last_value = value
            end
          end

          read += batch
          @mini_block_remaining -= batch
          @values_left -= batch
        end

        to_read
      end

      # Skips up to `num_values` values, returning the count skipped.
      def skip(num_values : Int32) : Int32
        to_skip = Math.min(num_values, @values_left)
        return 0 if to_skip == 0

        scratch = Array(T).new(to_skip) { default_value }
        get(scratch)
        to_skip
      end

      private def next_block : Nil
        min_delta = @bit_reader.get_zigzag_vlq_int || raise eof_err!("Not enough data to decode 'min_delta'")
        @min_delta = from_i64(min_delta)

        @mini_block_bit_widths.clear
        @bit_reader.get_aligned_bytes(@mini_block_bit_widths, @mini_blocks_per_block)

        offset = @bit_reader.get_byte_offset
        remaining = @values_left
        @mini_block_bit_widths.each do |bit_width|
          bit_width = 0_u8 if remaining == 0
          remaining = remaining > @values_per_mini_block ? remaining - @values_per_mini_block : 0
          offset += bit_width.to_i32 * @values_per_mini_block // 8
        end
        @block_end_offset = offset

        if @mini_block_bit_widths.size != @mini_blocks_per_block
          raise eof_err!("insufficient mini block bit widths")
        end

        @mini_block_remaining = @values_per_mini_block
        @mini_block_idx = 0
      end

      private def next_mini_block : Nil
        if @mini_block_idx + 1 < @mini_block_bit_widths.size
          @mini_block_idx += 1
          @mini_block_remaining = @values_per_mini_block
        else
          next_block
        end
      end

      private def check_bit_width(bit_width : Int32) : Nil
        if bit_width > sizeof(T) * 8
          raise general_err!("Invalid delta bit width #{bit_width} which is larger than expected #{sizeof(T) * 8} ")
        end
      end

      private def from_i64(value : Int64) : T
        {% if T == Int32 %}
          value.to_i32!
        {% else %}
          value
        {% end %}
      end

      private def default_value : T
        {% if T == Int32 %}
          0
        {% else %}
          0_i64
        {% end %}
      end
    end
  end
end
