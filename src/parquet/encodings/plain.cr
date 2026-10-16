require "./levels"

# Ported from `parquet/src/encodings/encoding/mod.rs` (PlainEncoder) and
# `parquet/src/encodings/decoding.rs` (PlainDecoder/PlainDecoderDetails)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the PLAIN codec. Divergence: Rust's `DataType`/`Encoder`/`Decoder`
# trait dispatch becomes `PlainEncoder(T)`/`PlainDecoder(T)` with `T.class`
# (macro branches per physical value type); no `get_encoder`/`get_decoder`
# registry yet.
module Parquet
  module Encodings
    # PLAIN encoding that supports all physical types.
    class PlainEncoder(T)
      def initialize
        @buffer = [] of UInt8
        @bit_writer = Util::BitWriter.new(256)
      end

      # Encodes `values` and returns the encoded bytes.
      def self.encode_all(values : Array(T)) : Bytes
        encoder = new
        encoder.put(values)
        encoder.flush_buffer
      end

      def encoding : Encoding
        Encoding::PLAIN
      end

      def estimated_data_encoded_size : Int32
        @buffer.size + @bit_writer.bytes_written
      end

      def estimated_memory_size : Int32
        @buffer.size + @bit_writer.estimated_memory_size
      end

      # Encodes `values`, appending to the internal buffer.
      def put(values : Array(T)) : Nil
        {% if T == Bool %}
          values.each { |v| @bit_writer.put_value(v ? 1_u64 : 0_u64, 1) }
        {% elsif T == Int32 || T == Int64 || T == Float32 || T == Float64 %}
          values.each { |v| @buffer.concat(le_bytes(v)) }
        {% elsif T == Int96 %}
          values.each { |v| @buffer.concat(v.as_bytes) }
        {% elsif T == ByteArray %}
          values.each do |v|
            @buffer.concat(int32_le(v.len))
            @buffer.concat(v.data)
          end
        {% elsif T == FixedLenByteArray %}
          values.each { |v| @buffer.concat(v.data) }
        {% else %}
          {% raise "PlainEncoder: unsupported type #{T}" %}
        {% end %}
      end

      # Flushes the buffered bits and returns the encoded bytes.
      def flush_buffer : Bytes
        @buffer.concat(@bit_writer.flush_buffer)
        @bit_writer.clear
        result = Slice.new(@buffer.to_unsafe, @buffer.size).dup
        @buffer.clear
        result
      end

      private def le_bytes(value : T) : Bytes forall T
        io = IO::Memory.new(sizeof(T))
        io.write_bytes(value, IO::ByteFormat::LittleEndian)
        io.to_slice.dup
      end

      private def int32_le(value : Int32) : Bytes
        buffer = Bytes.new(4)
        IO::Memory.new(buffer).write_bytes(value, IO::ByteFormat::LittleEndian)
        buffer
      end
    end

    # PLAIN decoding that supports all physical types.
    class PlainDecoder(T)
      def initialize(@type_length : Int32 = -1)
        @num_values = 0
        @start = 0
        @data = Bytes.new(0)
        @bit_reader = nil.as(Util::BitReader?)
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        {% if T == Bool %}
          @bit_reader = Util::BitReader.new(data)
          @num_values = num_values
        {% else %}
          @data = data
          @start = 0
          @num_values = num_values
        {% end %}
      end

      def values_left : Int32
        @num_values
      end

      def encoding : Encoding
        Encoding::PLAIN
      end

      # Decodes up to `buffer.size` values, returning the count decoded.
      def get(buffer : Array(T)) : Int32
        {% if T == Bool %}
          reader = @bit_reader.as(Util::BitReader)
          num_values = Math.min(buffer.size, @num_values)
          slice = Slice.new(buffer.to_unsafe, buffer.size)[0, num_values]
          values_read = reader.get_batch(T, slice, 1)
          @num_values -= values_read
          values_read
        {% elsif T == Int32 || T == Int64 || T == Float32 || T == Float64 %}
          num_values = Math.min(buffer.size, @num_values)
          bytes_to_decode = sizeof(T) * num_values
          raise eof_err!("Not enough bytes to decode") if (@data.size - @start) < bytes_to_decode

          num_values.times do |i|
            buffer[i] = IO::Memory.new(@data[@start + i * sizeof(T), sizeof(T)]).read_bytes(T, IO::ByteFormat::LittleEndian)
          end
          @start += bytes_to_decode
          @num_values -= num_values
          num_values
        {% elsif T == Int96 %}
          num_values = Math.min(buffer.size, @num_values)
          bytes_to_decode = 12 * num_values
          raise eof_err!("Not enough bytes to decode") if (@data.size - @start) < bytes_to_decode

          num_values.times do |i|
            buffer[i] = Int96.try_from_le_slice(@data[@start + i * 12, 12])
          end
          @start += bytes_to_decode
          @num_values -= num_values
          num_values
        {% elsif T == ByteArray %}
          num_values = Math.min(buffer.size, @num_values)
          num_values.times do |i|
            len = read_u32(@data, @start)
            @start += 4
            raise eof_err!("Not enough bytes to decode") if @data.size < @start + len

            buffer[i] = ByteArray.new(@data[@start, len])
            @start += len
          end
          @num_values -= num_values
          num_values
        {% elsif T == FixedLenByteArray %}
          num_values = Math.min(buffer.size, @num_values)
          num_values.times do |i|
            raise eof_err!("Not enough bytes to decode") if @data.size < @start + @type_length

            buffer[i] = FixedLenByteArray.new(ByteArray.new(@data[@start, @type_length]))
            @start += @type_length
          end
          @num_values -= num_values
          num_values
        {% else %}
          {% raise "PlainDecoder: unsupported type #{T}" %}
        {% end %}
      end

      # Decodes `buffer.size` values, interleaving `null_count` nulls by
      # `valid_bits`. Returns the number of values written.
      def get_spaced(buffer : Array(T), null_count : Int32, valid_bits : Bytes) : Int32
        return get(buffer) if null_count == 0

        num_values = buffer.size
        values_to_read = num_values - null_count
        values_read = get(buffer)
        if values_read != values_to_read
          raise general_err!(
            "Number of values read: #{values_read}, doesn't match expected: #{values_to_read}"
          )
        end

        values_to_move = values_read
        (num_values - 1).downto(0) do |i|
          if Util::BitUtil.get_bit(valid_bits, i)
            values_to_move -= 1
            buffer[i], buffer[values_to_move] = buffer[values_to_move], buffer[i]
          end
        end
        num_values
      end

      # Skips up to `num_values` values, returning the count skipped.
      def skip(num_values : Int32) : Int32
        {% if T == Bool %}
          reader = @bit_reader.as(Util::BitReader)
          to_skip = Math.min(num_values, @num_values)
          values_read = reader.skip(to_skip, 1)
          @num_values -= values_read
          values_read
        {% elsif T == Int32 || T == Int64 || T == Float32 || T == Float64 %}
          to_skip = Math.min(num_values, @num_values)
          bytes_to_skip = sizeof(T) * to_skip
          raise eof_err!("Not enough bytes to skip") if (@data.size - @start) < bytes_to_skip

          @start += bytes_to_skip
          @num_values -= to_skip
          to_skip
        {% elsif T == Int96 %}
          to_skip = Math.min(num_values, @num_values)
          bytes_to_skip = 12 * to_skip
          raise eof_err!("Not enough bytes to skip") if (@data.size - @start) < bytes_to_skip

          @start += bytes_to_skip
          @num_values -= to_skip
          to_skip
        {% elsif T == ByteArray %}
          to_skip = Math.min(num_values, @num_values)
          to_skip.times do
            len = read_u32(@data, @start)
            @start += 4 + len
          end
          @num_values -= to_skip
          to_skip
        {% elsif T == FixedLenByteArray %}
          to_skip = Math.min(num_values, @num_values)
          to_skip.times do
            raise eof_err!("Not enough bytes to skip") if @data.size < @start + @type_length

            @start += @type_length
          end
          @num_values -= to_skip
          to_skip
        {% else %}
          {% raise "PlainDecoder: unsupported type #{T}" %}
        {% end %}
      end

      private def read_u32(data : Bytes, start : Int32) : Int32
        IO::Memory.new(data[start, 4]).read_bytes(UInt32, IO::ByteFormat::LittleEndian).to_i32!
      end
    end
  end
end
