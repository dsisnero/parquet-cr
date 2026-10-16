require "./delta_bit_pack"

# Ported from `parquet/src/encodings/encoding/mod.rs`
# (DeltaLengthByteArrayEncoder/DeltaByteArrayEncoder) and
# `parquet/src/encodings/decoding.rs` (their decoders)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY for BYTE_ARRAY (and
# FIXED_LEN_BYTE_ARRAY for the delta-byte-array codec). Divergence: trait
# dispatch is `DeltaByteArrayEncoder(T)`/`DeltaByteArrayDecoder(T)` with
# `T.class`; `prefix_lengths` is exposed for the negative-prefix-length check.
module Parquet
  module Encodings
    # DELTA_LENGTH_BYTE_ARRAY encoder for BYTE_ARRAY.
    class DeltaLengthByteArrayEncoder(T) < Encoder(T)
      def initialize
        @len_encoder = DeltaBitPackEncoder(Int32).new
        @data = [] of ByteArray
        @encoded_size = 0
      end

      def encoding : Encoding
        Encoding::DELTA_LENGTH_BYTE_ARRAY
      end

      def estimated_data_encoded_size : Int32
        @len_encoder.estimated_data_encoded_size + @encoded_size
      end

      def estimated_memory_size : Int32
        @len_encoder.estimated_memory_size + @data.size
      end

      # Encodes `values`.
      def put(values : Array(T)) : Nil
        {% if T == ByteArray %}
          @len_encoder.put(values.map(&.len))
          values.each do |value|
            @encoded_size += value.len
            @data << value
          end
        {% else %}
          raise general_err!("DeltaLengthByteArrayEncoder only supports ByteArrayType")
        {% end %}
      end

      # Flushes and returns the encoded bytes.
      def flush_buffer : Bytes
        {% if T == ByteArray %}
          buffer = [] of UInt8
          buffer.concat(@len_encoder.flush_buffer)
          @data.each { |value| buffer.concat(value.data) }
          @data.clear
          @encoded_size = 0
          Slice.new(buffer.to_unsafe, buffer.size).dup
        {% else %}
          raise general_err!("DeltaLengthByteArrayEncoder only supports ByteArrayType")
        {% end %}
      end
    end

    # DELTA_LENGTH_BYTE_ARRAY decoder for BYTE_ARRAY.
    class DeltaLengthByteArrayDecoder(T) < Decoder(T)
      def initialize
        @lengths = [] of Int32
        @current_idx = 0
        @data = Bytes.new(0)
        @offset = 0
        @num_values = 0
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        {% if T == ByteArray %}
          len_decoder = DeltaBitPackDecoder(Int32).new
          len_decoder.set_data(data, num_values)
          num_lengths = len_decoder.values_left
          @lengths = Array(Int32).new(num_lengths, 0)
          len_decoder.get(@lengths)

          offset = len_decoder.get_offset
          @data = data[offset, data.size - offset]
          @offset = 0
          @current_idx = 0
          @num_values = num_lengths
        {% else %}
          raise general_err!("DeltaLengthByteArrayDecoder only support ByteArrayType")
        {% end %}
      end

      def values_left : Int32
        @num_values
      end

      def encoding : Encoding
        Encoding::DELTA_LENGTH_BYTE_ARRAY
      end

      # Decodes up to `buffer.size` values, returning the count decoded.
      def get(buffer : Array(T)) : Int32
        {% if T == ByteArray %}
          num_values = Math.min(buffer.size, @num_values)
          num_values.times do |i|
            len = @lengths[@current_idx]
            buffer[i] = ByteArray.new(@data[@offset, len])
            @offset += len
            @current_idx += 1
          end
          @num_values -= num_values
          num_values
        {% else %}
          raise general_err!("DeltaLengthByteArrayDecoder only support ByteArrayType")
        {% end %}
      end

      # Skips up to `num_values` values, returning the count skipped.
      def skip(num_values : Int32) : Int32
        num_values = Math.min(num_values, @num_values)
        delta = 0
        num_values.times { |i| delta += @lengths[@current_idx + i] }
        @current_idx += num_values
        @offset += delta
        @num_values -= num_values
        num_values
      end
    end

    # DELTA_BYTE_ARRAY encoder for BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY.
    class DeltaByteArrayEncoder(T) < Encoder(T)
      def initialize
        @prefix_len_encoder = DeltaBitPackEncoder(Int32).new
        @suffix_writer = DeltaLengthByteArrayEncoder(ByteArray).new
        @previous = Bytes.new(0)
      end

      def encoding : Encoding
        Encoding::DELTA_BYTE_ARRAY
      end

      def estimated_data_encoded_size : Int32
        @prefix_len_encoder.estimated_data_encoded_size + @suffix_writer.estimated_data_encoded_size
      end

      def estimated_memory_size : Int32
        @prefix_len_encoder.estimated_memory_size + @suffix_writer.estimated_memory_size + @previous.size
      end

      # Encodes `values`.
      def put(values : Array(T)) : Nil
        prefix_lengths = [] of Int32
        suffixes = [] of ByteArray

        values.each do |value|
          current = value_bytes(value)
          match_len = common_prefix_length(@previous, current)
          prefix_lengths << match_len
          suffixes << ByteArray.new(current[match_len, current.size - match_len])
          @previous = current
        end

        @prefix_len_encoder.put(prefix_lengths)
        @suffix_writer.put(suffixes)
      end

      # Flushes and returns the encoded bytes.
      def flush_buffer : Bytes
        buffer = [] of UInt8
        buffer.concat(@prefix_len_encoder.flush_buffer)
        buffer.concat(@suffix_writer.flush_buffer)
        @previous = Bytes.new(0)
        Slice.new(buffer.to_unsafe, buffer.size).dup
      end

      private def value_bytes(value : T) : Bytes
        {% if T == ByteArray || T == FixedLenByteArray %}
          value.data
        {% else %}
          raise general_err!("DeltaByteArrayEncoder only supports ByteArrayType and FixedLenByteArrayType")
        {% end %}
      end

      private def common_prefix_length(a : Bytes, b : Bytes) : Int32
        max = Math.min(a.size, b.size)
        i = 0
        while i < max && a[i] == b[i]
          i += 1
        end
        i
      end
    end

    # DELTA_BYTE_ARRAY decoder for BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY.
    class DeltaByteArrayDecoder(T) < Decoder(T)
      getter prefix_lengths : Array(Int32)

      def initialize
        @prefix_lengths = [] of Int32
        @current_idx = 0
        @suffix_decoder = nil.as(DeltaLengthByteArrayDecoder(ByteArray)?)
        @previous_value = Bytes.new(0)
        @num_values = 0
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        prefix_decoder = DeltaBitPackDecoder(Int32).new
        prefix_decoder.set_data(data, num_values)
        num_prefixes = prefix_decoder.values_left
        @prefix_lengths = Array(Int32).new(num_prefixes, 0)
        prefix_decoder.get(@prefix_lengths)

        offset = prefix_decoder.get_offset
        suffix_decoder = DeltaLengthByteArrayDecoder(ByteArray).new
        suffix_decoder.set_data(data[offset, data.size - offset], num_values)
        @suffix_decoder = suffix_decoder
        @num_values = num_prefixes
        @current_idx = 0
        @previous_value = Bytes.new(0)
      end

      def values_left : Int32
        @num_values
      end

      def encoding : Encoding
        Encoding::DELTA_BYTE_ARRAY
      end

      # Decodes up to `buffer.size` values, returning the count decoded.
      def get(buffer : Array(T)) : Int32
        num_values = Math.min(buffer.size, @num_values)
        suffix_buffer = Array(ByteArray).new(1, ByteArray.new)

        num_values.times do |i|
          suffix_decoder = @suffix_decoder.as(DeltaLengthByteArrayDecoder(ByteArray))
          suffix_decoder.get(suffix_buffer)
          suffix = suffix_buffer[0].data

          prefix_len = @prefix_lengths[@current_idx]
          if prefix_len < 0 || prefix_len > @previous_value.size
            raise general_err!("Invalid DELTA_BYTE_ARRAY prefix length #{prefix_len}")
          end

          result = Bytes.new(prefix_len + suffix.size)
          result[0, prefix_len].copy_from(@previous_value[0, prefix_len]) if prefix_len > 0
          result[prefix_len, suffix.size].copy_from(suffix)

          {% if T == FixedLenByteArray %}
            buffer[i] = FixedLenByteArray.new(ByteArray.new(result))
          {% else %}
            buffer[i] = ByteArray.new(result)
          {% end %}
          @previous_value = result
          @current_idx += 1
        end

        @num_values -= num_values
        num_values
      end

      # Skips up to `num_values` values, returning the count skipped.
      def skip(num_values : Int32) : Int32
        scratch = Array(T).new(num_values) { default_value }
        get(scratch)
      end

      private def default_value : T
        {% if T == FixedLenByteArray %}
          FixedLenByteArray.new(ByteArray.new)
        {% else %}
          ByteArray.new
        {% end %}
      end
    end
  end
end
