require "./rle"

# Ported from `parquet/src/encodings/encoding/mod.rs` (RleValueEncoder) and
# `parquet/src/encodings/decoding.rs` (RleValueDecoder)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the RLE value codec for BOOLEAN (data pages v2). Divergence: Rust's
# `DataType`/`Encoder`/`Decoder` trait dispatch is `RleValueEncoder(T)`/
# `RleValueDecoder(T)` with `T.class`; the decoder clamps `num_values` by
# decoding into a scratch array before copying into the output buffer (the
# Crystal `RleDecoder#get_batch` only accepts an owning `Array`).
module Parquet
  module Encodings
    # RLE/Bit-Packing hybrid encoding for values. Supports BOOLEAN only.
    class RleValueEncoder(T) < Encoder(T)
      def initialize
        @encoder = nil.as(RleEncoder?)
      end

      # Encodes `values`, which must be BOOLEAN.
      def put(values : Array(T)) : Nil
        {% if T == Bool %}
          rle = @encoder ||= RleEncoder.new_from_buf(1, [0_u8, 0_u8, 0_u8, 0_u8])
          values.each { |value| rle.put(value ? 1_u64 : 0_u64) }
        {% else %}
          raise general_err!("RleValueEncoder only supports BoolType")
        {% end %}
      end

      def encoding : Encoding
        Encoding::RLE
      end

      def estimated_data_encoded_size : Int32
        encoder = @encoder
        encoder ? encoder.len : 0
      end

      # Flushes all encoder buffers and raw values, prefixing the encoded bytes
      # with their `i32` length (excluding the prefix itself).
      def flush_buffer : Bytes
        {% if T == Bool %}
          encoder = @encoder || raise general_err!("RLE value encoder is not initialized")
          @encoder = nil

          buf = encoder.consume
          raise "should have had padding inserted" if buf.size < 4

          len = (buf.size - 4).to_i32
          IO::Memory.new(buf[0, 4]).write_bytes(len, IO::ByteFormat::LittleEndian)
          buf
        {% else %}
          raise general_err!("RleValueEncoder only supports BoolType")
        {% end %}
      end

      def estimated_memory_size : Int32
        encoder = @encoder
        encoder ? encoder.estimated_memory_size : 0
      end
    end

    # RLE/Bit-Packing hybrid decoding for values. Supports BOOLEAN only.
    class RleValueDecoder(T) < Decoder(T)
      def initialize
        @values_left = 0
        @decoder = RleDecoder.new(1)
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        {% if T == Bool %}
          # The stream is prefixed with the encoded length as an i32.
          if data.size < 4
            raise eof_err!("Not enough bytes to decode")
          end
          data_size = Util::BitUtil.read_num_bytes(Int32, 4, data).to_i32
          if data.size - 4 < data_size
            raise eof_err!("Not enough bytes to decode")
          end

          @decoder = RleDecoder.new(1)
          @decoder.set_data(data[4, data_size])
          @values_left = num_values
        {% else %}
          raise general_err!("RleValueDecoder only supports BoolType")
        {% end %}
      end

      def values_left : Int32
        @values_left
      end

      def encoding : Encoding
        Encoding::RLE
      end

      def get(buffer : Array(T)) : Int32
        {% if T == Bool %}
          num_values = Math.min(buffer.size, @values_left)
          scratch = Array(Bool).new(num_values, false)
          values_read = @decoder.get_batch(Bool, scratch)
          values_read.times { |i| buffer[i] = scratch[i] }
          @values_left -= values_read
          values_read
        {% else %}
          raise general_err!("RleValueDecoder only supports BoolType")
        {% end %}
      end

      def skip(num_values : Int32) : Int32
        {% if T == Bool %}
          to_skip = Math.min(num_values, @values_left)
          values_skipped = @decoder.skip(to_skip)
          @values_left -= values_skipped
          values_skipped
        {% else %}
          raise general_err!("RleValueDecoder only supports BoolType")
        {% end %}
      end
    end
  end
end
