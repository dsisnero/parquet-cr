require "./plain"

# Ported from `parquet/src/encodings/encoding/byte_stream_split_encoder.rs` and
# `parquet/src/encodings/decoding/byte_stream_split_decoder.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the BYTE_STREAM_SPLIT codec. Divergence: Rust's `DataType`/`Encoder`/
# `Decoder` trait dispatch is `ByteStreamSplitEncoder(T)` etc. with `T.class`
# (compile-time branches per physical value type); the fixed-width decoder joins
# the streams into a scratch buffer and decodes each value via
# `IO::ByteFormat::LittleEndian` rather than reinterpreting memory; the
# constant/variable split/join kernels are a single runtime `type_size` routine
# (the upstream block-4 variants write identical positions).
module Parquet
  module Encodings
    # BYTE_STREAM_SPLIT encoder for 4/8-byte physical types (FLOAT, DOUBLE,
    # INT32, INT64). Values are buffered contiguously, then transposed on flush.
    class ByteStreamSplitEncoder(T) < Encoder(T)
      def initialize
        @buffer = [] of UInt8
      end

      def put(values : Array(T)) : Nil
        {% if T == Float32 || T == Int32 || T == Float64 || T == Int64 %}
          values.each { |value| @buffer.concat(le_bytes(value)) }
        {% else %}
          raise general_err!("ByteStreamSplitEncoder does not support Int96, Boolean, or ByteArray types")
        {% end %}
      end

      def encoding : Encoding
        Encoding::BYTE_STREAM_SPLIT
      end

      def estimated_data_encoded_size : Int32
        @buffer.size
      end

      def flush_buffer : Bytes
        type_size = sizeof(T)
        {% if T == Float32 || T == Int32 || T == Float64 || T == Int64 %}
          encoded = Bytes.new(@buffer.size)
          ByteStreamSplit.split_streams(@buffer, encoded, type_size)
          @buffer.clear
          encoded
        {% else %}
          raise general_err!("byte stream split unsupported for data types of size #{type_size} bytes")
        {% end %}
      end

      def estimated_memory_size : Int32
        @buffer.size
      end

      private def le_bytes(value : T) : Bytes forall T
        io = IO::Memory.new(sizeof(T))
        io.write_bytes(value, IO::ByteFormat::LittleEndian)
        io.to_slice
      end
    end

    # BYTE_STREAM_SPLIT encoder for variable-width FIXED_LEN_BYTE_ARRAY columns.
    class VariableWidthByteStreamSplitEncoder(T) < Encoder(T)
      def initialize(@type_width : Int32)
        @buffer = [] of UInt8
      end

      def put(values : Array(T)) : Nil
        {% if T == FixedLenByteArray %}
          values.each do |value|
            bytes = value.as_bytes
            if bytes.size != @type_width
              raise general_err!("Mismatched FixedLenByteArray sizes: #{bytes.size} != #{@type_width}")
            end
            @buffer.concat(bytes)
          end
        {% else %}
          raise general_err!("VariableWidthByteStreamSplitEncoder only supports FixedLenByteArray types")
        {% end %}
      end

      def encoding : Encoding
        Encoding::BYTE_STREAM_SPLIT
      end

      def estimated_data_encoded_size : Int32
        @buffer.size
      end

      def flush_buffer : Bytes
        encoded = Bytes.new(@buffer.size)
        ByteStreamSplit.split_streams(@buffer, encoded, @type_width)
        @buffer.clear
        encoded
      end

      def estimated_memory_size : Int32
        @buffer.size
      end
    end

    # BYTE_STREAM_SPLIT decoder for 4/8-byte physical types.
    class ByteStreamSplitDecoder(T) < Decoder(T)
      def initialize
        @encoded_bytes = Bytes.new(0)
        @total_num_values = 0
        @values_decoded = 0
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        @encoded_bytes = data
        @total_num_values = num_values
        @values_decoded = 0
      end

      def get(buffer : Array(T)) : Int32
        total_remaining = values_left
        num_values = Math.min(buffer.size, total_remaining)

        type_size = sizeof(T)
        {% if T == Float32 || T == Int32 || T == Float64 || T == Int64 %}
          stride = @encoded_bytes.size // type_size
          raw = Bytes.new(num_values * type_size)
          ByteStreamSplit.join_streams(@encoded_bytes, raw, stride, type_size, @values_decoded)
          num_values.times do |i|
            buffer[i] = IO::Memory.new(raw[i * type_size, type_size]).read_bytes(T, IO::ByteFormat::LittleEndian)
          end
          @values_decoded += num_values
          num_values
        {% else %}
          raise general_err!("byte stream split unsupported for data types of size #{type_size} bytes")
        {% end %}
      end

      def values_left : Int32
        @total_num_values - @values_decoded
      end

      def encoding : Encoding
        Encoding::BYTE_STREAM_SPLIT
      end

      def skip(num_values : Int32) : Int32
        to_skip = Math.min(values_left, num_values)
        @values_decoded += to_skip
        to_skip
      end
    end

    # BYTE_STREAM_SPLIT decoder for variable-width FIXED_LEN_BYTE_ARRAY columns.
    class VariableWidthByteStreamSplitDecoder(T) < Decoder(T)
      def initialize(@type_width : Int32)
        @encoded_bytes = Bytes.new(0)
        @total_num_values = 0
        @values_decoded = 0
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        if data.size % @type_width != 0
          raise general_err!(
            "Input data length is not a multiple of type width #{@type_width}"
          )
        end

        {% if T == FixedLenByteArray %}
          @encoded_bytes = data
          @total_num_values = num_values
          @values_decoded = 0
        {% else %}
          raise general_err!("VariableWidthByteStreamSplitDecoder only supports FixedLenByteArrayType")
        {% end %}
      end

      def get(buffer : Array(T)) : Int32
        total_remaining = values_left
        num_values = Math.min(buffer.size, total_remaining)

        {% if T == FixedLenByteArray %}
          type_size = @type_width
          raw = Bytes.new(num_values * type_size)
          stride = @encoded_bytes.size // type_size
          ByteStreamSplit.join_streams(@encoded_bytes, raw, stride, type_size, @values_decoded)
          num_values.times do |i|
            buffer[i] = FixedLenByteArray.new(ByteArray.new(raw[i * type_size, type_size]))
          end
          @values_decoded += num_values
          num_values
        {% else %}
          raise general_err!("VariableWidthByteStreamSplitDecoder only supports FixedLenByteArrayType")
        {% end %}
      end

      def values_left : Int32
        @total_num_values - @values_decoded
      end

      def encoding : Encoding
        Encoding::BYTE_STREAM_SPLIT
      end

      def skip(num_values : Int32) : Int32
        to_skip = Math.min(values_left, num_values)
        @values_decoded += to_skip
        to_skip
      end
    end

    # Stream split/join kernels shared by the byte-stream-split codec.
    module ByteStreamSplit
      # Transposes `src` (contiguous values) into `dst` (byte-strided streams).
      def self.split_streams(src : Array(UInt8), dst : Bytes, type_size : Int32) : Nil
        stride = src.size // type_size
        stride.times do |i|
          type_size.times do |j|
            dst[i + j * stride] = src[i * type_size + j]
          end
        end
      end

      # Transposes byte-strided `src` (from `values_decoded` on) into contiguous
      # `dst`. `stride` is the number of values in the full source stream.
      def self.join_streams(src : Bytes, dst : Bytes, stride : Int32, type_size : Int32, values_decoded : Int32) : Nil
        (dst.size // type_size).times do |i|
          type_size.times do |j|
            dst[i * type_size + j] = src[values_decoded + i + j * stride]
          end
        end
      end
    end
  end
end
