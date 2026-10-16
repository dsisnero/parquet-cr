require "./plain"
require "../util/interner"

# Ported from `parquet/src/encodings/encoding/dict_encoder.rs` (DictEncoder)
# and `parquet/src/encodings/decoding.rs` (DictDecoder)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the dictionary codec. Divergence: Rust's `DataType`/`Encoder`/`Decoder`
# trait dispatch is `DictEncoder(T)`/`DictDecoder(T)` with `T.class` (macro
# branches per physical value type); `DictDecoder#set_dict` accepts any object
# responding to `values_left`/`get` (in practice a `PlainDecoder(T)`), rather
# than a `Box<dyn Decoder<T>>`. `DictDecoder#get` does not decrement
# `num_values`, matching upstream.
module Parquet
  module Encodings
    # Dictionary encoder. Builds a dictionary of values encountered in a given
    # column; the dictionary page is written first (PLAIN), followed by data
    # pages of RLE/bit-packed entry ids preceded by their bit width.
    class DictEncoder(T)
      @interner : Util::Interner(KeyStorage(T))

      def initialize(desc : Schema::ColumnDescriptor)
        @interner = Util::Interner.new(KeyStorage(T).new(desc.type_length))
        @indices = [] of UInt64
      end

      # Whether dictionary entries are sorted. Sorting is not supported.
      def sorted? : Bool
        false
      end

      # Number of unique values (keys) in the dictionary.
      def num_entries : Int32
        @interner.storage.uniques.size
      end

      # The distinct values interned so far, in dictionary order.
      def uniques : Array(T)
        @interner.storage.uniques
      end

      # Size of unique values (keys) in the dictionary, in bytes.
      def dict_encoded_size : Int32
        @interner.storage.size_in_bytes
      end

      # Writes the dictionary values with PLAIN encoding and returns the bytes.
      def write_dict : Bytes
        encoder = PlainEncoder(T).new
        encoder.put(@interner.storage.uniques)
        encoder.flush_buffer
      end

      # Writes the buffered indices with RLE encoding and returns the bytes.
      def write_indices : Bytes
        buffer = [] of UInt8
        buffer << bit_width.to_u8!
        encoder = RleEncoder.new_from_buf(bit_width, buffer)
        @indices.each { |index| encoder.put(index) }
        @indices.clear
        encoder.consume
      end

      # Encodes `values`, buffering an interned index for each.
      def put(values : Array(T)) : Nil
        values.each { |value| @indices << @interner.intern(value) }
      end

      def encoding : Encoding
        Encoding::PLAIN_DICTIONARY
      end

      # Estimated data page size in bytes.
      def estimated_data_encoded_size : Int32
        RleEncoder.max_buffer_size(bit_width, @indices.size)
      end

      # Flushes the buffered indices and returns the encoded bytes.
      def flush_buffer : Bytes
        write_indices
      end

      # Estimated memory use of this encoder, in bytes.
      def estimated_memory_size : Int32
        @interner.estimated_memory_size + @indices.size * sizeof(UInt64)
      end

      private def bit_width : Int32
        Util::BitUtil.num_required_bits(Math.max(num_entries - 1, 0).to_u64).to_i32
      end
    end

    # Dictionary decoder. The dictionary is supplied separately (it lives in a
    # dictionary page per column chunk); data pages carry RLE/bit-packed ids.
    class DictDecoder(T)
      def initialize
        @dictionary = [] of T
        @has_dictionary = false
        @rle_decoder = nil.as(RleDecoder?)
        @num_values = 0
      end

      # Decodes and sets values for the dictionary using `decoder`.
      # ameba:disable Naming/AccessorMethodName
      def set_dict(decoder) : Nil
        num_values = decoder.values_left
        @dictionary = Array(T).new(num_values) { DictDecoder.default_value(T) }
        decoder.get(@dictionary)
        @has_dictionary = true
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        raise eof_err!("Not enough bytes to decode bit_width") if data.empty?

        bit_width = data[0]
        if bit_width > MAX_RLE_DICTIONARY_BIT_WIDTH
          raise general_err!(
            "Invalid or corrupted RLE bit width #{bit_width}. Max allowed is #{MAX_RLE_DICTIONARY_BIT_WIDTH}"
          )
        end
        rle_decoder = RleDecoder.new(bit_width.to_i32)
        rle_decoder.set_data(data[1, data.size - 1])
        @num_values = num_values
        @rle_decoder = rle_decoder
      end

      def get(buffer : Array(T)) : Int32
        rle = @rle_decoder || raise general_err!("rle_decoder should be initialized")
        raise general_err!("Must call set_dict() first!") unless @has_dictionary

        num_values = Math.min(buffer.size, @num_values)
        rle.get_batch_with_dict(T, @dictionary, buffer, num_values)
      end

      # Number of values left in this decoder stream.
      def values_left : Int32
        @num_values
      end

      def encoding : Encoding
        Encoding::RLE_DICTIONARY
      end

      def skip(num_values : Int32) : Int32
        rle = @rle_decoder || raise general_err!("rle_decoder should be initialized")
        raise general_err!("Must call set_dict() first!") unless @has_dictionary

        num_values = Math.min(num_values, @num_values)
        rle.skip(num_values)
      end

      # Default value for `T`, used as a decode-buffer placeholder.
      def self.default_value(type : T.class) : T forall T
        {% if T == Bool %}
          false
        {% elsif T == Int32 %}
          0_i32
        {% elsif T == Int64 %}
          0_i64
        {% elsif T == Float32 %}
          0_f32
        {% elsif T == Float64 %}
          0_f64
        {% elsif T == Int96 %}
          Int96.new
        {% elsif T == ByteArray %}
          ByteArray.new
        {% elsif T == FixedLenByteArray %}
          FixedLenByteArray.new(ByteArray.new)
        {% else %}
          {% raise "DictDecoder: unsupported type #{T}" %}
        {% end %}
      end
    end

    # Upstream `KeyStorage<T>`: interned dictionary values plus their encoded
    # size, used as `Interner` storage by `DictEncoder`.
    class KeyStorage(T) < Util::Storage(UInt64, T)
      getter uniques : Array(T)
      getter size_in_bytes : Int32
      getter type_length : Int32

      def initialize(@type_length : Int32)
        @uniques = [] of T
        @size_in_bytes = 0
      end

      def get(idx : UInt64) : T
        @uniques[idx.to_i32]
      end

      def push(value : T) : UInt64
        base_size, num_elements = dict_encoding_size(value)
        unique_size =
          {% if T == ByteArray %}
            base_size + num_elements
          {% elsif T == FixedLenByteArray %}
            @type_length
          {% else %}
            base_size
          {% end %}
        @size_in_bytes += unique_size

        key = @uniques.size.to_u64
        @uniques << value
        key
      end

      def estimated_memory_size : Int32
        uniques_heap_bytes =
          {% if T == FixedLenByteArray %}
            @type_length * @uniques.size
          {% elsif T == ByteArray %}
            @uniques.sum { |value| value.len }
          {% else %}
            0
          {% end %}
        @uniques.size * sizeof(T) + uniques_heap_bytes
      end

      private def dict_encoding_size(value : T) : Tuple(Int32, Int32)
        {% if T == ByteArray || T == FixedLenByteArray %}
          {4, value.len}
        {% else %}
          {sizeof(T), 1}
        {% end %}
      end
    end
  end
end
