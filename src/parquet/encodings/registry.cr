require "./traits"

# Ported from `get_encoder` (`parquet/src/encodings/encoding/mod.rs`) and
# `get_decoder` (`parquet/src/encodings/decoding.rs`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Encoder/decoder registries: select a codec by physical value type and
# `Encoding`. Divergence: Rust's `GetEncoder`/`GetDecoder` traits with
# associated types are `T.class`-dispatched module methods; because Crystal
# instantiates generic classes per concrete type, combinations that upstream
# would construct-but-fail-later (e.g. `DeltaBitPackEncoder<T>` for a non-integer
# `T`) raise `ParquetError` directly.
module Parquet
  module Encodings
    # Physical Parquet type for the value type `T`.
    def self.physical_type(type : Bool.class) : Parquet::Type
      Parquet::Type::BOOLEAN
    end

    def self.physical_type(type : Int32.class) : Parquet::Type
      Parquet::Type::INT32
    end

    def self.physical_type(type : Int64.class) : Parquet::Type
      Parquet::Type::INT64
    end

    def self.physical_type(type : Int96.class) : Parquet::Type
      Parquet::Type::INT96
    end

    def self.physical_type(type : Float32.class) : Parquet::Type
      Parquet::Type::FLOAT
    end

    def self.physical_type(type : Float64.class) : Parquet::Type
      Parquet::Type::DOUBLE
    end

    def self.physical_type(type : ByteArray.class) : Parquet::Type
      Parquet::Type::BYTE_ARRAY
    end

    def self.physical_type(type : FixedLenByteArray.class) : Parquet::Type
      Parquet::Type::FIXED_LEN_BYTE_ARRAY
    end

    # Gets an encoder for the data type `T` and `encoding`.
    def self.get_encoder(type : T.class, encoding : Encoding, descr : Schema::ColumnDescriptor) : Encoder(T) forall T
      case encoding
      when Encoding::PLAIN
        PlainEncoder(T).new
      when Encoding::RLE_DICTIONARY, Encoding::PLAIN_DICTIONARY
        raise general_err!("Cannot initialize this encoding through this function")
      when Encoding::BIT_PACKED
        raise nyi_err!("Encoding #{encoding} is not supported")
      when Encoding::ALP
        {% if T == Float32 || T == Float64 %}
          AlpEncoder(T).new
        {% else %}
          raise general_err!("Encoding ALP only supports FLOAT and DOUBLE, got #{physical_type(T)}")
        {% end %}
      when Encoding::RLE
        RleValueEncoder(T).new
      when Encoding::DELTA_BINARY_PACKED
        {% if T == Int32 || T == Int64 %}
          DeltaBitPackEncoder(T).new
        {% else %}
          raise general_err!("DeltaBitPackEncoder only supports Int32Type and Int64Type")
        {% end %}
      when Encoding::DELTA_LENGTH_BYTE_ARRAY
        DeltaLengthByteArrayEncoder(T).new
      when Encoding::DELTA_BYTE_ARRAY
        DeltaByteArrayEncoder(T).new
      when Encoding::BYTE_STREAM_SPLIT
        {% if T == FixedLenByteArray %}
          VariableWidthByteStreamSplitEncoder(T).new(descr.type_length)
        {% else %}
          ByteStreamSplitEncoder(T).new
        {% end %}
      else
        raise nyi_err!("Encoding #{encoding} is not supported")
      end
    end

    # Gets a decoder for the column descriptor `descr` and `encoding`.
    def self.get_decoder(type : T.class, descr : Schema::ColumnDescriptor, encoding : Encoding) : Decoder(T) forall T
      case encoding
      when Encoding::PLAIN
        return PlainDecoder(T).new(descr.type_length)
      when Encoding::RLE_DICTIONARY, Encoding::PLAIN_DICTIONARY
        raise general_err!("Cannot initialize this encoding through this function")
      when Encoding::BIT_PACKED
        raise nyi_err!("Encoding #{encoding} is not supported")
      end

      {% if T == Bool %}
        return RleValueDecoder(T).new if encoding == Encoding::RLE
      {% elsif T == Int32 || T == Int64 %}
        return ByteStreamSplitDecoder(T).new if encoding == Encoding::BYTE_STREAM_SPLIT
        return DeltaBitPackDecoder(T).new if encoding == Encoding::DELTA_BINARY_PACKED
      {% elsif T == Float32 || T == Float64 %}
        return ByteStreamSplitDecoder(T).new if encoding == Encoding::BYTE_STREAM_SPLIT
        return AlpDecoder(T).new if encoding == Encoding::ALP
      {% elsif T == ByteArray %}
        return DeltaByteArrayDecoder(T).new if encoding == Encoding::DELTA_BYTE_ARRAY
        return DeltaLengthByteArrayDecoder(T).new if encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY
      {% elsif T == FixedLenByteArray %}
        if encoding == Encoding::BYTE_STREAM_SPLIT
          return VariableWidthByteStreamSplitDecoder(T).new(descr.type_length)
        end
        return DeltaByteArrayDecoder(T).new if encoding == Encoding::DELTA_BYTE_ARRAY
      {% end %}

      case encoding
      when Encoding::RLE, Encoding::DELTA_BINARY_PACKED, Encoding::DELTA_BYTE_ARRAY,
           Encoding::DELTA_LENGTH_BYTE_ARRAY, Encoding::ALP
        raise general_err!("Encoding #{encoding} is not supported for type")
      else
        raise nyi_err!("Encoding #{encoding} is not supported")
      end
    end
  end
end
