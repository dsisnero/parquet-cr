require "uuid"
require "./errors"
require "./utils"
require "./short_string"

# Ported from `parquet-variant/src/decoder.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `VariantBasicType`, `VariantPrimitiveType`, `get_basic_type`,
# `get_primitive_type`, `OffsetSizeBytes` (+ `unpack_u32*`),
# `map_bytes_to_offsets`, and the primitive `decode_*` helpers.
# Divergences: `&[u8]`/`&str` are `Bytes`/`String`; errors are raised;
# `unpack_u32*` live on the enum; `map_bytes_to_offsets` returns an
# `Iterator(Int32)`; chrono `NaiveDate`/`DateTime`/`NaiveTime` are `Time`/`Time`
# /`Time::Span` (date-only and time-of-day have no Crystal type), and `Uuid` is
# `UUID`.
module ParquetVariant
  # The basic (2-bit) type of a variant value.
  enum VariantBasicType
    Primitive   = 0
    ShortString = 1
    Object      = 2
    Array       = 3
  end

  # The primitive type of a `VariantBasicType::Primitive` value.
  enum VariantPrimitiveType
    Null               =  0
    BooleanTrue        =  1
    BooleanFalse       =  2
    Int8               =  3
    Int16              =  4
    Int32              =  5
    Int64              =  6
    Double             =  7
    Decimal4           =  8
    Decimal8           =  9
    Decimal16          = 10
    Date               = 11
    TimestampMicros    = 12
    TimestampNtzMicros = 13
    Float              = 14
    Binary             = 15
    String             = 16
    Time               = 17
    TimestampNanos     = 18
    TimestampNtzNanos  = 19
    Uuid               = 20

    # Builds a primitive type from its 6-bit code.
    def self.from_byte(value : UInt8) : VariantPrimitiveType
      VariantPrimitiveType.new(value)
    rescue ArgumentError
      raise InvalidArgumentError.new("unknown primitive type: #{value}")
    end
  end

  # The width, in bytes, of an offset/dictionary-size field.
  enum OffsetSizeBytes
    One   = 1
    Two   = 2
    Three = 3
    Four  = 4

    # Builds from the `offset_size_minus_one` bits (0–3).
    def self.try_new(offset_size_minus_one : UInt8) : OffsetSizeBytes
      case offset_size_minus_one
      when 0 then One
      when 1 then Two
      when 2 then Three
      when 3 then Four
      else
        raise InvalidArgumentError.new("offset_size_minus_one must be 0–3")
      end
    end

    # Returns one little-endian value from `bytes` at `index`.
    def unpack_u32(bytes : Bytes, index : Int32) : UInt32
      unpack_u32_at_offset(bytes, 0, index)
    end

    # Returns one little-endian value from `bytes` at
    # `byte_offset + size * offset_index`.
    def unpack_u32_at_offset(bytes : Bytes, byte_offset : Int32, offset_index : Int32) : UInt32
      offset = offset_index * value + byte_offset
      chunk = Utils.array_from_slice(bytes, offset, value)
      case self
      when One
        chunk[0].to_u32
      when Two
        chunk[0].to_u32 | (chunk[1].to_u32 << 8)
      when Three
        chunk[0].to_u32 | (chunk[1].to_u32 << 8) | (chunk[2].to_u32 << 16)
      else
        chunk[0].to_u32 | (chunk[1].to_u32 << 8) | (chunk[2].to_u32 << 16) | (chunk[3].to_u32 << 24)
      end
    end
  end

  # Value decoding helpers for the variant encoding.
  module Decoder
    # Converts a byte buffer to offset values based on the offset size.
    def self.map_bytes_to_offsets(buffer : Bytes, offset_size : OffsetSizeBytes) : Iterator(Int32)
      size = offset_size.value
      count = buffer.size // size
      (0...count).each.map do |i|
        chunk = buffer[i * size, size]
        case offset_size
        when OffsetSizeBytes::One
          chunk[0].to_i32
        when OffsetSizeBytes::Two
          chunk[0].to_i32 | (chunk[1].to_i32 << 8)
        when OffsetSizeBytes::Three
          chunk[0].to_i32 | (chunk[1].to_i32 << 8) | (chunk[2].to_i32 << 16)
        else
          chunk[0].to_i32 | (chunk[1].to_i32 << 8) | (chunk[2].to_i32 << 16) | (chunk[3].to_i32 << 24)
        end
      end
    end

    # Extracts the basic type from a header byte.
    def self.get_basic_type(header : UInt8) : VariantBasicType
      VariantBasicType.new(header & 0x03)
    end

    # Extracts the primitive type from a value-metadata byte.
    def self.get_primitive_type(metadata : UInt8) : VariantPrimitiveType
      VariantPrimitiveType.from_byte(metadata >> 2)
    end

    def self.decode_int8(data : Bytes) : Int8
      IO::ByteFormat::LittleEndian.decode(Int8, Utils.array_from_slice(data, 0, 1))
    end

    def self.decode_int16(data : Bytes) : Int16
      IO::ByteFormat::LittleEndian.decode(Int16, Utils.array_from_slice(data, 0, 2))
    end

    def self.decode_int32(data : Bytes) : Int32
      IO::ByteFormat::LittleEndian.decode(Int32, Utils.array_from_slice(data, 0, 4))
    end

    def self.decode_int64(data : Bytes) : Int64
      IO::ByteFormat::LittleEndian.decode(Int64, Utils.array_from_slice(data, 0, 8))
    end

    def self.decode_decimal4(data : Bytes) : Tuple(Int32, UInt8)
      scale = IO::ByteFormat::LittleEndian.decode(UInt8, Utils.array_from_slice(data, 0, 1))
      integer = IO::ByteFormat::LittleEndian.decode(Int32, Utils.array_from_slice(data, 1, 4))
      {integer, scale}
    end

    def self.decode_decimal8(data : Bytes) : Tuple(Int64, UInt8)
      scale = IO::ByteFormat::LittleEndian.decode(UInt8, Utils.array_from_slice(data, 0, 1))
      integer = IO::ByteFormat::LittleEndian.decode(Int64, Utils.array_from_slice(data, 1, 8))
      {integer, scale}
    end

    def self.decode_decimal16(data : Bytes) : Tuple(Int128, UInt8)
      scale = IO::ByteFormat::LittleEndian.decode(UInt8, Utils.array_from_slice(data, 0, 1))
      integer = IO::ByteFormat::LittleEndian.decode(Int128, Utils.array_from_slice(data, 1, 16))
      {integer, scale}
    end

    def self.decode_float(data : Bytes) : Float32
      IO::ByteFormat::LittleEndian.decode(Float32, Utils.array_from_slice(data, 0, 4))
    end

    def self.decode_double(data : Bytes) : Float64
      IO::ByteFormat::LittleEndian.decode(Float64, Utils.array_from_slice(data, 0, 8))
    end

    # Decodes a date (days since epoch) as a UTC `Time` at midnight.
    def self.decode_date(data : Bytes) : Time
      days_since_epoch = decode_int32(data)
      begin
        Time.unix(days_since_epoch.to_i64 * 86_400)
      rescue ArgumentError | OverflowError
        raise CastError.new("Could not cast `#{days_since_epoch}` days into a NaiveDate")
      end
    end

    # Decodes a timestamp (micros since epoch).
    def self.decode_timestamp_micros(data : Bytes) : Time
      micros_since_epoch = decode_int64(data)
      begin
        Time.unix_ns(micros_since_epoch * 1_000)
      rescue ArgumentError | OverflowError
        raise CastError.new("Could not cast `#{micros_since_epoch}` microseconds into a DateTime<Utc>")
      end
    end

    # Decodes a naive timestamp (micros since epoch).
    def self.decode_timestampntz_micros(data : Bytes) : Time
      decode_timestamp_micros(data)
    end

    # Decodes a time-of-day (micros since midnight) as a `Time::Span`.
    def self.decode_time_ntz(data : Bytes) : Time::Span
      micros_since_midnight = IO::ByteFormat::LittleEndian.decode(UInt64, Utils.array_from_slice(data, 0, 8))

      if micros_since_midnight >= 86_400_000_000
        raise CastError.new("Could not cast #{micros_since_midnight} microseconds into a NaiveTime")
      end

      Time::Span.new(nanoseconds: (micros_since_midnight * 1_000).to_i64)
    end

    # Decodes a timestamp (nanos since epoch).
    def self.decode_timestamp_nanos(data : Bytes) : Time
      nanos_since_epoch = decode_int64(data)
      begin
        Time.unix_ns(nanos_since_epoch)
      rescue ArgumentError | OverflowError
        raise CastError.new("Could not cast `#{nanos_since_epoch}` nanoseconds into a DateTime<Utc>")
      end
    end

    # Decodes a naive timestamp (nanos since epoch).
    def self.decode_timestampntz_nanos(data : Bytes) : Time
      decode_timestamp_nanos(data)
    end

    # Decodes a UUID.
    def self.decode_uuid(data : Bytes) : UUID
      UUID.new(Utils.array_from_slice(data, 0, 16))
    end

    # Decodes a length-prefixed binary value.
    def self.decode_binary(data : Bytes) : Bytes
      len = IO::ByteFormat::LittleEndian.decode(UInt32, Utils.array_from_slice(data, 0, 4)).to_i32
      Utils.slice_from_slice_at_offset(data, 4, 0...len)
    end

    # Decodes a length-prefixed long string.
    def self.decode_long_string(data : Bytes) : String
      len = IO::ByteFormat::LittleEndian.decode(UInt32, Utils.array_from_slice(data, 0, 4)).to_i32
      Utils.string_from_slice(data, 4, 0...len)
    end

    # Decodes a short string whose length is encoded in `metadata`.
    def self.decode_short_string(metadata : UInt8, data : Bytes) : ShortString
      len = (metadata >> 2).to_i32
      ShortString.try_new(Utils.string_from_slice(data, 0, 0...len))
    end
  end
end
