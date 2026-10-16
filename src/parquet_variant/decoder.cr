require "./errors"
require "./utils"

# Ported from `parquet-variant/src/decoder.rs` (`OffsetSizeBytes`,
# `map_bytes_to_offsets`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: `&[u8]` is `Bytes`; errors are raised; the `unpack_u32*` methods
# live on the enum; `map_bytes_to_offsets` returns an `Iterator(Int32)`.
module ParquetVariant
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

  # Decoding helpers for the variant encoding.
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
  end
end
