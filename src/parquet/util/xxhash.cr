# xxHash implementations used on the wire.
#
# `XxHash64` is ported from the xxHash64 algorithm used by
# `twox_hash::XxHash64::oneshot` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b); `XxHash32` is the standard XXH32
# used by the LZ4 frame format's header/content checksums.
#
# Divergence: Crystal has no `twox_hash` shard, so the standard XXH64 algorithm
# is implemented directly (seed 0 by default), preserving exact hash values
# (required for on-disk bloom-filter compatibility).
module Parquet
  # Standard XXH32 hashing (seed 0 by default).
  #
  # Used by the LZ4 frame format for its header and content checksums (the
  # `lz4_flex` crate's `xxh32`). Not used by the parquet metadata; XXH64 above
  # covers the bloom-filter hashing.
  module XxHash32
    PRIME32_1 = 2_654_435_761_u32
    PRIME32_2 = 2_246_822_519_u32
    PRIME32_3 = 3_266_489_917_u32
    PRIME32_4 =   668_265_263_u32
    PRIME32_5 =   374_761_393_u32

    def self.oneshot(data : Bytes, seed : UInt32 = 0_u32) : UInt32
      length = data.size
      index = 0

      if length >= 16
        v1 = seed &+ PRIME32_1 &+ PRIME32_2
        v2 = seed &+ PRIME32_2
        v3 = seed
        v4 = seed &- PRIME32_1

        while index + 16 <= length
          v1 = round(v1, read_u32(data, index))
          index += 4
          v2 = round(v2, read_u32(data, index))
          index += 4
          v3 = round(v3, read_u32(data, index))
          index += 4
          v4 = round(v4, read_u32(data, index))
          index += 4
        end

        hash = rotate_left(v1, 1) &+ rotate_left(v2, 7) &+ rotate_left(v3, 12) &+ rotate_left(v4, 18)
      else
        hash = seed &+ PRIME32_5
      end

      hash &+= length.to_u32

      while index + 4 <= length
        hash &+= read_u32(data, index) &* PRIME32_3
        hash = rotate_left(hash, 17) &* PRIME32_4
        index += 4
      end

      while index < length
        hash &+= data[index].to_u32 &* PRIME32_5
        hash = rotate_left(hash, 11) &* PRIME32_1
        index += 1
      end

      hash ^= hash >> 15
      hash &*= PRIME32_2
      hash ^= hash >> 13
      hash &*= PRIME32_3
      hash ^= hash >> 16
      hash
    end

    private def self.round(acc : UInt32, input : UInt32) : UInt32
      rotate_left(acc &+ input &* PRIME32_2, 13) &* PRIME32_1
    end

    private def self.rotate_left(value : UInt32, bits : Int32) : UInt32
      (value << bits) | (value >> (32 - bits))
    end

    private def self.read_u32(data : Bytes, offset : Int32) : UInt32
      IO::Memory.new(data[offset, 4]).read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    end
  end

  # Standard XXH64 hashing.
  module XxHash64
    PRIME64_1 = 0x9E3779B185EBCA87_u64
    PRIME64_2 = 0xC2B2AE3D27D4EB4F_u64
    PRIME64_3 = 0x165667B19E3779F9_u64
    PRIME64_4 = 0x85EBCA77C2B2AE63_u64
    PRIME64_5 = 0x27D4EB2F165667C5_u64

    def self.oneshot(data : Bytes, seed : UInt64 = 0_u64) : UInt64
      length = data.size
      index = 0

      if length >= 32
        v1 = seed &+ PRIME64_1 &+ PRIME64_2
        v2 = seed &+ PRIME64_2
        v3 = seed
        v4 = seed &- PRIME64_1

        while index + 32 <= length
          v1 = round(v1, read_u64(data, index))
          index += 8
          v2 = round(v2, read_u64(data, index))
          index += 8
          v3 = round(v3, read_u64(data, index))
          index += 8
          v4 = round(v4, read_u64(data, index))
          index += 8
        end

        hash = rotate_left(v1, 1) &+ rotate_left(v2, 7) &+ rotate_left(v3, 12) &+ rotate_left(v4, 18)
        hash = merge_round(hash, v1)
        hash = merge_round(hash, v2)
        hash = merge_round(hash, v3)
        hash = merge_round(hash, v4)
      else
        hash = seed &+ PRIME64_5
      end

      hash &+= length.to_u64

      while index + 8 <= length
        hash ^= round(0_u64, read_u64(data, index))
        hash = rotate_left(hash, 27) &* PRIME64_1 &+ PRIME64_4
        index += 8
      end

      if index + 4 <= length
        hash ^= read_u32(data, index).to_u64 &* PRIME64_1
        hash = rotate_left(hash, 23) &* PRIME64_2 &+ PRIME64_3
        index += 4
      end

      while index < length
        hash ^= data[index].to_u64 &* PRIME64_5
        hash = rotate_left(hash, 11) &* PRIME64_1
        index += 1
      end

      hash ^= hash >> 33
      hash &*= PRIME64_2
      hash ^= hash >> 29
      hash &*= PRIME64_3
      hash ^= hash >> 32
      hash
    end

    private def self.round(acc : UInt64, input : UInt64) : UInt64
      rotate_left(acc &+ input &* PRIME64_2, 31) &* PRIME64_1
    end

    private def self.merge_round(hash : UInt64, value : UInt64) : UInt64
      hash ^= round(0_u64, value)
      hash &* PRIME64_1 &+ PRIME64_4
    end

    private def self.rotate_left(value : UInt64, bits : Int32) : UInt64
      (value << bits) | (value >> (64 - bits))
    end

    private def self.read_u64(data : Bytes, offset : Int32) : UInt64
      IO::Memory.new(data[offset, 8]).read_bytes(UInt64, IO::ByteFormat::LittleEndian)
    end

    private def self.read_u32(data : Bytes, offset : Int32) : UInt32
      IO::Memory.new(data[offset, 4]).read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    end
  end
end
