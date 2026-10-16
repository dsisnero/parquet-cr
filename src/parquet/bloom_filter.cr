require "./basic"
require "./errors"
require "./util/xxhash"
require "./thrift"
require "./file/metadata/column_chunk"
require "./file/chunk_reader"

# Ported from `parquet/src/bloom_filter/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the split block Bloom filter (`Sbbf`), its `Block`, the
# `BloomFilterHeader` thrift struct, and the header/serialization helpers.
#
# Divergence: `twox_hash::XxHash64` is replaced by the in-repo
# `Parquet::XxHash64` (exact values preserved); trait-object `AsBytes` dispatch
# is a `String` overload plus a generic `as_bytes`; internal helpers used by the
# upstream white-box tests (`Block.mask`, `Sbbf#hash_to_block_index`,
# `insert_hash`, `fold_n`, `blocks`, `Block#words`) are exposed;
# `estimated_memory_size` uses the block count rather than capacity;
# `is_sorted`/`is_dict`-style names are preserved.
module Parquet
  # Parquet split block Bloom filter.
  module BloomFilter
    # Salt as defined in the spec.
    SALT = [
      0x47b6137b_u32,
      0x44974d91_u32,
      0x8824ad5b_u32,
      0xa2b7289d_u32,
      0x705495c7_u32,
      0x2df1424b_u32,
      0x9efc4947_u32,
      0x5c6bfb31_u32,
    ]

    # Estimated maximum serialized header size.
    SBBF_HEADER_SIZE_ESTIMATE = 20

    # The minimum/maximum number of bytes for a bloom filter bitset.
    BITSET_MIN_LENGTH = 32
    BITSET_MAX_LENGTH = 128 * 1024 * 1024

    # Per-spec xxHash seed.
    SEED = 0_u64

    # Bloom filter header stored at the beginning of a column's bloom filter
    # data, followed by its bitset.
    define_thrift_struct(BloomFilterHeader) do
      required 1, num_bytes, Int32
      required 2, algorithm, BloomFilterAlgorithm
      required 3, hash, BloomFilterHash
      required 4, compression, BloomFilterCompression
    end

    # A single 256-bit block: eight 32-bit words.
    struct Block
      getter words : StaticArray(UInt32, 8)

      def initialize(@words : StaticArray(UInt32, 8))
      end

      ZERO = Block.new(StaticArray[
        0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32,
      ])

      # Produce a block where each of the 8 words has exactly one bit set.
      def self.mask(x : UInt32) : Block
        result = StaticArray[0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32]
        8.times do |i|
          y = x &* SALT[i]
          y = y >> 27
          result[i] = 1_u32 << y
        end
        Block.new(result)
      end

      # OR the mask bits for `hash` into this block.
      def insert(hash : UInt32) : Nil
        mask = Block.mask(hash)
        8.times { |i| @words[i] |= mask.words[i] }
      end

      # Check membership: `true` when every mask bit is already set.
      def check(hash : UInt32) : Bool
        mask = Block.mask(hash)
        8.times do |i|
          return false if (@words[i] & mask.words[i]) == 0
        end
        true
      end

      # Count the total number of set bits across all 8 words.
      def count_ones : Int32
        total = 0_u64
        @words.each { |word| total += word.popcount }
        total.to_i32
      end

      # Bitwise OR of two blocks.
      def |(other : Block) : Block
        result = StaticArray[0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32]
        8.times { |i| result[i] = @words[i] | other.words[i] }
        Block.new(result)
      end
    end

    # Hash `bytes` with xxHash64/seed 0.
    def self.hash_as_bytes(bytes : Bytes) : UInt64
      XxHash64.oneshot(bytes, SEED)
    end

    # Next power of two `>= value` (value `>= 1`).
    def self.next_power_of_two(value : Int64) : Int64
      return 1_i64 if value <= 1

      x = value - 1
      x |= x >> 1
      x |= x >> 2
      x |= x >> 4
      x |= x >> 8
      x |= x >> 16
      x |= x >> 32
      x + 1
    end

    # Number of trailing zero bits of a positive power-of-two `Int32`.
    def self.trailing_zeros(value : Int32) : Int32
      return 32 if value == 0

      count = 0
      v = value
      while (v & 1) == 0
        count += 1
        v >>= 1
      end
      count
    end

    # The number of bytes adjusted to the next power of two, bounded.
    def self.optimal_num_of_bytes(num_bytes : Int64) : Int64
      n = Math.min(num_bytes, BITSET_MAX_LENGTH.to_i64)
      n = Math.max(n, BITSET_MIN_LENGTH.to_i64)
      next_power_of_two(n)
    end

    # Total number of bits needed for `ndv` distinct values at `fpp`.
    def self.num_of_bits_from_ndv_fpp(ndv : UInt64, fpp : Float64) : UInt64
      num_bits = -8.0 * ndv.to_f64 / Math.log(1.0 - fpp ** (1.0 / 8.0))
      num_bits.to_u64
    end

    # Reads a bloom filter header and returns it with the header length.
    def self.read_bloom_filter_header_and_length(buffer : Bytes) : Tuple(BloomFilterHeader, Int64)
      total_length = buffer.size
      protocol = Thrift::ThriftSliceInputProtocol.new(buffer)
      header =
        begin
          BloomFilterHeader.read_thrift(protocol)
        rescue ex
          raise general_err!("Could not read bloom filter header: #{ex.message}")
        end
      {header, (total_length - protocol.as_slice.size).to_i64}
    end

    # Reads a bloom filter header from `buffer` and returns it with the absolute
    # offset of the bitset.
    def self.chunk_read_bloom_filter_header_and_offset(offset : UInt64, buffer : Bytes) : Tuple(BloomFilterHeader, UInt64)
      header, length = read_bloom_filter_header_and_length(buffer)
      {header, offset + length.to_u64}
    end

    # A split block Bloom filter (SBBF).
    class Sbbf
      getter blocks : Array(Block)

      def initialize(@blocks : Array(Block))
      end

      # Creates a new `Sbbf` from a raw byte slice.
      def self.new(bitset : Bytes) : Sbbf
        num_blocks = bitset.size // 32
        blocks = Array(Block).new(num_blocks) do |i|
          words = StaticArray[0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32]
          8.times { |word| words[word] = read_le_u32(bitset, i * 32 + word * 4) }
          Block.new(words)
        end
        new(blocks)
      end

      # Creates a new `Sbbf` with `num_bytes` bytes (adjusted to a bounded power
      # of two).
      def self.new_with_num_of_bytes(num_bytes : Int64) : Sbbf
        num_bytes = BloomFilter.optimal_num_of_bytes(num_bytes)
        num_blocks = (num_bytes // 32).to_i32
        new(Array(Block).new(num_blocks, Block::ZERO))
      end

      # Creates a new `Sbbf` sized for `ndv` distinct values at `fpp`.
      def self.new_with_ndv_fpp(ndv : UInt64, fpp : Float64) : Sbbf
        unless 0.0 <= fpp < 1.0
          raise general_err!("False positive probability must be between 0.0 and 1.0, got #{fpp}")
        end
        num_bits = BloomFilter.num_of_bits_from_ndv_fpp(ndv, fpp)
        new_with_num_of_bytes((num_bits // 8).to_i64)
      end

      # Reads a bloom filter from `reader` at the offset recorded on
      # `column_metadata`, or `nil` if none is present.
      def self.read_from_column_chunk(column_metadata : File::Metadata::ColumnChunkMetaData, reader : File::ChunkReader) : Sbbf?
        offset = column_metadata.bloom_filter_offset
        return nil if offset.nil?

        offset = offset.to_u64
        buffer =
          if length = column_metadata.bloom_filter_length
            reader.get_bytes(offset, length)
          else
            reader.get_bytes(offset, SBBF_HEADER_SIZE_ESTIMATE)
          end

        header, bitset_offset = BloomFilter.chunk_read_bloom_filter_header_and_offset(offset, buffer)

        bitset =
          if column_metadata.bloom_filter_length
            buffer[(bitset_offset - offset).to_i32, buffer.size - (bitset_offset - offset).to_i32]
          else
            bitset_length = header.num_bytes
            reader.get_bytes(bitset_offset, bitset_length)
          end

        Sbbf.new(bitset)
      end

      def clone : Sbbf
        Sbbf.new(@blocks.dup)
      end

      # Maps a 64-bit hash to a block index in `[0, num_blocks)`.
      def hash_to_block_index(hash : UInt64) : Int32
        (((hash >> 32) &* @blocks.size.to_u64) >> 32).to_i32
      end

      # Inserts an `AsBytes`-ish value into the filter.
      def insert(value : String) : Nil
        insert_hash(BloomFilter.hash_as_bytes(value.to_slice))
      end

      def insert(value : T) : Nil forall T
        insert_hash(BloomFilter.hash_as_bytes(value.as_bytes))
      end

      # Inserts a precomputed hash.
      def insert_hash(hash : UInt64) : Nil
        block_index = hash_to_block_index(hash)
        block = @blocks[block_index]
        block.insert(low_u32(hash))
        @blocks[block_index] = block
      end

      # Returns whether `value` is probably present.
      def check(value : String) : Bool
        check_hash(BloomFilter.hash_as_bytes(value.to_slice))
      end

      def check(value : T) : Bool forall T
        check_hash(BloomFilter.hash_as_bytes(value.as_bytes))
      end

      # Checks a precomputed hash.
      def check_hash(hash : UInt64) : Bool
        block_index = hash_to_block_index(hash)
        @blocks[block_index].check(low_u32(hash))
      end

      # In-memory size of this bloom filter in bytes.
      def estimated_memory_size : Int32
        (@blocks.size * 32).to_i32
      end

      # Number of blocks in this bloom filter.
      def num_blocks : Int32
        @blocks.size
      end

      # Folds the filter down to the smallest size still meeting `target_fpp`.
      def fold_to_target_fpp(target_fpp : Float64) : Nil
        num_folds = num_folds_for_target_fpp(target_fpp)
        fold_n(num_folds) if num_folds > 0
      end

      # Number of folds that keep the estimated fpp within `target_fpp`.
      def num_folds_for_target_fpp(target_fpp : Float64) : Int32
        len = @blocks.size
        return 0 if len < 2

        total_set_bits = @blocks.sum(&.count_ones).to_u64
        avg_fill = total_set_bits.to_f64 / (len.to_f64 * 256.0)
        return BloomFilter.trailing_zeros(len) if avg_fill == 0.0

        max_folds = BloomFilter.trailing_zeros(len)
        one_minus_f = 1.0 - avg_fill
        one_minus_fk = one_minus_f
        num_folds = 0

        max_folds.times do
          one_minus_fk = one_minus_fk * one_minus_fk
          fk = 1.0 - one_minus_fk
          estimated_fpp = fk ** 8
          break if estimated_fpp > target_fpp

          num_folds += 1
        end

        num_folds
      end

      # Folds the filter `num_folds` times in a single pass.
      def fold_n(num_folds : Int32) : Nil
        raise "num_folds must be at least 1" if num_folds <= 0

        len = @blocks.size
        group_size = 1 << num_folds
        if group_size > len
          raise "Cannot fold #{num_folds} times: need at least #{group_size} blocks, have #{len}"
        end

        new_len = len // group_size
        new_len.times do |i|
          start = i * group_size
          merged = @blocks[start]
          (1...group_size).each { |j| merged = merged | @blocks[start + j] }
          @blocks[i] = merged
        end
        @blocks = @blocks[0, new_len]
      end

      # Writes the header and bitset to `io`.
      def write(io : IO) : Nil
        protocol = Thrift::ThriftCompactOutputProtocol.new(io)
        header.write_thrift(protocol)
        write_bitset(io)
      end

      # Writes the bitset in serialized (little-endian) form.
      def write_bitset(io : IO) : Nil
        @blocks.each do |block|
          8.times { |i| io.write_bytes(block.words[i], IO::ByteFormat::LittleEndian) }
        end
      end

      # Reads an `Sbbf` from Thrift-encoded bytes.
      def self.from_bytes(bytes : Bytes) : Sbbf
        header, header_len = BloomFilter.read_bloom_filter_header_and_length(bytes)

        bitset_length = header.num_bytes.to_u64
        if header_len + bitset_length != bytes.size.to_u64
          raise general_err!(
            "Bloom filter data contains extra bytes: expected #{header_len + bitset_length} total bytes, got #{bytes.size}"
          )
        end

        start = header_len.to_i32
        finish = start + bitset_length.to_i32
        bitset = bytes[start, finish - start]
        new(bitset)
      end

      private def header : BloomFilterHeader
        BloomFilterHeader.new(
          @blocks.size * 32,
          BloomFilterAlgorithm::BLOCK,
          BloomFilterHash::XXHASH,
          BloomFilterCompression::UNCOMPRESSED,
        )
      end

      private def self.read_le_u32(bytes : Bytes, offset : Int32) : UInt32
        IO::Memory.new(bytes[offset, 4]).read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      end

      private def low_u32(value : UInt64) : UInt32
        (value & 0xFFFF_FFFF_u64).to_u32!
      end
    end
  end
end
