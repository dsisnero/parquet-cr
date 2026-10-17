require "compress/gzip"
require "./basic"
require "./errors"

# Ported from `parquet/src/compression.rs` (`Codec`, `create_codec`, `SnappyCodec`,
# `GZipCodec`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `Codec` framework, the SNAPPY codec, and the GZIP codec (Crystal
# stdlib `Compress::Gzip`). Divergence: upstream uses the `snap` crate; Crystal
# has no equivalent shard, so `Snappy` implements the raw (block) snappy format
# directly (compressor + decompressor, wire-compatible with `snap`). The
# brotli/lz4/lzo/zstd codecs are still not ported — `create_codec` raises for
# them (see `plans/parity.md` Phase 1.8). The Rust `Codec` trait (mutating
# `Vec<u8>` buffers) becomes an abstract class appending to `Array(UInt8)`;
# `snap::Error` maps to `ExternalError` wrapping a `SnappyError`.
module Parquet
  # Compression codec implementations.
  module CompressionCodecs
    # A compression codec: compresses into / decompresses from an output buffer.
    abstract class Codec
      # Compresses `input`, appending the result to `output`.
      abstract def compress(input : Bytes, output : Array(UInt8)) : Nil

      # Decompresses `input`, appending the result to `output`, returning the
      # number of decompressed bytes appended.
      abstract def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
    end

    # Invalid snappy input or input that cannot be encoded.
    #
    # Divergence: upstream surfaces `snap::Error` (wrapped in
    # `ParquetError::External`); this stands in for that error type.
    class SnappyError < Exception
      def initialize(message : String)
        super("snappy: #{message}")
      end
    end

    # The snappy raw (block) format, as used by Parquet (no stream framing).
    #
    # Divergences: upstream delegates to the `snap` crate (`snap::raw::Encoder`/
    # `Decoder`, `decompress_len`, `max_compress_len`); this is a direct
    # implementation of the same wire format. It always appends to the output
    # (so a `max_compress_len` preallocation is unnecessary), and malformed input
    # raises `ExternalError` wrapping `SnappyError` (upstream
    # `ParquetError::External`).
    #
    # Wire format:
    # - a ULEB128 preamble holding the uncompressed length, then
    # - elements: literals (`tag & 0x03 == 0`) and copies with a 1-, 2-, or
    #   4-byte little-endian offset (`tag & 0x03 == 1/2/3`).
    #
    # See <https://github.com/google/snappy/blob/main/format_description.txt>.
    module Snappy
      # Minimum table size used by the match finder.
      MIN_HASH_TABLE_BITS = 8
      # Maximum table size used by the match finder (matches `snap`).
      MAX_HASH_TABLE_BITS = 14
      # Snappy's multiplicative hash constant for 4-byte sequences.
      HASH_MULTIPLIER = 0x1e35a7bd_u32
      # Maximum offset encodable by a 2-byte-offset copy.
      MAX_COPY_OFFSET = 0xFFFF
      # Maximum match length encodable by a single copy element.
      MAX_COPY_LENGTH = 64
      # Copied byte sequences shorter than this start a match.
      MIN_MATCH_LENGTH = 4

      # Appends the raw-snappy encoding of `input` to `output`.
      def self.compress(input : Bytes, output : Array(UInt8)) : Nil
        size = input.size
        write_length(size, output)

        if size < MIN_MATCH_LENGTH
          write_literal(input, 0, size, output) if size > 0
          return nil
        end

        bits = hash_table_bits(size)
        mask = (1 << bits) - 1
        table = Array(Int32).new(1 << bits, -1)

        ip = 0
        next_emit = 0
        limit = size - MIN_MATCH_LENGTH
        while ip <= limit
          index = hash_index(input, ip, bits) & mask
          candidate = table[index]
          table[index] = ip

          if match_at?(input, candidate, ip)
            write_literal(input, next_emit, ip - next_emit, output) if ip > next_emit
            length = match_length(input, candidate, ip)
            write_copy(ip - candidate, length, output)
            ip += length
            next_emit = ip
          else
            ip += 1
          end
        end

        write_literal(input, next_emit, size - next_emit, output) if next_emit < size
        nil
      end

      # Decompresses `input`, appending exactly the decoded bytes to `output`,
      # and returns the number of bytes appended.
      #
      # When `expected_size` is set it must equal the length in the preamble
      # (upstream `snap::raw::Decoder` requires the destination slice to match).
      def self.decompress(input : Bytes, output : Array(UInt8), expected_size : Int32?) : Int32
        length, pos = read_length(input)
        if (expected = expected_size) && expected != length
          raise corrupt("expected #{expected} bytes but got #{length}")
        end

        start = output.size
        decode_elements(input, pos, output, start, length)

        produced = output.size - start
        raise corrupt("expected #{length} bytes but produced #{produced}") unless produced == length

        produced
      end

      # Chooses the match-finder table size (8..=14 bits) for `size` bytes.
      private def self.hash_table_bits(size : Int32) : Int32
        bits = MIN_HASH_TABLE_BITS
        while bits < MAX_HASH_TABLE_BITS && (1 << bits) < size
          bits += 1
        end
        bits
      end

      # Multiplicative hash of the 4 bytes at `pos` (top `bits` of a u32).
      private def self.hash_index(input : Bytes, pos : Int32, bits : Int32) : Int32
        ((read_u32_le(input, pos) &* HASH_MULTIPLIER) >> (32 - bits)).to_i32
      end

      # Whether `candidate` is a usable 4-byte match for the sequence at `ip`.
      private def self.match_at?(input : Bytes, candidate : Int32, ip : Int32) : Bool
        !candidate.negative? &&
          ip - candidate <= MAX_COPY_OFFSET &&
          read_u32_le(input, candidate) == read_u32_le(input, ip)
      end

      # Length of the match starting at `candidate`/`ip` (already 4 bytes long).
      private def self.match_length(input : Bytes, candidate : Int32, ip : Int32) : Int32
        length = MIN_MATCH_LENGTH
        while ip + length < input.size && input[candidate + length] == input[ip + length]
          length += 1
        end
        length
      end

      # Appends a ULEB128 encoded length.
      private def self.write_length(size : Int32, output : Array(UInt8)) : Nil
        value = size.to_u32
        while value >= 0x80
          output << ((value & 0x7F) | 0x80).to_u8
          value >>= 7
        end
        output << value.to_u8
        nil
      end

      # Reads the ULEB128 length preamble, returning it and the bytes consumed.
      private def self.read_length(input : Bytes) : Tuple(Int32, Int32)
        length = 0_u32
        shift = 0
        pos = 0
        loop do
          raise corrupt("truncated length preamble") if pos >= input.size

          byte = input[pos]
          pos += 1
          length |= (byte & 0x7F).to_u32 << shift
          return {length.to_i32, pos} if byte & 0x80 == 0
          raise corrupt("length preamble overflow") if shift >= 28

          shift += 7
        end
      end

      # Appends a literal element covering `[start, start + len)` of `input`.
      private def self.write_literal(input : Bytes, start : Int32, len : Int32, output : Array(UInt8)) : Nil
        return if len <= 0

        value = (len - 1).to_u32
        if len <= 60
          output << (value.to_u8 << 2)
        else
          extra_bytes = Math.min(Math.max((value.bit_length + 7) // 8, 1), 4)
          output << ((59 + extra_bytes).to_u8 << 2)
          extra_bytes.times { |i| output << ((value >> (8 * i)) & 0xFF).to_u8 }
        end

        offset = 0
        while offset < len
          output << input[start + offset]
          offset += 1
        end
        nil
      end

      # Appends one or more copy elements for `length` bytes at `offset`.
      private def self.write_copy(offset : Int32, length : Int32, output : Array(UInt8)) : Nil
        raise SnappyError.new("copy offset #{offset} out of range") if offset <= 0

        remaining = length
        while remaining > 0
          # A 1-byte-offset copy covers lengths 4..=11 (offsets < 2048); anything
          # else uses 2-byte offsets in chunks of at most 64 bytes.
          if offset < 2048 && remaining >= MIN_MATCH_LENGTH && remaining <= 11
            output << (1_u8 | ((remaining - MIN_MATCH_LENGTH).to_u8 << 2) | ((offset >> 8).to_u8 << 5))
            output << (offset & 0xFF).to_u8
            remaining = 0
          else
            chunk = Math.min(remaining, MAX_COPY_LENGTH)
            output << (2_u8 | ((chunk - 1).to_u8 << 2))
            output << (offset & 0xFF).to_u8
            output << ((offset >> 8) & 0xFF).to_u8
            remaining -= chunk
          end
        end
        nil
      end

      # Reads a little-endian u32 from `bytes` at `pos`.
      private def self.read_u32_le(bytes : Bytes, pos : Int32) : UInt32
        bytes[pos].to_u32 |
          (bytes[pos + 1].to_u32 << 8) |
          (bytes[pos + 2].to_u32 << 16) |
          (bytes[pos + 3].to_u32 << 24)
      end

      # Decodes elements until the input is exhausted, returning the new offset.
      private def self.decode_elements(
        input : Bytes,
        pos : Int32,
        output : Array(UInt8),
        start : Int32,
        length : Int32,
      ) : Int32
        while pos < input.size
          tag = input[pos]
          pos = decode_element(input, pos + 1, tag, output, start, length)
        end
        pos
      end

      # Decodes one literal or copy element, returning the offset after it.
      private def self.decode_element(
        input : Bytes,
        pos : Int32,
        tag : UInt8,
        output : Array(UInt8),
        start : Int32,
        length : Int32,
      ) : Int32
        case tag & 0x03
        when 0 then decode_literal(input, pos, tag, output, start, length)
        when 1 then decode_copy1(input, pos, tag, output, start, length)
        when 2 then decode_copy2(input, pos, tag, output, start, length)
        else        decode_copy4(input, pos, tag, output, start, length)
        end
      end

      # Decodes a literal element (direct bytes, or a 1-4 byte length suffix).
      private def self.decode_literal(
        input : Bytes,
        pos : Int32,
        tag : UInt8,
        output : Array(UInt8),
        start : Int32,
        length : Int32,
      ) : Int32
        len = (tag >> 2).to_i32 + 1
        if len > 60
          extra = len - 60
          raise corrupt("truncated literal length") if pos + extra > input.size

          len = 0
          extra.times { |i| len |= input[pos + i].to_i32 << (8 * i) }
          len += 1
          pos += extra
        end
        raise corrupt("truncated literal") if pos + len > input.size

        check_output_budget(output.size - start + len, length, "literal")
        i = 0
        while i < len
          output << input[pos + i]
          i += 1
        end
        pos + len
      end

      # Decodes a copy with a 11-bit offset (length 4..=11).
      private def self.decode_copy1(
        input : Bytes,
        pos : Int32,
        tag : UInt8,
        output : Array(UInt8),
        start : Int32,
        length : Int32,
      ) : Int32
        raise corrupt("truncated copy") if pos >= input.size

        len = ((tag >> 2).to_i32 & 0x07) + MIN_MATCH_LENGTH
        offset = ((tag >> 5).to_i32 << 8) | input[pos].to_i32
        append_copy(offset, len, output, start, length)
        pos + 1
      end

      # Decodes a copy with a 16-bit offset (length 1..=64).
      private def self.decode_copy2(
        input : Bytes,
        pos : Int32,
        tag : UInt8,
        output : Array(UInt8),
        start : Int32,
        length : Int32,
      ) : Int32
        raise corrupt("truncated copy") if pos + 2 > input.size

        len = (tag >> 2).to_i32 + 1
        offset = input[pos].to_i32 | (input[pos + 1].to_i32 << 8)
        append_copy(offset, len, output, start, length)
        pos + 2
      end

      # Decodes a copy with a 32-bit offset (length 1..=64).
      private def self.decode_copy4(
        input : Bytes,
        pos : Int32,
        tag : UInt8,
        output : Array(UInt8),
        start : Int32,
        length : Int32,
      ) : Int32
        raise corrupt("truncated copy") if pos + 4 > input.size

        len = (tag >> 2).to_i32 + 1
        offset = input[pos].to_i32 |
                 (input[pos + 1].to_i32 << 8) |
                 (input[pos + 2].to_i32 << 16) |
                 (input[pos + 3].to_i32 << 24)
        append_copy(offset, len, output, start, length)
        pos + 4
      end

      # Validates that appending `pending` more bytes stays within the declared
      # uncompressed length.
      private def self.check_output_budget(pending : Int32, length : Int32, kind : String) : Nil
        if pending > length
          raise corrupt("#{kind} exceeds the declared #{length} bytes")
        end
        nil
      end

      # Appends a decompressed copy, allowing overlapping offsets (which repeat
      # the preceding `offset` bytes, per the snappy format).
      private def self.append_copy(
        offset : Int32,
        len : Int32,
        output : Array(UInt8),
        start : Int32,
        length : Int32,
      ) : Nil
        if offset <= 0 || offset > output.size - start
          raise corrupt("copy offset #{offset} out of range")
        end
        check_output_budget(output.size - start + len, length, "copy")

        index = output.size - offset
        remaining = len
        while remaining > 0
          output << output[index]
          index += 1
          remaining -= 1
        end
        nil
      end

      private def self.corrupt(detail : String) : ExternalError
        ExternalError.new(SnappyError.new("corrupt input (#{detail})"))
      end
    end

    # SNAPPY codec (raw/block format, see `Snappy`).
    class SnappyCodec < Codec
      def initialize
      end

      def compress(input : Bytes, output : Array(UInt8)) : Nil
        Snappy.compress(input, output)
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        Snappy.decompress(input, output, uncompress_size)
      end
    end

    # GZIP codec (Crystal stdlib `Compress::Gzip`).
    class GZipCodec < Codec
      def initialize(@level : GzipLevel)
      end

      def compress(input : Bytes, output : Array(UInt8)) : Nil
        io = IO::Memory.new
        writer = Compress::Gzip::Writer.new(io, @level.compression_level)
        writer.write(input)
        writer.close
        output.concat(io.to_slice)
        nil
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        _ = uncompress_size
        reader = Compress::Gzip::Reader.new(IO::Memory.new(input))
        decompressed = IO::Memory.new
        IO.copy(reader, decompressed)
        bytes = decompressed.to_slice
        output.concat(bytes)
        bytes.size
      end
    end

    # Returns the codec for `compression`, or `nil` for `UNCOMPRESSED`.
    def self.create_codec(compression : Compression, options : CodecOptions) : Codec?
      _ = options
      case compression.kind
      when Compression::Kind::UNCOMPRESSED
        nil
      when Compression::Kind::SNAPPY
        SnappyCodec.new
      when Compression::Kind::GZIP
        GZipCodec.new(GzipLevel.try_new(compression.level || GzipLevel.default.compression_level))
      else
        raise general_err!("The codec type #{compression} is not supported yet")
      end
    end
  end
end
