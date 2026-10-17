require "./errors"
require "./util/xxhash"

# Ported from the LZ4 support used by `parquet/src/compression.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream depends on the `lz4_flex` crate for two wire formats: the raw *block*
# format (`lz4_flex::block`, used by `LZ4_RAW` and by the data payload of
# `LZ4_HADOOP`) and the *frame* container (`lz4_flex::frame`, used by the
# experimental `LZ4Codec` that `LZ4HadoopCodec` falls back to for files written
# by older arrow-rs versions).
#
# Divergence: the block format is delegated to the `paddor/flint` shard (pure
# Crystal, MIT; `Flint::BlockCodec#compress_raw`/`#decompress_raw_into`), the
# one active Crystal LZ4 implementation. No active shard implements the frame
# container (`naqvis/lz4.cr` is a stale `liblz4` binding that only exposes
# frames), so the frame container lives here: magic, frame descriptor, block
# framing, skippable frames, and the XXH32 header/content checksums.
#
# The frame container is self-describing, so the exact block layout produced by
# `compress_frame` (block size, block/content checksums) is an internal detail
# that may differ from `lz4_flex::frame::FrameEncoder` while remaining a valid
# LZ4 frame.
module Parquet
  # LZ4 frame container support (the `lz4_flex::frame` replacement).
  module Lz4
    # Raised for malformed frames and blocks. Wrapped in `ExternalError` at the
    # codec boundary, mirroring upstream's `ParquetError::External(Box<dyn
    # Error>)` for `lz4_flex` errors.
    class Error < Exception
    end

    # Frame magic number (`0x184D2204`), little-endian on disk.
    FRAME_MAGIC = 0x184D_2204_u32
    # Skippable-frame magic range (`0x184D2A50..=0x184D2A5F`).
    SKIPPABLE_MAGIC_MASK = 0xFFFF_FFF0_u32
    SKIPPABLE_MAGIC      = 0x184D_2A50_u32
    # Size of a frame header word.
    SIZE_U32 = 4
    # Maximum block size advertised by `compress_frame`: descriptor code 6
    # (1 MiB). `compress_frame` never emits a bigger block.
    MAX_BLOCK_SIZE = 1024 * 1024

    # Decodes one or more concatenated LZ4 frames (plus any skippable frames)
    # into a newly allocated buffer.
    def self.decompress_frame(input : Bytes) : Bytes
      codec = Flint::BlockCodec.new
      output = IO::Memory.new
      position = 0

      while position < input.size
        if position + SIZE_U32 > input.size
          raise Error.new("truncated LZ4 frame")
        end

        magic = read_u32(input, position)
        position += SIZE_U32

        if magic & SKIPPABLE_MAGIC_MASK == SKIPPABLE_MAGIC
          if position + SIZE_U32 > input.size
            raise Error.new("truncated skippable LZ4 frame")
          end

          size = read_u32(input, position).to_i64
          position += SIZE_U32
          if position + size > input.size
            raise Error.new("truncated skippable LZ4 frame")
          end

          position += size.to_i32
          next
        end

        unless magic == FRAME_MAGIC
          raise Error.new("invalid LZ4 frame magic 0x#{magic.to_s(16)}")
        end

        position = decode_frame(codec, input, position, output)
      end

      output.to_slice.dup
    end

    # Encodes `input` as a single LZ4 frame with independent blocks of at most
    # `block_size` bytes.
    def self.compress_frame(input : Bytes, block_size : Int32 = MAX_BLOCK_SIZE) : Bytes
      unless 0 < block_size <= MAX_BLOCK_SIZE
        raise Error.new("invalid LZ4 frame block size #{block_size}")
      end

      codec = Flint::BlockCodec.new
      output = Array(UInt8).new(input.size // 2 + 32)
      append_u32(output, FRAME_MAGIC)

      # FLG: version 01, independent blocks, no block checksum, no content size,
      # no content checksum, no dictionary ID.
      flg = 0x60_u8
      # BD: block maximum size code 6 (1 MiB).
      bd = 0x60_u8
      output << flg << bd
      # Header checksum: high byte of the XXH32 of the frame descriptor.
      output << ((XxHash32.oneshot(Bytes[flg, bd]) >> 8) & 0xFF).to_u8

      offset = 0
      while offset < input.size
        chunk = input[offset, Math.min(block_size, input.size - offset)]
        compressed = compress_block(codec, chunk)

        if compressed.size >= chunk.size
          # Storing incompressible blocks verbatim is valid (and smaller).
          append_u32(output, chunk.size.to_u32 | 0x8000_0000_u32)
          output.concat(chunk)
        else
          append_u32(output, compressed.size.to_u32)
          output.concat(compressed)
        end

        offset += chunk.size
      end

      append_u32(output, 0_u32) # end mark
      Slice.new(output.to_unsafe, output.size).dup
    end

    # Compresses a single LZ4 block (a fresh `Flint::BlockCodec`).
    def self.compress_block(input : Bytes) : Bytes
      compress_block(Flint::BlockCodec.new, input)
    end

    # Decompresses a single LZ4 block into `destination`, returning the number of
    # bytes written. `destination` may be larger than the decoded block.
    def self.decompress_block(input : Bytes, destination : Bytes) : Int32
      decompress_block(Flint::BlockCodec.new, input, destination)
    end

    private def self.compress_block(codec : Flint::BlockCodec, input : Bytes) : Bytes
      codec.compress_raw(input)
    rescue error : Flint::Error
      raise Error.new(error.message.to_s)
    end

    private def self.decompress_block(codec : Flint::BlockCodec, input : Bytes, destination : Bytes) : Int32
      codec.decompress_raw_into(input, destination)
    rescue error : Flint::Error
      raise Error.new(error.message.to_s)
    end

    # Decodes the frame that starts at `position` (`input[position - 4]` is the
    # frame magic) into `output`, returning the offset just past the frame.
    private def self.decode_frame(codec : Flint::BlockCodec, input : Bytes, position : Int32, output : IO::Memory) : Int32
      body_start, block_max_size, block_checksum, content_checksum =
        read_frame_descriptor(input, position)

      frame_output = IO::Memory.new
      position = decode_frame_blocks(codec, input, body_start, block_max_size, block_checksum, frame_output)

      if content_checksum
        unless position + SIZE_U32 <= input.size
          raise Error.new("truncated LZ4 frame content checksum")
        end

        expected = read_u32(input, position)
        if XxHash32.oneshot(frame_output.to_slice) != expected
          raise Error.new("invalid LZ4 frame content checksum")
        end
        position += SIZE_U32
      end

      output.write(frame_output.to_slice)
      position
    end

    # Validates the frame descriptor that starts at `position` (right after the
    # magic) and returns `{body_start, block_max_size, block_checksum,
    # content_checksum}`.
    private def self.read_frame_descriptor(input : Bytes, position : Int32)
      if position + 2 > input.size
        raise Error.new("truncated LZ4 frame descriptor")
      end

      flg = input[position]
      bd = input[position + 1]

      unless ((flg >> 6) & 0b11) == 1
        raise Error.new("unsupported LZ4 frame version")
      end
      unless (flg & 0b10) == 0
        raise Error.new("reserved bit set in the LZ4 frame descriptor")
      end
      unless (flg & 0x20) != 0
        # Dependent (linked) blocks reference the previous 64 KiB window.
        raise Error.new("linked LZ4 frame blocks are not supported")
      end
      unless (flg & 0x01) == 0
        raise Error.new("dictionary-based LZ4 frames are not supported")
      end

      block_max_size = frame_block_max_size(bd)

      # FLG + BD, plus the optional content size; the dictionary ID flag was
      # rejected above.
      descriptor_size = 2 + ((flg & 0x08) != 0 ? 8 : 0)
      if position + descriptor_size + 1 > input.size
        raise Error.new("truncated LZ4 frame descriptor")
      end

      expected_checksum = ((XxHash32.oneshot(input[position, descriptor_size]) >> 8) & 0xFF).to_u8
      if input[position + descriptor_size] != expected_checksum
        raise Error.new("invalid LZ4 frame header checksum")
      end

      {
        position + descriptor_size + 1,
        block_max_size,
        (flg & 0x10) != 0,
        (flg & 0x04) != 0,
      }
    end

    # Maps the `BD` block-maximum-size code to a byte size.
    private def self.frame_block_max_size(bd : UInt8) : Int32
      case (bd >> 4) & 0b111
      when 4 then 64 * 1024
      when 5 then 256 * 1024
      when 6 then 1024 * 1024
      when 7 then 4 * 1024 * 1024
      else
        raise Error.new("invalid LZ4 frame block maximum size")
      end
    end

    # Decodes the blocks of one frame into `frame_output`, returning the offset
    # just past the end mark (and any block checksums).
    private def self.decode_frame_blocks(
      codec : Flint::BlockCodec,
      input : Bytes,
      position : Int32,
      block_max_size : Int32,
      block_checksum : Bool,
      frame_output : IO::Memory,
    ) : Int32
      buffer = Bytes.new(block_max_size)

      loop do
        if position + SIZE_U32 > input.size
          raise Error.new("truncated LZ4 frame block header")
        end

        block_size = read_u32(input, position)
        position += SIZE_U32
        break if block_size == 0

        size = (block_size & 0x7FFF_FFFF_u32).to_i64
        if position + size > input.size
          raise Error.new("truncated LZ4 frame block")
        end
        size = size.to_i32

        if (block_size & 0x8000_0000_u32) != 0
          # The block is stored verbatim.
          if size > block_max_size
            raise Error.new("LZ4 frame block exceeds the block maximum size")
          end
          frame_output.write(input[position, size])
        else
          written = decompress_block(codec, input[position, size], buffer)
          frame_output.write(buffer[0, written])
        end
        position += size

        next unless block_checksum
        if position + SIZE_U32 > input.size
          raise Error.new("truncated LZ4 frame block checksum")
        end
        position += SIZE_U32
      end

      position
    end

    private def self.read_u32(input : Bytes, position : Int32) : UInt32
      input[position].to_u32 |
        (input[position + 1].to_u32 << 8) |
        (input[position + 2].to_u32 << 16) |
        (input[position + 3].to_u32 << 24)
    end

    private def self.append_u32(output : Array(UInt8), value : UInt32) : Nil
      output << (value & 0xFF).to_u8
      output << ((value >> 8) & 0xFF).to_u8
      output << ((value >> 16) & 0xFF).to_u8
      output << ((value >> 24) & 0xFF).to_u8
    end
  end
end
