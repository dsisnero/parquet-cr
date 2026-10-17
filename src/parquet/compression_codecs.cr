require "compress/gzip"
require "snappy"
require "flint"
require "zinc"
require "brotli"
require "./basic"
require "./errors"
require "./lz4"

# Ported from `parquet/src/compression.rs` (`Codec`, `create_codec`, `SnappyCodec`,
# `GZipCodec`, `LZ4Codec`, `LZ4RawCodec`, `LZ4HadoopCodec`, `BrotliCodec`,
# `ZSTDCodec`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `Codec` framework and every codec upstream implements at 60.0.0.
# SNAPPY uses the `naqvis/snappy` shard, LZ4 blocks the `paddor/flint` shard,
# ZSTD the `paddor/zinc` shard, and BROTLI the `OrelSokolov/brotli` shard (all
# pure Crystal; upstream uses `snap`, `lz4_flex`, `zstd`, and `brotli`); the LZ4
# *frame* container and the Hadoop framing live in `Parquet::Lz4` and
# `LZ4HadoopCodec`, and GZIP uses the Crystal stdlib. LZO is not implemented —
# upstream's `create_codec` returns `nyi_err!` for it as well (see
# `plans/parity.md` Phase 1.8).
#
# Divergences: the Rust `Codec` trait (mutating `Vec<u8>` buffers) becomes an
# abstract class appending to `Array(UInt8)`; `snap::Error` maps to
# `ExternalError` wrapping `Compress::Snappy::SnappyError`; and because the shard
# allows a destination buffer larger than the recorded length, `SnappyCodec`
# enforces the exact-size check that `snap::raw::Decoder` performs.
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

    # SNAPPY codec (raw/block format, via the `naqvis/snappy` shard).
    class SnappyCodec < Codec
      def initialize
      end

      def compress(input : Bytes, output : Array(UInt8)) : Nil
        # `Compress::Snappy.encode` raises `IndexError` for empty input (shard
        # 0.2.0); the canonical empty block is a lone zero-length preamble.
        if input.empty?
          output << 0_u8
          return nil
        end

        output.concat(Compress::Snappy.encode(input))
        nil
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        decoded =
          if size = uncompress_size
            recorded = Compress::Snappy.decode_length(input)
            if recorded != size
              raise Compress::Snappy::SnappyError.new(
                "Recorded length is #{recorded} bytes but the buffer is #{size} bytes"
              )
            end

            dest = Bytes.new(size)
            Compress::Snappy.decode(input, dest)
            dest
          else
            Compress::Snappy.decode(input)
          end

        output.concat(decoded)
        decoded.size
      rescue ex : Compress::Snappy::SnappyError
        raise ExternalError.new(ex)
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

    # LZ4 frame-format codec (`lz4_flex::frame`).
    #
    # Upstream keeps this codec `experimental`-gated and only uses it internally
    # as the `LZ4_HADOOP` fallback for legacy files; Crystal has no feature
    # flags, so it is always available. Divergence: upstream streams 4 KiB
    # chunks into `lz4_flex::frame::FrameEncoder`; the port emits a single frame
    # with 1 MiB independent blocks (a valid frame, different block layout).
    class LZ4Codec < Codec
      def compress(input : Bytes, output : Array(UInt8)) : Nil
        output.concat(Lz4.compress_frame(input))
        nil
      rescue error : Lz4::Error
        raise ExternalError.new(error)
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        # The frame format records its own content lengths; upstream ignores
        # `uncompress_size` here as well.
        _ = uncompress_size
        decompressed = Lz4.decompress_frame(input)
        output.concat(decompressed)
        decompressed.size
      rescue error : Lz4::Error
        raise ExternalError.new(error)
      end
    end

    # LZ4 raw block codec (`lz4_flex::block`, via the `paddor/flint` shard).
    class LZ4RawCodec < Codec
      def initialize
        # `Flint::BlockCodec` keeps its hash tables alive across calls.
        @codec = Flint::BlockCodec.new
      end

      def compress(input : Bytes, output : Array(UInt8)) : Nil
        # `Flint::BlockCodec#compress_raw` emits a zero-length block for empty
        # input; the canonical empty LZ4 block is a single all-zero token byte,
        # which every decoder accepts.
        if input.empty?
          output << 0_u8
          return nil
        end

        output.concat(@codec.compress_raw(input))
        nil
      rescue error : Flint::Error
        raise ExternalError.new(error)
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        required =
          uncompress_size || raise general_err!("LZ4RawCodec unsupported without uncompress_size")

        destination = Bytes.new(required)
        written =
          begin
            @codec.decompress_raw_into(input, destination)
          rescue error : Flint::Error
            raise ExternalError.new(error)
          end

        unless written == required
          raise general_err!("LZ4RawCodec uncompress_size is not the expected one")
        end

        output.concat(destination[0, written])
        written
      end
    end

    # LZ4 codec using the Hadoop framing (`LZ4_HADOOP`).
    #
    # Each frame is the decompressed and compressed sizes as big-endian `u32`
    # values followed by a raw LZ4 block. On decode, `backward_compatible_lz4`
    # enables the upstream fallback chain (LZ4 frame format, then a raw LZ4
    # block) for files written by older arrow-rs/parquet-cpp versions.
    class LZ4HadoopCodec < Codec
      # Size of a `u32` field.
      SIZE_U32 = 4

      # Length of the LZ4_HADOOP prefix.
      PREFIX_LEN = SIZE_U32 * 2

      def initialize(@backward_compatible_lz4 : Bool)
      end

      def compress(input : Bytes, output : Array(UInt8)) : Nil
        offset = output.size
        PREFIX_LEN.times { output << 0_u8 }

        # Append LZ4_RAW compressed bytes after the prefix.
        LZ4RawCodec.new.compress(input, output)

        compressed_size = output.size - offset - PREFIX_LEN
        write_be_u32(output, offset, input.size)
        write_be_u32(output, offset + SIZE_U32, compressed_size)
        nil
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        required =
          uncompress_size || raise general_err!("LZ4HadoopCodec unsupported without uncompress_size")

        begin
          decompressed = try_decompress_hadoop(input, required)
          unless decompressed.size == required
            raise general_err!("LZ4HadoopCodec uncompress_size is not the expected one")
          end

          output.concat(decompressed)
          decompressed.size
        rescue error : Lz4::Error
          raise ExternalError.new(error) unless @backward_compatible_lz4

          # Fall back to the LZ4 frame format, then to a raw LZ4 block, to stay
          # compatible with older versions of this library and parquet-cpp.
          begin
            decompressed = Lz4.decompress_frame(input)
            output.concat(decompressed)
            decompressed.size
          rescue e : Lz4::Error
            LZ4RawCodec.new.decompress(input, output, uncompress_size)
          end
        end
      end

      # Tries to decompress `input` as a Hadoop Lz4Codec stream into `required`
      # bytes, raising `Lz4::Error` if the framing does not match.
      #
      # Ported from the apache arrow C++ `TryDecompressHadoop`: the stream may
      # hold several frames, and upstream keeps looking for another frame only
      # while the remaining input is longer than the frame just decoded.
      private def try_decompress_hadoop(input : Bytes, required : Int32) : Bytes
        position = 0
        input_len = input.size
        read_bytes = 0
        output_len = required
        output = Bytes.new(required)

        while input_len >= PREFIX_LEN
          expected_decompressed_size = read_be_u32(input, position).to_i64
          expected_compressed_size = read_be_u32(input, position + SIZE_U32).to_i64
          position += PREFIX_LEN
          input_len -= PREFIX_LEN

          if input_len.to_i64 < expected_compressed_size
            raise Lz4::Error.new("Not enough bytes for Hadoop frame")
          end

          if output_len.to_i64 < expected_decompressed_size
            raise Lz4::Error.new("Not enough bytes to hold advertised output")
          end

          decompressed_size =
            Lz4.decompress_block(
              input[position, expected_compressed_size.to_i32],
              output[read_bytes, output_len]
            )

          if decompressed_size.to_i64 != expected_decompressed_size
            raise Lz4::Error.new("Unexpected decompressed size")
          end

          position += expected_compressed_size.to_i32
          input_len -= expected_compressed_size.to_i32
          read_bytes += expected_decompressed_size.to_i32
          output_len -= expected_decompressed_size.to_i32

          break unless input_len > expected_compressed_size
        end

        unless input_len == 0
          raise Lz4::Error.new("Not all input are consumed")
        end

        output[0, read_bytes]
      end

      private def read_be_u32(input : Bytes, position : Int32) : UInt32
        (input[position].to_u32 << 24) |
          (input[position + 1].to_u32 << 16) |
          (input[position + 2].to_u32 << 8) |
          input[position + 3].to_u32
      end

      private def write_be_u32(output : Array(UInt8), position : Int32, value : Int32) : Nil
        bytes = value.to_u32
        output[position] = (bytes >> 24).to_u8
        output[position + 1] = ((bytes >> 16) & 0xFF).to_u8
        output[position + 2] = ((bytes >> 8) & 0xFF).to_u8
        output[position + 3] = (bytes & 0xFF).to_u8
      end
    end

    # BROTLI codec (via the `OrelSokolov/brotli` shard, replacing upstream's
    # `brotli` crate).
    class BrotliCodec < Codec
      # Upstream `BROTLI_DEFAULT_LG_WINDOW_SIZE` (recommended between 20-22).
      LG_WINDOW_SIZE = 22

      def initialize(@level : BrotliLevel)
      end

      def compress(input : Bytes, output : Array(UInt8)) : Nil
        encoder = Brotli::Enc::Encoder.new(quality: @level.compression_level, lgwin: LG_WINDOW_SIZE)
        output.concat(encoder.encode(input))
        nil
      rescue error : ParquetError
        raise error
      rescue error
        raise ExternalError.new(error)
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        # The brotli stream is self-terminating; upstream only uses
        # `uncompress_size` as the decoder's internal buffer size hint.
        _ = uncompress_size

        # `Brotli::Decoder` accumulates into its own output buffer and does not
        # reset it, so a fresh decoder is required per call (upstream's
        # `brotli::Decompressor::new` also creates one per call).
        decompressed = Brotli::Decoder.new.decode(input)
        output.concat(decompressed)
        decompressed.size
      rescue error : Brotli::DecodeError
        raise ExternalError.new(error)
      end
    end

    # ZSTD codec (via the `paddor/zinc` shard, replacing upstream's `zstd` crate).
    class ZSTDCodec < Codec
      # `zinc` implements levels -8..4 (raw-fast, Fast, and DFast strategies).
      # Upstream's libzstd accepts the full validated -131072..=22 range and
      # clamps internally; the port clamps to the levels `zinc` supports, which
      # changes the compression ratio for |level| > 4 but not the format.
      MINIMUM_LEVEL = -8
      MAXIMUM_LEVEL =  4

      def initialize(level : ZstdLevel)
        clamped = level.compression_level.clamp(MINIMUM_LEVEL, MAXIMUM_LEVEL)
        @codec = Zinc::FrameCodec.new(level: clamped)
      end

      def compress(input : Bytes, output : Array(UInt8)) : Nil
        output.concat(@codec.compress(input))
        nil
      rescue error : Zinc::Error
        raise ExternalError.new(error)
      end

      def decompress(input : Bytes, output : Array(UInt8), uncompress_size : Int32?) : Int32
        # Upstream only uses `uncompress_size` (then the frame content size,
        # then `input.len() * 4`) to pre-reserve the output buffer; the decoded
        # length always comes from the frame itself.
        _ = uncompress_size

        decompressed = @codec.decompress(input)
        output.concat(decompressed)
        decompressed.size
      rescue error : Zinc::Error
        raise ExternalError.new(error)
      end
    end

    # Returns the codec for `compression`, or `nil` for `UNCOMPRESSED`.
    def self.create_codec(compression : Compression, options : CodecOptions) : Codec?
      case compression.kind
      when Compression::Kind::UNCOMPRESSED
        nil
      when Compression::Kind::SNAPPY
        SnappyCodec.new
      when Compression::Kind::GZIP
        GZipCodec.new(GzipLevel.try_new(compression.level || GzipLevel.default.compression_level))
      when Compression::Kind::LZ4
        LZ4HadoopCodec.new(options.backward_compatible_lz4)
      when Compression::Kind::LZ4_RAW
        LZ4RawCodec.new
      when Compression::Kind::BROTLI
        BrotliCodec.new(BrotliLevel.try_new(compression.level || BrotliLevel.default.compression_level))
      when Compression::Kind::ZSTD
        ZSTDCodec.new(ZstdLevel.try_new(compression.level || ZstdLevel.default.compression_level))
      else
        # LZO has no implementation in arrow-rs 60.0.0 either.
        raise nyi_err!("The codec type #{compression} is not supported yet")
      end
    end
  end
end
