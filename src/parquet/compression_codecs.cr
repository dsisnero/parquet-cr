require "compress/gzip"
require "snappy"
require "./basic"
require "./errors"

# Ported from `parquet/src/compression.rs` (`Codec`, `create_codec`, `SnappyCodec`,
# `GZipCodec`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `Codec` framework, the SNAPPY codec, and the GZIP codec (Crystal
# stdlib `Compress::Gzip`). SNAPPY uses the `naqvis/snappy` shard, a pure-Crystal
# implementation of the snappy raw/block format (upstream uses the `snap` crate).
# The brotli/lz4/lzo/zstd codecs are still not ported — `create_codec` raises for
# them (see `plans/parity.md` Phase 1.8).
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
