require "compress/gzip"
require "./basic"
require "./errors"

# Ported from `parquet/src/compression.rs` (`Codec`, `create_codec`, `GZipCodec`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `Codec` framework and the GZIP codec (Crystal stdlib
# `Compress::Gzip`). Divergence: the snappy/lzo/brotli/lz4/zstd codecs need
# Crystal shards and are not ported — `create_codec` raises for them (see
# `plans/parity.md` Phase 1.8). The Rust `Codec` trait (mutating `Vec<u8>`
# buffers) becomes an abstract class appending to `Array(UInt8)`.
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
      when Compression::Kind::GZIP
        GZipCodec.new(GzipLevel.try_new(compression.level || GzipLevel.default.compression_level))
      else
        raise general_err!("The codec type #{compression} is not supported yet")
      end
    end
  end
end
