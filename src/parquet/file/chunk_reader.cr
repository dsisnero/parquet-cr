# Ported from `parquet/src/file/reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `Length`/`ChunkReader` traits (as an abstract class) and the `Bytes`
# and `File` implementations. Divergence: Rust's associated `T: Read` return of
# `get_read` maps to `IO`; `Bytes`/`File` impls wrap into explicit reader types.
require "../errors"

module Parquet
  module File
    # A source of Parquet file bytes, read random-access by offset.
    abstract class ChunkReader
      # Total length of the underlying file/source in bytes.
      abstract def len : UInt64

      # Returns the bytes at `[start, start + length)`.
      abstract def get_bytes(start : UInt64, length : Int32) : Bytes

      # Returns a reader positioned at `start`.
      abstract def get_read(start : UInt64) : IO
    end

    # `ChunkReader` over an in-memory byte buffer.
    class BytesChunkReader < ChunkReader
      def initialize(@bytes : Bytes)
      end

      def len : UInt64
        @bytes.size.to_u64
      end

      def get_bytes(start : UInt64, length : Int32) : Bytes
        from = start.to_i32
        if start > len || start + length.to_u64 > len
          raise eof_err!("Expected to read #{length} bytes at offset #{start}, while file has length #{len}")
        end
        @bytes[from, length]
      end

      def get_read(start : UInt64) : IO
        if start > len
          raise eof_err!("Expected to read at offset #{start}, while file has length #{len}")
        end
        from = start.to_i32
        IO::Memory.new(@bytes[from, @bytes.size - from])
      end
    end

    # `ChunkReader` over a file on disk.
    class FileChunkReader < ChunkReader
      def initialize(path : String)
        @file = ::File.open(path)
      end

      def initialize(@file : ::File)
      end

      def len : UInt64
        @file.size.to_u64
      end

      def get_bytes(start : UInt64, length : Int32) : Bytes
        buffer = Bytes.new(length)
        read = 0
        @file.read_at(start, length) do |io|
          read = io.read(buffer)
        end
        if read != length
          raise eof_err!("Expected to read #{length} bytes, read only #{read}")
        end
        buffer
      end

      def get_read(start : UInt64) : IO
        remaining = (@file.size - start).to_i32
        buffer = Bytes.new(remaining)
        @file.read_at(start, remaining) do |io|
          io.read_fully(buffer)
        end
        IO::Memory.new(buffer)
      end
    end
  end
end
