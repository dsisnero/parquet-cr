# Ported from `parquet/src/file/writer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `TrackedWrite` and `get_file_magic`. The writer counts bytes written so
# the metadata writer can record the file offsets of the page indexes.
module Parquet
  module File
    # The 4 magic bytes that identify a Parquet file.
    def self.file_magic : Bytes
      PARQUET_MAGIC
    end

    # Wraps an `IO`, tracking the number of bytes written through it.
    class TrackedWrite < IO
      getter bytes_written : Int64

      def initialize(@io : IO, @bytes_written : Int64 = 0_i64)
      end

      def read(slice : Bytes) : Int32
        @io.read(slice)
      end

      def write(slice : Bytes) : Nil
        @io.write(slice)
        @bytes_written += slice.size
      end
    end
  end
end
