require "../errors"
require "../file/chunk_reader"
require "../file/reader"
require "../file/serialized_reader"

# Ported from `parquet/src/bin/parquet-rowcount.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Prints the total number of rows found in each Parquet file.
# Divergence: `run` writes to an `IO` (upstream writes to stderr).
module Parquet
  module Bin
    # `parquet-rowcount` tool logic.
    module ParquetRowCount
      # Prints `File <path>: rowcount=<n>` for each of `file_paths`.
      def self.run(io : IO, file_paths : Array(String)) : Nil
        file_paths.each do |filename|
          reader = File::SerializedFileReader.new(File::FileChunkReader.new(filename))
          total_num_rows = reader.metadata.row_groups.sum(&.num_rows)
          io << "File " << filename << ": rowcount=" << total_num_rows << '\n'
        end
      end
    end
  end
end
