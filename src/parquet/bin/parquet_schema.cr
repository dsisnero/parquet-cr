require "../errors"
require "../file/chunk_reader"
require "../file/reader"
require "../file/serialized_reader"
require "../schema/printer"

# Ported from `parquet/src/bin/parquet-schema.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Prints the schema and (optionally) full metadata of a Parquet file.
# Divergence: `clap` parsing is in `bin/parquet-schema.cr`; `run` writes to an `IO`.
module Parquet
  module Bin
    # `parquet-schema` tool logic.
    module ParquetSchema
      # Prints the schema (or full metadata when `verbose`) of `file_path`.
      def self.run(io : IO, file_path : String, verbose : Bool) : Nil
        reader = File::SerializedFileReader.new(File::FileChunkReader.new(file_path))
        metadata = reader.metadata

        io << "Metadata for file: " << file_path << '\n'
        io << '\n'
        if verbose
          Schema.print_parquet_metadata(io, metadata)
        else
          Schema.print_file_metadata(io, metadata.file_metadata)
        end
      end
    end
  end
end
