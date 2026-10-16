require "../errors"
require "../file/chunk_reader"
require "../file/reader"
require "../file/serialized_reader"
require "../record/reader"

# Ported from `parquet/src/bin/parquet-read.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Reads records from a Parquet file and prints them (optionally as JSON lines).
# Divergence: `clap` parsing is in `bin/parquet-read.cr`; `run` writes to an `IO`.
module Parquet
  module Bin
    # `parquet-read` tool logic.
    module ParquetRead
      # Prints records from `file_name` to `io`.
      def self.run(io : IO, file_name : String, num_records : Int32, json : Bool) : Nil
        reader = File::SerializedFileReader.new(File::FileChunkReader.new(file_name))
        run(io, reader, num_records, json)
      end

      # Prints records from `reader` to `io`.
      def self.run(io : IO, reader : File::FileReader, num_records : Int32, json : Bool) : Nil
        iter = reader.get_row_iter(nil)
        start = 0
        all_records = num_records == 0

        while all_records || start < num_records
          row = iter.next
          break if row.is_a?(Iterator::Stop)

          print_row(io, row, json)
          start += 1
        end
      end

      # Prints records read from `stdin` to `io` (upstream `-` file name).
      def self.run_stdin(io : IO, stdin : IO, num_records : Int32, json : Bool) : Nil
        bytes = stdin.getb_to_end
        reader = File::SerializedFileReader.new(File::BytesChunkReader.new(bytes))
        run(io, reader, num_records, json)
      end

      # Prints a single row, either as a debug record or a JSON value.
      def self.print_row(io : IO, row : Record::Row, json : Bool) : Nil
        if json
          io << row.to_json_value.to_json << '\n'
        else
          io << row.to_s << '\n'
        end
      end
    end
  end
end
