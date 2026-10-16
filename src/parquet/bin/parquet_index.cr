require "../errors"
require "../file/chunk_reader"
require "../file/reader"
require "../file/serialized_reader"
require "../file/page_index/offset_index"
require "../file/page_index/column_index"
require "./cli"

# Ported from `parquet/src/bin/parquet-index.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Prints the page index of a Parquet column chunk.
# Divergences: `clap` argument parsing is done by the `bin/parquet-index.cr`
# entry point (`OptionParser`), and the `run` logic writes to an `IO` so it can
# be characterized from specs; errors are raised (not `Result`).
module Parquet
  module Bin
    # `parquet-index` tool logic.
    module ParquetIndex
      # Runs `parquet-index` for `file_path` and `column`, writing to `io`.
      def self.run(io : IO, file_path : String, column : String) : Nil
        run_with_reader(io, File::FileChunkReader.new(file_path), column)
      end

      # Runs `parquet-index` against a `ChunkReader`.
      # ameba:disable Metrics/CyclomaticComplexity
      def self.run_with_reader(io : IO, reader : File::ChunkReader, column : String) : Nil
        options = File::ReadOptionsBuilder.new.with_page_index.build
        file_reader = File::SerializedFileReader.new_with_options(reader, options)

        schema = file_reader.metadata.file_metadata.schema_descr
        column_path = Cli.parse_column_path(column)
        column_idx = schema.columns.index { |col| col.path.parts == column_path }
        raise general_err!("Failed to find column #{column}") if column_idx.nil?

        page_index = file_reader.metadata.page_index || raise general_err!("Page index not found")

        (0...file_reader.metadata.num_row_groups).each do |row_group_idx|
          row_group = file_reader.metadata.row_group(row_group_idx)
          io << "Row Group: " << row_group_idx << '\n'
          offset_index = page_index.offset_index(row_group_idx, column_idx)
          if offset_index.nil?
            raise general_err!("No offset index for row group #{row_group_idx} column chunk #{column_idx}")
          end

          row_counts = compute_row_counts(offset_index.page_locations, row_group.num_rows)
          case metadata = page_index.column_index(row_group_idx, column_idx)
          when Nil
            io << "NO INDEX\n"
          when File::PageIndex::ColumnIndexMetaData
            case inner = metadata.inner
            when File::PageIndex::PrimitiveColumnIndex(Bool)    then print_index(io, inner, offset_index, row_counts)
            when File::PageIndex::PrimitiveColumnIndex(Int32)   then print_index(io, inner, offset_index, row_counts)
            when File::PageIndex::PrimitiveColumnIndex(Int64)   then print_index(io, inner, offset_index, row_counts)
            when File::PageIndex::PrimitiveColumnIndex(Int96)   then print_index(io, inner, offset_index, row_counts)
            when File::PageIndex::PrimitiveColumnIndex(Float32) then print_index(io, inner, offset_index, row_counts)
            when File::PageIndex::PrimitiveColumnIndex(Float64) then print_index(io, inner, offset_index, row_counts)
            when File::PageIndex::ByteArrayColumnIndex          then print_bytes_index(io, inner, offset_index, row_counts)
            end
          end
        end
      end

      # Computes the number of rows in each page within a column chunk.
      def self.compute_row_counts(offset_index : Array(File::PageIndex::PageLocation), rows : Int64) : Array(Int64)
        return [] of Int64 if offset_index.empty?

        last = offset_index[0].first_row_index
        out = Array(Int64).new(offset_index.size)
        offset_index[1..].each do |location|
          out << (location.first_row_index - last)
          last = location.first_row_index
        end
        out << (rows - last)
        out
      end

      private def self.check_lengths(column_index, offset_index) : Nil
        if column_index.num_pages != offset_index.page_locations.size
          raise general_err!(
            "Index length mismatch, got #{column_index.num_pages} and #{offset_index.page_locations.size}"
          )
        end
      end

      private def self.print_index(
        io : IO,
        column_index : File::PageIndex::PrimitiveColumnIndex(T),
        offset_index : File::PageIndex::OffsetIndexMetaData,
        row_counts : Array(Int64),
      ) : Nil forall T
        check_lengths(column_index, offset_index)

        idx = 0
        column_index.min_values_iter.zip(
          column_index.max_values_iter,
          offset_index.page_locations.each,
          row_counts.each,
        ).each do |(min, max, location, row_count)|
          io << "Page " << ("%5d" % idx)
          io << " at offset " << ("%10d" % location.offset)
          io << " with length " << ("%10d" % location.compressed_page_size)
          io << " and row count " << ("%10d" % row_count)
          io << ", min " << ("%10s" % (min.nil? ? "NONE" : min.to_s))
          io << ", max " << ("%10s" % (max.nil? ? "NONE" : max.to_s))
          io << '\n'
          idx += 1
        end
      end

      private def self.print_bytes_index(
        io : IO,
        column_index : File::PageIndex::ByteArrayColumnIndex,
        offset_index : File::PageIndex::OffsetIndexMetaData,
        row_counts : Array(Int64),
      ) : Nil
        check_lengths(column_index, offset_index)

        idx = 0
        column_index.min_values_iter.zip(
          column_index.max_values_iter,
          offset_index.page_locations.each,
          row_counts.each,
        ).each do |(min, max, location, row_count)|
          io << "Page " << ("%5d" % idx)
          io << " at offset " << ("%10d" % location.offset)
          io << " with length " << ("%10d" % location.compressed_page_size)
          io << " and row count " << ("%10d" % row_count)
          io << ", min " << ("%10s" % bytes_field(min))
          # Upstream's byte-array max fallback (invalid UTF-8) prints the `min`
          # label (a copy-paste bug); mirrored here for output parity.
          if m = max
            begin
              io << ", max " << ("%10s" % String.new(m))
            rescue
              io << ", min " << ("%10s" % Parquet::ByteArray.new(m).to_s)
            end
          else
            io << ", max " << ("%10s" % "NONE")
          end
          io << '\n'
          idx += 1
        end
      end

      private def self.bytes_field(value : Bytes?) : String
        return "NONE" if value.nil?

        begin
          String.new(value)
        rescue
          Parquet::ByteArray.new(value).to_s
        end
      end
    end
  end
end
