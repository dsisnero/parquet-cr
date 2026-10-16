require "../errors"
require "../bloom_filter"
require "../column/writer"
require "../file/chunk_reader"
require "../file/metadata"
require "../file/metadata/reader"
require "../file/properties"
require "../file/writer"

# Ported from `parquet/src/bin/parquet-concat.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Concatenates the column data of one or more Parquet files.
# Divergences: `clap` parsing is in `bin/parquet-concat.cr`; `concat` takes an
# output `IO` and `ChunkReader`s (upstream opens paths); page indexes and bloom
# filters of the inputs are preserved via `append_column`.
module Parquet
  module Bin
    # `parquet-concat` tool logic.
    module ParquetConcat
      # Concatenates `input_paths` into `output_path`.
      def self.run(output_path : String, input_paths : Array(String)) : Nil
        io = ::File.open(output_path, "w")
        concat(io, input_paths.map { |path| File::FileChunkReader.new(path).as(File::ChunkReader) })
      end

      # Concatenates `inputs` into `output`.
      def self.concat(output : IO, inputs : Array(File::ChunkReader)) : Nil
        raise general_err!("Must provide at least one input file") if inputs.empty?

        metadatas = inputs.map do |reader|
          File::Metadata::ParquetMetaDataReader.new
            .with_page_index_policy(File::Metadata::PageIndexPolicy::Optional)
            .parse_and_finish(reader)
        end

        expected = metadatas[0].file_metadata.schema
        metadatas[1..].each do |metadata|
          actual = metadata.file_metadata.schema
          unless expected == actual
            raise general_err!("inputs must have the same schema, #{expected} vs #{actual}")
          end
        end

        props = File::Properties::WriterProperties.builder.build
        schema = metadatas[0].file_metadata.schema_descr.root_schema
        writer = File::SerializedFileWriter.new(output, schema, props)

        inputs.each_with_index do |reader, i|
          metadata = metadatas[i]
          page_index = metadata.page_index

          metadata.row_groups.each_with_index do |row_group, rg_idx|
            row_group_writer = writer.next_row_group
            row_group.columns.each_with_index do |column, col_idx|
              bloom_filter = read_bloom_filter(column, reader)
              column_index = page_index.try(&.column_index(rg_idx, col_idx))
              offset_index = page_index.try(&.offset_index(rg_idx, col_idx))
              result = Column::Writer::ColumnCloseResult.new(
                bytes_written: column.compressed_size.to_u64,
                rows_written: row_group.num_rows.to_u64,
                metadata: column,
                bloom_filter: bloom_filter,
                column_index: column_index,
                offset_index: offset_index,
              )
              row_group_writer.append_column(reader, result)
            end
            row_group_writer.close
          end
        end

        writer.close
      end

      # Reads the bloom filter for `column`, or `nil` if none is present.
      def self.read_bloom_filter(
        column : File::Metadata::ColumnChunkMetaData,
        reader : File::ChunkReader,
      ) : BloomFilter::Sbbf?
        BloomFilter::Sbbf.read_from_column_chunk(column, reader)
      end
    end
  end
end
