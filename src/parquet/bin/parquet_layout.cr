require "json"
require "../errors"
require "../basic"
require "../file/chunk_reader"
require "../file/metadata"
require "../file/metadata/reader"

# Ported from `parquet/src/bin/parquet-layout.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Prints the physical layout of a Parquet file as JSON.
# Divergences: `clap` parsing is in `bin/parquet-layout.cr`; the serde
# `DebugSerialize` wrapper is replaced by `to_s`, and the JSON is emitted with
# `JSON.build` (pretty, 2-space indent) instead of `serde_json`.
module Parquet
  module Bin
    # `parquet-layout` tool logic.
    module ParquetLayout
      # Writes the physical layout of `file_name` to `io` as JSON.
      def self.run(io : IO, file_name : String) : Nil
        writer(io, File::FileChunkReader.new(file_name))
      end

      # Writes the physical layout of `reader` to `io` as JSON.
      def self.writer(io : IO, reader : File::ChunkReader) : Nil
        metadata_reader = File::Metadata::ParquetMetaDataReader.new
        metadata_reader.try_parse(reader)
        metadata_size = metadata_reader.metadata_size
        metadata = metadata_reader.finish
        schema = metadata.file_metadata.schema_descr

        JSON.build(io, indent: 2) do |json|
          json.object do
            json.field "row_groups" do
              json.array do
                (0...metadata.num_row_groups).each do |row_group_idx|
                  row_group = metadata.row_group(row_group_idx)
                  json.object do
                    json.field "columns" do
                      json.array do
                        row_group.columns.each_with_index do |column, column_idx|
                          write_column(json, schema.column(column_idx).path.parts.join("."), column)
                        end
                      end
                    end
                    json.field "row_count", row_group.num_rows
                  end
                end
              end
            end
            json.field "footer" do
              json.object do
                write_optional_field(json, "metadata_size", metadata_size)
              end
            end
          end
        end
      end

      private def self.write_column(
        json : JSON::Builder,
        path : String,
        column : File::Metadata::ColumnChunkMetaData,
      ) : Nil
        json.object do
          json.field "path", path
          json.field "has_offset_index", !column.offset_index_offset.nil?
          json.field "has_column_index", !column.column_index_offset.nil?
          json.field "has_bloom_filter", !column.bloom_filter_offset.nil?
          write_index(json, "offset_index", column.offset_index_offset, column.offset_index_length)
          write_index(json, "column_index", column.column_index_offset, column.column_index_length)
          write_index(json, "bloom_filter", column.bloom_filter_offset, column.bloom_filter_length)
          json.field "compression", column.compression.to_s
          json.field "encodings" do
            json.array do
              column.encodings_list.each { |encoding| json.string encoding.to_s }
            end
          end
        end
      end

      private def self.write_index(
        json : JSON::Builder,
        name : String,
        offset : Int64?,
        length : Int32?,
      ) : Nil
        if offset.nil?
          json.field name, nil
        else
          json.field name do
            json.object do
              json.field "offset", offset
              write_optional_field(json, "length", length)
            end
          end
        end
      end

      private def self.write_optional_field(json : JSON::Builder, name : String, value) : Nil
        if value.nil?
          json.field name, nil
        else
          json.field name, value
        end
      end
    end
  end
end
