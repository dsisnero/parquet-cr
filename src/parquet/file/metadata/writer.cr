# Ported from `parquet/src/file/metadata/writer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ThriftMetadataWriter` and `ParquetMetaDataWriter`, which serialize the
# page indexes (column index then offset index), back-fill their offsets on the
# column chunks, then write the thrift `FileMetaData` and the footer tail.
# Divergences: encryption is not ported (no `MetadataObjectWriter`/magic
# variants), and row groups are shallow-copied (writing sets index offsets on
# the source `ColumnChunkMetaData` objects).
module Parquet
  module File
    module Metadata
      # Writes `ParquetMetaData` structs to a thrift-encoded byte stream.
      class ThriftMetadataWriter
        def initialize(
          @buf : Parquet::File::TrackedWrite,
          @schema_descr : Parquet::Schema::SchemaDescriptor,
          @row_groups : Array(RowGroupMetaData),
          @created_by : String?,
          @writer_version : Int32,
          @write_path_in_schema : Bool,
        )
          @column_indexes = nil
          @offset_indexes = nil
          @key_value_metadata = nil
        end

        # Serializes the column indexes and back-fills their offsets.
        def with_column_indexes(column_indexes : Array(Array(ColumnIndexMetaData?))) : self
          @column_indexes = column_indexes
          self
        end

        # Serializes the offset indexes and back-fills their offsets.
        def with_offset_indexes(offset_indexes : Array(Array(OffsetIndexMetaData?))) : self
          @offset_indexes = offset_indexes
          self
        end

        def with_key_value_metadata(key_value_metadata : Array(KeyValue)) : self
          @key_value_metadata = key_value_metadata
          self
        end

        # Assembles and writes the final metadata, returning it.
        def finish : ParquetMetaData
          num_rows = @row_groups.sum(&.num_rows)

          column_indexes = finalize_column_indexes
          offset_indexes = finalize_offset_indexes

          column_orders = @schema_descr.columns.map do |col|
            ColumnOrder.column_order_for_type(col.logical_type_ref, col.converted_type, col.physical_type)
          end

          file_metadata = FileMetaData.new(
            @writer_version, num_rows, @created_by, @key_value_metadata, @schema_descr, column_orders
          )

          start_pos = @buf.bytes_written
          protocol = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(@buf)
          Metadata.write_file_metadata(file_metadata, @row_groups, protocol, @write_path_in_schema)
          end_pos = @buf.bytes_written

          metadata_len = (end_pos - start_pos).to_u32!
          @buf.write_bytes(metadata_len, IO::ByteFormat::LittleEndian)
          @buf.write(Parquet::File::PARQUET_MAGIC)

          page_index = PageIndex.new(column_indexes, offset_indexes)
          ParquetMetaData.new(file_metadata, @row_groups, page_index)
        end

        private def finalize_column_indexes : Array(Array(ColumnIndexMetaData?))?
          column_indexes = @column_indexes
          @column_indexes = nil

          if column_indexes
            write_column_indexes(column_indexes)
          end

          return nil if column_indexes.nil? || column_indexes.all?(&.all?(Nil))

          column_indexes
        end

        private def finalize_offset_indexes : Array(Array(OffsetIndexMetaData?))?
          offset_indexes = @offset_indexes
          @offset_indexes = nil

          if offset_indexes
            write_offset_indexes(offset_indexes)
          end

          return nil if offset_indexes.nil? || offset_indexes.all?(&.all?(Nil))

          offset_indexes
        end

        private def write_column_indexes(column_indexes : Array(Array(ColumnIndexMetaData?))) : Nil
          @row_groups.each_with_index do |row_group, rg_idx|
            row_group.columns.each_with_index do |column_metadata, col_idx|
              index = column_indexes[rg_idx][col_idx]?
              next if index.nil?

              start_offset = @buf.bytes_written
              write_thrift_object(index, @buf)
              end_offset = @buf.bytes_written
              column_metadata.column_index_offset = start_offset
              column_metadata.column_index_length = (end_offset - start_offset).to_i32!
            end
          end
        end

        private def write_offset_indexes(offset_indexes : Array(Array(OffsetIndexMetaData?))) : Nil
          @row_groups.each_with_index do |row_group, rg_idx|
            row_group.columns.each_with_index do |column_metadata, col_idx|
              index = offset_indexes[rg_idx][col_idx]?
              next if index.nil?

              start_offset = @buf.bytes_written
              write_thrift_object(index, @buf)
              end_offset = @buf.bytes_written
              column_metadata.offset_index_offset = start_offset
              column_metadata.offset_index_length = (end_offset - start_offset).to_i32!
            end
          end
        end

        private def write_thrift_object(object : T, io : IO) : Nil forall T
          protocol = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
          object.write_thrift(protocol)
        end
      end

      # Writes `ParquetMetaData` to a byte stream.
      #
      # The output is: optional column indexes, optional offset indexes, the
      # thrift `FileMetaData`, the 4-byte metadata length, and the `PAR1` magic.
      class ParquetMetaDataWriter
        def initialize(@metadata : ParquetMetaData, @tracked : Parquet::File::TrackedWrite)
          @write_path_in_schema = true
        end

        def self.new(buf : IO, metadata : ParquetMetaData) : ParquetMetaDataWriter
          new(metadata, Parquet::File::TrackedWrite.new(buf))
        end

        def self.new_with_tracked(buf : Parquet::File::TrackedWrite, metadata : ParquetMetaData) : ParquetMetaDataWriter
          new(metadata, buf)
        end

        # Sets whether to write the `path_in_schema` field of `ColumnMetaData`.
        def with_write_path_in_schema(val : Bool) : self
          @write_path_in_schema = val
          self
        end

        # Writes the metadata to the buffer.
        def finish : Nil
          file_metadata = @metadata.file_metadata
          schema_descr = file_metadata.schema_descr
          row_groups = @metadata.row_groups.dup

          encoder = ThriftMetadataWriter.new(
            @tracked, schema_descr, row_groups, file_metadata.created_by,
            file_metadata.version, @write_path_in_schema
          )

          if page_index = @metadata.page_index
            if page_index.is_a?(PageIndex)
              if column_indexes = page_index.column_indexes_raw
                encoder = encoder.with_column_indexes(column_indexes)
              end
              if offset_indexes = page_index.offset_indexes_raw
                encoder = encoder.with_offset_indexes(offset_indexes)
              end
            end
          end

          if key_value_metadata = file_metadata.key_value_metadata
            encoder = encoder.with_key_value_metadata(key_value_metadata)
          end

          encoder.finish
        end
      end
    end
  end
end
