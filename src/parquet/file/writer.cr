require "../errors"
require "../schema/types"
require "../column/page"
require "../column/writer/writer"
require "../bloom_filter"
require "./tracked_write"
require "./serialized_writer"
require "./metadata"
require "./metadata/row_group"
require "./metadata/column_chunk"
require "./metadata/writer"
require "./metadata/footer_tail"
require "./page_index/column_index"

# Ported from `parquet/src/file/writer.rs` (`SerializedFileWriter`,
# `SerializedRowGroupWriter`, `SerializedColumnWriter`, `write_bloom_filters`,
# `OnCloseColumnChunk`, `OnCloseRowGroup`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: `SerializedFileWriter<W: Write>` becomes a single
# `SerializedFileWriter` over an `IO`; the `OnClose*` callbacks are Crystal
# `Proc`s (closures capturing the owning writer); encryption and the
# arrow-only `append_column*` splicing paths are not ported;
# `SerializedRowGroupWriter::close` returns the same `RowGroupMetaData`
# instance recorded in the file (upstream returns a pre-bloom-filter clone).
module Parquet
  module File
    # Callback invoked on closing a column chunk.
    alias OnCloseColumnChunk = Proc(Column::Writer::ColumnCloseResult, Nil)

    # Callback invoked on closing a row group: the writer, the row group
    # metadata, and the bloom filter / column index / offset index arrays.
    alias OnCloseRowGroup = Proc(
      TrackedWrite,
      Metadata::RowGroupMetaData,
      Array(BloomFilter::Sbbf?),
      Array(PageIndex::ColumnIndexMetaData?),
      Array(Metadata::OffsetIndexMetaData?),
      Nil,
    )

    # Serializes all the bloom filters of `row_group` into `buf`, back-filling
    # each column chunk's bloom filter offset and length.
    def self.write_bloom_filters(
      buf : TrackedWrite,
      bloom_filters : Array(Array(BloomFilter::Sbbf?)),
      row_group : Metadata::RowGroupMetaData,
    ) : Nil
      row_group_idx = row_group.ordinal || raise general_err!("Missing row group ordinal")
      if row_group_idx < 0
        raise general_err!("Negative row group ordinal: #{row_group_idx})")
      end

      columns = row_group.columns_mut
      columns.each_with_index do |column_chunk, column_idx|
        if bloom_filter = bloom_filters[row_group_idx][column_idx]
          bloom_filters[row_group_idx][column_idx] = nil
          start_offset = buf.bytes_written
          bloom_filter.write(buf)
          end_offset = buf.bytes_written
          columns[column_idx] = column_chunk.into_builder
            .set_bloom_filter_offset(start_offset)
            .set_bloom_filter_length((end_offset - start_offset).to_i32)
            .build
        end
      end
      nil
    end

    # Parquet file writer: tracks the location of row groups and column chunks,
    # and writes the metadata and file footer.
    class SerializedFileWriter
      getter buf : TrackedWrite
      getter descr : Schema::SchemaDescriptor
      getter props : Properties::WriterProperties

      @row_groups : Array(Metadata::RowGroupMetaData)
      @bloom_filters : Array(Array(BloomFilter::Sbbf?))
      @column_indexes : Array(Array(PageIndex::ColumnIndexMetaData?))
      @offset_indexes : Array(Array(Metadata::OffsetIndexMetaData?))
      @row_group_index : Int32
      @kv_metadatas : Array(Metadata::KeyValue)
      @finished : Bool

      def initialize(buf : IO, schema : Schema::Type, properties : Properties::WriterProperties)
        @buf = TrackedWrite.new(buf)
        @descr = Schema::SchemaDescriptor.new(schema)
        @props = properties
        @row_groups = [] of Metadata::RowGroupMetaData
        @bloom_filters = [] of Array(BloomFilter::Sbbf?)
        @column_indexes = [] of Array(PageIndex::ColumnIndexMetaData?)
        @offset_indexes = [] of Array(Metadata::OffsetIndexMetaData?)
        @row_group_index = 0
        @kv_metadatas = [] of Metadata::KeyValue
        @finished = false
        self.class.start_file(@buf)
      end

      # Writes magic bytes at the beginning of the file.
      def self.start_file(buf : TrackedWrite) : Nil
        buf.write(PARQUET_MAGIC)
        nil
      end

      # Creates a new row group from this file writer.
      def next_row_group : SerializedRowGroupWriter
        assert_previous_writer_closed
        ordinal = @row_group_index
        @row_group_index += 1

        bloom_filter_position = @props.bloom_filter_position
        on_close = ->(buf : TrackedWrite, metadata : Metadata::RowGroupMetaData, row_group_bloom_filter : Array(BloomFilter::Sbbf?), row_group_column_index : Array(PageIndex::ColumnIndexMetaData?), row_group_offset_index : Array(Metadata::OffsetIndexMetaData?)) do
          @bloom_filters << row_group_bloom_filter
          @column_indexes << row_group_column_index
          @offset_indexes << row_group_offset_index
          if bloom_filter_position == Properties::BloomFilterPosition::AfterRowGroup
            Parquet::File.write_bloom_filters(buf, @bloom_filters, metadata)
          end
          @row_groups << metadata
          nil
        end

        SerializedRowGroupWriter.new(@descr, @props, @buf, ordinal, on_close)
      end

      # Returns metadata for any flushed row groups.
      def flushed_row_groups : Array(Metadata::RowGroupMetaData)
        @row_groups
      end

      # Closes and finalizes the underlying writer without consuming it.
      def finish : Metadata::ParquetMetaData
        assert_previous_writer_closed
        metadata = write_metadata
        @buf.flush
        metadata
      end

      # Closes and finalizes the file writer, returning the file metadata.
      def close : Metadata::ParquetMetaData
        finish
      end

      # Assembles and writes the metadata at the end of the file.
      private def write_metadata : Metadata::ParquetMetaData
        @finished = true

        @row_groups.each do |row_group|
          Parquet::File.write_bloom_filters(@buf, @bloom_filters, row_group)
        end

        key_value_metadata =
          if kv = @props.key_value_metadata
            kv + @kv_metadatas
          elsif @kv_metadatas.empty?
            nil
          else
            @kv_metadatas.dup
          end

        row_groups = @row_groups
        column_indexes = @column_indexes
        offset_indexes = @offset_indexes

        encoder = Metadata::ThriftMetadataWriter.new(
          @buf,
          @descr,
          row_groups,
          @props.created_by,
          @props.writer_version.as_num,
          @props.write_path_in_schema,
        )

        if key_value_metadata
          encoder = encoder.with_key_value_metadata(key_value_metadata)
        end

        encoder = encoder.with_column_indexes(column_indexes)
        unless @props.offset_index_disabled
          encoder = encoder.with_offset_indexes(offset_indexes)
        end
        encoder.finish
      end

      private def assert_previous_writer_closed : Nil
        if @finished
          raise general_err!("SerializedFileWriter already finished")
        end

        if @row_group_index != @row_groups.size
          raise general_err!("Previous row group writer was not closed")
        end
        nil
      end

      # Adds a `KeyValue` to the file writer's metadata.
      def append_key_value_metadata(kv_metadata : Metadata::KeyValue) : Nil
        @kv_metadatas << kv_metadata
        nil
      end

      # Returns a reference to the schema descriptor.
      def schema_descr : Schema::SchemaDescriptor
        @descr
      end

      # Returns a reference to the writer properties.
      def properties : Properties::WriterProperties
        @props
      end

      # Returns the underlying writer.
      def inner : IO
        @buf.inner
      end

      # Writes raw bytes to the internal buffered writer.
      def write_all(buf : Bytes) : Nil
        @buf.write(buf)
        nil
      end

      def flush : Nil
        @buf.flush
        nil
      end

      # Returns the underlying writer (writes bypass buffering and counting).
      def inner_mut : IO
        @buf.inner_mut
      end

      # Writes the file footer and returns the underlying writer.
      def into_inner : IO
        assert_previous_writer_closed
        write_metadata
        @buf.into_inner
      end

      # Returns the number of bytes written to this instance.
      def bytes_written : Int64
        @buf.bytes_written
      end
    end

    # Parquet row group writer: provides access to column writers in
    # schema-leaf order.
    class SerializedRowGroupWriter
      @total_rows_written : UInt64?
      @total_bytes_written : UInt64
      @total_uncompressed_bytes : Int64
      @column_index : Int32
      @row_group_metadata : Metadata::RowGroupMetaData?
      @column_chunks : Array(Metadata::ColumnChunkMetaData)
      @bloom_filters : Array(BloomFilter::Sbbf?)
      @column_indexes : Array(PageIndex::ColumnIndexMetaData?)
      @offset_indexes : Array(Metadata::OffsetIndexMetaData?)
      @file_offset : Int64

      def initialize(
        @descr : Schema::SchemaDescriptor,
        @props : Properties::WriterProperties,
        @buf : TrackedWrite,
        @row_group_index : Int32,
        @on_close : OnCloseRowGroup?,
      )
        @total_rows_written = nil
        @total_bytes_written = 0_u64
        @total_uncompressed_bytes = 0_i64
        @column_index = 0
        @row_group_metadata = nil
        @column_chunks = [] of Metadata::ColumnChunkMetaData
        @bloom_filters = [] of BloomFilter::Sbbf?
        @column_indexes = [] of PageIndex::ColumnIndexMetaData?
        @offset_indexes = [] of Metadata::OffsetIndexMetaData?
        @file_offset = @buf.bytes_written
      end

      # Advances `@column_index`, returning the next `ColumnDescriptor` if any.
      private def next_column_desc : Schema::ColumnDescriptor?
        descr = @descr.columns[@column_index]?
        return nil if descr.nil?

        @column_index += 1
        descr
      end

      # Returns the next column writer, if available, otherwise `nil`.
      def next_column : SerializedColumnWriter?
        assert_previous_writer_closed

        descr = next_column_desc
        return nil if descr.nil?

        page_writer = SerializedPageWriter.new(@buf)
        column_writer = Column::Writer.get_column_writer(descr, @props, page_writer)

        on_close = ->(r : Column::Writer::ColumnCloseResult) do
          @total_bytes_written += r.bytes_written
          @total_uncompressed_bytes += r.metadata.uncompressed_size
          @column_chunks << r.metadata
          @bloom_filters << r.bloom_filter
          @column_indexes << r.column_index
          @offset_indexes << r.offset_index

          if rows = @total_rows_written
            if rows != r.rows_written
              raise general_err!("Incorrect number of rows, expected #{rows} != #{r.rows_written} rows")
            end
          else
            @total_rows_written = r.rows_written
          end
          nil
        end

        SerializedColumnWriter.new(column_writer, on_close)
      end

      # Closes this row group writer and returns the row group metadata.
      def close : Metadata::RowGroupMetaData
        if @row_group_metadata.nil?
          assert_previous_writer_closed

          metadata = Metadata::RowGroupMetaData.builder(@descr)
            .set_column_metadata(@column_chunks)
            .set_total_byte_size(@total_uncompressed_bytes)
            .set_num_rows((@total_rows_written || 0_u64).to_i64)
            .set_sorting_columns(@props.sorting_columns.try(&.dup))
            .set_ordinal(@row_group_index)
            .set_file_offset(@file_offset)
            .build

          @row_group_metadata = metadata

          if on_close = @on_close
            @on_close = nil
            on_close.call(@buf, metadata, @bloom_filters, @column_indexes, @offset_indexes)
          end
        end

        @row_group_metadata.not_nil!
      end

      private def assert_previous_writer_closed : Nil
        if @column_index != @column_chunks.size
          raise general_err!("Previous column writer was not closed")
        end
        nil
      end
    end

    # A wrapper around a `ColumnWriter` that invokes a callback on `close`.
    class SerializedColumnWriter
      def initialize(@inner : Column::Writer::ColumnWriter, @on_close : OnCloseColumnChunk?)
      end

      # Returns a reference to the untyped `ColumnWriter`.
      def untyped : Column::Writer::ColumnWriter
        @inner
      end

      # Returns a reference to the typed `GenericColumnWriter(T)`.
      def typed(type : T.class) : Column::Writer::GenericColumnWriter(T) forall T
        Column::Writer.get_typed_column_writer(type, @inner)
      end

      # Closes this writer, invoking the close callback if present.
      def close : Nil
        r = @inner.close
        if on_close = @on_close
          @on_close = nil
          on_close.call(r)
        end
        nil
      end
    end
  end
end
