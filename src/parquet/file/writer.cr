require "../errors"
require "../schema/types"
require "../column/page"
require "../column/writer/writer"
require "../bloom_filter"
require "./tracked_write"
require "./chunk_reader"
require "./serialized_writer"
require "./metadata"
require "./metadata/row_group"
require "./metadata/column_chunk"
require "./metadata/writer"
require "./metadata/footer_tail"
require "./page_index/column_index"
require "../encryption/encrypt"

# Ported from `parquet/src/file/writer.rs` (`SerializedFileWriter`,
# `SerializedRowGroupWriter`, `SerializedColumnWriter`, `write_bloom_filters`,
# `OnCloseColumnChunk`, `OnCloseRowGroup`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: `SerializedFileWriter<W: Write>` becomes a single
# `SerializedFileWriter` over an `IO`; the `OnClose*` callbacks are Crystal
# `Proc`s (closures capturing the owning writer); column chunks are spliced
# from IO or streamed page-store blobs, preserving encryption metadata;
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
      getter file_encryptor : Encryption::FileEncryptor?

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
        @file_encryptor = properties.file_encryption_properties.try do |encryption|
          Encryption::FileEncryptor.new(encryption)
        end
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

        SerializedRowGroupWriter.new(@descr, @props, @buf, ordinal, on_close, @file_encryptor)
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
        encoder = encoder.with_file_encryptor(@file_encryptor)

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
        @file_encryptor : Encryption::FileEncryptor? = nil,
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

      # Returns the callback invoked when a column writer is closed.
      private def build_on_close : OnCloseColumnChunk
        ->(r : Column::Writer::ColumnCloseResult) do
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
      end

      # Returns the next column writer, if available, otherwise `nil`.
      def next_column : SerializedColumnWriter?
        assert_previous_writer_closed

        column_index = @column_index
        descr = next_column_desc
        return nil if descr.nil?

        page_writer = SerializedPageWriter.new(@buf)
        if page_encryptor = Column::PageEncryptor.create_if_column_encrypted(
             @file_encryptor, @row_group_index, column_index, descr.path.string
           )
          page_writer = page_writer.with_page_encryptor(page_encryptor)
        end
        column_writer = Column::Writer.get_column_writer(descr, @props, page_writer)
        SerializedColumnWriter.new(column_writer, build_on_close)
      end

      # Appends an already-encoded column chunk from `reader` directly to the
      # underlying writer.
      def append_column(reader : ChunkReader, close : Column::Writer::ColumnCloseResult) : Nil
        metadata = close.metadata
        src_offset = metadata.dictionary_page_offset || metadata.data_page_offset
        read = reader.get_read(src_offset.to_u64)
        append_column_from_read(read, close)
      end

      # Splices an already-encoded column chunk into the row group, reading its
      # bytes sequentially from `read`.
      def append_column_from_read(read : IO, close : Column::Writer::ColumnCloseResult) : Nil
        src_offset, src_length, write_offset = begin_appended_column(close)

        sized = IO::Sized.new(read, src_length)
        write_length = IO.copy(sized, @buf)
        if write_length != src_length.to_u64
          raise general_err!("Failed to splice column data, expected #{src_length} got #{write_length}")
        end

        finish_appended_column(close, src_offset, write_offset)
      end

      # Splice lazily, reclaiming a single stored page blob at a time.
      def append_column_from_pages(pages, close : Column::Writer::ColumnCloseResult) : Nil
        src_offset, src_length, write_offset = begin_appended_column(close)
        written = 0_i64
        pages.each do |bytes|
          @buf.write(bytes)
          written += bytes.size
        end
        if written != src_length
          raise general_err!("Failed to splice column data, expected #{src_length} got #{written}")
        end
        finish_appended_column(close, src_offset, write_offset)
      end

      # Validates the writer state and that `close` matches the next expected
      # column, returning `(src_offset, src_length, write_offset)`.
      private def begin_appended_column(
        close : Column::Writer::ColumnCloseResult,
      ) : Tuple(Int64, Int64, Int64)
        assert_previous_writer_closed
        descr = next_column_desc || raise general_err!("exhausted columns in SerializedRowGroupWriter")

        metadata = close.metadata
        if metadata.column_descr != descr
          raise general_err!("column descriptor mismatch, expected #{descr} got #{metadata.column_descr}")
        end

        src_offset = metadata.dictionary_page_offset || metadata.data_page_offset
        src_length = metadata.compressed_size
        write_offset = @buf.bytes_written
        {src_offset, src_length, write_offset}
      end

      # Rewrites the buffer-relative page offsets recorded in `close` to their
      # final positions in the output file and closes the column.
      private def finish_appended_column(
        close : Column::Writer::ColumnCloseResult,
        src_offset : Int64,
        write_offset : Int64,
      ) : Nil
        metadata = close.metadata
        src_dictionary_offset = metadata.dictionary_page_offset
        src_data_offset = metadata.data_page_offset

        map_offset = ->(x : Int64) { x - src_offset + write_offset }
        builder = Metadata::ColumnChunkMetaData.builder(metadata.column_descr)
          .set_compression_codec(metadata.compression)
          .set_encodings_mask(metadata.encodings_mask)
          .set_total_compressed_size(metadata.compressed_size)
          .set_total_uncompressed_size(metadata.uncompressed_size)
          .set_num_values(metadata.num_values)
          .set_data_page_offset(map_offset.call(src_data_offset))
          .set_dictionary_page_offset(src_dictionary_offset.try { |off| map_offset.call(off) })
          .set_unencoded_byte_array_data_bytes(metadata.unencoded_byte_array_data_bytes)
          .set_column_crypto_metadata(metadata.crypto_metadata)
          .set_encrypted_column_metadata(metadata.encrypted_column_metadata)

        if rep_hist = metadata.repetition_level_histogram
          builder = builder.set_repetition_level_histogram(rep_hist.dup)
        end
        if def_hist = metadata.definition_level_histogram
          builder = builder.set_definition_level_histogram(def_hist.dup)
        end
        if statistics = metadata.statistics
          builder = builder.set_statistics(statistics)
        end
        if geo_statistics = metadata.geo_statistics
          builder = builder.set_geo_statistics(geo_statistics)
        end
        if page_encoding_stats = metadata.page_encoding_stats
          builder = builder.set_page_encoding_stats(page_encoding_stats.dup)
        end
        close.metadata = builder.build

        if offsets = close.offset_index
          locations = offsets.page_locations
          locations.each_with_index do |location, i|
            location.offset = map_offset.call(location.offset)
            locations[i] = location
          end
        end

        build_on_close.call(close)
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
