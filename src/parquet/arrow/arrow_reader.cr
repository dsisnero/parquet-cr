require "arrow-cr"
require "../errors"
require "./array_reader"
require "./array_reader/builder"
require "./mod"
require "./schema"
require "../file/reader"
require "../file/metadata"

# Ported from `parquet/src/arrow/arrow_reader/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the batch-reading core: `ArrowReaderMetadata` (parquet metadata + arrow
# schema + projected field tree), `ParquetRecordBatchReaderBuilder` (batch size,
# projection mask, `build`) and `ParquetRecordBatchReader` (an
# `Iterator(Arrow::RecordBatch)` over an `ArrayReader`).
#
# Divergences (none of the corresponding infrastructure is ported yet):
# - row selection (`RowSelection`, cursors/selectors), offset/limit plans,
#   `RowFilter` evaluation and statistics pruning are not modelled: the reader
#   always reads all rows of all row groups;
# - no reader metrics, page-index/column-index policies, supplied-schema
#   overrides or virtual columns in `ArrowReaderMetadata` (only the
#   `skip_arrow_metadata` option exists);
# - the builder takes a `File::FileReader` (upstream takes any `ChunkReader`),
#   because the port's `RowGroups` implementation (`FileRowGroups`) needs a
#   reader; `ReaderRowGroups`/`ReaderPageIterator` are therefore not needed yet;
# - `usize` counts are `Int32`; the reader's schema carries the projected field
#   metadata (upstream strips it from `RecordBatchReader::schema`).
module Parquet
  module Arrow
    # The parquet metadata, arrow schema and projected field tree needed to read
    # a file (upstream `ArrowReaderMetadata`).
    class ArrowReaderMetadata
      getter metadata : File::Metadata::ParquetMetaData
      getter schema : ::Arrow::Schema
      getter fields : ParquetField?

      def initialize(@metadata : File::Metadata::ParquetMetaData, @schema : ::Arrow::Schema,
                     @fields : ParquetField?)
      end

      # Loads the metadata for `reader` (upstream `ArrowReaderMetadata::load`).
      def self.load(reader : File::FileReader) : ArrowReaderMetadata
        try_new(reader.metadata)
      end

      # Creates the metadata from pre-existing parquet metadata. When
      # `skip_arrow_metadata` is set the embedded `ARROW:schema` key/value entry
      # is ignored.
      def self.try_new(metadata : File::Metadata::ParquetMetaData,
                       skip_arrow_metadata : Bool = false) : ArrowReaderMetadata
        file_metadata = metadata.file_metadata
        key_value_metadata = skip_arrow_metadata ? nil : file_metadata.key_value_metadata

        schema, fields = Parquet::Arrow.parquet_to_arrow_schema_and_fields(
          file_metadata.schema_descr, Parquet::Arrow::ProjectionMask.all, key_value_metadata
        )

        new(metadata, schema, fields)
      end

      # The parquet schema descriptor.
      def parquet_schema : Schema::SchemaDescriptor
        @metadata.file_metadata.schema_descr
      end
    end

    # Builds a `ParquetRecordBatchReader`
    # (upstream `ParquetRecordBatchReaderBuilder`).
    class ParquetRecordBatchReaderBuilder
      getter metadata : File::Metadata::ParquetMetaData
      getter schema : ::Arrow::Schema
      getter fields : ParquetField?

      def initialize(
        @input : File::FileReader,
        @metadata : File::Metadata::ParquetMetaData,
        @schema : ::Arrow::Schema,
        @fields : ParquetField?,
        @batch_size : Int32 = ArrayReaderBuilder::DEFAULT_BATCH_SIZE,
        @projection : ProjectionMask = ProjectionMask.all,
      )
      end

      # Creates a builder from a file reader (upstream `try_new`).
      def self.try_new(reader : File::FileReader) : ParquetRecordBatchReaderBuilder
        metadata = ArrowReaderMetadata.load(reader)
        new(reader, metadata.metadata, metadata.schema, metadata.fields)
      end

      # Creates a builder from a file reader and pre-loaded metadata (upstream
      # `new_with_metadata`).
      def self.new_with_metadata(reader : File::FileReader,
                                 metadata : ArrowReaderMetadata) : ParquetRecordBatchReaderBuilder
        new(reader, metadata.metadata, metadata.schema, metadata.fields)
      end

      # Sets the batch size (the file row count is used when it is smaller).
      def with_batch_size(batch_size : Int32) : self
        @batch_size = batch_size
        self
      end

      # Only reads the leaves selected by `mask`.
      def with_projection(projection : ProjectionMask) : self
        @projection = projection
        self
      end

      # The parquet schema descriptor.
      def parquet_schema : Schema::SchemaDescriptor
        @metadata.file_metadata.schema_descr
      end

      # Builds the reader.
      def build : ParquetRecordBatchReader
        # Try to avoid allocating a large buffer.
        row_count = @metadata.file_metadata.num_rows
        batch_size = row_count < @batch_size ? row_count.to_i32 : @batch_size
        batch_size = 0 if batch_size < 0

        row_groups = FileRowGroups.new(@input)
        array_reader = ArrayReaderBuilder.new(row_groups, batch_size)
          .build_array_reader(@fields, @projection)

        ParquetRecordBatchReader.new(array_reader, batch_size)
      end
    end

    # Reads a parquet file as a sequence of `Arrow::RecordBatch`
    # (upstream `ParquetRecordBatchReader`).
    class ParquetRecordBatchReader
      include Iterator(::Arrow::RecordBatch)

      # The projected schema (derived from the array reader's data type).
      getter schema : ::Arrow::Schema
      # The number of rows requested per batch.
      getter batch_size : Int32

      def initialize(@array_reader : ArrayReader, @batch_size : Int32)
        data_type = @array_reader.data_type
        fields = data_type.fields || ::Arrow::Fields.empty
        @schema = ::Arrow::Schema.new(fields)
      end

      # Creates a reader for `reader` with the given batch size (upstream
      # `ParquetRecordBatchReader::try_new`).
      def self.try_new(reader : File::FileReader, batch_size : Int32) : ParquetRecordBatchReader
        ParquetRecordBatchReaderBuilder.try_new(reader)
          .with_batch_size(batch_size)
          .build
      end

      # Returns the next batch, or `nil` at the end of the file.
      def next_batch : ::Arrow::RecordBatch?
        return nil if @batch_size == 0

        @array_reader.read_records(@batch_size)
        batch = consume_record_batch(@array_reader)
        batch.num_rows > 0 ? batch : nil
      end

      # `Iterator(Arrow::RecordBatch)` implementation.
      def next : ::Arrow::RecordBatch | Iterator::Stop
        next_batch || Iterator::Stop::INSTANCE
      end

      private def consume_record_batch(array_reader : ArrayReader) : ::Arrow::RecordBatch
        array = array_reader.consume_batch
        struct_array = array.as(::Arrow::StructArray)
        ::Arrow::RecordBatch.from(struct_array)
      end
    end
  end
end
