require "../errors"
require "../schema/types"
require "../column/page"
require "../column/reader"
require "../bloom_filter"
require "./chunk_reader"
require "./serialized_reader"
require "./properties"
require "./metadata"
require "./metadata/row_group"
require "./metadata/parquet_metadata"
require "./metadata/page_index"
require "./metadata/reader"

# Ported from `parquet/src/file/reader.rs` and
# `parquet/src/file/serialized_reader.rs` (`FileReader`, `RowGroupReader`,
# `SerializedFileReader`, `SerializedRowGroupReader`, `ReadOptions`,
# `ReadOptionsBuilder`, `get_midpoint_offset`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: the `FileReader`/`RowGroupReader` traits are abstract classes;
# `SerializedFileReader<R>`/`SerializedRowGroupReader<R>` are non-generic over
# `ChunkReader`; `get_row_iter` (record API) and `FilePageIterator` (the
# `PageIterator` trait) are deferred to the record chunk, and the encryption
# `with_decryption_properties` path is not ported.
module Parquet
  module File
    # Parquet file reader API.
    abstract class FileReader
      # Returns metadata information about this file.
      abstract def metadata : Metadata::ParquetMetaData

      # Returns the total number of row groups for this file.
      abstract def num_row_groups : Int32

      # Returns the `i`-th row group reader.
      abstract def get_row_group(i : Int32) : RowGroupReader
    end

    # Parquet row group reader API.
    abstract class RowGroupReader
      # Returns metadata information about this row group.
      abstract def metadata : Metadata::RowGroupMetaData

      # Returns the total number of column chunks in this row group.
      abstract def num_columns : Int32

      # Returns the page reader for the `i`-th column chunk.
      abstract def get_column_page_reader(i : Int32) : Column::PageReader

      # Returns the bloom filter for the `i`-th column chunk, if present.
      abstract def get_column_bloom_filter(i : Int32) : BloomFilter::Sbbf?

      # Returns the value reader for the `i`-th column chunk.
      def get_column_reader(i : Int32) : Column::ColumnReader
        col_descr = metadata.schema_descr.column(i)
        col_page_reader = get_column_page_reader(i)
        Column.get_column_reader(col_descr, col_page_reader)
      end
    end

    # A predicate for filtering row groups, invoked with the metadata and index
    # of each row group in the file.
    alias ReadGroupPredicate = Proc(Metadata::RowGroupMetaData, Int32, Bool)

    # A collection of options for reading a Parquet file.
    class ReadOptions
      getter predicates : Array(ReadGroupPredicate)
      getter enable_page_index : Bool
      getter props : Properties::ReaderProperties
      getter metadata_options : Metadata::ParquetMetaDataOptions

      def initialize(
        @predicates : Array(ReadGroupPredicate),
        @enable_page_index : Bool,
        @props : Properties::ReaderProperties,
        @metadata_options : Metadata::ParquetMetaDataOptions,
      )
      end
    end

    # A builder for `ReadOptions`. Predicates are chained with AND.
    class ReadOptionsBuilder
      def initialize
        @predicates = [] of ReadGroupPredicate
        @enable_page_index = false
        @props = nil.as(Properties::ReaderProperties?)
        @metadata_options = Metadata::ParquetMetaDataOptions.new
      end

      # Adds a predicate on row group metadata to the reading option.
      def with_predicate(predicate : ReadGroupPredicate) : self
        @predicates << predicate
        self
      end

      # Adds a range predicate, keeping row groups whose midpoint is within the
      # closed-open range `[start, end)`.
      def with_range(start : Int64, end_ : Int64) : self
        raise general_err!("assertion failed: start < end") unless start < end_

        predicate = ->(rg : Metadata::RowGroupMetaData, _idx : Int32) do
          mid = Parquet::File.get_midpoint_offset(rg)
          mid >= start && mid < end_
        end
        @predicates << predicate
        self
      end

      # Enables reading the page index structures.
      def with_page_index : self
        @enable_page_index = true
        self
      end

      # Sets the `ReaderProperties` configuration.
      def with_reader_properties(properties : Properties::ReaderProperties) : self
        @props = properties
        self
      end

      # Provides a Parquet schema to use when decoding the metadata.
      def with_parquet_schema(schema : Schema::SchemaDescriptor) : self
        @metadata_options.set_schema(schema)
        self
      end

      # Sets whether to convert encoding stats to a bitmask.
      def with_encoding_stats_as_mask(val : Bool) : self
        @metadata_options.set_encoding_stats_as_mask(val)
        self
      end

      # Sets the decoding policy for `encoding_stats`.
      def with_encoding_stats_policy(policy : Metadata::ParquetStatisticsPolicy) : self
        @metadata_options.set_encoding_stats_policy(policy)
        self
      end

      # Sets the decoding policy for `statistics`.
      def with_column_stats_policy(policy : Metadata::ParquetStatisticsPolicy) : self
        @metadata_options.set_column_stats_policy(policy)
        self
      end

      # Sets the decoding policy for `size_statistics`.
      def with_size_stats_policy(policy : Metadata::ParquetStatisticsPolicy) : self
        @metadata_options.set_size_stats_policy(policy)
        self
      end

      # Seals the builder and returns the read options.
      def build : ReadOptions
        props = @props || Properties::ReaderProperties.builder.build
        ReadOptions.new(@predicates, @enable_page_index, props, @metadata_options)
      end
    end

    # Returns the midpoint offset for a row group.
    def self.get_midpoint_offset(meta : Metadata::RowGroupMetaData) : Int64
      col = meta.column(0)
      offset = col.data_page_offset
      if dic_offset = col.dictionary_page_offset
        offset = dic_offset if offset > dic_offset
      end
      offset + meta.compressed_size // 2
    end

    # A serialized implementation for Parquet `FileReader`.
    class SerializedFileReader < FileReader
      getter chunk_reader : ChunkReader
      getter metadata : Metadata::ParquetMetaData
      getter props : Properties::ReaderProperties

      def initialize(
        @chunk_reader : ChunkReader,
        @metadata : Metadata::ParquetMetaData,
        @props : Properties::ReaderProperties,
      )
      end

      # Creates a file reader from a Parquet source.
      def self.new(chunk_reader : ChunkReader) : SerializedFileReader
        metadata = Metadata::ParquetMetaDataReader.new.parse_and_finish(chunk_reader)
        new(chunk_reader, metadata, Properties::ReaderProperties.builder.build)
      end

      # Creates a file reader from a Parquet source with read options.
      def self.new_with_options(chunk_reader : ChunkReader, options : ReadOptions) : SerializedFileReader
        metadata_builder = Metadata::ParquetMetaDataReader.new
          .with_metadata_options(options.metadata_options)
          .parse_and_finish(chunk_reader)
          .into_builder

        predicates = options.predicates
        metadata_builder.take_row_groups.each_with_index do |rg_meta, i|
          keep = true
          predicates.each do |predicate|
            unless predicate.call(rg_meta, i)
              keep = false
              break
            end
          end
          metadata_builder.add_row_group(rg_meta) if keep
        end

        metadata = metadata_builder.build

        if options.enable_page_index
          reader = Metadata::ParquetMetaDataReader.new_with_metadata(metadata)
            .with_page_index_policy(Metadata::PageIndexPolicy::Required)
          reader.read_page_indexes(chunk_reader)
          metadata = reader.finish
        end

        new(chunk_reader, metadata, options.props)
      end

      def num_row_groups : Int32
        @metadata.num_row_groups
      end

      def get_row_group(i : Int32) : RowGroupReader
        row_group_metadata = @metadata.row_group(i)
        page_index = @metadata.page_index_for_row_group(i)
        SerializedRowGroupReader.new(@chunk_reader, row_group_metadata, page_index, @props)
      end
    end

    # A serialized implementation for Parquet `RowGroupReader`.
    class SerializedRowGroupReader < RowGroupReader
      @bloom_filters : Array(BloomFilter::Sbbf?)

      def initialize(
        @chunk_reader : ChunkReader,
        @metadata : Metadata::RowGroupMetaData,
        @page_index : Metadata::RowGroupPageIndex,
        @props : Properties::ReaderProperties,
      )
        @bloom_filters =
          if @props.read_bloom_filter
            @metadata.columns.map { |col| BloomFilter::Sbbf.read_from_column_chunk(col, @chunk_reader) }
          else
            Array(BloomFilter::Sbbf?).new(@metadata.columns.size, nil)
          end
      end

      def metadata : Metadata::RowGroupMetaData
        @metadata
      end

      def num_columns : Int32
        @metadata.num_columns
      end

      def get_column_page_reader(i : Int32) : Column::PageReader
        col = @metadata.column(i)
        page_locations = @page_index.page_locations(i)
        SerializedPageReader.new_for_column(
          @chunk_reader,
          col,
          @metadata.num_rows.to_i32,
          page_locations,
          @props,
        )
      end

      def get_column_bloom_filter(i : Int32) : BloomFilter::Sbbf?
        @bloom_filters[i]
      end
    end

    # An iterator over the page readers of one column across a file's row
    # groups.
    class FilePageIterator
      include Column::PageIterator

      # Creates a page iterator for all row groups in the file.
      def self.new(column_index : Int32, file_reader : FileReader) : FilePageIterator
        num_row_groups = file_reader.metadata.num_row_groups
        with_row_groups(column_index, (0...num_row_groups).each, file_reader)
      end

      # Creates a page iterator over the given row group indices.
      def self.with_row_groups(
        column_index : Int32,
        row_group_indices : Iterator(Int32),
        file_reader : FileReader,
      ) : FilePageIterator
        num_columns = file_reader.metadata.file_metadata.schema_descr.num_columns
        if column_index >= num_columns
          raise Parquet::IndexOutOfBoundError.new(column_index.to_u64, num_columns.to_u64)
        end

        new(column_index, row_group_indices, file_reader)
      end

      def initialize(
        @column_index : Int32,
        @row_group_indices : Iterator(Int32),
        @file_reader : FileReader,
      )
      end

      def next : Column::PageReader | Iterator::Stop
        index = @row_group_indices.next
        return stop if index.is_a?(Iterator::Stop)

        @file_reader.get_row_group(index).get_column_page_reader(@column_index)
      end
    end
  end
end
