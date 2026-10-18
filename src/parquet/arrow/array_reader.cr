require "arrow-cr"
require "../basic"
require "../errors"
require "../file/metadata/row_group"
require "../file/reader"
require "../column/page"

# Ported from `parquet/src/arrow/array_reader/mod.rs` (`ArrayReader`,
# `RowGroups`) and `parquet/src/arrow/array_reader/empty_array.rs`
# (`EmptyArrayReader`, `make_empty_array_reader`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Reads parquet data into arrow arrays. `ArrayReader` is an internal
# implementation detail of the arrow reader.
#
# Divergences: the `ArrayReader`/`RowGroups` traits are abstract classes;
# `as_any` is not modelled (Crystal's `is_a?` covers it); `usize` counts are
# `Int32`; upstream's `impl RowGroups for Arc<dyn FileReader>` becomes
# `FileRowGroups`; the private `read_records`/`skip_records` helpers (which need
# `GenericRecordReader`) are deferred with the record reader.
module Parquet
  module Arrow
    # Reads parquet data into arrow arrays.
    #
    # Implementations buffer decoded data internally and can either read whole
    # batches (`next_batch`) or read/skip incrementally.
    abstract class ArrayReader
      # The arrow type produced by this reader.
      abstract def data_type : ::Arrow::DataType

      # Reads at most `batch_size` records into the in-progress array, returning
      # the number of records read.
      abstract def read_records(batch_size : Int32) : Int32

      # Consumes the buffered data into an arrow array.
      abstract def consume_batch : ::Arrow::ArrayBase

      # Skips `num_records` records, returning the number skipped.
      abstract def skip_records(num_records : Int32) : Int32

      # Definition levels of the data read by the last call, if any.
      def def_levels : Array(Int16)?
        nil
      end

      # Repetition levels of the data read by the last call, if any.
      def rep_levels : Array(Int16)?
        nil
      end

      # The maximum definition level of this reader's parquet column.
      #
      # Synthetic readers (row-number, empty struct) do not track levels.
      def max_def_level : Int16
        raise general_err!(
          "max_def_level called on a reader that does not track definition levels"
        )
      end

      # Reads at most `batch_size` records and returns them as an array
      # (upstream `ArrayReader::next_batch`).
      def next_batch(batch_size : Int32) : ::Arrow::ArrayBase
        read_records(batch_size)
        consume_batch
      end
    end

    # Reads the data pages of one or more row groups (upstream `RowGroups`).
    abstract class RowGroups
      # The number of rows in this collection.
      abstract def num_rows : Int64

      # Returns a page iterator over all pages of the given column across all row
      # groups.
      abstract def column_chunks(i : Int32) : Column::PageIterator

      # Returns an iterator over the row groups in this collection.
      abstract def row_groups : Iterator(File::Metadata::RowGroupMetaData)

      # Returns the parquet metadata.
      abstract def metadata : File::Metadata::ParquetMetaData
    end

    # `RowGroups` over a file reader (upstream `impl RowGroups for
    # Arc<dyn FileReader>`).
    class FileRowGroups < RowGroups
      def initialize(@file_reader : File::FileReader)
      end

      def num_rows : Int64
        @file_reader.metadata.file_metadata.num_rows
      end

      def column_chunks(i : Int32) : Column::PageIterator
        File::FilePageIterator.new(i, @file_reader)
      end

      def row_groups : Iterator(File::Metadata::RowGroupMetaData)
        metadata = @file_reader.metadata
        (0...metadata.num_row_groups).map { |idx| metadata.row_group(idx).as(File::Metadata::RowGroupMetaData) }.each
      end

      def metadata : File::Metadata::ParquetMetaData
        @file_reader.metadata
      end
    end

    # Returns an `ArrayReader` yielding a `StructArray` with no columns but with
    # row counts that match the file's data (upstream
    # `make_empty_array_reader`).
    #
    # This is useful when projection eliminates all columns.
    def self.make_empty_array_reader(row_count : Int32) : ArrayReader
      EmptyArrayReader.new(row_count)
    end

    # An `ArrayReader` that yields empty `StructArray`s.
    class EmptyArrayReader < ArrayReader
      @data_type : ::Arrow::DataType
      @remaining_rows : Int32
      @need_consume_records : Int32

      def initialize(row_count : Int32)
        @data_type = ::Arrow::DataType.struct(::Arrow::Fields.empty)
        @remaining_rows = row_count
        @need_consume_records = 0
      end

      def initialize
        initialize(0)
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        len = Math.min(@remaining_rows, batch_size)
        @remaining_rows -= len
        @need_consume_records += len
        len
      end

      def consume_batch : ::Arrow::ArrayBase
        data = ::Arrow::ArrayData.builder(@data_type)
          .len(@need_consume_records)
          .build
        @need_consume_records = 0
        ::Arrow::StructArray.from(data)
      end

      def skip_records(num_records : Int32) : Int32
        skipped = Math.min(@remaining_rows, num_records)
        @remaining_rows -= skipped
        skipped
      end
    end
  end
end
