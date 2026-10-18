require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../record_reader"

# Ported from `parquet/src/arrow/array_reader/primitive_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `PrimitiveArrayReader<T>` for the numeric physical types (INT32, INT64,
# INT96, FLOAT, DOUBLE) plus the `read_records`/`skip_records` helpers that drive
# a record reader from a page iterator (`array_reader/mod.rs`).
#
# Divergences: `T` is the Crystal value type (`Int32`, `Int64`, `Float32`,
# `Float64`, `Int96`) rather than a parquet `DataType` marker; upstream's
# `Vec<T::T>: IntoBuffer` view conversion is unnecessary because Crystal reads
# the decoded values directly; `coerce_array` (temporal/decimal/dictionary
# coercion against the embedded arrow schema) is deferred until the arrow cast
# layer is wired in, so the produced array uses the physical type's arrow data
# type unless `arrow_type` is a compatible primitive; `usize` counts are `Int32`.
module Parquet
  module Arrow
    # Reads up to `batch_size` records from `pages` into `record_reader`,
    # returning the number of records read (upstream
    # `array_reader::read_records`).
    def self.read_records_from_pages(
      record_reader : RecordReader::GenericRecordReader(T),
      pages : Column::PageIterator,
      batch_size : Int32,
    ) : Int32 forall T
      records_read = 0
      while records_read < batch_size
        records_to_read = batch_size - records_read
        records_read_once = record_reader.read_records(records_to_read)
        records_read += records_read_once

        # Record reader exhausted
        if records_read_once < records_to_read
          page_reader = pages.next
          break if page_reader.is_a?(Iterator::Stop)
          record_reader.set_page_reader(page_reader)
        end
      end
      records_read
    end

    # Skips up to `batch_size` records from `pages` (upstream
    # `array_reader::skip_records`).
    def self.skip_records_from_pages(
      record_reader : RecordReader::GenericRecordReader(T),
      pages : Column::PageIterator,
      batch_size : Int32,
    ) : Int32 forall T
      records_skipped = 0
      while records_skipped < batch_size
        records_to_skip = batch_size - records_skipped
        records_skipped_once = record_reader.skip_records(records_to_skip)
        records_skipped += records_skipped_once

        if records_skipped_once < records_to_skip
          page_reader = pages.next
          break if page_reader.is_a?(Iterator::Stop)
          record_reader.set_page_reader(page_reader)
        end
      end
      records_skipped
    end

    # Reads parquet data into arrow primitive arrays.
    class PrimitiveArrayReader(T) < ArrayReader
      def initialize(
        @pages : Column::PageIterator,
        column_desc : Schema::ColumnDescriptor,
        arrow_type : ::Arrow::DataType?,
        batch_size : Int32,
        padding_threshold : Int16?,
      )
        @data_type = arrow_type || Arrow.parquet_to_arrow_field(column_desc).data_type
        @record_reader = RecordReader::GenericRecordReader(T).new(column_desc, batch_size, default_value)
        if threshold = padding_threshold
          @record_reader.set_padding_threshold(threshold)
        end
        @def_levels_buffer = nil.as(Array(Int16)?)
        @rep_levels_buffer = nil.as(Array(Int16)?)
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        Arrow.read_records_from_pages(@record_reader, @pages, batch_size)
      end

      def skip_records(num_records : Int32) : Int32
        Arrow.skip_records_from_pages(@record_reader, @pages, num_records)
      end

      def consume_batch : ::Arrow::ArrayBase
        target_type = @data_type
        len = @record_reader.values_written
        values = @record_reader.consume_record_data
        nulls = Arrow.null_buffer_from(@record_reader.consume_compact_bitmap, len)

        array = build_array(values, nulls, target_type)

        # Save definition and repetition buffers, then reset the reader.
        @def_levels_buffer = @record_reader.consume_def_levels
        @rep_levels_buffer = @record_reader.consume_rep_levels
        @record_reader.reset
        array
      end

      def def_levels : Array(Int16)?
        @def_levels_buffer
      end

      def rep_levels : Array(Int16)?
        @rep_levels_buffer
      end

      def max_def_level : Int16
        @record_reader.max_def_level
      end

      # The value used for null slots (upstream `T::default`).
      private def default_value_for(type : Bool.class) : Bool
        false
      end

      private def default_value_for(type : Int32.class) : Int32
        0
      end

      private def default_value_for(type : Int64.class) : Int64
        0_i64
      end

      private def default_value_for(type : Float32.class) : Float32
        0.0_f32
      end

      private def default_value_for(type : Float64.class) : Float64
        0.0_f64
      end

      private def default_value : T
        default_value_for(T)
      end

      # BOOLEAN values are packed into a `BooleanBuffer` (upstream
      # `Vec<bool>.into_buffer`).
      private def build_array(values : Array(Bool), nulls : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
        ::Arrow::BooleanArray.new(::Arrow::BooleanBuffer.from_slice(values), nulls)
      end

      private def build_array(values : Array(Int32), nulls : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
        ::Arrow::PrimitiveArray.new(::Arrow::ScalarBuffer(Int32).from(values), nulls, data_type)
      end

      private def build_array(values : Array(Int64), nulls : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
        ::Arrow::PrimitiveArray.new(::Arrow::ScalarBuffer(Int64).from(values), nulls, data_type)
      end

      private def build_array(values : Array(Float32), nulls : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
        ::Arrow::PrimitiveArray.new(::Arrow::ScalarBuffer(Float32).from(values), nulls, data_type)
      end

      private def build_array(values : Array(Float64), nulls : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
        ::Arrow::PrimitiveArray.new(::Arrow::ScalarBuffer(Float64).from(values), nulls, data_type)
      end
    end

    # Builds a `NullBuffer` from a packed validity bitmap when `len > 0`
    # (upstream `NullBuffer::from_unsliced_buffer(bitmap, len)`).
    def self.null_buffer_from(bitmap : Bytes?, len : Int32) : ::Arrow::NullBuffer?
      return nil if len == 0
      bytes = bitmap
      return nil unless bytes

      ::Arrow::NullBuffer.new(::Arrow::BooleanBuffer.from_bits(bytes, 0, len))
    end
  end
end
