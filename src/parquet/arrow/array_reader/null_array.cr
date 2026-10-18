require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../record_reader"

# Ported from `parquet/src/arrow/array_reader/null_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `NullArrayReader`: reads a parquet column typed `DataType::Null`
# (`LogicalType::Unknown`) — which can only hold nulls — as an `Arrow::NullArray`.
# Only the definition/repetition level data is used; the null bitmap is consumed
# and discarded.
#
# Divergences: upstream is generic over the physical data type but the builder
# instantiates it for INT32 (a Null column has no non-null values, so the value
# decoder is never driven); the port follows that and fixes `T = Int32`.
# `usize` counts are `Int32`.
module Parquet
  module Arrow
    # An `ArrayReader` yielding `Arrow::NullArray`s (upstream
    # `NullArrayReader<T>`).
    class NullArrayReader < ArrayReader
      def initialize(
        @pages : Column::PageIterator,
        column_desc : Schema::ColumnDescriptor,
        batch_size : Int32,
        padding_threshold : Int16?,
      )
        @record_reader = RecordReader::GenericRecordReader(Int32).new(column_desc, batch_size, 0)
        if threshold = padding_threshold
          @record_reader.set_padding_threshold(threshold)
        end
        @def_levels_buffer = nil.as(Array(Int16)?)
        @rep_levels_buffer = nil.as(Array(Int16)?)
      end

      def data_type : ::Arrow::DataType
        ::Arrow::DataType.null
      end

      def read_records(batch_size : Int32) : Int32
        Arrow.read_records_from_pages(@record_reader, @pages, batch_size)
      end

      def skip_records(num_records : Int32) : Int32
        Arrow.skip_records_from_pages(@record_reader, @pages, num_records)
      end

      def consume_batch : ::Arrow::ArrayBase
        array = ::Arrow::NullArray.new(@record_reader.values_written).as(::Arrow::ArrayBase)

        # Save the level buffers and consume (discard) the null bitmap.
        @def_levels_buffer = @record_reader.consume_def_levels
        @rep_levels_buffer = @record_reader.consume_rep_levels
        @record_reader.consume_compact_bitmap

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
    end
  end
end
