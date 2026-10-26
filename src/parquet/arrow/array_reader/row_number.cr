require "../array_reader"
require "deque"

# parquet/src/arrow/array_reader/row_number.rs, arrow-rs 60.0.0.
module Parquet
  module Arrow
    # Lazily buffers file-global row ranges in selected row-group order.
    class RowNumberReader < ArrayReader
      @remaining_row_ranges : Deque(Range(Int64, Int64))
      @buffered_row_ranges = [] of Range(Int64, Int64)

      def initialize(@remaining_row_ranges : Deque(Range(Int64, Int64)))
      end

      def self.try_new(metadata : File::Metadata::ParquetMetaData,
                       row_groups : Iterator(File::Metadata::RowGroupMetaData)) : self
        offsets = {} of Int32 => Int64
        offset = 0_i64
        missing = 0
        metadata.row_groups.each do |group|
          if ordinal = group.ordinal
            offsets[ordinal] = offset
          else
            missing += 1
          end
          offset += group.num_rows
        end
        if missing > 0 && !offsets.empty?
          raise general_err!("Cannot compute row numbers: file has inconsistent row-group ordinals (#{missing} of #{metadata.num_row_groups} row groups are missing the ordinal field)")
        end
        ranges = Deque(Range(Int64, Int64)).new
        row_groups.each do |group|
          ordinal = group.ordinal || raise general_err!("Row group missing ordinal field, required to compute row numbers")
          first = offsets[ordinal]? || raise general_err!("Row group with ordinal #{ordinal} not found in metadata")
          ranges << (first...first + group.num_rows)
        end
        new(ranges)
      end

      private def take_range(count : Int32) : Range(Int64, Int64)?
        first = @remaining_row_ranges.first?
        return nil unless first
        if first.end - first.begin <= count
          @remaining_row_ranges.shift
        else
          split = first.begin + count
          @remaining_row_ranges[0] = split...first.end
          first.begin...split
        end
      end

      def data_type : ::Arrow::DataType
        ::Arrow::DataType.int64
      end

      def read_records(batch_size : Int32) : Int32
        remaining = batch_size
        while remaining > 0
          range = take_range(remaining)
          break unless range
          remaining -= (range.end - range.begin).to_i32
          @buffered_row_ranges << range
        end
        batch_size - remaining
      end

      def skip_records(num_records : Int32) : Int32
        remaining = num_records
        while remaining > 0
          range = take_range(remaining)
          break unless range
          remaining -= (range.end - range.begin).to_i32
        end
        num_records - remaining
      end

      def consume_batch : ::Arrow::ArrayBase
        values = [] of Int64
        @buffered_row_ranges.each { |range| range.each { |value| values << value } }
        @buffered_row_ranges.clear
        ::Arrow::PrimitiveArray(Int64).from_values(values)
      end
    end
  end
end
