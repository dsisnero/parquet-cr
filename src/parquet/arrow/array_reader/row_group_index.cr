require "../array_reader"
require "deque"

# parquet/src/arrow/array_reader/row_group_index.rs, arrow-rs 60.0.0.
module Parquet
  module Arrow
    # Emits physical metadata indices. A deque of (index, count) runs replaces
    # Rust's flattened RepeatN iterator; the single-group fast path avoids a map.
    class RowGroupIndexReader < ArrayReader
      @buffered_indices = [] of Int64

      def initialize(@remaining : Deque(Tuple(Int64, Int64)))
      end

      def self.try_new(metadata : File::Metadata::ParquetMetaData,
                       row_groups : Iterator(File::Metadata::RowGroupMetaData)) : self
        groups = row_groups.to_a
        indices = {} of Int32 => Int64
        if groups.size == 1
          ordinal = groups[0].ordinal || raise general_err!("Row group missing ordinal field, required to compute row group indices")
          index = metadata.row_groups.index { |group| group.ordinal == ordinal }
          raise general_err!("Row group with ordinal #{ordinal} not found in metadata") unless index
          return new(Deque{ {index.to_i64, groups[0].num_rows} })
        end
        metadata.row_groups.each_with_index do |group, index|
          indices[ordinal] = index.to_i64 if ordinal = group.ordinal
        end
        runs = Deque(Tuple(Int64, Int64)).new
        groups.each do |group|
          ordinal = group.ordinal || raise general_err!("Row group missing ordinal field, required to compute row group indices")
          index = indices[ordinal]? || raise general_err!("Row group with ordinal #{ordinal} not found in metadata")
          runs << {index, group.num_rows}
        end
        new(runs)
      end

      def data_type : ::Arrow::DataType
        ::Arrow::DataType.int64
      end

      private def advance(count : Int32, buffer : Bool) : Int32
        remaining = count
        while remaining > 0 && (run = @remaining.first?)
          index, rows = run
          take = Math.min(remaining.to_i64, rows).to_i32
          take.times { @buffered_indices << index } if buffer
          remaining -= take
          if take == rows
            @remaining.shift
          else
            @remaining[0] = {index, rows - take}
          end
        end
        count - remaining
      end

      def read_records(batch_size : Int32) : Int32
        advance(batch_size, true)
      end

      def skip_records(num_records : Int32) : Int32
        advance(num_records, false)
      end

      def consume_batch : ::Arrow::ArrayBase
        values = @buffered_indices
        @buffered_indices = [] of Int64
        ::Arrow::PrimitiveArray(Int64).from_values(values)
      end
    end
  end
end
