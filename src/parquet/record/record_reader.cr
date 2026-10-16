require "../file/reader"

# Ported from `parquet/src/record/record_reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `RecordReader<T>` trait as an abstract class. The type parameter `T`
# mirrors upstream's orphan-rule workaround and is unused here.
module Parquet
  module Record
    # Read up to `num_records` records from `row_group_reader` into `self`.
    abstract class RecordReader(T)
      abstract def read_from_row_group(row_group_reader : File::RowGroupReader, num_records : Int32) : Nil
    end
  end
end
