require "../file/writer"
require "../schema/types"

# Ported from `parquet/src/record/record_writer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `RecordWriter<T>` trait as an abstract class. `parquet_derive`
# implements it for user structs. The type parameter `T` mirrors upstream's
# orphan-rule workaround and is unused here.
module Parquet
  module Record
    # Writes records (`self`) into a row group.
    abstract class RecordWriter(T)
      # Writes from `self` into `row_group_writer`.
      abstract def write_to_row_group(row_group_writer : File::SerializedRowGroupWriter) : Nil

      # Returns the schema used by `row_group_writer`.
      abstract def schema : Schema::Type
    end
  end
end
