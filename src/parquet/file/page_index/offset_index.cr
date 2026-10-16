require "../metadata"

# Ported from `parquet/src/file/page_index/offset_index.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `PageLocation` and `OffsetIndexMetaData`. The hand-coded
# `try_from_fast` decoder is not ported (the macro-generated `read_thrift`
# decodes the same wire form); column index and `PageIndexBuilder` follow
# (see `plans/parity.md`).
module Parquet
  module File
    module PageIndex
      alias Int64List = Array(Int64)
      alias PageLocationList = Array(PageLocation)

      # Page location information for `OffsetIndexMetaData`.
      define_thrift_struct(PageLocation) do
        required 1, offset, Int64
        required 2, compressed_page_size, Int32
        required 3, first_row_index, Int64
      end

      # `OffsetIndex` information for a column chunk.
      define_thrift_struct(OffsetIndexMetaData) do
        required 1, page_locations, PageLocationList
        optional 2, unencoded_byte_array_data_bytes, Int64List
      end
    end
  end
end
