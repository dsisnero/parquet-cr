require "./writer"
require "./chunker/cdc"

# Ported from `parquet/src/column/chunker/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Content-defined chunking (CDC) for Parquet data pages.
#
# CDC creates data page boundaries based on content rather than fixed sizes,
# enabling efficient deduplication in content-addressable storage (CAS) systems.
# See `Parquet::File::Properties::CdcOptions` for configuration.
module Parquet
  module Column
    # Content-defined chunking (CDC) for Parquet data pages.
    module Chunker
      # A chunk of data with level and value offsets for record-shredded nested
      # data.
      struct CdcChunk
        # The start offset of this chunk inside the given levels.
        getter level_offset : Int32
        # The number of levels in this chunk.
        getter num_levels : Int32
        # The start index into `non_null_indices` for this chunk.
        getter value_offset : Int32
        # The number of `non_null_indices` entries in this chunk.
        getter num_values : Int32

        def initialize(
          @level_offset : Int32,
          @num_levels : Int32,
          @value_offset : Int32,
          @num_values : Int32,
        )
        end
      end
    end
  end
end
