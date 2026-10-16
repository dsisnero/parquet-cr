# Ported from `parquet/src/file/metadata/reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `PageIndexPolicy`. The pull-based `ParquetMetaDataReader` IO machinery
# is ported separately (see `plans/parity.md`).
module Parquet
  module File
    module Metadata
      # Describes the policy for reading page indexes.
      enum PageIndexPolicy
        # Do not read the page index.
        Skip
        # Read the page index if it exists, otherwise do not error.
        Optional
        # Require the page index to exist, and error if it does not.
        #
        # Only enforced for the offset index; this is the same as `Optional` for
        # the column index.
        Required

        # Mirrors Rust `From<bool>`: `true` -> `Required`, `false` -> `Skip`.
        def self.from(value : Bool) : PageIndexPolicy
          value ? Required : Skip
        end
      end
    end
  end
end
