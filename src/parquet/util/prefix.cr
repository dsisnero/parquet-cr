# Ported from `parquet/src/util/prefix.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `common_prefix_length` for DELTA_BYTE_ARRAY. Divergence: the upstream
# implementation compares 32-byte blocks (SIMD-friendly); Crystal scans byte by
# byte (behaviourally identical).
module Parquet
  module Util
    # Shared-prefix scanning for `DELTA_BYTE_ARRAY`.
    module Prefix
      # Returns the length in bytes of the longest common prefix of `a` and `b`.
      def self.common_prefix_length(a : Bytes, b : Bytes) : Int32
        n = Math.min(a.size, b.size)
        matched = 0
        while matched < n && a[matched] == b[matched]
          matched += 1
        end
        matched
      end
    end
  end
end
