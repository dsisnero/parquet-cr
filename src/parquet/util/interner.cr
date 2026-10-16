# Ported from `parquet/src/util/interner.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the generic `Interner`/`Storage` pair used by the dictionary encoder.
# Divergence: Rust's `Storage` trait (with `Key`/`Value` associated types) is an
# abstract generic class `Storage(Key, Value)`; Rust's `hashbrown::HashTable`
# dedup table becomes a `Hash(Bytes, Key)` keyed on `value.as_bytes`, preserving
# the upstream byte-wise comparison (so identical NaN bit patterns dedup).
# `estimated_memory_size` uses element counts rather than Rust's retained
# capacity (`Array` exposes no capacity), and the dedup table contributes a
# fixed preallocation estimate.
module Parquet
  module Util
    # Upstream `DEFAULT_DEDUP_CAPACITY`: slots preallocated in the dedup table.
    DEFAULT_DEDUP_CAPACITY = 4096

    # Storage trait for `Interner`. `Key` is the interned index type; `Value` is
    # the interned value type (which must respond to `as_bytes`).
    abstract class Storage(Key, Value)
      abstract def get(idx : Key) : Value
      abstract def push(value : Value) : Key
      abstract def estimated_memory_size : Int32
    end

    # A generic value interner supporting various different `Storage`s.
    class Interner(S)
      def initialize(@storage : S)
        @dedup = Hash(Bytes, UInt64).new
      end

      # Interns the value, returning the interned key.
      def intern(value) : UInt64
        bytes = value.as_bytes
        existing = @dedup[bytes]?
        return existing if existing

        key = @storage.push(value)
        @dedup[bytes] = key
        key
      end

      # Returns an estimate of the memory used, in bytes.
      def estimated_memory_size : Int32
        @storage.estimated_memory_size + dedup_allocation_size
      end

      # Returns the storage for this interner.
      def storage : S
        @storage
      end

      private def dedup_allocation_size : Int32
        DEFAULT_DEDUP_CAPACITY * (sizeof(UInt64) + sizeof(Bytes))
      end
    end
  end
end
