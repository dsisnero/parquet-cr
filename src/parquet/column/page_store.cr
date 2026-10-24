require "../errors"
require "../schema/types"

# Ported from parquet/src/column/page_store.rs (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
module Parquet::Column
  # An opaque store-allocated handle, meaningful only to its issuing store.
  struct PageKey
    include Comparable(self)

    def_equals_and_hash @raw

    def initialize(@raw : UInt64)
    end

    def get : UInt64
      @raw
    end

    def <=>(other : PageKey) : Int32
      @raw <=> other.get
    end
  end

  # Pluggable storage of serialized blobs, owned by one column writer at a time.
  # Callers transfer byte buffers to put and reclaim them through take; they
  # must not mutate retained slices. Errors raise ParquetError subclasses.
  abstract class PageStore
    abstract def put(value : Bytes) : PageKey
    abstract def take(key : PageKey) : Bytes

    # Off-heap stores report zero unless they retain an in-memory buffer.
    def memory_size : Int64
      0_i64
    end
  end

  # Context for a factory call, retaining the leaf column descriptor by reference.
  struct PageStoreArgs
    getter column_index : Int32
    getter column_descriptor : Schema::ColumnDescriptor

    def initialize(@column_index : Int32, @column_descriptor : Schema::ColumnDescriptor)
    end
  end

  # Creates a fresh store for every leaf column chunk.
  abstract class PageStoreFactory
    abstract def create(args : PageStoreArgs) : PageStore
  end

  # Keeps serialized blobs on the heap until each is taken by the writer.
  class InMemoryPageStore < PageStore
    @blobs = [] of Bytes
    @resident = 0_i64

    def put(value : Bytes) : PageKey
      key = PageKey.new(@blobs.size.to_u64)
      @resident += value.size
      @blobs << value
      key
    end

    def take(key : PageKey) : Bytes
      raw = key.get
      raise GeneralError.new("invalid page key #{raw}") if raw >= @blobs.size.to_u64
      index = raw.to_i32
      blob = @blobs[index]
      # Keep the slot so keys are never recycled; a second take yields empty.
      @blobs[index] = Bytes.empty
      @resident -= blob.size
      blob
    end

    def memory_size : Int64
      @resident
    end
  end

  # Default factory; no buffered data is shared between its stores.
  class InMemoryPageStoreFactory < PageStoreFactory
    def create(args : PageStoreArgs) : PageStore
      InMemoryPageStore.new
    end
  end
end
