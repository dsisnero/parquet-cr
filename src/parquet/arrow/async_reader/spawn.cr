# parquet/src/arrow/async_reader/spawn.rs, apache/arrow-rs 60.0.0.
require "sync/mutex"

module Parquet::Arrow
  # Dispatches IO onto an explicit Crystal execution context. Unlike Rust Clone,
  # Crystal shares the reader reference; a fiber-safe mutex serializes its use.
  # The capacity-one reply channel cannot leave a sender blocked on completion.
  class SpawnedReader < AsyncFileReader
    include MetadataSuffixFetch
    @mutex = Sync::Mutex.new

    def initialize(@inner : AsyncFileReader, @context : Fiber::ExecutionContext)
    end

    def into_inner : AsyncFileReader
      @inner
    end

    def get_bytes(range : Range(UInt64, UInt64)) : Bytes
      dispatch { @inner.get_bytes(range) }
    end

    def get_byte_ranges(ranges : Array(Range(UInt64, UInt64))) : Array(Bytes)
      dispatch { @inner.get_byte_ranges(ranges) }
    end

    def get_metadata(options : ArrowReaderOptions? = nil) : File::Metadata::ParquetMetaData
      dispatch { @inner.get_metadata(options) }
    end

    def fetch_suffix(length : Int32) : Bytes
      dispatch do
        source = @inner
        unless source.is_a?(MetadataSuffixFetch)
          raise NYIError.new("Inner reader does not support suffix fetches")
        end
        source.fetch_suffix(length)
      end
    end

    private def dispatch(&operation : -> T) : T forall T
      result = Channel(T | Exception).new(1)
      @context.spawn do
        value = @mutex.synchronize { operation.call }
        result.send(value)
      rescue error : Exception
        result.send(error)
      end
      value = result.receive
      raise value if value.is_a?(Exception)
      value
    end
  end
end
