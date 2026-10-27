# apache/arrow-rs 60.0.0 parquet/src/arrow/async_reader/metadata.rs.
# Awaiting a boxed future maps to a Crystal method that may yield its fiber.
module Parquet::Arrow
  # Fetches an absolute, half-open range without requiring a complete file.
  module MetadataFetch
    abstract def fetch(range : Range(UInt64, UInt64)) : Bytes
  end

  # Fetches a suffix without first issuing a HEAD request for the object size.
  module MetadataSuffixFetch
    include MetadataFetch

    abstract def fetch_suffix(length : Int32) : Bytes
  end

  # Closure adapter for metadata fetches.
  class MetadataFetchFn
    include MetadataFetch

    def initialize(&@fetcher : Range(UInt64, UInt64) -> Bytes)
    end

    def fetch(range : Range(UInt64, UInt64)) : Bytes
      @fetcher.call(range)
    end
  end

  # Closure adapter for absolute and suffix metadata fetches.
  class MetadataSuffixFetchFn
    include MetadataSuffixFetch

    def initialize(@fetcher : Proc(Range(UInt64, UInt64), Bytes), @suffix_fetcher : Proc(Int32, Bytes))
    end

    def fetch(range : Range(UInt64, UInt64)) : Bytes
      @fetcher.call(range)
    end

    def fetch_suffix(length : Int32) : Bytes
      @suffix_fetcher.call(length)
    end
  end
end
