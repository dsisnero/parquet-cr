require "./push_decoder"
require "./async_reader/metadata"
require "../file/metadata/async_reader"
require "sync/mutex"

# parquet/src/arrow/async_reader/mod.rs, apache/arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b). BoxFuture/Stream become yielding
# methods and Iterator: callers can spawn scans on any execution context.
module Parquet::Arrow
  # Storage interface. Each method may suspend the calling fiber. Implementors
  # must return exactly the requested bytes; default multi-range IO is ordered.
  abstract class AsyncFileReader
    include MetadataFetch

    abstract def get_bytes(range : Range(UInt64, UInt64)) : Bytes
    abstract def get_metadata(options : ArrowReaderOptions? = nil) : File::Metadata::ParquetMetaData

    def get_byte_ranges(ranges : Array(Range(UInt64, UInt64))) : Array(Bytes)
      ranges.map { |range| get_bytes(range) }
    end

    def fetch(range : Range(UInt64, UInt64)) : Bytes
      get_bytes(range)
    end
  end

  # Explicit adapter for Rust's AsyncRead + AsyncSeek blanket implementation.
  # The borrowed IO is never closed; the mutex protects seek/read as one action.
  class AsyncSeekableReader < AsyncFileReader
    include MetadataSuffixFetch
    @mutex = Sync::Mutex.new

    def initialize(@io : IO)
    end

    def get_bytes(range : Range(UInt64, UInt64)) : Bytes
      raise general_err!("Invalid byte range #{range}") if range.end < range.begin
      @mutex.synchronize do
        @io.seek(range.begin.to_i64)
        count = (range.end - range.begin).to_i32
        buffer = Bytes.new(count)
        read = read_into(buffer)
        unless read == count
          raise Parquet::EOFError.new("expected to read #{count} bytes, got #{read}")
        end
        buffer
      end
    end

    def fetch_suffix(length : Int32) : Bytes
      raise ArgumentError.new("suffix must be nonnegative") if length < 0
      @mutex.synchronize do
        @io.seek(-length.to_i64, IO::Seek::End)
        buffer = Bytes.new(length)
        read = read_into(buffer)
        buffer[0, read]
      end
    end

    private def read_into(buffer : Bytes) : Int32
      read = 0
      while read < buffer.size
        count = @io.read(buffer[read..])
        break if count == 0
        read += count
      end
      read
    end

    def get_metadata(options : ArrowReaderOptions? = nil) : File::Metadata::ParquetMetaData
      File::Metadata::ParquetMetaDataReader.new.with_arrow_reader_options(options).load_via_suffix_and_finish(self)
    end
  end

  class ArrowReaderMetadata
    def self.load_async(input : AsyncFileReader, options : ArrowReaderOptions = ArrowReaderOptions.new) : self
      try_new(input.get_metadata(options), options)
    end
  end

  # ChunkReader adapter used for bounded auxiliary reads such as bloom filters.
  private class AsyncChunkReader < File::ChunkReader
    def initialize(@input : AsyncFileReader)
    end

    def len : UInt64
      UInt64::MAX
    end

    def get_bytes(start : UInt64, length : Int32) : Bytes
      @input.get_bytes(start...(start + length))
    end

    def get_read(start : UInt64) : IO
      raise NYIError.new("Unbounded async reads are not supported")
    end
  end

  # Full streaming builder sharing projection, selection, filters, metrics and
  # row budgeting with the push decoder, matching the pinned upstream pipeline.
  class ParquetRecordBatchStreamBuilder < ArrowReaderBuilder(AsyncFileReader)
    def self.new(input : AsyncFileReader) : self
      new_with_options(input, ArrowReaderOptions.new)
    end

    def self.new_with_options(input : AsyncFileReader, options : ArrowReaderOptions) : self
      new_with_metadata(input, ArrowReaderMetadata.load_async(input, options))
    end

    def self.new_with_metadata(input : AsyncFileReader, metadata : ArrowReaderMetadata) : self
      new(input, metadata.metadata, metadata.schema, metadata.fields)
    end

    def with_row_group_selections(selections : Array(RowGroupSelection)) : self
      @row_group_plan.set_row_group_selections(selections)
      self
    end

    def get_row_group_column_bloom_filter(row_group_idx : Int32, column_idx : Int32) : BloomFilter::Sbbf?
      column = @metadata.row_group(row_group_idx).column(column_idx)
      if offset = column.bloom_filter_offset
        raise general_err!("Bloom filter offset is invalid") if offset < 0
      end
      BloomFilter::Sbbf.read_from_column_chunk(column, AsyncChunkReader.new(@input))
    end

    def build : ParquetRecordBatchStream
      decoder = ParquetPushDecoderBuilder.new(
        PushDecoderInput.new, @metadata, @schema, @fields, @batch_size,
        @row_group_plan, @projection, @filter, @row_selection_policy,
        @limit, @offset, @metrics, @max_predicate_cache_size,
      ).build
      virtual = @schema.fields.to_a.select { |field| Parquet::Arrow.virtual_column?(field) }
      physical = ::Arrow::Fields.from(@schema.fields.to_a.reject { |field| Parquet::Arrow.virtual_column?(field) })
      levels = Parquet::Arrow.parquet_to_arrow_field_levels_with_virtual(parquet_schema, @projection, physical, virtual)
      projected_schema = ::Arrow::Schema.new(levels.fields)
      ParquetRecordBatchStream.new(@input, decoder, projected_schema)
    end
  end

  # Demand-driven batch stream. No background prefetch task survives the scan;
  # storage errors raise once and fuse both batch and row-group iteration.
  class ParquetRecordBatchStream
    include Iterator(::Arrow::RecordBatch)
    getter schema : ::Arrow::Schema
    @done = false

    def initialize(@input : AsyncFileReader, @decoder : ParquetPushDecoder, @schema : ::Arrow::Schema)
    end

    def next : ::Arrow::RecordBatch | Iterator::Stop
      return stop if @done
      loop do
        result = @decoder.try_decode
        case result.kind
        in .needs_data? then fetch_ranges(result.ranges)
        in .data?       then return result.data
        in .finished?
          @done = true
          return stop
        end
      end
    rescue error : Exception
      @done = true
      raise error
    end

    def next_row_group : ParquetRecordBatchReader?
      return nil if @done
      loop do
        result = @decoder.try_next_reader
        case result.kind
        in .needs_data? then fetch_ranges(result.ranges)
        in .data?       then return result.data
        in .finished?
          @done = true
          return nil
        end
      end
    rescue error : Exception
      @done = true
      raise error
    end

    private def fetch_ranges(ranges : Array(Range(UInt64, UInt64))) : Nil
      data = @input.get_byte_ranges(ranges)
      raise general_err!("Expected #{ranges.size} buffers, got #{data.size}") unless data.size == ranges.size
      ranges.zip(data).each do |range, bytes|
        expected = range.end - range.begin
        raise Parquet::EOFError.new("expected to read #{expected} bytes, got #{bytes.size}") unless expected == bytes.size
      end
      @decoder.push_ranges(ranges, data)
    end
  end
end

require "./async_reader/spawn"
require "./async_reader/store"
