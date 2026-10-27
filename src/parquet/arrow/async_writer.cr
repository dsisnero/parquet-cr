require "./arrow_writer"
require "./async_reader/store"

# parquet/src/arrow/async_writer/mod.rs, apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b. Futures map to yielding calls;
# each writer must be owned by one fiber. See docs/async-storage.md.
module Parquet::Arrow
  abstract class AsyncFileWriter
    # The bytes are owned by the sink and may be retained after this returns.
    abstract def write(bytes : Bytes) : Nil

    # Flush buffered bytes and finalize the destination.
    abstract def complete : Nil
  end

  # Native counterpart of the AsyncWrite blanket implementation. IO behavior
  # determines whether writes yield. By default completion flushes and closes IO;
  # use close_on_complete: false for a borrowed destination.
  class AsyncIOWriter < AsyncFileWriter
    getter inner : IO

    def initialize(@inner : IO, @close_on_complete : Bool = true)
    end

    def write(bytes : Bytes) : Nil
      @inner.write(bytes)
    rescue error : IO::Error
      raise Parquet.from_io_error(error)
    end

    def complete : Nil
      @inner.flush
      @inner.close if @close_on_complete
      nil
    rescue error : IO::Error
      raise Parquet.from_io_error(error)
    end
  end

  # Encodes through ArrowWriter and transfers completed row groups to the sink.
  # Calls wait for the sink, providing backpressure without a background queue.
  class AsyncArrowWriter
    @sync_writer : ArrowWriter
    @async_writer : AsyncFileWriter
    @buffer : IO::Memory

    def self.try_new(writer : AsyncFileWriter | IO, schema : ::Arrow::Schema,
                     properties : File::Properties::WriterProperties? = nil) : self
      try_new_with_options(writer, schema, ArrowWriterOptions.new.with_properties(properties || File::Properties::WriterProperties.builder.build))
    end

    def self.try_new_with_options(writer : AsyncFileWriter | IO, schema : ::Arrow::Schema,
                                  options : ArrowWriterOptions) : self
      new(writer, schema, options)
    end

    def initialize(writer : AsyncFileWriter | IO, schema : ::Arrow::Schema, options : ArrowWriterOptions)
      @async_writer = writer.is_a?(IO) ? AsyncIOWriter.new(writer) : writer
      @buffer = IO::Memory.new
      @sync_writer = ArrowWriter.try_new_with_options(@buffer, schema, options)
    end

    delegate flushed_row_groups, memory_size, in_progress_size, in_progress_rows,
      bytes_written, append_key_value_metadata, to: @sync_writer

    def write(batch : ::Arrow::RecordBatch) : Nil
      before = @sync_writer.flushed_row_groups.size
      @sync_writer.write(batch)
      do_write if before != @sync_writer.flushed_row_groups.size
    end

    def flush : Nil
      @sync_writer.flush
      do_write
    end

    def finish : File::Metadata::ParquetMetaData
      metadata = @sync_writer.finish
      do_write
      @async_writer.complete
      metadata
    end

    def close : File::Metadata::ParquetMetaData
      finish
    end

    # No flush or finalization. Crystal returns the shared sink reference.
    def into_inner : AsyncFileWriter
      @async_writer
    end

    private def do_write : Nil
      # Detach before yielding: the sink can retain bytes without aliasing the
      # encoder's next chunk. TrackedWrite retains cumulative absolute offsets.
      bytes = @buffer.to_slice.dup
      @buffer.clear
      @async_writer.write(bytes)
    rescue error : Exception
      raise ExternalError.new(error)
    end
  end
end

require "./async_writer/store"
