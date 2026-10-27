# parquet/src/arrow/async_writer/store.rs, apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b. The native buffered writer uses one
# full-object PUT. Provider multipart buffering/retries belong to custom sinks.
module Parquet::Arrow
  # Publishes the complete object at completion, retaining the previous object
  # until then. This adapter buffers the object in memory (Int32-sized buffers).
  class BufferedObjectWriter < AsyncFileWriter
    @buffer = IO::Memory.new
    @completed = false

    def initialize(@store : ObjectStore, @path : String)
    end

    def write(bytes : Bytes) : Nil
      raise GeneralError.new("Object writer already completed") if @completed
      @buffer.write(bytes)
    end

    def complete : Nil
      raise GeneralError.new("Object writer already completed") if @completed
      @store.put(@path, @buffer.to_slice)
      @completed = true
      @buffer = IO::Memory.new
      nil
    end
  end

  # Compatibility facade for upstream's deprecated ParquetObjectWriter. Prefer
  # passing BufferedObjectWriter (or an application SDK sink) directly.
  class ParquetObjectWriter < AsyncFileWriter
    def self.new(store : ObjectStore, path : String) : self
      new(BufferedObjectWriter.new(store, path))
    end

    def initialize(@writer : BufferedObjectWriter)
    end

    def self.from_buf_writer(writer : BufferedObjectWriter) : self
      new(writer)
    end

    def self.from(writer : BufferedObjectWriter) : self
      from_buf_writer(writer)
    end

    def into_inner : BufferedObjectWriter
      @writer
    end

    def write(bytes : Bytes) : Nil
      @writer.write(bytes)
    rescue error : Exception
      raise ExternalError.new(error)
    end

    def complete : Nil
      @writer.complete
    rescue error : Exception
      raise ExternalError.new(error)
    end
  end
end
