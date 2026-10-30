# Complete parquet/examples/object_store.rs workflow, apache/arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b). Native storage and buffered-writer
# interfaces replace the object_store crate; calls may suspend Crystal fibers.
module Parquet::Examples::ObjectStoreExample
  alias A = Parquet::Arrow
  alias M = Parquet::File::Metadata

  # Minimal suffix-capable reader: no HEAD request or file size is required.
  class ObjectStoreReader < A::AsyncFileReader
    include A::MetadataSuffixFetch

    def initialize(@store : A::ObjectStore, @path : String)
    end

    def clone : self
      dup
    end

    def get_bytes(range : Range(UInt64, UInt64)) : Bytes
      @store.get_range(@path, range)
    rescue error : Exception
      raise Parquet::ExternalError.new(error)
    end

    def get_byte_ranges(ranges : Array(Range(UInt64, UInt64))) : Array(Bytes)
      @store.get_ranges(@path, ranges)
    rescue error : Exception
      raise Parquet::ExternalError.new(error)
    end

    def fetch_suffix(length : Int32) : Bytes
      @store.get_suffix(@path, length)
    rescue error : Exception
      raise Parquet::ExternalError.new(error)
    end

    def get_metadata(options : A::ArrowReaderOptions? = nil) : M::ParquetMetaData
      M::ParquetMetaDataReader.new.with_arrow_reader_options(options).load_via_suffix_and_finish(self)
    end
  end

  # The exact non-null Int64 column used by upstream's two equality assertions.
  def self.batch : ::Arrow::RecordBatch
    column = ::Arrow::PrimitiveArray(Int64).from_values([1_i64, 2_i64, 3_i64])
    schema = ::Arrow::Schema.new(::Arrow::Fields.from([::Arrow::Field.new("col", column.data_type, false)]))
    ::Arrow::RecordBatch.try_new(schema, [column] of ::Arrow::ArrayBase)
  end

  # Publish once, then validate normal and dedicated-context reads against the
  # complete original batch. The one-worker context is managed by Crystal.
  def self.run(output : IO = STDOUT, store : A::ObjectStore = A::MemoryObjectStore.new) : Nil
    expected = batch
    path = "example.parquet"
    sink = A::BufferedObjectWriter.new(store, path)
    writer = A::AsyncArrowWriter.try_new(sink, expected.schema)
    writer.write(expected)
    writer.close

    reader = ObjectStoreReader.new(store, path)
    read = A::ParquetRecordBatchStreamBuilder.new(reader).build.to_a
    raise general_err!("Object-store example batch mismatch") unless read == [expected]
    output.puts "read #{read[0].num_rows} rows"

    context = Fiber::ExecutionContext::Parallel.new("parquet-example-io", maximum: 1)
    spawned = A::SpawnedReader.new(ObjectStoreReader.new(store, path), context)
    read = A::ParquetRecordBatchStreamBuilder.new(spawned).build.to_a
    raise general_err!("Object-store example spawned batch mismatch") unless read == [expected]
    output.puts "read #{read[0].num_rows} rows via dedicated I/O runtime"
  end
end
