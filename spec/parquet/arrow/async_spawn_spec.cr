require "../../spec_helper"
require "../../support/async_reader_helpers"

alias SpawnA = Parquet::Arrow

private class ContextReader < AsyncReaderSpecs::TestReader
  getter contexts = [] of Fiber::ExecutionContext

  def get_bytes(range : Range(UInt64, UInt64)) : Bytes
    @contexts << Fiber.current.execution_context
    super
  end

  def get_metadata(options : SpawnA::ArrowReaderOptions? = nil) : Parquet::File::Metadata::ParquetMetaData
    @contexts << Fiber.current.execution_context
    super
  end
end

describe SpawnA::SpawnedReader do
  it "ports test_spawned_reader using an explicit Crystal execution context" do
    context = Fiber::ExecutionContext::Concurrent.new("parquet-spec-io")
    source = ContextReader.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet"))
    reader = SpawnA::SpawnedReader.new(source, context)
    batches = SpawnA::ParquetRecordBatchStreamBuilder.new(reader).build.to_a
    batches.sum(&.num_rows).should eq 8
    source.contexts.should_not be_empty
    source.contexts.all? { |observed| observed.same?(context) }.should be_true
    reader.into_inner.should be source
  end

  it "propagates the same storage exception across contexts" do
    context = Fiber::ExecutionContext::Concurrent.new("parquet-error-io")
    source = ContextReader.new(AsyncReaderSpecs.fixture)
    error = IO::Error.new("async task error")
    source.failure = error
    reader = SpawnA::SpawnedReader.new(source, context)
    observed = expect_raises(IO::Error, "async task error") { reader.get_bytes(0_u64...4_u64) }
    observed.should be error
  end

  it "adapts seekable IO with strict short reads and suffix metadata" do
    bytes = AsyncReaderSpecs.fixture
    input = IO::Memory.new(bytes)
    reader = SpawnA::AsyncSeekableReader.new(input)
    String.new(reader.get_bytes(0_u64...4_u64)).should eq "PAR1"
    reader.fetch_suffix(4).should eq bytes[bytes.size - 4, 4]
    reader.get_metadata.file_metadata.num_rows.should eq 12
    expect_raises(Parquet::EOFError, "expected to read") { reader.get_bytes((bytes.size - 1).to_u64...(bytes.size + 3).to_u64) }
    input.closed?.should be_false
  end

  it "serializes shared seekable IO while independent scans run concurrently" do
    bytes = AsyncReaderSpecs.fixture
    source = SpawnA::AsyncSeekableReader.new(IO::Memory.new(bytes))
    context = Fiber::ExecutionContext::Parallel.new("parquet-parallel-io", maximum: 2)
    reader = SpawnA::SpawnedReader.new(source, context)
    results = Channel(Array(Int32?) | Exception).new(2)
    2.times do
      spawn do
        stream = SpawnA::ParquetRecordBatchStreamBuilder.new(reader).build
        results.send(AsyncReaderSpecs.ids(stream.to_a))
      rescue error
        results.send(error)
      end
    end
    2.times do
      value = results.receive
      raise value if value.is_a?(Exception)
      value.should eq (0...12).to_a
    end
  end
end
