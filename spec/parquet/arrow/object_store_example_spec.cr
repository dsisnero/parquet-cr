require "../../spec_helper"
require "../../support/async_reader_helpers"

# Tests-first port of both object_store.rs main assertions plus adapter
# characterizations, apache/arrow-rs 60.0.0 ef1fa157. No upstream unit tests.
private alias OSExample = Parquet::Examples::ObjectStoreExample
private alias OSA = Parquet::Arrow
private alias OSM = Parquet::File::Metadata

# Calls are serialized by the workflow/SpawnedReader; observations are read
# after its reply channel has synchronized with the caller.
private class ExampleStore < OSA::MemoryObjectStore
  getter contexts = [] of Fiber::ExecutionContext
  getter threads = [] of UInt64
  getter suffixes = [] of Int32
  getter ranges = [] of Range(UInt64, UInt64)
  getter multi_ranges = [] of Array(Range(UInt64, UInt64))
  getter heads = 0
  property failure : Exception? = nil

  def head(path : String) : UInt64
    @heads += 1
    super
  end

  private def observe : Nil
    @contexts << Fiber.current.execution_context
    @threads << Thread.current.object_id
    if error = @failure
      raise error
    end
  end

  def get_range(path : String, range : Range(UInt64, UInt64)) : Bytes
    observe
    @ranges << range
    super
  end

  def get_ranges(path : String, ranges : Array(Range(UInt64, UInt64))) : Array(Bytes)
    @multi_ranges << ranges.dup
    super
  end

  def get_suffix(path : String, length : Int32) : Bytes
    observe
    @suffixes << length
    super
  end
end

describe "Object-store example" do
  it "ports both complete batch equality assertions and dedicated IO context" do
    store = ExampleStore.new
    output = IO::Memory.new
    caller_context = Fiber.current.execution_context
    caller_thread = Thread.current.object_id
    OSExample.run(output, store)
    output.to_s.should eq "read 3 rows\nread 3 rows via dedicated I/O runtime\n"
    store.heads.should eq 0
    store.contexts.any? { |context| context.same?(caller_context) }.should be_true
    store.contexts.any? { |context| !context.same?(caller_context) }.should be_true
    store.threads.any? { |thread| thread != caller_thread }.should be_true
    reader = OSExample::ObjectStoreReader.new(store, "example.parquet")
    batches = OSA::ParquetRecordBatchStreamBuilder.new(reader).build.to_a
    batches.should eq [OSExample.batch]
    batches.first.column(0).should eq Arrow::PrimitiveArray(Int64).from_values([1_i64, 2_i64, 3_i64])
  end

  it "delegates ordered multi-range reads, bounded suffixes and cloned readers to the shared store" do
    store = ExampleStore.new
    store.put("data", Bytes[1, 2, 3, 4])
    reader = OSExample::ObjectStoreReader.new(store, "data")
    ranges = [2_u64...4_u64, 0_u64...1_u64]
    reader.get_byte_ranges(ranges).should eq [Bytes[3, 4], Bytes[1]]
    store.multi_ranges.should eq [ranges]
    reader.fetch_suffix(2).should eq Bytes[3, 4]
    reader.fetch_suffix(20).should eq Bytes[1, 2, 3, 4]
    reader.fetch_suffix(0).should eq Bytes.empty
    clone = reader.clone
    store.put("data", Bytes[9, 8])
    clone.get_bytes(0_u64...2_u64).should eq Bytes[9, 8]
    reader.get_bytes(0_u64...2_u64).should eq Bytes[9, 8]
    store.heads.should eq 0
  end

  it "loads footer metadata via suffixes and respects offset-only index policy" do
    store = ExampleStore.new
    store.put("indexed", ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages.parquet"))
    reader = OSExample::ObjectStoreReader.new(store, "indexed")
    plain = reader.get_metadata
    plain.page_index.should be_nil
    options = OSA::ArrowReaderOptions.new.with_offset_index_policy(OSM::PageIndexPolicy::Required)
    metadata = reader.get_metadata(options)
    metadata.num_rows.should eq plain.num_rows
    index = metadata.page_index || raise "missing index"
    index.offset_indexes?.should be_true
    index.column_indexes?.should be_false
    store.suffixes.should_not be_empty
    store.heads.should eq 0
  end

  it "converts storage errors to External while retaining the original cause" do
    store = ExampleStore.new
    failure = IO::Error.new("object transport failed")
    store.failure = failure
    reader = OSExample::ObjectStoreReader.new(store, "missing")
    [-> { reader.get_bytes(0_u64...1_u64); nil },
     -> { reader.get_byte_ranges([0_u64...1_u64]); nil },
     -> { reader.fetch_suffix(8); nil },
     -> { reader.get_metadata; nil }].each do |operation|
      error = expect_raises(Parquet::ExternalError, "object transport failed") { operation.call }
      error.error.should be failure
    end
  end

  it "propagates metadata parse errors without treating them as storage errors" do
    store = ExampleStore.new
    store.put("invalid", Bytes.new(8))
    reader = OSExample::ObjectStoreReader.new(store, "invalid")
    error = expect_raises(Parquet::ParquetError) { reader.get_metadata }
    error.should_not be_a Parquet::ExternalError
  end

  it "publishes only on writer completion before either reader runs" do
    store = OSA::MemoryObjectStore.new
    writer = OSA::AsyncArrowWriter.try_new(OSA::BufferedObjectWriter.new(store, "pending"), OSExample.batch.schema)
    writer.write(OSExample.batch)
    expect_raises(OSA::ObjectStoreError) { store.head("pending") }
    writer.close
    OSA::ParquetRecordBatchStreamBuilder.new(OSExample::ObjectStoreReader.new(store, "pending")).build.to_a.should eq [OSExample.batch]
  end
end
