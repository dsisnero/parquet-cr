require "../../spec_helper"
require "../../support/async_reader_helpers"

# parquet/src/arrow/async_writer/{mod,store}.rs, arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b. Lifecycle/error tests characterize
# the pinned implementation's draining and completion order.
private alias WriteA = Parquet::Arrow

private def async_write_batch(rows = 3) : Arrow::RecordBatch
  schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("col", Arrow::DataType.int32, false)]))
  Arrow::RecordBatch.try_new(schema, [Arrow::PrimitiveArray(Int32).from_values((0...rows).to_a).as(Arrow::ArrayBase)])
end

private def async_written_batches(bytes : Bytes) : Array(Arrow::RecordBatch)
  WriteA::ParquetRecordBatchReaderBuilder.try_new(AsyncReaderSpecs.file(bytes)).build.to_a
end

private class RecordingAsyncSink < WriteA::AsyncFileWriter
  getter chunks = [] of Bytes
  getter events = [] of Symbol
  property write_error : Exception? = nil
  property complete_error : Exception? = nil

  def write(bytes : Bytes) : Nil
    Fiber.yield
    @events << :write
    if error = @write_error
      raise error
    end
    @chunks << bytes # Must own bytes even after the encoder reuses its buffer.
  end

  def complete : Nil
    Fiber.yield
    @events << :complete
    if error = @complete_error
      raise error
    end
  end

  def bytes : Bytes
    io = IO::Memory.new
    @chunks.each { |chunk| io.write(chunk) }
    io.to_slice
  end
end

describe WriteA::AsyncArrowWriter do
  it "ports test_async_writer and test_async_writer_bytes_written" do
    batch = async_write_batch
    output = IO::Memory.new
    writer = WriteA::AsyncArrowWriter.try_new(output, batch.schema)
    writer.write(batch)
    writer.finish.file_metadata.num_rows.should eq 3
    writer.bytes_written.should eq output.size
    async_written_batches(output.to_slice).should eq [batch]
  end

  it "ports test_async_writer_with_sync_writer on the full tiny-pages fixture" do
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages_plain.parquet")
    reader = WriteA::ParquetRecordBatchReaderBuilder.try_new(AsyncReaderSpecs.file(bytes)).build
    props = Parquet::File::Properties::WriterProperties.builder.set_max_row_group_row_count(64).build
    output = IO::Memory.new
    sink = RecordingAsyncSink.new
    sync = WriteA::ArrowWriter.try_new(output, reader.schema, props)
    writer = WriteA::AsyncArrowWriter.try_new(sink, reader.schema, props)
    reader.each do |batch|
      sync.write(batch)
      writer.write(batch)
    end
    sync.close
    writer.close
    sink.bytes.should eq output.to_slice
    sink.chunks.size.should be > 1
    writer.bytes_written.should eq output.size
  end

  it "ports in_progress_accounting" do
    batch = async_write_batch(512)
    sink = RecordingAsyncSink.new
    writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema)
    writer.in_progress_size.should eq 0
    writer.in_progress_rows.should eq 0
    writer.bytes_written.should eq 4
    writer.write(batch)
    size, memory = writer.in_progress_size, writer.memory_size
    size.should be > 0
    memory.should be >= size
    writer.in_progress_rows.should eq 512
    sink.chunks.should be_empty
    writer.write(batch)
    writer.in_progress_rows.should eq 1024
    writer.in_progress_size.should be > size
    writer.memory_size.should be > memory
    writer.memory_size.should be >= writer.in_progress_size
    before = writer.bytes_written
    writer.flush
    writer.in_progress_size.should eq 0
    writer.memory_size.should eq 0
    writer.in_progress_rows.should eq 0
    writer.bytes_written.should be > before
    sink.chunks.size.should eq 1
    writer.close
  end

  it "ports test_async_writer_file with large binary values" do
    binary = Arrow::GenericBinaryArray(Int32).from_values([Bytes.new(500_000, 0_u8)] * 3)
    schema = Arrow::Schema.new(Arrow::Fields.from([
      Arrow::Field.new("col", Arrow::DataType.int64, false),
      Arrow::Field.new("col2", Arrow::DataType.binary, false),
    ]))
    batch = Arrow::RecordBatch.try_new(schema, [
      Arrow::PrimitiveArray(Int64).from_values([1_i64, 2_i64, 3_i64]).as(Arrow::ArrayBase),
      binary.as(Arrow::ArrayBase),
    ])
    ::File.tempfile("async-parquet") do |file|
      writer = WriteA::AsyncArrowWriter.try_new(file, schema)
      writer.write(batch)
      writer.close
      file.closed?.should be_true
      async_written_batches(::File.read(file.path).to_slice).should eq [batch]
      writer.bytes_written.should eq ::File.size(file.path)
    end
  end

  it "drains automatically at row-group limits, preserves chunks and finalizes once" do
    batch = async_write_batch
    sink = RecordingAsyncSink.new
    props = Parquet::File::Properties::WriterProperties.builder.set_max_row_group_row_count(3).build
    writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema, props)
    writer.write(batch)
    sink.events.should eq [:write]
    writer.flushed_row_groups.map(&.num_rows).should eq [3]
    first = sink.chunks.first.dup
    writer.write(batch)
    writer.close.row_groups.map(&.num_rows).should eq [3, 3]
    sink.events.should eq [:write, :write, :write, :complete]
    sink.chunks.first.should eq first
    async_written_batches(sink.bytes).flat_map { |b| b.column(0).as(Arrow::PrimitiveArray(Int32)).to_a }.should eq [0, 1, 2, 0, 1, 2]
    expect_raises(Parquet::GeneralError) { writer.write(batch) }
    expect_raises(Parquet::GeneralError) { writer.finish }
    sink.events.count(:complete).should eq 1
  end

  it "passes options and appended metadata through empty and explicit flushes" do
    batch = async_write_batch
    sink = RecordingAsyncSink.new
    options = WriteA::ArrowWriterOptions.new.with_schema_root("custom").with_skip_arrow_metadata(true)
    writer = WriteA::AsyncArrowWriter.try_new_with_options(sink, batch.schema, options)
    writer.flush
    sink.chunks.first.should eq "PAR1".to_slice
    writer.write(batch)
    writer.flush
    writer.write(batch)
    writer.append_key_value_metadata(Parquet::File::Metadata::KeyValue.new("extra", "value"))
    metadata = writer.finish
    metadata.row_groups.map(&.num_rows).should eq [3, 3]
    metadata.file_metadata.schema_descr.root_schema.name.should eq "custom"
    entries = metadata.file_metadata.key_value_metadata || [] of Parquet::File::Metadata::KeyValue
    entries.any? { |entry| entry.key == "extra" && entry.value == "value" }.should be_true
    entries.any? { |entry| entry.key == WriteA::ARROW_SCHEMA_META_KEY }.should be_false
    async_written_batches(sink.bytes).sum(&.num_rows).should eq 6
  end

  it "creates a readable empty file and returns the sink without implicit finalization" do
    batch = async_write_batch
    sink = RecordingAsyncSink.new
    writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema)
    writer.write(batch)
    writer.into_inner.same?(sink).should be_true
    sink.events.should be_empty
    empty = WriteA::AsyncArrowWriter.try_new(sink, batch.schema)
    empty.close.file_metadata.num_rows.should eq 0
    async_written_batches(sink.bytes).should be_empty
  end

  it "waits for the yielding sink before returning from flush" do
    batch = async_write_batch
    entered, release, done = Channel(Bool).new(1), Channel(Bool).new(1), Channel(Bool).new(1)
    sink = GatedAsyncSink.new(entered, release)
    writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema)
    spawn do
      writer.write(batch)
      writer.flush
      done.send(true)
    end
    entered.receive
    select
    when done.receive
      fail "flush returned before the sink accepted its bytes"
    else
      # No completion is available while the sink is waiting.
    end
    release.send(true)
    done.receive.should be_true
  end

  it "wraps write failures and never completes after a failed footer write" do
    batch = async_write_batch
    sink = RecordingAsyncSink.new
    failure = IO::Error.new("write failed")
    sink.write_error = failure
    writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema)
    writer.write(batch)
    error = expect_raises(Parquet::ExternalError, "write failed") { writer.finish }
    error.error.same?(failure).should be_true
    sink.events.should eq [:write]
  end

  it "propagates automatic and explicit row-group write failures before completion" do
    [true, false].each do |automatic|
      batch = async_write_batch
      sink = RecordingAsyncSink.new
      failure = IO::Error.new("row-group failed")
      sink.write_error = failure
      props = Parquet::File::Properties::WriterProperties.builder.set_max_row_group_row_count(automatic ? 3 : 100).build
      writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema, props)
      error = expect_raises(Parquet::ExternalError, "row-group failed") do
        writer.write(batch)
        writer.flush unless automatic
      end
      error.error.same?(failure).should be_true
      sink.events.should eq [:write]
    end
  end

  it "rejects incompatible batches before delivering bytes to the sink" do
    sink = RecordingAsyncSink.new
    writer = WriteA::AsyncArrowWriter.try_new(sink, async_write_batch.schema)
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("col", Arrow::DataType.float64, false)]))
    batch = Arrow::RecordBatch.try_new(schema, [Arrow::PrimitiveArray(Float64).from_values([1.0]).as(Arrow::ArrayBase)])
    expect_raises(Parquet::ArrowError, "Incompatible type") { writer.write(batch) }
    sink.events.should be_empty
  end

  it "propagates completion failures after delivering all file bytes" do
    batch = async_write_batch
    sink = RecordingAsyncSink.new
    failure = IO::Error.new("complete failed")
    sink.complete_error = failure
    writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema)
    writer.write(batch)
    expect_raises(IO::Error, "complete failed") { writer.close }.same?(failure).should be_true
    async_written_batches(sink.bytes).should eq [batch]
    sink.events.should eq [:write, :complete]
  end
end

private class GatedAsyncSink < WriteA::AsyncFileWriter
  def initialize(@entered : Channel(Bool), @release : Channel(Bool))
  end

  def write(bytes : Bytes) : Nil
    @entered.send(true)
    @release.receive
  end

  def complete : Nil
  end
end

describe WriteA::ParquetObjectWriter do
  it "ports store::test_async_writer, publishing only after complete" do
    batch = async_write_batch
    store = WriteA::MemoryObjectStore.new
    buffer = WriteA::BufferedObjectWriter.new(store, "test")
    sink = WriteA::ParquetObjectWriter.from_buf_writer(buffer)
    sink.into_inner.same?(buffer).should be_true
    writer = WriteA::AsyncArrowWriter.try_new(sink, batch.schema)
    writer.write(batch)
    writer.flush
    expect_raises(WriteA::ObjectStoreError) { store.head("test") }
    writer.close
    read = WriteA::ParquetRecordBatchStreamBuilder.new(WriteA::ParquetObjectReader.new(store, "test")).build.to_a
    read.should eq [batch]
    store.head("test").should eq writer.bytes_written
    expect_raises(Parquet::GeneralError) { buffer.write(Bytes[1]) }
  end

  it "accepts the native buffered writer directly and preserves the previous object until close" do
    store = WriteA::MemoryObjectStore.new
    store.put("test", Bytes[1, 2])
    batch = async_write_batch
    writer = WriteA::AsyncArrowWriter.try_new(WriteA::BufferedObjectWriter.new(store, "test"), batch.schema)
    writer.write(batch)
    writer.flush
    store.get_suffix("test", 2).should eq Bytes[1, 2]
    writer.close
    async_written_batches(store.get_suffix("test", store.head("test").to_i32)).should eq [batch]
  end
end
