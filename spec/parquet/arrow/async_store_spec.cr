require "../../spec_helper"
require "../../support/async_reader_helpers"
require "http/server"

alias StoreA = Parquet::Arrow
alias StoreM = Parquet::File::Metadata

private class RecordingObjectStore < StoreA::MemoryObjectStore
  getter ranges = [] of Range(UInt64, UInt64)
  getter suffixes = [] of Int32
  getter heads = 0
  getter contexts = [] of Fiber::ExecutionContext

  def get_range(path : String, range : Range(UInt64, UInt64)) : Bytes
    @contexts << Fiber.current.execution_context
    @ranges << range
    super
  end

  def get_suffix(path : String, length : Int32) : Bytes
    @contexts << Fiber.current.execution_context
    @suffixes << length
    super
  end

  def head(path : String) : UInt64
    @heads += 1
    super
  end
end

private def with_range_server(bytes : Bytes, mode = :valid, &)
  requests = [] of String
  server = HTTP::Server.new do |context|
    requests << "#{context.request.method} #{context.request.path} #{context.request.headers["Range"]?}"
    response = context.response
    if context.request.method == "HEAD"
      response.status_code = 200
      response.headers["Content-Length"] = bytes.size.to_s
    elsif context.request.path.ends_with?("missing")
      response.status_code = 404
    elsif mode == :ignore_range
      response.print("not a range response")
    else
      range = context.request.headers["Range"]? || ""
      if match = /\Abytes=(\d+)-(\d+)\z/.match(range)
        start = match[1].to_i
        finish = match[2].to_i + 1
      elsif match = /\Abytes=-(\d+)\z/.match(range)
        start = Math.max(0, bytes.size - match[1].to_i)
        finish = bytes.size
      else
        start = 0
        finish = bytes.size
      end
      if start >= bytes.size || finish > bytes.size
        response.status_code = 416
      else
        response.status_code = 206
        response.headers["Content-Range"] = "bytes #{start}-#{finish - 1}/#{bytes.size}"
        response.headers["Content-Range"] = "bytes 0-0/#{bytes.size}" if mode == :wrong_range
        data = bytes[start, finish - start]
        data = data[0, data.size - 1] if mode == :short_body
        response.write(data)
      end
    end
  end
  address = server.bind_tcp("127.0.0.1", 0)
  done = Channel(Bool).new(1)
  spawn do
    server.listen
  ensure
    done.send(true)
  end
  begin
    yield "http://127.0.0.1:#{address.port}/objects/", requests
  ensure
    server.close
    done.receive
  end
end

describe StoreA::ParquetObjectReader do
  it "ports test_simple and test_simple_without_file_length without HEAD" do
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
    [false, true].each do |known|
      store = RecordingObjectStore.new
      store.put("file.parquet", bytes)
      reader = StoreA::ParquetObjectReader.new(store, "file.parquet")
      reader.with_file_size(bytes.size.to_u64) if known
      stream = StoreA::ParquetRecordBatchStreamBuilder.new(reader).build
      batches = stream.to_a
      batches.size.should eq 1
      batches.first.num_rows.should eq 8
      expected = StoreA::ParquetRecordBatchReaderBuilder.try_new(AsyncReaderSpecs.file(bytes)).build.to_a
      batches.should eq expected
      store.heads.should eq 0
      known ? store.suffixes.should(be_empty) : store.suffixes.size.should(eq 2)
    end
  end

  it "ports test_not_found with the object path in its error" do
    store = StoreA::MemoryObjectStore.new
    expect_raises(StoreA::ObjectStoreError, "I don't exist.parquet not found:") do
      StoreA::ParquetRecordBatchStreamBuilder.new(StoreA::ParquetObjectReader.new(store, "I don't exist.parquet"))
    end
  end

  it "uses the footer hint to avoid redundant metadata and index IO" do
    bytes = AsyncReaderSpecs.fixture
    store = RecordingObjectStore.new
    store.put("fixture", bytes)
    reader = StoreA::ParquetObjectReader.new(store, "fixture").with_file_size(bytes.size.to_u64).with_footer_size_hint(bytes.size)
      .with_preload_column_index(true).with_preload_offset_index(true)
    reader.get_metadata.page_index.try(&.complete?).should eq true
    store.ranges.size.should eq 1
    store.suffixes.should be_empty
  end

  it "ports page-index skip/preload and explicit policy precedence" do
    bytes = AsyncReaderSpecs.fixture
    store = StoreA::MemoryObjectStore.new
    store.put("fixture", bytes)
    reader = StoreA::ParquetObjectReader.new(store, "fixture").with_preload_column_index(true).with_preload_offset_index(true)
    reader.get_metadata(StoreA::ArrowReaderOptions.new).page_index.try(&.complete?).should eq true
    reader = StoreA::ParquetObjectReader.new(store, "fixture")
    reader.get_metadata(StoreA::ArrowReaderOptions.new.with_page_index_policy(StoreM::PageIndexPolicy::Optional)).page_index.try(&.complete?).should eq true
    options = StoreA::ArrowReaderOptions.new.with_offset_index_policy(StoreM::PageIndexPolicy::Optional)
    metadata = StoreA::ParquetObjectReader.new(store, "fixture").with_preload_column_index(true).get_metadata(options)
    metadata.page_index.try(&.column_indexes?).should eq false
    metadata.page_index.try(&.offset_indexes?).should eq true
  end

  it "ports the deprecated runtime option by dispatching all storage IO onto its context" do
    store = RecordingObjectStore.new
    store.put("fixture", AsyncReaderSpecs.fixture)
    context = Fiber::ExecutionContext::Concurrent.new("object-store-io")
    reader = StoreA::ParquetObjectReader.new(store, "fixture").with_runtime(context)
    stream = StoreA::ParquetRecordBatchStreamBuilder.new(reader).build
    AsyncReaderSpecs.ids(stream.to_a).should eq (0...12).to_a
    store.contexts.should_not be_empty
    store.contexts.all? { |observed| observed.same?(context) }.should be_true
  end

  it "preserves ordered multi-range results and validates file bounds" do
    store = StoreA::MemoryObjectStore.new
    store.put("small", "abcdef".to_slice)
    reader = StoreA::ParquetObjectReader.new(store, "small")
    reader.get_byte_ranges([4_u64...6_u64, 0_u64...2_u64]).map { |bytes| String.new(bytes) }.should eq ["ef", "ab"]
    String.new(reader.fetch_suffix(100)).should eq "abcdef"
    store.head("small").should eq 6
    expect_raises(Parquet::ParquetError) { reader.get_bytes(0_u64...7_u64) }
    expect_raises(Parquet::ParquetError) { reader.get_bytes(5_u64...2_u64) }
  end

  it "performs actual HTTP suffix and bounded range reads, yielding to other fibers" do
    bytes = AsyncReaderSpecs.fixture
    with_range_server(bytes) do |url, requests|
      store = StoreA::HttpObjectStore.new(url)
      reader = StoreA::ParquetObjectReader.new(store, "test file.parquet")
      stream = StoreA::ParquetRecordBatchStreamBuilder.new(reader).with_projection(StoreA::ProjectionMask.leaves(AsyncReaderSpecs.file(bytes).metadata.file_metadata.schema_descr, [0])).with_limit(3).build
      AsyncReaderSpecs.ids(stream.to_a).should eq [0, 1, 2]
      requests.first.should contain "bytes=-8"
      requests.any?(&.includes?("test%20file.parquet")).should be_true
      requests.none?(&.starts_with?("HEAD")).should be_true
      requests.all?(&.includes?("bytes=")).should be_true
      requests.any?(&.includes?("bytes=0-#{bytes.size - 1}")).should be_false
    end
  end

  it "supports explicit HTTP sizing, clamped suffixes and empty ranges" do
    bytes = AsyncReaderSpecs.fixture
    with_range_server(bytes) do |url, requests|
      store = StoreA::HttpObjectStore.new(url)
      store.head("fixture").should eq bytes.size
      requests.size.should eq 1
      store.get_range("fixture", 4_u64...4_u64).should be_empty
      store.get_suffix("fixture", 0).should be_empty
      requests.size.should eq 1
      store.get_suffix("fixture", bytes.size * 2).should eq bytes
      requests.size.should eq 2
    end
  end

  it "rejects ignored ranges, incorrect Content-Range and truncated HTTP bodies" do
    [:ignore_range, :wrong_range, :short_body].each do |mode|
      with_range_server(AsyncReaderSpecs.fixture, mode) do |url, _requests|
        expect_raises(Parquet::ParquetError) do
          StoreA::ParquetRecordBatchStreamBuilder.new(StoreA::ParquetObjectReader.new(StoreA::HttpObjectStore.new(url), "fixture"))
        end
      end
    end
  end

  it "surfaces HTTP missing objects with the key in the error" do
    with_range_server(AsyncReaderSpecs.fixture) do |url, _requests|
      expect_raises(StoreA::ObjectStoreError, "missing") do
        StoreA::ParquetRecordBatchStreamBuilder.new(StoreA::ParquetObjectReader.new(StoreA::HttpObjectStore.new(url), "missing"))
      end
    end
  end
end
