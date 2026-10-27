require "../../spec_helper"
require "../../support/async_reader_helpers"
require "http/server"

private alias UploadA = Parquet::Arrow

# Characterizes the native single-PUT storage mapping; no external service.
private def with_upload_server(status = 201, &)
  objects = {} of String => Bytes
  requests = [] of Tuple(String, String, String?)
  server = HTTP::Server.new do |context|
    request, response = context.request, context.response
    requests << {request.method, request.resource, request.headers["Authorization"]?}
    path = request.path
    if request.method == "PUT"
      data = request.body.try(&.gets_to_end).to_s.to_slice.dup
      if status < 300
        objects[path] = data
      end
      response.status_code = status
    elsif data = objects[path]?
      if request.method == "HEAD"
        response.headers["Content-Length"] = data.size.to_s
      else
        range = request.headers["Range"]
        if match = /\Abytes=-(\d+)\z/.match(range)
          start = Math.max(0, data.size - match[1].to_i)
          ending = data.size
        elsif match = /\Abytes=(\d+)-(\d+)\z/.match(range)
          start, ending = match[1].to_i, match[2].to_i + 1
        else
          raise "Unexpected range #{range}"
        end
        response.status_code = 206
        response.headers["Content-Range"] = "bytes #{start}-#{ending - 1}/#{data.size}"
        response.write(data[start, ending - start])
      end
    else
      response.status_code = 404
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
    yield "http://127.0.0.1:#{address.port}/objects/?token=test", objects, requests
  ensure
    server.close
    done.receive
  end
end

describe "Asynchronous object-store writes" do
  it "uploads finalized Parquet via HTTP PUT and reads it back through ranged HTTP" do
    [200, 201, 204].each do |status|
      with_upload_server(status) do |url, objects, requests|
        store = UploadA::HttpObjectStore.new(url, HTTP::Headers{"Authorization" => "Bearer example"})
        input = UploadA::ParquetRecordBatchReaderBuilder.try_new(AsyncReaderSpecs.file(AsyncReaderSpecs.fixture)).build
        sink = UploadA::ParquetObjectWriter.new(store, "space name.parquet")
        writer = UploadA::AsyncArrowWriter.try_new(sink, input.schema)
        input.each { |batch| writer.write(batch) }
        writer.flush
        objects.should be_empty
        writer.close
        objects.size.should eq 1
        requests.count { |entry| entry[0] == "PUT" }.should eq 1
        request = requests.first
        request.should eq({"PUT", "/objects/space%20name.parquet?token=test", "Bearer example"})
        output = UploadA::ParquetRecordBatchStreamBuilder.new(UploadA::ParquetObjectReader.new(store, "space name.parquet")).build.to_a
        AsyncReaderSpecs.ids(output).should eq (0...12).to_a
        store.head("space name.parquet").should eq writer.bytes_written
      end
    end
  end

  it "propagates rejected PUTs without publishing a partial object or swallowing the key" do
    [403, 500, 307].each do |status|
      with_upload_server(status) do |url, objects, _|
        store = UploadA::HttpObjectStore.new(url)
        input = UploadA::ParquetRecordBatchReaderBuilder.try_new(AsyncReaderSpecs.file(AsyncReaderSpecs.fixture)).build
        writer = UploadA::AsyncArrowWriter.try_new(UploadA::ParquetObjectWriter.new(store, "failed.parquet"), input.schema)
        input.each { |batch| writer.write(batch) }
        error = expect_raises(Parquet::ExternalError, "failed.parquet: HTTP #{status}") { writer.close }
        error.error.should be_a(UploadA::ObjectStoreError)
        objects.should be_empty
      end
    end
  end

  it "retains buffered bytes on failed publication and rejects writes after completion" do
    store = RetryUploadStore.new
    store.put("test", Bytes[9])
    store.fail = true
    buffer = UploadA::BufferedObjectWriter.new(store, "test")
    buffer.write(Bytes[1, 2])
    expect_raises(IO::Error, "upload failed") { buffer.complete }
    store.get_suffix("test", 8).should eq Bytes[9]
    store.fail = false
    buffer.complete
    store.get_suffix("test", 8).should eq Bytes[1, 2]
    expect_raises(Parquet::GeneralError) { buffer.complete }
    expect_raises(Parquet::GeneralError) { buffer.write(Bytes[3]) }
  end

  it "wraps native IO failures and optionally leaves a borrowed destination open" do
    output = IO::Memory.new
    sink = UploadA::AsyncIOWriter.new(output, close_on_complete: false)
    sink.write(Bytes[1, 2])
    sink.complete
    output.closed?.should be_false
    output.to_slice.should eq Bytes[1, 2]
    output.close
    expect_raises(Parquet::ExternalError) { sink.write(Bytes[3]) }
  end
end

private class RetryUploadStore < UploadA::MemoryObjectStore
  property? fail = false

  def put(path : String, bytes : Bytes) : Nil
    raise IO::Error.new("upload failed") if @fail
    super
  end
end
