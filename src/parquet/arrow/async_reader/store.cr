require "http/client"
require "uri"
require "sync/mutex"

# parquet/src/arrow/async_reader/store.rs, apache/arrow-rs 60.0.0.
# Rust object_store's storage-independent contract maps to a small native
# interface. SDK-specific signing/listing/uploading is outside this read API.
module Parquet::Arrow
  # Storage errors retain the object key and original transport cause.
  class ObjectStoreError < ParquetError
  end

  # Range/suffix object access. IO methods can yield; callers may implement this
  # interface with a cloud SDK without making that SDK a parquet dependency.
  abstract class ObjectStore
    abstract def get_range(path : String, range : Range(UInt64, UInt64)) : Bytes
    abstract def get_suffix(path : String, length : Int32) : Bytes
    abstract def head(path : String) : UInt64

    def get_ranges(path : String, ranges : Array(Range(UInt64, UInt64))) : Array(Bytes)
      ranges.map { |range| get_range(path, range) }
    end
  end

  # Owned immutable object snapshots, suitable for deterministic embedded scans.
  class MemoryObjectStore < ObjectStore
    @objects = {} of String => Bytes
    @mutex = Sync::Mutex.new

    def put(path : String, bytes : Bytes) : Nil
      @mutex.synchronize { @objects[path] = bytes.dup }
    end

    def head(path : String) : UInt64
      object(path).size.to_u64
    end

    def get_range(path : String, range : Range(UInt64, UInt64)) : Bytes
      bytes = object(path)
      if range.end < range.begin || range.end > bytes.size
        raise ObjectStoreError.new("#{path}: invalid byte range #{range}")
      end
      bytes[range.begin.to_i32, (range.end - range.begin).to_i32].dup
    end

    def get_suffix(path : String, length : Int32) : Bytes
      raise ArgumentError.new("suffix must be nonnegative") if length < 0
      bytes = object(path)
      count = Math.min(length, bytes.size)
      bytes[bytes.size - count, count].dup
    end

    private def object(path : String) : Bytes
      @mutex.synchronize { @objects[path]? || raise ObjectStoreError.new("#{path} not found: object is missing") }
    end
  end

  # HTTP(S) range transport for object endpoints. It requires 206 responses and
  # validates Content-Range before reading a bounded body; it never falls back
  # to downloading the whole object when a server ignores Range. Headers and
  # base URL query parameters support externally supplied authentication.
  class HttpObjectStore < ObjectStore
    @base : URI
    @headers : HTTP::Headers

    def initialize(base_url : String, headers : HTTP::Headers = HTTP::Headers.new)
      @base = URI.parse(base_url)
      unless %w[http https].includes?(@base.scheme) && @base.host
        raise ArgumentError.new("Object endpoint must be an absolute HTTP(S) URL")
      end
      @headers = headers.dup
    end

    def head(path : String) : UInt64
      HTTP::Client.exec("HEAD", object_uri(path), @headers) do |response|
        unless response.status_code == 200
          raise ObjectStoreError.new("#{path}: HTTP #{response.status_code}")
        end
        size = response.headers["Content-Length"]?.try(&.to_u64?)
        size || raise ObjectStoreError.new("#{path}: missing Content-Length")
      end
    rescue error : IO::Error
      raise ObjectStoreError.new("#{path}: #{error.message}", cause: error)
    end

    def get_range(path : String, range : Range(UInt64, UInt64)) : Bytes
      raise ObjectStoreError.new("#{path}: invalid byte range #{range}") if range.end < range.begin
      return Bytes.empty if range.end == range.begin
      request_range(path, "bytes=#{range.begin}-#{range.end - 1}", range, nil)
    end

    def get_suffix(path : String, length : Int32) : Bytes
      raise ArgumentError.new("suffix must be nonnegative") if length < 0
      return Bytes.empty if length == 0
      request_range(path, "bytes=-#{length}", nil, length)
    end

    private def object_uri(path : String) : URI
      URI.new(scheme: @base.scheme, host: @base.host, port: @base.port,
        path: @base.path.rstrip('/') + "/" + URI.encode_path(path.lstrip('/')),
        query: @base.query, user: @base.user, password: @base.password)
    end

    private def request_range(path : String, header : String, requested : Range(UInt64, UInt64)?, suffix : Int32?) : Bytes
      headers = @headers.dup
      headers["Range"] = header
      headers["Accept-Encoding"] = "identity"
      HTTP::Client.exec("GET", object_uri(path), headers) do |response|
        unless response.status_code == 206
          raise ObjectStoreError.new("#{path}: expected HTTP 206 for #{header}, got #{response.status_code}")
        end
        content_range = response.headers["Content-Range"]?
        match = content_range.try { |value| /\Abytes (\d+)-(\d+)\/(\d+)\z/.match(value) }
        raise ObjectStoreError.new("#{path}: invalid Content-Range") unless match
        start, last, total = match[1].to_u64, match[2].to_u64, match[3].to_u64
        if last < start || last >= total
          raise ObjectStoreError.new("#{path}: invalid Content-Range #{content_range}")
        end
        ending = last + 1
        expected_start, expected_end = if requested
                                         {requested.begin, requested.end}
                                       else
                                         count = Math.min((suffix || 0).to_u64, total)
                                         {total - count, total}
                                       end
        unless start == expected_start && ending == expected_end
          raise ObjectStoreError.new("#{path}: Content-Range #{content_range} does not match #{header}")
        end
        bytes = Bytes.new((ending - start).to_i32)
        count = response.body_io.read_greedy(bytes)
        if count != bytes.size || response.body_io.read_byte
          raise ObjectStoreError.new("#{path}: short or oversized range response (expected #{bytes.size}, got #{count})")
        end
        bytes
      end
    rescue error : IO::Error
      raise ObjectStoreError.new("#{path}: #{error.message}", cause: error)
    end
  end

  # Object-store AsyncFileReader, including the pinned deprecated adapter's
  # footer hint, known-size, page-index preload and runtime options.
  class ParquetObjectReader < AsyncFileReader
    include MetadataSuffixFetch
    @file_size : UInt64? = nil
    @metadata_size_hint : Int32? = nil
    @preload_column_index = false
    @preload_offset_index = false
    @context : Fiber::ExecutionContext? = nil

    def initialize(@store : ObjectStore, @path : String)
    end

    def with_file_size(@file_size : UInt64) : self
      self
    end

    def with_footer_size_hint(@metadata_size_hint : Int32) : self
      self
    end

    def with_preload_column_index(@preload_column_index : Bool) : self
      self
    end

    def with_preload_offset_index(@preload_offset_index : Bool) : self
      self
    end

    def with_runtime(@context : Fiber::ExecutionContext) : self
      self
    end

    def get_bytes(range : Range(UInt64, UInt64)) : Bytes
      dispatch { @store.get_range(@path, range) }
    end

    def get_byte_ranges(ranges : Array(Range(UInt64, UInt64))) : Array(Bytes)
      dispatch { @store.get_ranges(@path, ranges) }
    end

    def fetch_suffix(length : Int32) : Bytes
      dispatch { @store.get_suffix(@path, length) }
    end

    def get_metadata(options : ArrowReaderOptions? = nil) : File::Metadata::ParquetMetaData
      reader = File::Metadata::ParquetMetaDataReader.new
        .with_metadata_options(options.try(&.metadata_options))
        .with_file_decryption_properties(options.try(&.file_decryption_properties))
        .with_column_index_policy(File::Metadata::PageIndexPolicy.from(@preload_column_index))
        .with_offset_index_policy(File::Metadata::PageIndexPolicy.from(@preload_offset_index))
        .with_prefetch_hint(@metadata_size_hint)
      if options && (!options.column_index_policy.skip? || !options.offset_index_policy.skip?)
        reader.with_column_index_policy(options.column_index_policy).with_offset_index_policy(options.offset_index_policy)
      end
      if size = @file_size
        reader.load_and_finish(self, size)
      else
        reader.load_via_suffix_and_finish(self)
      end
    end

    private def dispatch(&operation : -> T) : T forall T
      context = @context
      return operation.call unless context
      reply = Channel(T | Exception).new(1)
      context.spawn do
        reply.send(operation.call)
      rescue error : Exception
        reply.send(error)
      end
      value = reply.receive
      raise value if value.is_a?(Exception)
      value
    end
  end
end
