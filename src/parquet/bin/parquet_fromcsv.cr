require "option_parser"
require "../arrow/arrow_writer"
require "../schema/parser"

# parquet/src/bin/parquet-fromcsv.rs, apache/arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b). Native OptionParser replaces Clap.
module Parquet::Bin::ParquetFromCsv
  private alias Props = File::Properties

  # Validated CLI options. Character options use Rust's truncating char-to-u8
  # conversion; collection/batch sizes use Crystal Int32 rather than usize.
  class Args
    OPTIONS = {
      "schema" => "s", "input-file" => "i", "output-file" => "o", "input-format" => "f",
      "batch-size" => "b", "delimiter" => "d", "record-terminator" => "r",
      "escape-char" => "e", "quote-char" => "q", "double-quote" => "D",
      "csv-compression" => "C", "parquet-compression" => "c", "writer-version" => "w",
      "max-row-group-size" => "m", "enable-bloom-filter" => nil,
    }
    @values = {} of String => String
    @parser : OptionParser
    getter? help = false
    getter? version = false
    getter? has_header = false

    def self.parse(argv : Array(String), *, batch_size_env : String? = ENV["PARQUET_FROM_CSV_BATCHSIZE"]?) : self
      new(argv, batch_size_env)
    rescue error : OptionParser::Exception
      raise ArgumentError.new(error.message)
    end

    def initialize(argv : Array(String), batch_size_env : String?)
      @parser = OptionParser.new
      @parser.banner = "Binary to convert csv to Parquet\nUsage: parquet-fromcsv --schema PATH --input-file PATH --output-file PATH [OPTIONS]"
      OPTIONS.each do |name, short|
        register_option(name, short)
      end
      @parser.on("-h", "--has-header", "Input has a header") do
        raise ArgumentError.new("Option --has-header cannot be used multiple times") if @has_header
        @has_header = true
      end
      @parser.on("--help", "Display usage help") { @help = true }
      @parser.on("-V", "--version", "Print version") { @version = true }
      @parser.unknown_args do |before, after|
        raise ArgumentError.new("Unexpected argument: #{(before + after).first}") unless before.empty? && after.empty?
      end
      @parser.parse(argv.dup)
      return if @help || @version
      %w[schema input-file output-file].each do |name|
        raise ArgumentError.new("Missing required option --#{name}") unless @values.has_key?(name)
      end
      add("batch-size", batch_size_env) if batch_size_env && !@values.has_key?("batch-size")
    end

    private def register_option(name : String, short : String?) : Nil
      description = case name
                    when "input-format"                           then "csv or tsv (default: csv)"
                    when "record-terminator"                      then "lf, crlf or cr"
                    when "batch-size"                             then "Batch size (default: 1000; env: PARQUET_FROM_CSV_BATCHSIZE)"
                    when "double-quote", "enable-bloom-filter"    then "true or false (explicit value required)"
                    when "csv-compression", "parquet-compression" then "UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4 or ZSTD"
                    else                                               name.gsub('-', ' ')
                    end
      if short
        @parser.on("-#{short} VALUE", "--#{name} VALUE", description) { |value| add(name, value) }
      else
        @parser.on("--#{name} VALUE", description) { |value| add(name, value) }
      end
    end

    private def add(name : String, value : String) : Nil
      raise ArgumentError.new("Option --#{name} cannot be used multiple times") if @values.has_key?(name)
      validate(name, value)
      @values[name] = value
    end

    private def validate(name : String, value : String) : Nil
      case name
      when "input-format"
        raise ArgumentError.new("Invalid --input-format: #{value}") unless %w[csv tsv].includes?(value)
      when "record-terminator"
        raise ArgumentError.new("Invalid --record-terminator: #{value}") unless %w[lf crlf cr].includes?(value)
      when "batch-size", "max-row-group-size"
        size_value(value)
      when "delimiter", "escape-char", "quote-char"
        raise ArgumentError.new("--#{name} expects a single character") unless value.size == 1
      when "double-quote", "enable-bloom-filter"
        bool_value(value)
      when "csv-compression", "parquet-compression"
        ParquetFromCsv.compression_from_str(value)
      when "writer-version"
        ParquetFromCsv.writer_version_from_str(value)
      end
    end

    private def size_value(value : String) : Int32
      number = value.to_u64?(whitespace: false)
      unless /\A\+?[0-9]+\z/.matches?(value) && number && number <= Int32::MAX
        raise ArgumentError.new("Invalid Int32 size: #{value}")
      end
      number.to_i32
    end

    private def bool_value(value : String) : Bool
      case value
      when "true"  then true
      when "false" then false
      else              raise ArgumentError.new("Expected true or false, got '#{value}'")
      end
    end

    def schema_path : String
      @values.fetch("schema", "")
    end

    {% for option, default in {"input-file" => "", "output-file" => "", "input-format" => "csv"} %}
      def {{option.gsub(/-/, "_").id}} : String
        @values.fetch({{option}}, {{default}})
      end
    {% end %}

    def batch_size : Int32
      size_value(@values.fetch("batch-size", "1000"))
    end

    def max_row_group_size : Int32?
      @values["max-row-group-size"]?.try { |value| size_value(value) }
    end

    def double_quote : Bool?
      @values["double-quote"]?.try { |value| bool_value(value) }
    end

    def bloom_filter_enabled? : Bool?
      @values["enable-bloom-filter"]?.try { |value| bool_value(value) }
    end

    def csv_compression : Compression
      ParquetFromCsv.compression_from_str(@values.fetch("csv-compression", "UNCOMPRESSED"))
    end

    def parquet_compression : Compression
      ParquetFromCsv.compression_from_str(@values.fetch("parquet-compression", "SNAPPY"))
    end

    def writer_version : Props::WriterVersion?
      @values["writer-version"]?.try { |value| ParquetFromCsv.writer_version_from_str(value) }
    end

    def delimiter_byte : UInt8
      character_byte("delimiter") || (input_format == "tsv" ? 9_u8 : 44_u8)
    end

    def terminator_byte : UInt8?
      case @values["record-terminator"]?
      when "lf"   then 10_u8
      when "cr"   then 13_u8
      when "crlf" then nil
      else             input_format == "tsv" ? 10_u8 : nil
      end
    end

    def quote_byte : UInt8?
      character_byte("quote-char") || (input_format == "csv" ? 34_u8 : nil)
    end

    def escape_byte : UInt8?
      character_byte("escape-char")
    end

    private def character_byte(name : String) : UInt8?
      @values[name]?.try(&.[0].ord.to_u8!)
    end

    def help_text : String
      @parser.to_s
    end
  end

  # This CLI deliberately accepts the older codec spellings, case-insensitively.
  def self.compression_from_str(value : String) : Compression
    case name = value.upcase
    when "UNCOMPRESSED" then Compression::UNCOMPRESSED
    when "SNAPPY"       then Compression::SNAPPY
    when "GZIP"         then Compression.gzip
    when "LZO"          then Compression::LZO
    when "BROTLI"       then Compression.brotli
    when "LZ4"          then Compression::LZ4
    when "ZSTD"         then Compression.zstd
    else
      raise ArgumentError.new("Unknown compression #{name} : possible values UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD \n\nFor more information try --help")
    end
  end

  def self.writer_version_from_str(value : String) : Props::WriterVersion
    case name = value.upcase
    when "1" then Props::WriterVersion::PARQUET_1_0
    when "2" then Props::WriterVersion::PARQUET_2_0
    else          raise ArgumentError.new("Unknown writer version #{name} : possible values 1, 2")
    end
  end

  def self.configure_writer_properties(args : Args) : Props::WriterProperties
    builder = Props::WriterProperties.builder.set_compression(args.parquet_compression)
    if version = args.writer_version
      builder.set_writer_version(version)
    end
    if size = args.max_row_group_size
      builder.set_max_row_group_row_count(size)
    end
    if !(enabled = args.bloom_filter_enabled?).nil?
      builder.set_bloom_filter_enabled(enabled)
    end
    builder.build
  end

  def self.configure_reader_builder(args : Args, schema : ::Arrow::Schema) : ::Arrow::Csv::ReaderBuilder
    builder = ::Arrow::Csv::ReaderBuilder.new(schema).with_batch_size(args.batch_size).with_header(args.has_header?).with_delimiter(args.delimiter_byte)
    if byte = args.terminator_byte
      builder.with_terminator(byte)
    end
    if byte = args.escape_byte
      builder.with_escape(byte)
    end
    if byte = args.quote_byte
      builder.with_quote(byte)
    end
    # double_quote is parsed but unused by the pinned binary. TSV's nil quote
    # leaves ReaderBuilder's default double quote active, also as upstream.
    builder
  end

  # Native exceptions replace the Rust error enum; cause preserves the original
  # error type and nested context messages retain the upstream display order.
  class Error < Exception
    def initialize(error : Exception, context : String)
      super("#{error.message}\ncontext: #{context}", cause: error)
    end
  end

  private def self.with_context(message : String, & : -> T) : T forall T
    yield
  rescue error : Exception
    raise Error.new(error, message)
  end

  # Converts batches without retaining the complete table. Brotli/LZ4/Zstd use
  # existing one-shot codec APIs; their decompressed input is buffered in memory.
  def self.convert_csv_to_parquet(args : Args) : Nil
    schema_text = with_context("Failed to open schema file #{args.schema_path.inspect}") { ::File.read(args.schema_path) }
    descriptor = Schema::SchemaDescriptor.new(Schema.parse_message_type(schema_text))
    schema = Arrow.parquet_to_arrow_schema(descriptor, nil)
    output = with_context("Failed to create output file #{args.output_file.inspect}") { ::File.new(args.output_file, "w") }
    options = Arrow::ArrowWriterOptions.new.with_properties(configure_writer_properties(args)).with_schema_root(descriptor.name)
    writer = with_context("Failed to create ArrowWriter") { Arrow::ArrowWriter.try_new_with_options(output, schema, options) }
    input = with_context("Failed to open input file #{args.input_file.inspect}") { ::File.new(args.input_file, "r") }
    decoder = input_decoder(input, args.csv_compression)
    reader = configure_reader_builder(args, schema).build(decoder)
    while batch = with_context("Failed to read RecordBatch from CSV") { reader.maybe_next }
      with_context("Failed to write RecordBatch to parquet") { writer.write(batch) }
    end
    with_context("Failed to close parquet") { writer.close }
    nil
  ensure
    decoder.try(&.close) unless decoder == input
    input.try(&.close)
    output.try(&.close)
  end

  private def self.input_decoder(input : IO, compression : Compression) : IO
    return input if compression.codec.uncompressed?
    raise NYIError.new("compression type #{compression}") if compression.codec.lzo?
    InputDecoder.new(input, compression)
  end

  # Delay decoding until the first CSV read, as upstream's streaming readers do.
  # This also puts malformed compressed-data failures under CSV read context.
  private class InputDecoder < IO
    @codec_reader : IO?
    getter? closed = false

    def initialize(@input : IO, @compression : Compression)
    end

    def read(slice : Bytes) : Int32
      raise IO::Error.new("Closed CSV decoder") if @closed
      return 0 if slice.empty?
      decoder = @codec_reader ||= build_decoder
      decoder.read(slice)
    end

    def write(slice : Bytes) : NoReturn
      raise IO::Error.new("CSV decoder is read-only")
    end

    def close : Nil
      @closed = true
      @codec_reader.try(&.close)
    end

    private def build_decoder : IO
      # File's buffered peek distinguishes EOF without consuming stream bytes.
      # snap/lz4 accept a missing stream, while gzip/brotli/zstd reject it.
      if @input.peek.try(&.empty?)
        return IO::Memory.new if @compression.codec.snappy? || @compression.codec.lz4?
        raise IO::EOFError.new("Incomplete #{@compression} stream")
      end
      case @compression.codec
      when .snappy? then Compress::Snappy::Reader.new(@input)
      when .gzip?   then Compress::Gzip::Reader.new(@input)
      when .brotli? then IO::Memory.new(Brotli::Decoder.new.decode(@input.gets_to_end.to_slice))
      when .lz4?    then IO::Memory.new(Lz4.decompress_frame(@input.gets_to_end.to_slice))
      when .zstd?   then decode_zstd(@input.gets_to_end.to_slice)
      else               raise NYIError.new("compression type #{@compression}")
      end
    end

    # Zinc decodes one frame per call. zstd::Decoder reads concatenated frames
    # and skips RFC 8878 skippable frames, so preserve those container semantics.
    private def decode_zstd(bytes : Bytes) : IO::Memory
      output = IO::Memory.new
      offset = 0
      while offset < bytes.size
        remaining = bytes[offset..]
        if remaining.size >= 4 && IO::ByteFormat::LittleEndian.decode(UInt32, remaining) & 0xfffffff0_u32 == 0x184d2a50_u32
          raise IO::EOFError.new unless remaining.size >= 8
          size = IO::ByteFormat::LittleEndian.decode(UInt32, remaining[4, 4]).to_u64 + 8
          raise IO::EOFError.new if size > remaining.size
          offset += size.to_i32
        else
          decoded, consumed = Zinc::Frame::Decoder.decode(remaining)
          raise IO::Error.new("Zstd decoder made no progress") if consumed <= 0
          output.write(decoded)
          offset += consumed
        end
      end
      output.rewind
      output
    end
  end

  # Testable entry point with native help/version rendering and contextual
  # runtime errors. Argument errors exit 2, runtime errors exit 1.
  def self.main(argv : Array(String), output : IO = STDOUT, errors : IO = STDERR) : Int32
    begin
      args = Args.parse(argv)
    rescue error : ArgumentError
      errors.puts(error.message)
      return 2
    end
    if args.help?
      output.puts(args.help_text)
    elsif args.version?
      output.puts("parquet-fromcsv #{Parquet::VERSION}")
    else
      convert_csv_to_parquet(args)
    end
    0
  rescue error : Exception
    errors.puts(error.message || error.class.to_s)
    1
  end
end
