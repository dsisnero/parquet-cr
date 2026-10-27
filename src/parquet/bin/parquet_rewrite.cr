require "option_parser"
require "../arrow/arrow_reader"
require "../arrow/arrow_writer"

# parquet/src/bin/parquet-rewrite.rs, apache/arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b). Clap is represented by
# OptionParser with explicit validation; diagnostic rendering is Crystal-native.
module Parquet::Bin::ParquetRewrite
  private alias Props = File::Properties

  # Parsed CLI settings. Properties are applied in upstream order, independently
  # of the order in which flags appeared. Int32 limits replace Rust usize.
  class Args
    COMPRESSION = %w[none snappy gzip lzo brotli lz4 zstd lz4-raw]
    ENCODING    = %w[plain plain-dictionary rle bit-packed delta-binary-packed delta-length-byte-array delta-byte-array rle-dictionary byte-stream-split alp]
    BOOLS       = %w[dictionary-enabled write-page-header-statistics write-path-in-schema bloom-filter-enabled coerce-types]
    SIZES       = %w[dictionary-page-size-limit max-row-group-size data-page-row-count-limit data-page-size-limit statistics-truncate-length column-index-truncate-length write-batch-size]
    ENUMS       = {"compression" => COMPRESSION, "encoding" => ENCODING, "statistics-enabled" => %w[none chunk page], "writer-version" => %w[1.0 2.0], "bloom-filter-position" => %w[after-row-group end]}

    @values = {} of String => String
    @parser : OptionParser
    getter? help = false
    getter? version = false

    def self.parse(argv : Array(String)) : self
      new(argv)
    rescue error : OptionParser::Exception
      raise ArgumentError.new(error.message)
    end

    def initialize(argv : Array(String))
      @parser = OptionParser.new
      @parser.banner = "Read and write parquet file with potentially different settings\nUsage: parquet-rewrite --input <INPUT> --output <OUTPUT> [OPTIONS]"
      @parser.on("-i PATH", "--input PATH", "Path to input parquet file") { |value| add("input", value) }
      @parser.on("-o PATH", "--output PATH", "Path to output parquet file") { |value| add("output", value) }
      (ENUMS.keys + BOOLS + SIZES + %w[compression-level bloom-filter-fpp bloom-filter-ndv]).each do |name|
        register_option(name)
      end
      @parser.on("-h", "--help", "Print help") { @help = true }
      @parser.on("-V", "--version", "Print version") { @version = true }
      @parser.unknown_args do |before, after|
        raise ArgumentError.new("Unexpected argument: #{(before + after).first}") unless before.empty? && after.empty?
      end
      @parser.parse(argv.dup)
      unless @help || @version
        raise ArgumentError.new("Missing required option --input") unless @values.has_key?("input")
        raise ArgumentError.new("Missing required option --output") unless @values.has_key?("output")
      end
    end

    def input : String
      @values.fetch("input", "")
    end

    def output : String
      @values.fetch("output", "")
    end

    def help_text : String
      @parser.to_s
    end

    private def register_option(name : String) : Nil
      description = if choices = ENUMS[name]?
                      "Possible values: #{choices.join(", ")}"
                    elsif BOOLS.includes?(name)
                      "true or false (explicit value required)"
                    else
                      name.gsub('-', ' ')
                    end
      @parser.on("--#{name} VALUE", description) { |value| add(name, value) }
    end

    private def add(name : String, value : String) : Nil
      raise ArgumentError.new("Option --#{name} cannot be used multiple times") if @values.has_key?(name)
      if choices = ENUMS[name]?
        raise ArgumentError.new("Invalid value '#{value}' for --#{name}; expected #{choices.join(", ")}") unless choices.includes?(value)
      elsif BOOLS.includes?(name)
        bool_value(value)
      elsif SIZES.includes?(name)
        size_value(value)
      elsif name == "compression-level"
        raise ArgumentError.new("Invalid u32: #{value}") if unsigned_value(value) > UInt32::MAX
      elsif name == "bloom-filter-ndv"
        unsigned_value(value)
      elsif name == "bloom-filter-fpp"
        float_value(value)
      end
      @values[name] = value
    end

    private def bool_value(value : String) : Bool
      case value
      when "true"  then true
      when "false" then false
      else              raise ArgumentError.new("Expected true or false, got '#{value}'")
      end
    end

    private def unsigned_value(value : String) : UInt64
      raise ArgumentError.new("Invalid unsigned integer: #{value}") unless /\A\+?[0-9]+\z/.matches?(value)
      value.to_u64?(whitespace: false) || raise ArgumentError.new("Invalid unsigned integer: #{value}")
    end

    private def size_value(value : String) : Int32
      number = unsigned_value(value)
      raise ArgumentError.new("Size exceeds Crystal Int32 limit: #{value}") if number > Int32::MAX
      number.to_i32
    end

    private def float_value(value : String) : Float64
      value.to_f64?(whitespace: false) || raise ArgumentError.new("Invalid float: #{value}")
    end

    # Upstream compression_from_args; only Zstd interprets the u32 bits as i32.
    private def compression : Compression
      level = @values["compression-level"]?.try { |value| unsigned_value(value).to_u32.to_i32! }
      case @values["compression"]
      when "none"    then Compression::UNCOMPRESSED
      when "snappy"  then Compression::SNAPPY
      when "gzip"    then Compression.gzip(level || GzipLevel.default.compression_level)
      when "lzo"     then Compression::LZO
      when "brotli"  then Compression.brotli(level || BrotliLevel.default.compression_level)
      when "lz4"     then Compression::LZ4
      when "zstd"    then Compression.zstd(level || ZstdLevel.default.compression_level)
      when "lz4-raw" then Compression::LZ4_RAW
      else                raise ArgumentError.new("Unknown compression")
      end
    end

    # Applies options to fresh default properties, preserving source key/value
    # metadata. ArrowWriter replaces the embedded Arrow schema as upstream does.
    def writer_properties(metadata : Array(File::Metadata::KeyValue)? = nil) : Props::WriterProperties
      builder = Props::WriterProperties.builder.set_key_value_metadata(metadata.try(&.dup))
      builder.set_compression(compression) if @values.has_key?("compression")
      if encoding = @values["encoding"]?
        builder.set_encoding(Encoding.from_str(encoding.gsub('-', '_')))
      end
      {% for config in [
                         {"dictionary-enabled", "set_dictionary_enabled", "bool_value"},
                         {"dictionary-page-size-limit", "set_dictionary_page_size_limit", "size_value"},
                         {"max-row-group-size", "set_max_row_group_row_count", "size_value"},
                         {"data-page-row-count-limit", "set_data_page_row_count_limit", "size_value"},
                         {"data-page-size-limit", "set_data_page_size_limit", "size_value"},
                         {"statistics-truncate-length", "set_statistics_truncate_length", "size_value"},
                         {"column-index-truncate-length", "set_column_index_truncate_length", "size_value"},
                       ] %}
        if value = @values[{{config[0]}}]?
          builder.{{config[1].id}}({{config[2].id}}(value))
        end
      {% end %}
      apply_bloom(builder)
      if value = @values["statistics-enabled"]?
        builder.set_statistics_enabled(Props::EnabledStatistics.from_str(value))
      end
      if value = @values["write-page-header-statistics"]?
        enabled = bool_value(value)
        builder.set_write_page_header_statistics(enabled)
        builder.set_statistics_enabled(Props::EnabledStatistics::Page) if enabled
      end
      if value = @values["writer-version"]?
        builder.set_writer_version(value == "1.0" ? Props::WriterVersion::PARQUET_1_0 : Props::WriterVersion::PARQUET_2_0)
      end
      {% for config in [{"coerce-types", "set_coerce_types", "bool_value"}, {"write-path-in-schema", "set_write_path_in_schema", "bool_value"}, {"write-batch-size", "set_write_batch_size", "size_value"}] %}
        if value = @values[{{config[0]}}]?
          builder.{{config[1].id}}({{config[2].id}}(value))
        end
      {% end %}
      builder.build
    end

    private def apply_bloom(builder : Props::WriterPropertiesBuilder) : Nil
      return unless value = @values["bloom-filter-enabled"]?
      enabled = bool_value(value)
      builder.set_bloom_filter_enabled(enabled)
      return unless enabled
      builder.set_bloom_filter_fpp(float_value(value)) if value = @values["bloom-filter-fpp"]?
      builder.set_bloom_filter_max_ndv(unsigned_value(value)) if value = @values["bloom-filter-ndv"]?
      if value = @values["bloom-filter-position"]?
        builder.set_bloom_filter_position(value == "end" ? Props::BloomFilterPosition::End : Props::BloomFilterPosition::AfterRowGroup)
      end
    end
  end

  # Prepares the reader and validates properties before opening the destination,
  # matching the upstream command's ordering of fallible operations.
  private def self.prepare(input : File::ChunkReader, args : Args) : Tuple(Arrow::ParquetRecordBatchReader, Props::WriterProperties)
    file = File::SerializedFileReader.new(input)
    reader = Arrow::ParquetRecordBatchReaderBuilder.try_new(file).build
    {reader, args.writer_properties(file.metadata.file_metadata.key_value_metadata)}
  end

  private def self.write(reader : Arrow::ParquetRecordBatchReader, output : IO, properties : Props::WriterProperties) : Nil
    writer = Arrow::ArrowWriter.try_new(output, reader.schema, properties)
    reader.each { |batch| writer.write(batch) }
    writer.close
  end

  # Rewrites a source into an output IO without collecting reader batches.
  def self.rewrite(input : File::ChunkReader, output : IO, args : Args) : Nil
    reader, properties = prepare(input, args)
    write(reader, output, properties)
  end

  # Opens CLI paths and closes both handles on success or failure.
  def self.run(args : Args) : Nil
    ::File.open(args.input, "r") do |input|
      reader, properties = prepare(File::FileChunkReader.new(input), args)
      ::File.open(args.output, "w") { |output| write(reader, output, properties) }
    end
  end

  # Testable entry point. Clap-style argument errors exit 2; runtime errors exit
  # 1 with Crystal-native messages instead of Rust panic/backtrace formatting.
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
      output.puts("parquet-rewrite #{Parquet::VERSION}")
    else
      run(args)
    end
    0
  rescue error : Exception
    errors.puts(error.message || error.class.to_s)
    1
  end
end
