require "../../spec_helper"

# Tests ported from parquet/src/bin/parquet-fromcsv.rs at arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b), plus CLI boundary cases.
alias FromCsv = Parquet::Bin::ParquetFromCsv

private def fromcsv_args(extra = [] of String) : FromCsv::Args
  FromCsv::Args.parse(["--schema", "test.schema", "--input-file", "infile.csv", "--output-file", "out.parquet"] + extra, batch_size_env: nil)
end

private def fromcsv_schema : Arrow::Schema
  Arrow::Schema.new(Arrow::Fields.from((1..5).map { |index| Arrow::Field.new("field#{index}", Arrow::DataType.utf8, false) }))
end

describe FromCsv::Args do
  it "ports test_parse_arg_minimum with exact defaults" do
    args = fromcsv_args
    args.schema_path.should eq "test.schema"
    args.input_file.should eq "infile.csv"
    args.output_file.should eq "out.parquet"
    args.input_format.should eq "csv"
    args.batch_size.should eq 1000
    args.has_header?.should be_false
    args.delimiter_byte.should eq ','.ord.to_u8
    args.terminator_byte.should be_nil
    args.quote_byte.should eq '"'.ord.to_u8
    args.escape_byte.should be_nil
    args.double_quote.should be_nil
    args.csv_compression.should eq Parquet::Compression::UNCOMPRESSED
    args.parquet_compression.should eq Parquet::Compression::SNAPPY
  end

  it "ports test_parse_arg_format_variants" do
    csv = fromcsv_args(["--input-format", "csv", "--escape-char", "\\"])
    csv.delimiter_byte.should eq 44_u8
    csv.terminator_byte.should be_nil
    csv.quote_byte.should eq 34_u8
    csv.escape_byte.should eq 92_u8
    tsv = fromcsv_args(["--input-format", "tsv"])
    tsv.delimiter_byte.should eq 9_u8
    tsv.terminator_byte.should eq 10_u8
    tsv.quote_byte.should be_nil
    tsv.escape_byte.should be_nil
    fromcsv_args(["--input-format", "tsv", "--delimiter", ":"]).delimiter_byte.should eq 58_u8
  end

  it "ports test_parse_arg_format_error" do
    expect_raises(ArgumentError, "input-format") { fromcsv_args(["--input-format", "excel"]) }
  end

  it "ports test_parse_arg_compression_format for both input and output" do
    {"uncompressed" => Parquet::Compression::UNCOMPRESSED, "snappy" => Parquet::Compression::SNAPPY,
     "gzip" => Parquet::Compression.gzip, "lzo" => Parquet::Compression::LZO,
     "brotli" => Parquet::Compression.brotli, "lz4" => Parquet::Compression::LZ4,
     "zstd" => Parquet::Compression.zstd}.each do |name, codec|
      args = fromcsv_args(["--parquet-compression", name, "--csv-compression", name.upcase])
      args.parquet_compression.should eq codec
      args.csv_compression.should eq codec
    end
  end

  it "ports the invalid compression diagnostic without Clap rendering" do
    expect_raises(ArgumentError, "Unknown compression ZIP : possible values UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD") do
      fromcsv_args(["--parquet-compression", "zip"])
    end
    expect_raises(ArgumentError) { fromcsv_args(["--csv-compression", "lz4-raw"]) }
  end

  it "ports test_configure_reader_builder and preserves actual defaults" do
    reader = FromCsv.configure_reader_builder(fromcsv_args, fromcsv_schema)
    reader.batch_size.should eq 1000
    reader.format.header?.should be_false
    reader.format.delimiter.should eq 44_u8
    reader.format.quote.should eq 34_u8
    reader.format.terminator.should be_nil
    reader.format.escape.should be_nil
    args = fromcsv_args(["-f", "tsv", "-h", "-b", "2000", "-e", "\\"])
    reader = FromCsv.configure_reader_builder(args, fromcsv_schema)
    reader.batch_size.should eq 2000
    reader.format.header?.should be_true
    reader.format.delimiter.should eq 9_u8
    reader.format.quote.should be_nil
    reader.format.terminator.should eq 10_u8
    reader.format.escape.should eq 92_u8
  end

  it "maps terminators and explicit character overrides with Rust u8 casts" do
    {"lf" => 10_u8, "cr" => 13_u8, "crlf" => nil}.each do |name, byte|
      fromcsv_args(["-r", name]).terminator_byte.should eq byte
    end
    args = fromcsv_args(["-q", "'", "-e", "!", "-d", "Ā", "-D", "false"])
    args.quote_byte.should eq 39_u8
    args.escape_byte.should eq 33_u8
    args.delimiter_byte.should eq 0_u8
    args.double_quote.should eq false
  end

  it "configures writer version, group limits and explicit bloom on/off" do
    path = Parquet::Schema::ColumnPath.new(["id"])
    [true, false].each do |enabled|
      props = FromCsv.configure_writer_properties(fromcsv_args(["-c", "gzip", "-w", "2", "-m", "37", "--enable-bloom-filter", enabled.to_s]))
      props.compression(path).should eq Parquet::Compression.gzip
      props.writer_version.should eq Parquet::File::Properties::WriterVersion::PARQUET_2_0
      props.max_row_group_row_count.should eq 37
      props.bloom_filter_properties(path).nil?.should eq !enabled
    end
    FromCsv.configure_writer_properties(fromcsv_args(["-w", "1"])).writer_version.should eq Parquet::File::Properties::WriterVersion::PARQUET_1_0
    expect_raises(ArgumentError, "Unknown writer version") { fromcsv_args(["-w", "3"]) }
    expect_raises(Exception, "Cannot have a 0 max row group row count") { FromCsv.configure_writer_properties(fromcsv_args(["-m", "0"])) }
  end

  it "uses the batch environment default and lets the CLI override it" do
    base = ["-s", "schema", "-i", "input", "-o", "output"]
    FromCsv::Args.parse(base, batch_size_env: "23").batch_size.should eq 23
    FromCsv::Args.parse(base + ["-b", "7"], batch_size_env: "bad").batch_size.should eq 7
    expect_raises(ArgumentError) { FromCsv::Args.parse(base, batch_size_env: "bad") }
  end

  it "rejects missing, duplicate, unknown and malformed arguments" do
    expect_raises(ArgumentError) { FromCsv::Args.parse([] of String, batch_size_env: nil) }
    [["-b", "-1"], ["-b", "2147483648"], ["-b", "1_0"], ["-b", " 1"], ["-m", "no"],
     ["-d", "ab"], ["-q", ""], ["-D", "yes"], ["--enable-bloom-filter"], ["-r", "LF"],
     ["--has-header", "false"], ["--unknown"], ["-b", "1", "-b", "2"]].each do |options|
      expect_raises(ArgumentError) { fromcsv_args(options) }
    end
  end

  it "ports help coverage with native formatting and reserves -h for headers" do
    args = FromCsv::Args.parse(["--help"], batch_size_env: nil)
    args.help?.should be_true
    %w[schema input-file output-file input-format batch-size has-header delimiter record-terminator escape-char quote-char double-quote csv-compression parquet-compression writer-version max-row-group-size enable-bloom-filter help version].each do |name|
      args.help_text.should contain "--#{name}"
    end
    fromcsv_args(["-h"]).has_header?.should be_true
  end
end
