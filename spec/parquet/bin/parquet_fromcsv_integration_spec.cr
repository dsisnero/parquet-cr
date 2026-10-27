require "../../spec_helper"

# parquet-fromcsv.rs test_convert_csv_to_parquet, plus end-to-end
# characterizations of the pinned reader/writer configuration and error paths.
alias CsvCommand = Parquet::Bin::ParquetFromCsv

private CSV_MESSAGE = "message my_amazing_schema { optional int32 id; optional binary name (STRING); }"

private def convert_csv_fixture(schema : String, input : Bytes, extra = [] of String) : Bytes
  File.tempfile("fromcsv-schema") do |schema_file|
    schema_file.print(schema)
    schema_file.flush
    File.tempfile("fromcsv-input") do |input_file|
      input_file.write(input)
      input_file.flush
      File.tempfile("fromcsv-output") do |output|
        args = CsvCommand::Args.parse(["-s", schema_file.path, "-i", input_file.path, "-o", output.path] + extra, batch_size_env: nil)
        CsvCommand.convert_csv_to_parquet(args)
        return File.read(output.path).to_slice.dup
      end
    end
  end
  raise "CSV fixture conversion did not return output"
end

private def csv_file(bytes : Bytes) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(bytes))
end

private def csv_batches(bytes : Bytes) : Array(Arrow::RecordBatch)
  Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(csv_file(bytes)).build.to_a
end

private def compress_csv_fixture(text : String, codec : String) : Bytes
  input = text.to_slice
  output = IO::Memory.new
  case codec
  when "UNCOMPRESSED" then input.dup
  when "SNAPPY"
    writer = Compress::Snappy::Writer.new(output)
    writer.write(input)
    writer.close
    output.to_slice.dup
  when "GZIP"
    Compress::Gzip::Writer.open(output) { |writer| writer.write(input) }
    output.to_slice.dup
  when "BROTLI" then Brotli.encode(input, quality: 2)
  when "LZ4"    then Parquet::Lz4.compress_frame(input)
  when "ZSTD"   then Zinc.compress(input, level: 1)
  else               raise "Unexpected codec"
  end
end

describe "CSV import" do
  %w[UNCOMPRESSED SNAPPY GZIP BROTLI LZ4 ZSTD].each do |codec|
    it "ports upstream compressed CSV conversion for #{codec}, asserting every value" do
      csv = String.build { |text| (1...2000).each { |index| text << "#{index},\"name_#{index}\"\r\n" } }
      bytes = convert_csv_fixture(CSV_MESSAGE, compress_csv_fixture(csv, codec), ["-C", codec, "--enable-bloom-filter", "true"])
      file = csv_file(bytes)
      file.metadata.file_metadata.schema_descr.name.should eq "my_amazing_schema"
      file.metadata.file_metadata.num_rows.should eq 1999
      file.metadata.row_groups.each do |group|
        group.columns.each do |column|
          column.compression.should eq Parquet::CompressionCodec::SNAPPY
          column.bloom_filter_offset.should_not be_nil
        end
      end
      batches = csv_batches(bytes)
      batches.flat_map { |batch| batch.column(0).as(Arrow::PrimitiveArray(Int32)).to_a }.should eq (1...2000).to_a
      batches.flat_map { |batch| batch.column(1).as(Arrow::StringArray).to_a }.should eq (1...2000).map { |index| "name_#{index}" }
    end
  end

  it "preserves typed values, nulls, schema root, group boundaries and V2 compression" do
    schema = "message typed_csv { required int32 id; optional boolean active; optional double score; optional binary name (STRING); optional int32 day (DATE); optional int64 stamp (TIMESTAMP(MICROS,false)); optional int64 amount (DECIMAL(12,2)); }"
    csv = "id,active,score,name,day,stamp,amount\n1,TrUe,1.25,\"a,b\",1970-01-02,1970-01-01T00:00:01,12.34\n2,false,-2.5,\"line\nnext\",1969-12-31,1969-12-31T23:59:59,-0.01\n3,,,,,,\n"
    bytes = convert_csv_fixture(schema, csv.to_slice, ["-h", "-b", "1", "-m", "2", "-w", "2", "-c", "gzip"])
    file = csv_file(bytes)
    file.metadata.file_metadata.schema_descr.name.should eq "typed_csv"
    file.metadata.row_groups.map(&.num_rows).should eq [2_i64, 1_i64]
    file.metadata.row_groups.each { |group| group.columns.each { |column| column.compression.should eq Parquet::CompressionCodec::GZIP } }
    batches = csv_batches(bytes)
    batches.flat_map { |batch| batch.column(0).as(Arrow::PrimitiveArray(Int32)).to_a }.should eq [1, 2, 3]
    batches.flat_map { |batch| batch.column(1).as(Arrow::BooleanArray).to_a }.should eq [true, false, nil]
    batches.flat_map { |batch| batch.column(2).as(Arrow::PrimitiveArray(Float64)).to_a }.should eq [1.25, -2.5, nil]
    batches.flat_map { |batch| batch.column(3).as(Arrow::StringArray).to_a }.should eq ["a,b", "line\nnext", nil]
    batches.flat_map { |batch| batch.column(4).as(Arrow::PrimitiveArray(Int32)).to_a }.should eq [1, -1, nil]
    batches.flat_map { |batch| batch.column(5).as(Arrow::PrimitiveArray(Int64)).to_a }.should eq [1_000_000_i64, -1_000_000_i64, nil]
    batches.flat_map { |batch| batch.column(6).as(Arrow::PrimitiveArray(Int128)).to_a }.should eq [1234_i128, -1_i128, nil]
    batches.first.schema.fields[0].nullable?.should be_false
    expected = Parquet::Arrow.parquet_to_arrow_schema(Parquet::Schema::SchemaDescriptor.new(Parquet::Schema.parse_message_type(schema)), nil)
    batches.first.schema.fields.should eq expected.fields
  end

  it "uses TSV default quoting and ignores double-quote as the pinned command does" do
    bytes = convert_csv_fixture(CSV_MESSAGE, "1\t\"a\tb\"\n2\t\"a\"\"b\"\n".to_slice, ["-f", "tsv", "-D", "false"])
    csv_batches(bytes).flat_map { |batch| batch.column(1).as(Arrow::StringArray).to_a }.should eq ["a\tb", "a\"b"]
  end

  it "applies custom delimiter, escape, quote and CR record terminator" do
    bytes = convert_csv_fixture(CSV_MESSAGE, "1;'a!'b'\r2;'c;d'\r".to_slice, ["-d", ";", "-e", "!", "-q", "'", "-r", "cr"])
    csv_batches(bytes).flat_map { |batch| batch.column(1).as(Arrow::StringArray).to_a }.should eq ["a'b", "c;d"]
  end

  it "reads concatenated Gzip/LZ4 streams and skippable Zstd frames without dropping rows" do
    %w[GZIP ZSTD LZ4].each do |codec|
      input = IO::Memory.new
      input.write(compress_csv_fixture("1,one\n", codec))
      if codec == "ZSTD"
        input.write_bytes(0x184d2a50_u32, IO::ByteFormat::LittleEndian)
        input.write_bytes(3_u32, IO::ByteFormat::LittleEndian)
        input.write(Bytes[1, 2, 3])
      end
      input.write(compress_csv_fixture("2,two\n", codec))
      bytes = convert_csv_fixture(CSV_MESSAGE, input.to_slice, ["-C", codec])
      csv_batches(bytes).flat_map { |batch| batch.column(0).as(Arrow::PrimitiveArray(Int32)).to_a }.should eq [1, 2]
    end
  end

  it "produces a valid empty Parquet for empty and header-only inputs" do
    [{"", [] of String}, {"id,name\n", ["-h"]}].each do |csv, options|
      bytes = convert_csv_fixture(CSV_MESSAGE, csv.to_slice, options)
      csv_file(bytes).metadata.file_metadata.num_rows.should eq 0
      csv_batches(bytes).should be_empty
      csv_file(bytes).metadata.file_metadata.schema_descr.name.should eq "my_amazing_schema"
    end
  end

  it "matches pinned Rust empty-stream semantics for every input codec" do
    %w[UNCOMPRESSED SNAPPY LZ4].each do |codec|
      bytes = convert_csv_fixture(CSV_MESSAGE, Bytes.empty, ["-C", codec])
      csv_file(bytes).metadata.file_metadata.num_rows.should eq 0
    end
    %w[GZIP BROTLI ZSTD].each do |codec|
      expect_raises(CsvCommand::Error, "Failed to read RecordBatch from CSV") do
        convert_csv_fixture(CSV_MESSAGE, Bytes.empty, ["-C", codec])
      end
    end
  end

  it "matches upstream's empty result for a zero batch size" do
    bytes = convert_csv_fixture(CSV_MESSAGE, "1,one\n".to_slice, ["-b", "0"])
    csv_file(bytes).metadata.file_metadata.num_rows.should eq 0
    csv_batches(bytes).should be_empty
  end

  it "rejects a null in a required field" do
    schema = "message required_value { required int32 id; optional binary name (STRING); }"
    expect_raises(CsvCommand::Error, "Failed to read RecordBatch from CSV") do
      convert_csv_fixture(schema, ",name\n".to_slice)
    end
  end

  it "attaches CSV read context to malformed compressed input" do
    %w[SNAPPY GZIP BROTLI LZ4 ZSTD].each do |codec|
      expect_raises(CsvCommand::Error, "Failed to read RecordBatch from CSV") do
        convert_csv_fixture(CSV_MESSAGE, "bad compressed data".to_slice, ["-C", codec])
      end
    end
  end

  it "reports CSV conversion errors with read context and rejects unsupported input" do
    ["bad,name\n", "1,name,extra\n", "1\n"].each do |csv|
      expect_raises(CsvCommand::Error, "Failed to read RecordBatch from CSV") { convert_csv_fixture(CSV_MESSAGE, csv.to_slice) }
    end
    expect_raises(CsvCommand::Error) { convert_csv_fixture(CSV_MESSAGE, Bytes[0xff, 44, 0xfe, 10]) }
    expect_raises(Parquet::NYIError, "LZO") { convert_csv_fixture(CSV_MESSAGE, Bytes.empty, ["-C", "lzo"]) }
    nested = "message nested { optional group values (LIST) { repeated group list { optional int32 element; } } }"
    expect_raises(CsvCommand::Error, "Failed to read RecordBatch from CSV") { convert_csv_fixture(nested, "1\n".to_slice) }
  end
end

describe "CSV import CLI" do
  it "returns help/version, argument and runtime statuses with useful context" do
    output = IO::Memory.new
    errors = IO::Memory.new
    CsvCommand.main(["--help"], output, errors).should eq 0
    output.to_s.should contain "--schema"
    errors.to_s.should be_empty
    output.clear
    CsvCommand.main(["--version"], output, errors).should eq 0
    output.to_s.should eq "parquet-fromcsv #{Parquet::VERSION}\n"
    CsvCommand.main([] of String, IO::Memory.new, errors).should eq 2
    errors.to_s.should contain "--schema"
    errors.clear
    CsvCommand.main(["-s", "/no/such/schema", "-i", "input", "-o", "output"], IO::Memory.new, errors).should eq 1
    errors.to_s.should contain "Failed to open schema file"
  end

  it "runs the public CLI silently and preserves the caller's arguments" do
    File.tempfile("fromcsv-cli-schema") do |schema|
      schema.print CSV_MESSAGE
      schema.flush
      File.tempfile("fromcsv-cli-input") do |input|
        input.print "1,name\n"
        input.flush
        File.tempfile("fromcsv-cli-output") do |output|
          argv = ["--schema=#{schema.path}", "-i", input.path, "-o", output.path]
          original = argv.dup
          stdout = IO::Memory.new
          stderr = IO::Memory.new
          CsvCommand.main(argv, stdout, stderr).should eq 0
          stdout.to_s.should be_empty
          stderr.to_s.should be_empty
          argv.should eq original
          bytes = File.read(output.path).to_slice
          csv_batches(bytes).first.column(0).as(Arrow::PrimitiveArray(Int32)).to_a.should eq [1]
        end
      end
    end
  end

  it "preserves upstream schema-before-output-before-input error ordering" do
    File.tempfile("fromcsv-schema") do |schema|
      File.tempfile("fromcsv-output") do |output|
        output.print "keep"
        output.flush
        args = CsvCommand::Args.parse(["-s", schema.path, "-i", schema.path + ".missing", "-o", output.path], batch_size_env: nil)
        expect_raises(Parquet::ParquetError) { CsvCommand.convert_csv_to_parquet(args) }
        File.read(output.path).should eq "keep"
        schema.print CSV_MESSAGE
        schema.flush
        expect_raises(CsvCommand::Error, "Failed to open input file") { CsvCommand.convert_csv_to_parquet(args) }
        File.read(output.path).should_not eq "keep"
      end
    end
  end
end
