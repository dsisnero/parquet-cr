require "../../spec_helper"

# End-to-end characterizations of pinned parquet-rewrite.rs's reader/write loop.
private alias RW = Parquet::Bin::ParquetRewrite
private alias RA = Parquet::Arrow
private alias RF = Parquet::File
private alias RM = Parquet::File::Metadata

private def rewrite_file(bytes : Bytes) : RF::SerializedFileReader
  RF::SerializedFileReader.new(RF::BytesChunkReader.new(bytes))
end

private def rewrite_batches(bytes : Bytes) : Array(Arrow::RecordBatch)
  RA::ParquetRecordBatchReaderBuilder.try_new(rewrite_file(bytes)).build.to_a
end

private def rewrite_fixture(rows = 2500) : Bytes
  ids = Arrow::PrimitiveArray(Int64).from_nullable(Array(Int64?).new(rows) { |index| index % 13 == 0 ? nil : index.to_i64 })
  strings = Arrow::GenericStringBuilder(Int32).new
  lists = [] of Array(Int32?)?
  rows.times do |index|
    index % 11 == 0 ? strings.append_null : strings.append_value("value-#{index}-#{"abc" * (index % 20)}")
    lists << case index % 4
    when 0 then nil
    when 1 then [] of Int32?
    else        [index, nil, -index] of Int32?
    end
  end
  arrays = [ids, strings.finish, Arrow::GenericListArray(Int32).from_iter_primitive(Int32, lists)] of Arrow::ArrayBase
  fields = arrays.map_with_index { |array, index| Arrow::Field.new("col#{index}", array.data_type, true) }
  fields[0].with_metadata({"user-field" => "preserved"})
  schema = Arrow::Schema.new(Arrow::Fields.from(fields))
  props = RF::Properties::WriterProperties.builder.set_max_row_group_row_count(700)
    .set_key_value_metadata([RM::KeyValue.new("user", "one"), RM::KeyValue.new("no-value", nil), RM::KeyValue.new("user", "two")]).build
  sink = IO::Memory.new
  writer = RA::ArrowWriter.try_new(sink, schema, props)
  writer.write(Arrow::RecordBatch.try_new(schema, arrays)) unless rows == 0
  writer.close
  sink.to_slice
end

private def run_rewrite(bytes : Bytes, extra : Array(String) = [] of String) : Bytes
  output = IO::Memory.new
  args = RW::Args.parse(["-i", "unused-input", "-o", "unused-output"] + extra)
  RW.rewrite(RF::BytesChunkReader.new(bytes), output, args)
  output.to_slice
end

private def assert_rewritten_values(input : Bytes, output : Bytes) : Nil
  expected = rewrite_batches(input)
  actual = rewrite_batches(output)
  actual.map(&.num_rows).should eq expected.map(&.num_rows)
  actual.each_with_index do |batch, index|
    batch.schema.fields.should eq expected[index].schema.fields
    batch.columns.should eq expected[index].columns
  end
end

describe "Parquet rewrite streaming integration" do
  it "streams multiple reader batches with nested/null values and preserves user metadata" do
    input = rewrite_fixture
    source = rewrite_file(input)
    original_metadata = source.metadata.file_metadata.key_value_metadata || raise "Missing metadata"
    output = run_rewrite(input, ["--max-row-group-size", "333", "--dictionary-enabled", "false", "--compression", "gzip"])
    actual = rewrite_file(output)
    actual.metadata.file_metadata.num_rows.should eq 2500
    actual.metadata.row_groups.map(&.num_rows).should eq([333_i64] * 7 + [169_i64])
    actual.metadata.row_groups.each do |group|
      group.columns.each do |column|
        column.compression.should eq Parquet::CompressionCodec::GZIP
        column.dictionary_page_offset.should be_nil
      end
    end
    actual_metadata = actual.metadata.file_metadata.key_value_metadata || raise "Missing output metadata"
    original_metadata.reject { |entry| entry.key == RA::ARROW_SCHEMA_META_KEY }.should eq actual_metadata.reject { |entry| entry.key == RA::ARROW_SCHEMA_META_KEY }
    actual_metadata.count { |entry| entry.key == RA::ARROW_SCHEMA_META_KEY }.should eq 1
    assert_rewritten_values(input, output)
  end

  it "round trips every supported codec with actual encoded output" do
    input = rewrite_fixture(37)
    ["none", "snappy", "gzip", "brotli", "lz4", "zstd", "lz4-raw"].each do |codec|
      output = run_rewrite(input, ["--compression", codec])
      assert_rewritten_values(input, output)
    end
    expect_raises(Parquet::NYIError) { run_rewrite(input, ["--compression", "lzo"]) }
  end

  it "uses V2 pages, non-dictionary encoding, header statistics and bloom filters" do
    # Delta applies to all columns, so use a homogeneous integer input.
    array = Arrow::PrimitiveArray(Int64).from_values((0_i64...200_i64).to_a)
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("col", array.data_type, false)]))
    sink = IO::Memory.new
    writer = RA::ArrowWriter.try_new(sink, schema)
    writer.write(Arrow::RecordBatch.try_new(schema, [array] of Arrow::ArrayBase))
    writer.close
    input = sink.to_slice
    output = run_rewrite(input, ["--writer-version", "2.0", "--dictionary-enabled", "false", "--encoding", "delta-binary-packed",
                                 "--write-batch-size", "10", "--data-page-row-count-limit", "20", "--statistics-enabled", "none",
                                 "--write-page-header-statistics", "true", "--bloom-filter-enabled", "true", "--bloom-filter-position", "end"])
    # Like upstream's column-writer test, opt in to reading page statistics.
    options = RF::ReadOptionsBuilder.new.with_reader_properties(RF::Properties::ReaderProperties.builder.set_read_page_statistics(true).build).build
    file = RF::SerializedFileReader.new_with_options(RF::BytesChunkReader.new(output), options)
    column = file.metadata.row_group(0).column(0)
    column.encodings.set?(Parquet::Encoding::DELTA_BINARY_PACKED).should be_true
    column.dictionary_page_offset.should be_nil
    column.statistics.should_not be_nil
    column.bloom_filter_offset.should_not be_nil
    pages = file.get_row_group(0).get_column_page_reader(0)
    count = 0
    while page = pages.get_next_page
      page.should be_a(Parquet::Column::Page::DataPageV2)
      page.as(Parquet::Column::Page::DataPageV2).statistics.should_not(be_nil, "page #{count}, values #{page.num_values}")
      count += 1
    end
    count.should eq 10
    assert_rewritten_values(input, output)
  end

  it "writes a valid empty output file" do
    input = rewrite_fixture(0)
    output = run_rewrite(input)
    rewrite_file(output).metadata.file_metadata.num_rows.should eq 0
    rewrite_batches(output).should be_empty
    rewrite_file(output).metadata.file_metadata.schema_descr.schema.should eq rewrite_file(input).metadata.file_metadata.schema_descr.schema
  end

  it "propagates malformed input and unsupported encoding/type errors" do
    expect_raises(Parquet::ParquetError) { run_rewrite("not parquet".to_slice) }
    expect_raises(Exception) do
      run_rewrite(rewrite_fixture(8), ["--dictionary-enabled", "false", "--encoding", "delta-binary-packed"])
    end
  end
end

describe "Parquet rewrite CLI" do
  it "shows help/version without files and reports argument failures with status 2" do
    ["--help", "-h"].each do |flag|
      output = IO::Memory.new
      errors = IO::Memory.new
      RW.main([flag], output, errors).should eq 0
      output.to_s.should contain "--input"
      output.to_s.should contain "--write-page-header-statistics"
      output.to_s.should contain "--coerce-types"
      errors.to_s.should be_empty
    end
    output = IO::Memory.new
    RW.main(["--version"], output, IO::Memory.new).should eq 0
    output.to_s.should eq "parquet-rewrite #{Parquet::VERSION}\n"
    errors = IO::Memory.new
    RW.main([] of String, IO::Memory.new, errors).should eq 2
    errors.to_s.should contain "--input"
  end

  it "opens paths, writes a real file silently and returns success" do
    File.tempfile("parquet-rewrite-input", ".parquet") do |source|
      source.write(rewrite_fixture(37))
      source.flush
      File.tempfile("parquet-rewrite-output", ".parquet") do |destination|
        output = IO::Memory.new
        errors = IO::Memory.new
        argv = ["--input=#{source.path}", "-o", destination.path, "--compression", "zstd", "--writer-version", "2.0"]
        original = argv.dup
        RW.main(argv, output, errors).should eq 0
        argv.should eq original
        output.to_s.should be_empty
        errors.to_s.should be_empty
        assert_rewritten_values(File.read(source.path).to_slice, File.read(destination.path).to_slice)
      end
    end
  end

  it "fails before truncating the destination when input or properties are invalid" do
    File.tempfile("parquet-rewrite-bad-input") do |source|
      source.print "not parquet"
      source.flush
      File.tempfile("parquet-rewrite-protected-output") do |destination|
        destination.print "keep me"
        destination.flush
        errors = IO::Memory.new
        RW.main(["-i", source.path, "-o", destination.path], IO::Memory.new, errors).should eq 1
        errors.to_s.should_not be_empty
        File.read(destination.path).should eq "keep me"
        RW.main(["-i", source.path + ".missing", "-o", destination.path], IO::Memory.new, errors).should eq 1
        File.read(destination.path).should eq "keep me"
        source.seek(0)
        source.truncate(0)
        source.write(rewrite_fixture(1))
        source.flush
        RW.main(["-i", source.path, "-o", destination.path, "--write-batch-size", "0"], IO::Memory.new, errors).should eq 1
        File.read(destination.path).should eq "keep me"
      end
    end
  end
end

describe "Parquet rewrite legacy timestamp input" do
  it "rewrites the pinned alltypes fixture including its INT96 timestamp column" do
    input = ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
    output = run_rewrite(input, ["--compression", "snappy", "--writer-version", "2.0"])
    rewrite_file(output).metadata.file_metadata.num_rows.should eq 8
    assert_rewritten_values(input, output)
  end
end
