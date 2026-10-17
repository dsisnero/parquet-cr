require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/mod.rs` (`test_read_lz4_raw`,
# `test_read_lz4_hadoop_fallback`, `test_read_lz4_hadoop_large`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The upstream tests read through `ParquetRecordBatchReader`; the arrow layer is
# deferred, so these are adapted to the record API (`get_row_iter`). The LZ4
# codecs are ported (Phase 1.8b), which is what makes the fixtures readable:
# `lz4_raw_compressed*` use `LZ4_RAW`, `hadoop_lz4_compressed*` use the
# deprecated `LZ4` codec with Hadoop framing, and `non_hadoop_lz4_compressed`
# uses `LZ4` with a raw (non-Hadoop) block, which only decodes through the
# `backward_compatible_lz4` fallback.
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias PComp = Parquet::Compression
private alias ReaderProperties = Parquet::File::Properties::ReaderProperties

private def lz4_reader(file_name : String) : FReader
  FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))
end

private def lz4_rows(file_name : String) : Array(Parquet::Record::Row)
  lz4_reader(file_name).get_row_iter(nil).to_a
end

# The three `LZ4`-codec fixtures all hold the same four rows (parquet-testing
# PR 18): a timestamp, a binary column, and a double.
private def assert_lz4_rows(rows : Array(Parquet::Record::Row)) : Nil
  rows.size.should eq(4)

  rows.map { |row| row.get_long(0) }.should eq([1593604800_i64, 1593604800_i64, 1593604801_i64, 1593604801_i64])
  rows.map { |row| String.new(row.get_bytes(1).data) }.should eq(["abc", "def", "abc", "def"])
  rows.map { |row| row.get_double(2) }.should eq([42.0, 7.7, 42.125, 7.7])
end

# `lz4_raw_compressed_larger.parquet` and `hadoop_lz4_compressed_larger.parquet`
# hold the same 10,000 UUID strings.
private def assert_lz4_larger_rows(rows : Array(Parquet::Record::Row)) : Nil
  rows.size.should eq(10_000)

  rows[0].get_string(0).should eq("c7ce6bef-d5b0-4863-b199-8ea8c7fb117b")
  rows[1].get_string(0).should eq("e8fb9197-cb9f-4118-b67f-fbfa65f61843")
  rows[-2].get_string(0).should eq("ab52a0cc-c6bb-4d61-8a8f-166dc4b8b13c")
  rows[-1].get_string(0).should eq("85440778-460a-41ac-aa2e-ac3ee41696bf")
end

describe "LZ4 fixtures" do
  # Upstream `test_read_lz4_raw`.
  it "reads LZ4_RAW compressed data" do
    reader = lz4_reader("lz4_raw_compressed.parquet")
    metadata = reader.metadata
    metadata.file_metadata.num_rows.should eq(4)

    row_group = metadata.row_group(0)
    row_group.num_columns.should eq(3)
    row_group.num_columns.times do |i|
      Parquet::Compression.from_codec(row_group.column(i).compression).should eq(PComp::LZ4_RAW)
    end

    assert_lz4_rows(reader.get_row_iter(nil).to_a)
  end

  # Upstream `test_read_lz4_hadoop_fallback`: both files use the `LZ4` codec but
  # different payload algorithms (Hadoop framing vs a raw block).
  it "reads LZ4 hadoop framing and the raw-block fallback" do
    ["hadoop_lz4_compressed.parquet", "non_hadoop_lz4_compressed.parquet"].each do |file_name|
      reader = lz4_reader(file_name)
      reader.metadata.file_metadata.num_rows.should eq(4)
      Parquet::Compression.from_codec(reader.metadata.row_group(0).column(0).compression)
        .should eq(PComp::LZ4)

      assert_lz4_rows(reader.get_row_iter(nil).to_a)
    end
  end

  # The fallback is opt-in: with `backward_compatible_lz4` disabled, a raw block
  # behind the `LZ4` codec is rejected (the Hadoop framing does not match).
  it "rejects the raw-block fallback when backward compatibility is disabled" do
    options = Parquet::File::ReadOptionsBuilder.new
      .with_reader_properties(ReaderProperties.builder.set_backward_compatible_lz4(false).build)
      .build
    reader = FReader.new_with_options(
      BytesCR.new(ParquetTestHelpers.fixture_bytes("non_hadoop_lz4_compressed.parquet")),
      options
    )

    expect_raises(Parquet::ExternalError) do
      reader.get_row_iter(nil).to_a
    end
  end

  # Upstream `test_read_lz4_hadoop_large`.
  it "reads large LZ4 hadoop-framed data" do
    assert_lz4_larger_rows(lz4_rows("hadoop_lz4_compressed_larger.parquet"))
  end

  it "reads large LZ4_RAW data" do
    assert_lz4_larger_rows(lz4_rows("lz4_raw_compressed_larger.parquet"))
  end
end
