require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/mod.rs`
# (`test_read_float32_float64_byte_stream_split`) and
# `parquet/tests/arrow_reader/parquet_testing.rs` (`test_alp_extended`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The upstream tests read through `ParquetRecordBatchReader`; the arrow layer is
# deferred, so these are adapted to the record API (`get_row_iter`) and typed
# column readers. They require the ZSTD and BROTLI codecs (Phase 1.8c):
# `byte_stream_split.zstd.parquet` couples BYTE_STREAM_SPLIT with ZSTD,
# `alp_extended.zstd.parquet` compresses its `*_plain` reference columns with
# ZSTD (the ALP columns are uncompressed), and `large_string_map.brotli.parquet`
# uses BROTLI.
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias PComp = Parquet::Compression

private def compressed_fixture_reader(file_name : String) : FReader
  FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))
end

# Bit pattern of a float/double field, or `nil` when the value is null (arrow
# array equality compares float buffers bytewise and the null bitmap).
private def float_bits(row : Parquet::Record::Row, index : Int32) : UInt32?
  field = row.fields[index][1]
  field.is_a?(Parquet::Record::Field::Float) ? field.value.unsafe_as(UInt32) : nil
end

private def double_bits(row : Parquet::Record::Row, index : Int32) : UInt64?
  field = row.fields[index][1]
  field.is_a?(Parquet::Record::Field::Double) ? field.value.unsafe_as(UInt64) : nil
end

describe "ZSTD and BROTLI fixtures" do
  # Upstream `test_read_float32_float64_byte_stream_split`.
  it "reads ZSTD-compressed BYTE_STREAM_SPLIT floats" do
    reader = compressed_fixture_reader("byte_stream_split.zstd.parquet")
    metadata = reader.metadata
    metadata.file_metadata.num_rows.should eq(300)
    metadata.row_group(0).num_columns.should eq(2)
    metadata.row_group(0).num_columns.times do |i|
      Parquet::Compression.from_codec(metadata.row_group(0).column(i).compression).should eq(PComp.zstd)
    end

    rows = reader.get_row_iter(nil).to_a
    rows.size.should eq(300)

    # The fixture holds floats from a standard normal distribution.
    rows.each do |row|
      f32 = row.get_float(0)
      f64 = row.get_double(1)
      f32.should be > -10.0f32
      f32.should be < 10.0f32
      f64.should be > -10.0
      f64.should be < 10.0
    end
  end

  # Upstream `test_alp_extended`: the ALP columns must decode bit-identically to
  # the ZSTD-compressed reference columns, for every vector size. Upstream
  # compares arrow arrays, which compare float buffers bytewise, so the check
  # compares bit patterns (NaN equals NaN; `-0.0` differs from `0.0`).
  it "reads ZSTD reference columns next to ALP columns" do
    reader = compressed_fixture_reader("alp_extended.zstd.parquet")
    metadata = reader.metadata
    metadata.file_metadata.num_rows.should eq(9032)
    metadata.row_group(0).num_columns.should eq(8)
    Parquet::Compression.from_codec(metadata.row_group(0).column(0).compression).should eq(PComp.zstd)

    rows = reader.get_row_iter(nil).to_a
    rows.size.should eq(9032)

    non_null = 0
    rows.each do |row|
      float_plain = float_bits(row, 0)
      double_plain = double_bits(row, 1)
      non_null += 1 unless float_plain.nil?
      [2, 4, 6].each { |i| float_bits(row, i).should eq(float_plain) }
      [3, 5, 7].each { |i| double_bits(row, i).should eq(double_plain) }
    end
    non_null.should be > 0
  end

  # `large_string_map.brotli.parquet` has no upstream test (arrow-rs only ships
  # the fixture). Each of its two rows maps a ~1 GiB string of `'a'` to `1`, so
  # the test reads the small INT32 value column and checks the key column's
  # metadata instead of materializing the 2 GiB of keys.
  it "reads the BROTLI-compressed value column of a large string map" do
    reader = compressed_fixture_reader("large_string_map.brotli.parquet")
    metadata = reader.metadata
    metadata.file_metadata.num_rows.should eq(2)

    key_chunk = metadata.row_group(0).column(0)
    value_chunk = metadata.row_group(0).column(1)
    Parquet::Compression.from_codec(key_chunk.compression).should eq(PComp.brotli)
    Parquet::Compression.from_codec(value_chunk.compression).should eq(PComp.brotli)
    key_chunk.uncompressed_size.should be > 1_000_000_000_i64
    value_chunk.uncompressed_size.should be < 100_i64

    column_reader = Parquet::Column.get_typed_column_reader(
      Int32, reader.get_row_group(0).get_column_reader(1)
    )
    values = [] of Int32
    def_levels = [] of Int16
    rep_levels = [] of Int16
    records, values_read, _levels = column_reader.read_records(2, def_levels, rep_levels, values)
    records.should eq(2)
    values_read.should eq(2)
    values.should eq([1, 1])
  end
end
