require "../../spec_helper"

# Ported from `parquet/src/file/page_index/column_index.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe Parquet::File::PageIndex::PrimitiveColumnIndex do
  # Upstream `test_page_index_min_max_null`.
  it "returns min/max and null counts for a non-null page" do
    column_index = PPI::ColumnIndex.new(
      [false], Parquet::BoundaryOrder::ASCENDING, [0_i64], nil, [1_i64, 2_i64], [1_i64, 2_i64, 3_i64],
    )
    index = PPI::PrimitiveColumnIndex(Int32).new(column_index, [-123_i32], [234_i32])

    index.min_value(0).should eq(-123)
    index.max_value(0).should eq(234)
    index.null_count(0).should eq(0_i64)
    index.repetition_level_histogram(0).should eq([1_i64, 2_i64])
    index.definition_level_histogram(0).should eq([1_i64, 2_i64, 3_i64])
  end

  # Upstream `test_page_index_min_max_null_none`.
  it "returns nil min/max for a null page" do
    column_index = PPI::ColumnIndex.new(
      [true], Parquet::BoundaryOrder::ASCENDING, [1_i64], nil, nil, [1_i64, 0_i64],
    )
    index = PPI::PrimitiveColumnIndex(Int32).new(column_index, [0_i32], [0_i32])

    index.min_value(0).should be_nil
    index.max_value(0).should be_nil
    index.null_count(0).should eq(1_i64)
    index.repetition_level_histogram(0).should be_nil
    index.definition_level_histogram(0).should eq([1_i64, 0_i64])
  end

  # Upstream `test_invalid_column_index`.
  it "rejects an invalid column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [true, false],
      [Bytes.new(0), Bytes.new(0)],
      [Bytes.new(0), Bytes.new(0)],
      Parquet::BoundaryOrder::UNORDERED,
      nil, nil, nil, nil,
    )

    expect_raises(Parquet::ParquetError, "error converting value, expected 4 bytes got 0") do
      PPI::PrimitiveColumnIndex(Int32).try_from_thrift(thrift)
    end
  end

  # Upstream `test_column_index_rejects_mismatched_min_max_lengths`.
  it "rejects mismatched min/max lengths" do
    thrift = PPI::ThriftColumnIndex.new(
      [false, false],
      [Bytes[1_u8, 0_u8, 0_u8, 0_u8]],
      [Bytes[10_u8, 0_u8, 0_u8, 0_u8]],
      Parquet::BoundaryOrder::UNORDERED,
      nil, nil, nil, nil,
    )

    err = expect_raises(Parquet::ParquetError) do
      PPI::PrimitiveColumnIndex(Int32).try_from_thrift(thrift)
    end
    err.to_s.should contain("length mismatch")
  end

  # Characterization for the `WriteThrift for PrimitiveColumnIndex<T>` impl
  # (apache/arrow-rs 60.0.0). Upstream covers this through the writer
  # round-trip tests; ported here against the wire struct.
  it "round-trips a primitive column index through thrift" do
    index = PPI::PrimitiveColumnIndex(Int32).try_new(
      [false, true],
      Parquet::BoundaryOrder::ASCENDING,
      [0_i64, 1_i64],
      nil,
      [1_i64, 2_i64, 3_i64, 4_i64],
      [1_i64, 0_i64, 1_i64, 0_i64],
      [Bytes[1_u8, 0_u8, 0_u8, 0_u8], Bytes.new(0)],
      [Bytes[10_u8, 0_u8, 0_u8, 0_u8], Bytes.new(0)],
    )

    decoded = ThriftRoundTrip.read(ThriftRoundTrip.encode(index), PPI::ThriftColumnIndex)
    round = PPI::PrimitiveColumnIndex(Int32).try_from_thrift(decoded)

    round.num_pages.should eq(2)
    round.null_page?(0).should be_false
    round.null_page?(1).should be_true
    round.min_value(0).should eq(1)
    round.max_value(0).should eq(10)
    round.min_value(1).should be_nil
    round.null_count(0).should eq(0_i64)
    round.repetition_level_histogram(0).should eq([1_i64, 2_i64])
    round.repetition_level_histogram(1).should eq([3_i64, 4_i64])
    round.definition_level_histogram(0).should eq([1_i64, 0_i64])
  end
end

describe Parquet::File::PageIndex::ByteArrayColumnIndex do
  # Characterization from `SerializedFileReader::test_page_index_reader`
  # (`data_index_bloom_encoding_stats.parquet`, BYTE_ARRAY min "Hello" /
  # max "today").
  it "reports min/max for a variable-length byte array page" do
    index = PPI::ByteArrayColumnIndex.try_new(
      [false],
      Parquet::BoundaryOrder::ASCENDING,
      nil, nil, nil, nil,
      ["Hello".to_slice],
      ["today".to_slice],
    )

    index.num_pages.should eq(1)
    index.boundary_order.should eq(Parquet::BoundaryOrder::ASCENDING)
    index.min_value(0).should eq("Hello".to_slice)
    index.max_value(0).should eq("today".to_slice)
  end

  # Characterization from `SerializedFileReader::test_fixed_length_index`
  # (OPTIONAL FIXED_LEN_BYTE_ARRAY(11) DECIMAL(25,2), one page, one null).
  it "reports min/max for a fixed-length byte array page" do
    thrift = PPI::ThriftColumnIndex.new(
      [false],
      [Bytes.new(11, 0_u8)],
      [Bytes.new(11, 5_u8)],
      Parquet::BoundaryOrder::UNORDERED,
      [1_i64], nil, nil, nil,
    )

    index = PPI::ByteArrayColumnIndex.try_from_thrift(thrift)

    index.num_pages.should eq(1)
    index.null_count(0).should eq(1_i64)
    index.min_value(0).should eq(Bytes.new(11, 0_u8))
    index.max_value(0).should eq(Bytes.new(11, 5_u8))
  end

  it "packs min/max bytes contiguously and skips null pages" do
    index = PPI::ByteArrayColumnIndex.try_new(
      [false, true, false],
      Parquet::BoundaryOrder::ASCENDING,
      nil, nil, nil, nil,
      [Bytes[1_u8, 2_u8], Bytes.new(0), Bytes[3_u8, 4_u8, 5_u8]],
      [Bytes[9_u8, 8_u8], Bytes.new(0), Bytes[7_u8, 6_u8]],
    )

    index.num_pages.should eq(3)
    index.min_bytes.should eq(Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8])
    index.max_bytes.should eq(Bytes[9_u8, 8_u8, 7_u8, 6_u8])
    index.min_value(0).should eq(Bytes[1_u8, 2_u8])
    index.min_value(1).should be_nil
    index.max_value(1).should be_nil
    index.min_value(2).should eq(Bytes[3_u8, 4_u8, 5_u8])
    index.max_value(2).should eq(Bytes[7_u8, 6_u8])
    index.min_values_iter.to_a.should eq([Bytes[1_u8, 2_u8], nil, Bytes[3_u8, 4_u8, 5_u8]])
    index.max_values_iter.to_a.should eq([Bytes[9_u8, 8_u8], nil, Bytes[7_u8, 6_u8]])
  end

  it "rejects mismatched min/max lengths" do
    err = expect_raises(Parquet::ParquetError) do
      PPI::ByteArrayColumnIndex.try_new(
        [false, false], Parquet::BoundaryOrder::UNORDERED, nil, nil, nil, nil,
        [Bytes[1_u8]], [Bytes[2_u8]],
      )
    end
    err.to_s.should contain("length mismatch")
    err.to_s.should contain("expected 2")
  end

  it "rejects mismatched null_counts length" do
    err = expect_raises(Parquet::ParquetError) do
      PPI::ByteArrayColumnIndex.try_new(
        [false], Parquet::BoundaryOrder::UNORDERED, [0_i64, 1_i64], nil, nil, nil,
        [Bytes[1_u8]], [Bytes[2_u8]],
      )
    end
    err.to_s.should contain("null_counts length mismatch")
  end

  it "rejects invalid repetition level histogram length" do
    err = expect_raises(Parquet::ParquetError) do
      PPI::ByteArrayColumnIndex.try_new(
        [false, false], Parquet::BoundaryOrder::UNORDERED, nil, nil, [1_i64, 2_i64, 3_i64], nil,
        [Bytes[1_u8], Bytes[2_u8]], [Bytes[1_u8], Bytes[2_u8]],
      )
    end
    err.to_s.should eq("Parquet error: Invalid repetition_level_histograms length")
  end

  # Characterization for the `WriteThrift for ByteArrayColumnIndex` impl
  # (apache/arrow-rs 60.0.0).
  it "round-trips a byte array column index through thrift" do
    index = PPI::ByteArrayColumnIndex.try_new(
      [false, true, false],
      Parquet::BoundaryOrder::DESCENDING,
      [0_i64, 1_i64, 0_i64],
      nil, nil, nil,
      [Bytes[1_u8, 2_u8], Bytes.new(0), Bytes[3_u8, 4_u8, 5_u8]],
      [Bytes[9_u8, 8_u8], Bytes.new(0), Bytes[7_u8, 6_u8]],
    )

    decoded = ThriftRoundTrip.read(ThriftRoundTrip.encode(index), PPI::ThriftColumnIndex)
    round = PPI::ByteArrayColumnIndex.try_from_thrift(decoded)

    round.num_pages.should eq(3)
    round.boundary_order.should eq(Parquet::BoundaryOrder::DESCENDING)
    round.null_page?(1).should be_true
    round.min_value(0).should eq(Bytes[1_u8, 2_u8])
    round.max_value(0).should eq(Bytes[9_u8, 8_u8])
    round.min_value(1).should be_nil
    round.min_value(2).should eq(Bytes[3_u8, 4_u8, 5_u8])
    round.null_counts.should eq([0_i64, 1_i64, 0_i64])
  end
end

describe Parquet::File::PageIndex::ColumnIndexMetaData do
  # Characterization for the `ColumnIndexMetaData` dispatch surface
  # (apache/arrow-rs 60.0.0, `column_index.rs`).
  it "dispatches the common accessors to the inner index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false, true],
      [Bytes[1_u8, 0_u8, 0_u8, 0_u8], Bytes.new(0)],
      [Bytes[10_u8, 0_u8, 0_u8, 0_u8], Bytes.new(0)],
      Parquet::BoundaryOrder::ASCENDING,
      [0_i64, 1_i64], [1_i64, 2_i64], [1_i64, 0_i64], [3_i64, 4_i64],
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::INT32)

    meta.kind.should eq(Parquet::Type::INT32)
    meta.sorted?.should be_true
    meta.boundary_order.should eq(Parquet::BoundaryOrder::ASCENDING)
    meta.num_pages.should eq(2)
    meta.null_counts.should eq([0_i64, 1_i64])
    meta.nan_counts.should eq([3_i64, 4_i64])
    meta.null_count(0).should eq(0_i64)
    meta.nan_count(1).should eq(4_i64)
    meta.null_page?(0).should be_false
    meta.null_page?(1).should be_true
    meta.repetition_level_histogram(1).should eq([2_i64])
    meta.definition_level_histogram(1).should eq([0_i64])
  end

  it "reports unordered indexes as not sorted" do
    thrift = PPI::ThriftColumnIndex.new(
      [false],
      [Bytes[1_u8, 0_u8, 0_u8, 0_u8]],
      [Bytes[2_u8, 0_u8, 0_u8, 0_u8]],
      Parquet::BoundaryOrder::UNORDERED,
      nil, nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::INT32)

    meta.sorted?.should be_false
    meta.boundary_order.should eq(Parquet::BoundaryOrder::UNORDERED)
  end

  # Characterization for `WriteThrift for ColumnIndexMetaData` (dispatches to
  # the inner index). Upstream exercises it via the page-index builder tests.
  it "round-trips through thrift for a byte array index" do
    original = PPI::ByteArrayColumnIndex.try_new(
      [false, false],
      Parquet::BoundaryOrder::ASCENDING,
      nil, nil, nil, nil,
      [Bytes[1_u8], Bytes[2_u8]],
      [Bytes[3_u8], Bytes[4_u8]],
    )
    meta = PPI::ColumnIndexMetaData.new(Parquet::Type::BYTE_ARRAY, original)

    io = IO::Memory.new
    meta.write_thrift(Parquet::Thrift::ThriftCompactOutputProtocol.new(io))
    decoded = PPI.decode_column_index(io.to_slice.dup, Parquet::Type::BYTE_ARRAY)
    round = decoded.inner.as(PPI::ByteArrayColumnIndex)

    round.min_value(0).should eq(Bytes[1_u8])
    round.max_value(1).should eq(Bytes[4_u8])
  end
end
