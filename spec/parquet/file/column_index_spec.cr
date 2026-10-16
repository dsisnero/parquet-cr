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
end
