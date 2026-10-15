require "../../spec_helper"

# Ported from `parquet/src/file/statistics.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe Parquet::Statistics do
  # Upstream `test_statistics_min_max_bytes`.
  it "returns min/max as bytes" do
    stats = Parquet::Statistics.int32(-123, 234, nil, 1_u64, false)
    stats.min_bytes_opt.should eq(Parquet::StatsBytes.as_bytes(-123))
    stats.max_bytes_opt.should eq(Parquet::StatsBytes.as_bytes(234))

    stats = Parquet::Statistics.byte_array(
      Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8]),
      Parquet::ByteArray.new(Bytes[3_u8, 4_u8, 5_u8]),
      nil, 1_u64, true,
    )
    stats.min_bytes_opt.should eq(Bytes[1_u8, 2_u8, 3_u8])
    stats.max_bytes_opt.should eq(Bytes[3_u8, 4_u8, 5_u8])
  end

  # Upstream `test_statistics_negative_null_count`.
  it "errors on a negative null count" do
    thrift_stats = Parquet::Format::PageStatistics.new(nil, nil, -10_i64, nil, nil, nil, nil, nil, nil)
    expect_raises(Parquet::ParquetError, /Statistics null count is negative -10/) do
      Parquet::Statistics.from_thrift_page_stats(Parquet::Type::INT32, thrift_stats)
    end
  end

  # Upstream `test_statistics_thrift_none`.
  it "returns nil for absent thrift statistics" do
    Parquet::Statistics.from_thrift_page_stats(Parquet::Type::INT32, nil).should be_nil
    Parquet::Statistics.from_thrift_page_stats(Parquet::Type::BYTE_ARRAY, nil).should be_nil
  end

  # Upstream `test_statistics_debug`.
  it "renders Rust-compatible Debug output" do
    stats = Parquet::Statistics.int32(1, 12, nil, 12_u64, true)
    stats.inspect.should eq(
      "Int32({min: Some(1), max: Some(12), distinct_count: None, null_count: Some(12), " \
      "min_max_deprecated: true, min_max_backwards_compatible: true, max_value_exact: true, min_value_exact: true})"
    )

    stats = Parquet::Statistics.int32(nil, nil, nil, 7_u64, false)
    stats.inspect.should eq(
      "Int32({min: None, max: None, distinct_count: None, null_count: Some(7), " \
      "min_max_deprecated: false, min_max_backwards_compatible: false, max_value_exact: false, min_value_exact: false})"
    )
  end

  # Upstream `test_statistics_display`.
  it "renders Display output" do
    stats = Parquet::Statistics.int32(1, 12, nil, 12_u64, true)
    stats.to_s.should eq(
      "{min: 1, max: 12, distinct_count: N/A, null_count: 12, " \
      "min_max_deprecated: true, max_value_exact: true, min_value_exact: true}"
    )

    stats = Parquet::Statistics.int64(nil, nil, nil, 7_u64, false)
    stats.to_s.should eq(
      "{min: N/A, max: N/A, distinct_count: N/A, null_count: 7, " \
      "min_max_deprecated: false, max_value_exact: false, min_value_exact: false}"
    )

    stats = Parquet::Statistics.int96(
      Parquet::Int96.from([1_u32, 0_u32, 0_u32]),
      Parquet::Int96.from([2_u32, 3_u32, 4_u32]),
      nil, 3_u64, true,
    )
    stats.to_s.should eq(
      "{min: [1, 0, 0], max: [2, 3, 4], distinct_count: N/A, null_count: 3, " \
      "min_max_deprecated: true, max_value_exact: true, min_value_exact: true}"
    )

    stats = Parquet::Statistics.new(
      Parquet::ValueStatistics(Parquet::ByteArray).new(
        Parquet::ByteArray.new(Bytes[1_u8]),
        Parquet::ByteArray.new(Bytes[2_u8]),
        5_u64, 7_u64, false,
      ).with_max_is_exact(false).with_min_is_exact(false)
    )
    stats.to_s.should eq(
      "{min: [1], max: [2], distinct_count: 5, null_count: 7, " \
      "min_max_deprecated: false, max_value_exact: false, min_value_exact: false}"
    )
  end

  # Upstream `test_statistics_partial_eq`.
  it "compares statistics for equality" do
    expected = Parquet::Statistics.int32(12, 45, nil, 11_u64, true)

    Parquet::Statistics.int32(12, 45, nil, 11_u64, true).should eq(expected)
    Parquet::Statistics.int32(11, 45, nil, 11_u64, true).should_not eq(expected)
    Parquet::Statistics.int32(12, 44, nil, 11_u64, true).should_not eq(expected)
    Parquet::Statistics.int32(12, 45, nil, 23_u64, true).should_not eq(expected)
    Parquet::Statistics.int32(12, 45, nil, 11_u64, false).should_not eq(expected)
    Parquet::Statistics.int32(12, 45, nil, 11_u64, false)
      .should_not eq(Parquet::Statistics.int64(12, 45, nil, 11_u64, false))
  end
end
