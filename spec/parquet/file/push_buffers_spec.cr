require "../../spec_helper"

# Ported from `parquet/src/util/push_buffers.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe Parquet::Util::PushBuffers do
  # Upstream `push_range_accepts_matching_length`.
  it "accepts a range whose length matches the buffer" do
    buffers = Parquet::Util::PushBuffers.new(100_u64)
    buffers.push_range(10_u64...14_u64, "abcd".to_slice)
    buffers.has_range?(10_u64...14_u64).should be_true
  end

  # Upstream `push_range_rejects_short_buffer`.
  it "rejects a buffer shorter than its range" do
    buffers = Parquet::Util::PushBuffers.new(100_u64)
    err = expect_raises(Parquet::ParquetError) do
      buffers.push_range(10_u64...20_u64, "abcd".to_slice)
    end
    err.to_s.should eq("Parquet error: Buffer length (4) does not match length (10) of range 10..20")
    buffers.has_range?(10_u64...20_u64).should be_false
  end

  # Upstream `push_ranges_rejects_mismatched_counts`.
  it "rejects mismatched range and buffer counts" do
    buffers = Parquet::Util::PushBuffers.new(100_u64)
    err = expect_raises(Parquet::ParquetError) do
      buffers.push_ranges([0_u64...4_u64, 4_u64...8_u64], ["abcd".to_slice])
    end
    err.to_s.should eq("Parquet error: Number of ranges (2) must match number of buffers (1)")
  end

  it "returns bytes for a covered range and raises otherwise" do
    buffers = Parquet::Util::PushBuffers.new(100_u64)
    buffers.push_range(10_u64...14_u64, "abcd".to_slice)

    buffers.get_bytes(10_u64, 4).should eq("abcd".to_slice)
    expect_raises(Parquet::NeedMoreDataRangeError) { buffers.get_bytes(0_u64, 4) }
  end

  it "clears all ranges" do
    buffers = Parquet::Util::PushBuffers.new(100_u64)
    buffers.push_range(0_u64...4_u64, "abcd".to_slice)
    buffers.clear_all_ranges
    buffers.has_range?(0_u64...4_u64).should be_false
  end
end
