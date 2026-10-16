require "../../spec_helper"

# Ported from `parquet/src/file/page_index/offset_index.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

describe Parquet::File::PageIndex::OffsetIndexMetaData do
  # Upstream `test_offset_idx_roundtrip`.
  it "round-trips an offset index through thrift" do
    page_locations = [
      PPI::PageLocation.new(0_i64, 10, 0_i64),
      PPI::PageLocation.new(10_i64, 20, 100_i64),
    ]
    unenc = [0_i64, 100_i64]

    ThriftRoundTrip.roundtrip(PPI::OffsetIndexMetaData.new(page_locations, unenc))
      .should eq(PPI::OffsetIndexMetaData.new(page_locations, unenc))

    ThriftRoundTrip.roundtrip(PPI::OffsetIndexMetaData.new(page_locations, nil))
      .should eq(PPI::OffsetIndexMetaData.new(page_locations, nil))
  end
end
