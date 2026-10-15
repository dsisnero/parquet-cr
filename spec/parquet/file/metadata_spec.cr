require "../../spec_helper"

# Ported from `parquet/src/file/metadata/mod.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
alias PLH = Parquet::File::Metadata::LevelHistogram

describe Parquet::File::Metadata::LevelHistogram do
  # Upstream `test_level_histogram_update_from_levels_compat`.
  it "updates from levels" do
    histogram = PLH.try_new(2_i16).as(PLH)
    histogram.update_from_levels([0_i16, 2_i16, 1_i16, 2_i16, 2_i16])
    histogram.values.should eq([1_i64, 1_i64, 3_i64])
  end

  # Characterization of the remaining `LevelHistogram` API.
  it "constructs, reads, aggregates, and resets" do
    PLH.try_new(0_i16).should be_nil
    PLH.try_new(-1_i16).should be_nil
    PLH.try_new(1_i16).as(PLH).values.should eq([0_i64, 0_i64])

    histogram = PLH.from([1_i64, 2_i64, 3_i64])
    histogram.len.should eq(3)
    histogram.empty?.should be_false
    histogram.get(1).should eq(2_i64)
    histogram.get(3).should be_nil

    histogram.increment_by(0_i16, 10_i64)
    histogram.values.should eq([11_i64, 2_i64, 3_i64])

    histogram.add(PLH.from([1_i64, 1_i64, 1_i64]))
    histogram.values.should eq([12_i64, 3_i64, 4_i64])

    histogram.reset
    histogram.values.should eq([0_i64, 0_i64, 0_i64])

    PLH.from([5_i64, 6_i64]).into_inner.should eq([5_i64, 6_i64])
    PLH.from([0_i64]).empty?.should be_false
    PLH.from([] of Int64).empty?.should be_true
  end
end
