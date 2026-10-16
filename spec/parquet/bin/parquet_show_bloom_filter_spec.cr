require "../../spec_helper"

# Characterization specs for `parquet/src/bin/parquet-show-bloom-filter.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias ShowBloom = Parquet::Bin::ParquetShowBloomFilter

private def fixture_path(name : String) : String
  File.join(ParquetTestHelpers::FIXTURE_DIR, name)
end

describe Parquet::Bin::ParquetShowBloomFilter do
  it "reports present and absent values" do
    io = IO::Memory.new
    ShowBloom.run(io, fixture_path("data_index_bloom_encoding_stats.parquet"), "String", ["Hello", "World"])
    output = io.to_s
    output.should contain("Row group #0\n")
    output.should contain("Value Hello is present in bloom filter\n")
    output.should contain("Value World is absent in bloom filter\n")
  end

  it "reports a missing column with candidates" do
    io = IO::Memory.new
    ShowBloom.run(io, fixture_path("data_index_bloom_encoding_stats.parquet"), "nope", ["Hello"])
    io.to_s.should contain("No column named nope found, candidate columns are: String")
  end

  it "rejects an invalid column path" do
    expect_raises(Parquet::GeneralError, /Invalid escape sequence/) do
      ShowBloom.run(IO::Memory.new, fixture_path("data_index_bloom_encoding_stats.parquet"), %q(a.\b), ["x"])
    end
  end
end
