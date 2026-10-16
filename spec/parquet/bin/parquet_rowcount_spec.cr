require "../../spec_helper"

# Characterization specs for `parquet/src/bin/parquet-rowcount.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias ParquetRowCount = Parquet::Bin::ParquetRowCount

private def fixture_path(name : String) : String
  File.join(ParquetTestHelpers::FIXTURE_DIR, name)
end

describe Parquet::Bin::ParquetRowCount do
  it "prints the row count of each file" do
    io = IO::Memory.new
    ParquetRowCount.run(io, [fixture_path("alltypes_plain.parquet")])
    io.to_s.should eq("File #{fixture_path("alltypes_plain.parquet")}: rowcount=8\n")
  end

  it "prints a line per file" do
    io = IO::Memory.new
    ParquetRowCount.run(io, [
      fixture_path("alltypes_plain.parquet"),
      fixture_path("alltypes_plain.parquet"),
    ])
    io.to_s.lines.size.should eq(2)
  end
end
