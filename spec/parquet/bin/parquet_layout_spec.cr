require "../../spec_helper"

# Characterization specs for `parquet/src/bin/parquet-layout.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias ParquetLayout = Parquet::Bin::ParquetLayout

private def fixture_path(name : String) : String
  File.join(ParquetTestHelpers::FIXTURE_DIR, name)
end

describe Parquet::Bin::ParquetLayout do
  it "prints the physical layout as JSON" do
    io = IO::Memory.new
    ParquetLayout.run(io, fixture_path("alltypes_plain.parquet"))
    json = JSON.parse(io.to_s)

    row_groups = json["row_groups"].as_a
    row_groups.size.should eq(1)
    row_groups[0]["row_count"].should eq(8)

    columns = row_groups[0]["columns"].as_a
    columns.size.should eq(11)
    columns[0]["path"].should eq("id")
    columns[0]["compression"].should eq("UNCOMPRESSED")
    columns[0]["encodings"].as_a.should_not be_empty
    columns[0]["has_offset_index"].should eq(false)

    json["footer"]["metadata_size"].as_i.should be > 0
  end
end
