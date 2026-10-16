require "../../spec_helper"

# Characterization specs for `parquet/src/bin/parquet-schema.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias ParquetSchema = Parquet::Bin::ParquetSchema

private def fixture_path(name : String) : String
  File.join(ParquetTestHelpers::FIXTURE_DIR, name)
end

describe Parquet::Bin::ParquetSchema do
  it "prints the schema (non-verbose)" do
    path = fixture_path("alltypes_plain.parquet")
    io = IO::Memory.new
    ParquetSchema.run(io, path, false)
    output = io.to_s
    output.should start_with("Metadata for file: #{path}\n\n")
    output.should contain("version: 1\n")
    output.should contain("num of rows: 8\n")
    output.should contain("created by: impala version")
    output.should contain("message schema {")
  end

  it "prints full metadata (verbose)" do
    io = IO::Memory.new
    ParquetSchema.run(io, fixture_path("alltypes_plain.parquet"), true)
    output = io.to_s
    output.should contain("num of row groups: 1\n")
    output.should contain("row groups:\n")
    output.should contain("column 0:")
    output.should contain("column type: INT32")
    output.should contain("column path: \"id\"")
  end
end
