require "../../spec_helper"

# Characterization specs for `parquet/src/bin/parquet-read.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias ParquetRead = Parquet::Bin::ParquetRead
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader

private def alltypes_reader : FReader
  FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")))
end

describe Parquet::Bin::ParquetRead do
  it "prints all rows" do
    io = IO::Memory.new
    ParquetRead.run(io, alltypes_reader, 0, false)
    lines = io.to_s.lines
    lines.size.should eq(8)
    lines[0].should start_with("{id: 4, bool_col: true")
  end

  it "limits the number of records" do
    io = IO::Memory.new
    ParquetRead.run(io, alltypes_reader, 2, false)
    io.to_s.lines.size.should eq(2)
  end

  it "prints rows as JSON" do
    io = IO::Memory.new
    ParquetRead.run(io, alltypes_reader, 2, true)
    lines = io.to_s.lines
    lines.size.should eq(2)
    JSON.parse(lines[0])["id"].should eq(4)
  end
end
