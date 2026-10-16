require "../../spec_helper"

# Characterization specs for `parquet/src/bin/parquet-concat.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for this binary; the splice path
# (`SerializedRowGroupWriter#append_column`) and `Args::run` are characterized
# here over `alltypes_plain.parquet`.

private alias ParquetConcat = Parquet::Bin::ParquetConcat
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader

private def alltypes_bytes : Bytes
  ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
end

describe Parquet::Bin::ParquetConcat do
  it "concatenates two files with identical schemas" do
    io = IO::Memory.new
    ParquetConcat.concat(io, [
      BytesCR.new(alltypes_bytes),
      BytesCR.new(alltypes_bytes),
    ] of Parquet::File::ChunkReader)

    reader = FReader.new(BytesCR.new(io.to_slice))
    reader.metadata.num_row_groups.should eq(2)
    reader.metadata.file_metadata.num_rows.should eq(16)
    ids = reader.get_row_iter(nil).flat_map { |row| [row.get_int(0)] }.to_a
    ids.should eq([4, 5, 6, 7, 2, 3, 0, 1] * 2)
  end

  it "requires at least one input file" do
    expect_raises(Parquet::GeneralError, /Must provide at least one input file/) do
      ParquetConcat.concat(IO::Memory.new, [] of Parquet::File::ChunkReader)
    end
  end
end
