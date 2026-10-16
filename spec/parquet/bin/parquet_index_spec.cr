require "../../spec_helper"

# Characterization specs for `parquet/src/bin/parquet-index.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for this binary; the `run` logic is characterized
# here over `alltypes_tiny_pages.parquet` (which carries page indexes).

private alias ParquetIndex = Parquet::Bin::ParquetIndex
private alias PageLocation = Parquet::File::PageIndex::PageLocation
private alias BytesCR = Parquet::File::BytesChunkReader

private def tiny_pages_reader : BytesCR
  BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages.parquet"))
end

describe Parquet::Bin::ParquetIndex do
  # Upstream `compute_row_counts`.
  describe ".compute_row_counts" do
    it "returns empty for no pages" do
      ParquetIndex.compute_row_counts([] of PageLocation, 0_i64).should eq([] of Int64)
    end

    it "computes per-page row counts" do
      locations = [
        PageLocation.new(0_i64, 10_i32, 0_i64),
        PageLocation.new(10_i64, 10_i32, 5_i64),
        PageLocation.new(20_i64, 10_i32, 12_i64),
      ]
      ParquetIndex.compute_row_counts(locations, 20_i64).should eq([5_i64, 7_i64, 8_i64])
    end

    it "computes a single-page count from the first row index" do
      locations = [PageLocation.new(0_i64, 10_i32, 3_i64)]
      ParquetIndex.compute_row_counts(locations, 10_i64).should eq([7_i64])
    end
  end

  # Upstream `Args::run` (characterized).
  it "prints the page index of a column chunk" do
    io = IO::Memory.new
    ParquetIndex.run_with_reader(io, tiny_pages_reader, "id")
    output = io.to_s
    output.should contain("Row Group: 0\n")
    output.should contain(
      "Page     0 at offset          4 with length        109 and row count         21, min        122, max        142\n"
    )
    output.should contain(
      "Page     1 at offset        113 with length        109 and row count         21, min         98, max        145\n"
    )
  end

  it "errors for a missing column" do
    expect_raises(Parquet::GeneralError, /Failed to find column/) do
      ParquetIndex.run_with_reader(IO::Memory.new, tiny_pages_reader, "does.not.exist")
    end
  end
end
