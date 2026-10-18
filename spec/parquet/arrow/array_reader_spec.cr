require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/mod.rs` (`ArrayReader`,
# `RowGroups`) and `parquet/src/arrow/array_reader/empty_array.rs`
# (`EmptyArrayReader`, `make_empty_array_reader`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `empty_array.rs` has no upstream unit tests (it is exercised through the
# `arrow_reader` tests, which need the record reader), so these are
# characterization specs of the documented contract.
private alias ParquetArrow = Parquet::Arrow
private alias AR = ParquetArrow::ArrayReader
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader

describe Parquet::Arrow::EmptyArrayReader do
  it "reports an empty struct data type" do
    reader = ParquetArrow.make_empty_array_reader(5)
    reader.data_type.should eq(Arrow::DataType.struct(Arrow::Fields.empty))
    reader.def_levels.should be_nil
    reader.rep_levels.should be_nil
  end

  it "reads at most the remaining rows" do
    reader = ParquetArrow.make_empty_array_reader(5)
    reader.read_records(3).should eq(3)
    reader.read_records(3).should eq(2)
    reader.read_records(3).should eq(0)
  end

  it "consumes the buffered rows as a struct array" do
    reader = ParquetArrow.make_empty_array_reader(5)
    reader.read_records(3)
    batch = reader.consume_batch
    batch.data_type.should eq(Arrow::DataType.struct(Arrow::Fields.empty))
    batch.len.should eq(3)

    # Consuming resets the pending count.
    reader.read_records(3)
    reader.consume_batch.len.should eq(2)
    reader.consume_batch.len.should eq(0)
  end

  it "skips at most the remaining rows" do
    reader = ParquetArrow.make_empty_array_reader(4)
    reader.skip_records(3).should eq(3)
    reader.skip_records(3).should eq(1)
    reader.skip_records(3).should eq(0)
  end

  # Upstream `ArrayReader::next_batch` (read then consume).
  it "supports next_batch" do
    reader = ParquetArrow.make_empty_array_reader(2)
    reader.next_batch(10).len.should eq(2)
    reader.next_batch(10).len.should eq(0)
  end
end

describe Parquet::Arrow::RowGroups do
  it "adapts a file reader" do
    reader = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")))
    groups = ParquetArrow::FileRowGroups.new(reader)

    groups.num_rows.should eq(reader.metadata.file_metadata.num_rows)
    groups.metadata.should eq(reader.metadata)
    groups.row_groups.to_a.size.should eq(reader.metadata.num_row_groups)

    pages = groups.column_chunks(0).to_a
    pages.size.should be >= 1
    pages.each { |page_reader| page_reader.should be_a(Parquet::Column::PageReader) }
  end
end
