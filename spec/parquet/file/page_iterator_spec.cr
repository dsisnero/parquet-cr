require "../../spec_helper"

# Ported from `parquet/src/file/serialized_reader.rs` (`test_page_iterator`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias PageIterator = Parquet::File::FilePageIterator

private def alltypes_reader : FReader
  FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")))
end

describe Parquet::File::FilePageIterator do
  # Upstream `test_page_iterator`.
  it "iterates pages of one column across all row groups" do
    iterator = PageIterator.new(0, alltypes_reader)

    page = iterator.next
    page.should_not be_a(Iterator::Stop)
    page.as(Parquet::Column::PageReader)

    iterator.next.should be_a(Iterator::Stop)
  end

  # Upstream `test_page_iterator` (row-group subset variant).
  it "iterates pages with an explicit row group list" do
    iterator = PageIterator.with_row_groups(0, (0...1).each, alltypes_reader)

    page = iterator.next
    page.should_not be_a(Iterator::Stop)
    page.as(Parquet::Column::PageReader)

    iterator.next.should be_a(Iterator::Stop)
  end

  it "rejects an out-of-bounds column index" do
    expect_raises(Parquet::IndexOutOfBoundError) do
      PageIterator.new(11, alltypes_reader)
    end
  end
end
