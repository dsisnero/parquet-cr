require "../../spec_helper"

# Ported from `parquet/src/file/serialized_reader.rs`
# (`SerializedPageReaderState::Pages`, `peek_next_page_offset`,
# `test_peek_next_page_offset_matches_actual`, and the `Iterator` impl)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Reading pages through the offset index requires `ReadOptionsBuilder#with_page_index`
# (upstream's arrow layer enables the page index; its core `SerializedFileReader`
# always passes `None`). `alltypes_tiny_pages.parquet` has 325+ pages per column
# and dictionary pages, `alltypes_plain.parquet` has no offset index, so it
# exercises the offset-stream state.
#
# Divergence: `peek_next_page_offset` is `#[cfg(test)]`-only upstream; the port
# exposes it (same as the CDC chunker's test-facing helpers).
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias Page = Parquet::Column::Page

private def reader_for_pages(file_name : String, column : Int32, row_group : Int32 = 0, page_index : Bool = true) : Parquet::Column::PageReader
  builder = Parquet::File::ReadOptionsBuilder.new
  builder = builder.with_page_index if page_index
  reader = FReader.new_with_options(
    BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)),
    builder.build
  )
  reader.get_row_group(row_group).get_column_page_reader(column)
end

private def indexed_reader(file_name : String) : FReader
  FReader.new_with_options(
    BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)),
    Parquet::File::ReadOptionsBuilder.new.with_page_index.build
  )
end

private def collect_pages(reader : Parquet::Column::PageReader) : Array(Page)
  pages = [] of Page
  while page = reader.get_next_page
    pages << page
  end
  pages
end

describe Parquet::File::SerializedPageReader do
  # The offset-index state must produce exactly the pages the offset-stream state
  # produces (same page types, value counts, and decoded buffers).
  it "reads the same pages through the offset index and the offset stream" do
    file_name = "alltypes_tiny_pages.parquet"
    indexed = indexed_reader(file_name)
    streamed = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))

    [0, 2, 8].each do |column|
      from_index = collect_pages(indexed.get_row_group(0).get_column_page_reader(column))
      from_stream = collect_pages(streamed.get_row_group(0).get_column_page_reader(column))

      from_index.size.should eq(from_stream.size)
      from_index.size.should be > 1
      from_index.zip(from_stream).each do |(indexed_page, streamed_page)|
        indexed_page.page_type.should eq(streamed_page.page_type)
        indexed_page.num_values.should eq(streamed_page.num_values)
        indexed_page.buffer.should eq(streamed_page.buffer)
      end
    end
  end

  # A column chunk whose first data page does not start at the chunk start has a
  # dictionary page in front of it; the offset-index state synthesizes the
  # dictionary location from the chunk's byte range.
  it "reads the dictionary page before the indexed data pages" do
    reader = indexed_reader("alltypes_tiny_pages.parquet")
    column = 2
    chunk = reader.metadata.row_group(0).column(column)
    locations = reader.metadata.page_index.not_nil!.page_locations(0, column).not_nil!
    chunk_start = (chunk.dictionary_page_offset || chunk.data_page_offset).to_u64
    locations.first.offset.to_u64.should be > chunk_start

    page_reader = reader.get_row_group(0).get_column_page_reader(column)
    page_reader.peek_next_page.not_nil!.is_dict.should be_true
    page_reader.peek_next_page_offset.should eq(chunk_start)

    page_reader.get_next_page.not_nil!.should be_a(Page::DictionaryPage)
    page_reader.peek_next_page.not_nil!.is_dict.should be_false
    page_reader.peek_next_page_offset.should eq(locations.first.offset.to_u64)
  end

  # `PageMetadata#num_rows` for indexed pages is derived from the
  # `first_row_index` deltas (and the chunk's total row count for the last page).
  it "reports row counts for pages from the offset index" do
    reader = indexed_reader("alltypes_tiny_pages.parquet")
    column = 0 # no dictionary page: 325 data pages
    metadata = reader.metadata
    locations = metadata.page_index.not_nil!.page_locations(0, column).not_nil!
    total_rows = metadata.row_group(0).num_rows

    page_reader = reader.get_row_group(0).get_column_page_reader(column)
    locations.size.times do |i|
      page_metadata = page_reader.peek_next_page.not_nil!
      page_metadata.is_dict.should be_false

      next_row_index = i + 1 < locations.size ? locations[i + 1].first_row_index : total_rows
      page_metadata.num_rows.should eq((next_row_index - locations[i].first_row_index).to_i32)
      page_reader.get_next_page.should_not be_nil
    end

    page_reader.peek_next_page.should be_nil
    page_reader.get_next_page.should be_nil
  end

  # Upstream `test_peek_next_page_offset_matches_actual`: every peeked offset
  # identifies the next page, peeking is idempotent, and no offset repeats.
  it "peeks page offsets that match the pages actually read" do
    ["alltypes_plain.parquet", "alltypes_tiny_pages.parquet"].each do |file_name|
      reader =
        if file_name == "alltypes_plain.parquet"
          FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))
        else
          indexed_reader(file_name)
        end

      metadata = reader.metadata
      offsets = Set(UInt64).new
      pages_read = 0
      metadata.num_row_groups.times do |rg|
        metadata.row_group(rg).num_columns.times do |column|
          page_reader = reader.get_row_group(rg).get_column_page_reader(column)

          while (offset = page_reader.peek_next_page_offset)
            # Peeking again must report the same page.
            page_reader.peek_next_page_offset.should eq(offset)
            page_reader.get_next_page.should_not be_nil
            offsets << offset
            pages_read += 1
          end

          page_reader.peek_next_page_offset.should be_nil
        end
      end

      pages_read.should be > 0
      offsets.size.should eq(pages_read)
    end
  end

  # For the offset-index state the peeked offsets follow the synthesized
  # dictionary page, then the recorded page locations in order.
  it "peeks indexed page offsets in page-location order" do
    reader = indexed_reader("alltypes_tiny_pages.parquet")
    [0, 2].each do |column|
      chunk = reader.metadata.row_group(0).column(column)
      locations = reader.metadata.page_index.not_nil!.page_locations(0, column).not_nil!
      chunk_start = (chunk.dictionary_page_offset || chunk.data_page_offset).to_u64

      # `alltypes_tiny_pages.parquet` does not set `dictionary_page_offset`, so
      # the dictionary page is the un-indexed gap before the first data page.
      expected = [] of UInt64
      expected << chunk_start if locations.first.offset.to_u64 != chunk_start
      locations.each { |location| expected << location.offset.to_u64 }

      page_reader = reader.get_row_group(0).get_column_page_reader(column)
      actual = [] of UInt64
      while offset = page_reader.peek_next_page_offset
        actual << offset
        page_reader.get_next_page.should_not be_nil
      end

      actual.should eq(expected)
    end
  end

  # `skip_next_page` must advance the location queue (and consume the dictionary
  # page first) so the following reads stay in sync.
  it "skips pages through the offset index" do
    file_name = "alltypes_tiny_pages.parquet"
    column = 2
    indexed = indexed_reader(file_name)
    streamed = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))

    indexed_reader_pages = indexed.get_row_group(0).get_column_page_reader(column)
    streamed_reader_pages = streamed.get_row_group(0).get_column_page_reader(column)

    # Skip the dictionary page and the first data page on both readers.
    2.times do
      indexed_reader_pages.skip_next_page
      streamed_reader_pages.skip_next_page
    end

    indexed_page = indexed_reader_pages.get_next_page.not_nil!
    streamed_page = streamed_reader_pages.get_next_page.not_nil!
    indexed_page.page_type.should eq(streamed_page.page_type)
    indexed_page.num_values.should eq(streamed_page.num_values)
    indexed_page.buffer.should eq(streamed_page.buffer)
  end

  # Upstream returns `true` unconditionally for the offset-index state.
  it "reports record boundaries for indexed pages" do
    reader = reader_for_pages("alltypes_tiny_pages.parquet", 0)
    reader.peek_next_page.should_not be_nil
    reader.at_record_boundary.should be_true
  end

  # The row iterator goes through `get_column_page_reader`, so enabling the page
  # index switches it onto the offset-index state end to end.
  it "reads the same rows with and without the page index" do
    file_name = "alltypes_tiny_pages.parquet"
    indexed_rows = indexed_reader(file_name).get_row_iter(nil).to_a
    streamed_rows = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))
      .get_row_iter(nil).to_a

    indexed_rows.size.should eq(7300)
    indexed_rows.should eq(streamed_rows)
  end

  # Upstream `impl Iterator for SerializedPageReader`.
  it "iterates over the pages of a column chunk" do
    reader = indexed_reader("alltypes_tiny_pages.parquet")
    page_reader = reader.get_row_group(0).get_column_page_reader(0)

    counted = 0
    page_reader.each { |_page| counted += 1 }
    counted.should eq(reader.metadata.page_index.not_nil!.page_locations(0, 0).not_nil!.size)
    page_reader.next.should be(Iterator::Stop::INSTANCE)
    page_reader.to_a.should be_empty
  end
end
