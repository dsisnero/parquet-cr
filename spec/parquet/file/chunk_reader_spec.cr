require "../../spec_helper"

# Characterizations of File's ChunkReader implementation in pinned
# parquet/src/file/reader.rs: clone the handle, seek, then buffer reads.
private class TrackedFileChunkReader < Parquet::File::FileChunkReader
  getter streams = [] of IO

  def get_read(start : UInt64) : IO
    stream = super
    @streams << stream
    stream
  end
end

private def with_chunk_file(prefix : String, suffix : String, & : File ->)
  file = File.tempfile(prefix, suffix)
  begin
    yield file
  ensure
    file.close
    File.delete(file.path) if File.exists?(file.path)
  end
end

describe Parquet::File::FileChunkReader do
  it "streams a sparse file beyond Int32::MAX without allocating the tail" do
    with_chunk_file("parquet-large-reader-", ".bin") do |file|
      marker_offset = Int32::MAX.to_i64 + 1_i64
      file.seek(marker_offset)
      file.write_byte(42_u8)
      file.flush
      reader = Parquet::File::FileChunkReader.new(file)
      stream = reader.get_read(0_u64)
      begin
        stream.read_byte.should eq(0_u8)
      ensure
        stream.close
      end
      stream = reader.get_read(marker_offset.to_u64)
      begin
        stream.read_byte.should eq(42_u8)
        stream.read_byte.should be_nil
      ensure
        stream.close
      end
    end
  end

  it "reads successive buffers through EOF" do
    with_chunk_file("parquet-stream-", ".bin") do |file|
      bytes = Bytes.new(20_000) { |index| (index % 251).to_u8 }
      file.write(bytes)
      file.flush
      stream = Parquet::File::FileChunkReader.new(file).get_read(3_u64)
      begin
        stream.read(Bytes.empty).should eq(0)
        first = Bytes.new(9_000)
        rest = Bytes.new(bytes.size - 3 - first.size)
        stream.read_fully(first)
        stream.read_fully(rest)
        first.should eq(bytes[3, first.size])
        rest.should eq(bytes[3 + first.size, rest.size])
        stream.read_byte.should be_nil
      ensure
        stream.close
      end
    end
  end

  it "allows seeks at and beyond EOF like upstream File" do
    with_chunk_file("parquet-eof-", ".bin") do |file|
      file.write(Bytes[10, 20, 30, 40])
      file.flush
      reader = Parquet::File::FileChunkReader.new(file)
      [4_u64, 5_u64].each do |offset|
        stream = reader.get_read(offset)
        begin
          stream.read_byte.should be_nil
        ensure
          stream.close
        end
      end
    end
  end

  it "shares the underlying seek position between cloned handles" do
    with_chunk_file("parquet-shared-position-", ".bin") do |file|
      file.write(Bytes[10, 20, 30, 40])
      file.flush
      reader = Parquet::File::FileChunkReader.new(file)
      first = reader.get_read(0_u64)
      second = reader.get_read(2_u64)
      begin
        # Neither reader has buffered data yet; the second seek affects both.
        first.read_byte.should eq(30_u8)
      ensure
        first.close
        second.close
      end
      file.closed?.should be_false
    end
  end

  it "keeps a cloned reader usable after closing the original handle" do
    with_chunk_file("parquet-owned-handle-", ".bin") do |file|
      file.write(Bytes[10, 20])
      file.flush
      stream = Parquet::File::FileChunkReader.new(file).get_read(0_u64)
      begin
        file.close
        stream.read_byte.should eq(10_u8)
        stream.read_byte.should eq(20_u8)
      ensure
        stream.close
      end
    end
  end

  it "does not reopen the source by path" do
    with_chunk_file("parquet-unlinked-", ".bin") do |file|
      file.write(Bytes[42])
      file.flush
      File.delete(file.path)
      stream = Parquet::File::FileChunkReader.new(file).get_read(0_u64)
      begin
        stream.read_byte.should eq(42_u8)
      ensure
        stream.close
      end
    end
  end

  it "closes block-scoped readers on success and exceptions" do
    with_chunk_file("parquet-block-reader-", ".bin") do |file|
      file.write(Bytes[42])
      file.flush
      source = TrackedFileChunkReader.new(file)
      value = source.get_read(0_u64) { |stream| stream.read_byte }
      value.should eq(42_u8)
      source.streams.each { |stream| stream.closed?.should be_true }
      expect_raises(IO::Error, "block failed") do
        source.get_read(0_u64) { raise IO::Error.new("block failed") }
      end
      source.streams.size.should eq(2)
      source.streams.each { |stream| stream.closed?.should be_true }
      file.closed?.should be_false
    end
  end

  it "releases page-header streams after reading, peeking and skipping" do
    with_chunk_file("parquet-page-stream-", ".parquet") do |file|
      file.write(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet"))
      file.flush
      source = TrackedFileChunkReader.new(file)
      reader = Parquet::File::SerializedFileReader.new(source)
      4.times do |operation|
        pages = reader.get_row_group(0).get_column_page_reader(0)
        case operation
        when 0 then pages.get_next_page.should_not be_nil
        when 1 then pages.peek_next_page.should_not be_nil
        when 2 then pages.skip_next_page
        when 3
          pages.as(Parquet::File::SerializedPageReader).peek_next_page_offset.should_not be_nil
        end
        source.streams.should_not be_empty
        source.streams.each { |stream| stream.closed?.should be_true }
      end
      file.closed?.should be_false
    end
  end
  it "releases the page-header stream when parsing fails" do
    with_chunk_file("parquet-invalid-page-", ".parquet") do |file|
      file.write(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet"))
      file.flush
      source = TrackedFileChunkReader.new(file)
      reader = Parquet::File::SerializedFileReader.new(source)
      pages = reader.get_row_group(0).get_column_page_reader(0)
      file.truncate(0)
      expect_raises(Parquet::ParquetError) { pages.get_next_page }
      source.streams.should_not be_empty
      source.streams.each { |stream| stream.closed?.should be_true }
    end
  end

  it "releases spliced column streams on success and failure" do
    with_chunk_file("parquet-splice-stream-", ".parquet") do |file|
      file.write(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet"))
      file.flush
      source = TrackedFileChunkReader.new(file)
      reader = Parquet::File::SerializedFileReader.new(source)
      metadata = reader.metadata
      output = IO::Memory.new
      writer = Parquet::File::SerializedFileWriter.new(
        output, metadata.file_metadata.schema, Parquet::File::Properties::WriterProperties.default)
      group = writer.next_row_group
      results = metadata.row_group(0).columns.map do |column|
        Parquet::Column::Writer::ColumnCloseResult.new(
          column.compressed_size.to_u64, metadata.row_group(0).num_rows.to_u64, column)
      end
      results.each { |result| group.append_column(source, result) }
      source.streams.each { |stream| stream.closed?.should be_true }
      expect_raises(Parquet::GeneralError, "exhausted columns") do
        group.append_column(source, results[0])
      end
      source.streams.each { |stream| stream.closed?.should be_true }
      group.close
      writer.close
      copied = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(output.to_slice))
      copied.metadata.file_metadata.num_rows.should eq(metadata.file_metadata.num_rows)
      file.closed?.should be_false
    end
  end
end
