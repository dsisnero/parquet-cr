require "../../spec_helper"

# Ported from `parquet/src/column/reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `test_parse_v1_level_invalid_length` is ported verbatim; the remaining specs
# are characterization tests built on hand-encoded pages (the upstream
# `ColumnReaderTester`/`make_pages` random infrastructure is not ported).

private alias ColumnReader = Parquet::Column::ColumnReader
private alias ColumnReaderImpl = Parquet::Column::ColumnReaderImpl
private alias Page = Parquet::Column::Page
private alias PageReader = Parquet::Column::PageReader
private alias PType = Parquet::Type
private alias Enc = Parquet::Encoding
private alias LevelEncoder = Parquet::Encodings::LevelEncoder

private def reader_desc(physical : PType, max_def : Int16, max_rep : Int16) : Parquet::Schema::ColumnDescriptor
  ty = Parquet::Schema::Type.primitive_type_builder("col", physical).with_length(-1).build
  Parquet::Schema::ColumnDescriptor.new(ty, max_def, max_rep, Parquet::Schema::ColumnPath.new("col"))
end

private def concat_bytes(*parts : Bytes) : Bytes
  array = [] of UInt8
  parts.each { |part| array.concat(part) }
  Slice.new(array.to_unsafe, array.size).dup
end

private def encode_levels_v1(max_level : Int16, levels : Array(Int16)) : Bytes
  encoder = LevelEncoder.v1_streaming(max_level)
  encoder.put_with_observer(levels) { |_, _| }
  encoder.consume
end

private def encode_levels_v2(max_level : Int16, levels : Array(Int16)) : Bytes
  encoder = LevelEncoder.v2_streaming(max_level)
  encoder.put_with_observer(levels) { |_, _| }
  encoder.consume
end

private class TestPageReader < Parquet::Column::PageReader
  def initialize(@pages : Array(Page))
    @index = 0
  end

  def get_next_page : Page?
    return nil if @index >= @pages.size

    page = @pages[@index]
    @index += 1
    page
  end

  def peek_next_page : Parquet::Column::PageMetadata?
    return nil if @index >= @pages.size

    page = @pages[@index]
    if page.is_a?(Page::DataPageV2)
      return Parquet::Column::PageMetadata.new(page.num_rows.to_i32, page.num_values.to_i32, false)
    end
    Parquet::Column::PageMetadata.new(nil, page.num_values.to_i32, page.is_a?(Page::DictionaryPage))
  end

  def skip_next_page : Nil
    @index += 1
  end
end

describe Parquet::Column::ColumnReaderImpl do
  # Upstream `test_parse_v1_level_invalid_length`.
  it "rejects an invalid v1 level length" do
    err = expect_raises(Parquet::ParquetError) do
      Parquet::Column.parse_v1_level(1_i16, 100_u32, Enc::RLE, Bytes[10_u8, 0_u8, 0_u8, 0_u8])
    end
    err.to_s.should eq("Parquet error: not enough data to read levels")

    err = expect_raises(Parquet::ParquetError) do
      Parquet::Column.parse_v1_level(1_i16, 100_u32, Enc::RLE, Bytes[4_u8, 0_u8, 0_u8])
    end
    err.to_s.should eq("Parquet error: not enough data to read levels")
  end

  it "reads required PLAIN v1 values" do
    descr = reader_desc(PType::INT32, 0_i16, 0_i16)
    values_buf = Parquet::Encodings::PlainEncoder(Int32).encode_all([1_i32, 2_i32, 3_i32])
    page = Page::DataPage.new(values_buf, 3_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, nil)

    reader = ColumnReaderImpl(Int32).new(descr, TestPageReader.new([page] of Parquet::Column::Page))
    values = [] of Int32
    reader.read_records(3, nil, nil, values).should eq({3, 3, 3})
    values.should eq([1_i32, 2_i32, 3_i32])
  end

  it "reads optional PLAIN v1 values with definition levels" do
    descr = reader_desc(PType::INT32, 1_i16, 0_i16)
    def_buf = encode_levels_v1(1_i16, [1_i16, 0_i16, 1_i16, 1_i16, 0_i16])
    values_buf = Parquet::Encodings::PlainEncoder(Int32).encode_all([10_i32, 20_i32, 30_i32])
    page = Page::DataPage.new(concat_bytes(def_buf, values_buf), 5_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, nil)

    reader = ColumnReaderImpl(Int32).new(descr, TestPageReader.new([page] of Parquet::Column::Page))
    def_levels = [] of Int16
    values = [] of Int32
    reader.read_records(5, def_levels, nil, values).should eq({5, 3, 5})
    def_levels.should eq([1_i16, 0_i16, 1_i16, 1_i16, 0_i16])
    values.should eq([10_i32, 20_i32, 30_i32])
  end

  it "reads repeated PLAIN v1 values with repetition and definition levels" do
    descr = reader_desc(PType::INT32, 1_i16, 1_i16)
    rep_buf = encode_levels_v1(1_i16, [0_i16, 1_i16, 1_i16, 0_i16, 1_i16])
    def_buf = encode_levels_v1(1_i16, [1_i16, 1_i16, 1_i16, 1_i16, 1_i16])
    values_buf = Parquet::Encodings::PlainEncoder(Int32).encode_all([1_i32, 2_i32, 3_i32, 4_i32, 5_i32])
    page = Page::DataPage.new(concat_bytes(rep_buf, def_buf, values_buf), 5_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, nil)

    reader = ColumnReaderImpl(Int32).new(descr, TestPageReader.new([page] of Parquet::Column::Page))
    def_levels = [] of Int16
    rep_levels = [] of Int16
    values = [] of Int32
    reader.read_records(2, def_levels, rep_levels, values).should eq({2, 5, 5})
    rep_levels.should eq([0_i16, 1_i16, 1_i16, 0_i16, 1_i16])
    values.should eq([1_i32, 2_i32, 3_i32, 4_i32, 5_i32])
  end

  it "reads DATA_PAGE_V2 values with definition levels" do
    descr = reader_desc(PType::INT32, 1_i16, 0_i16)
    def_buf = encode_levels_v2(1_i16, [1_i16, 0_i16, 1_i16])
    values_buf = Parquet::Encodings::PlainEncoder(Int32).encode_all([7_i32, 8_i32])
    buf = concat_bytes(def_buf, values_buf)
    page = Page::DataPageV2.new(buf, 3_u32, Enc::PLAIN, 1_u32, 3_u32, def_buf.size.to_u32, 0_u32, true, nil)

    reader = ColumnReaderImpl(Int32).new(descr, TestPageReader.new([page] of Parquet::Column::Page))
    def_levels = [] of Int16
    values = [] of Int32
    reader.read_records(3, def_levels, nil, values).should eq({3, 2, 3})
    def_levels.should eq([1_i16, 0_i16, 1_i16])
    values.should eq([7_i32, 8_i32])
  end

  it "skips records" do
    descr = reader_desc(PType::INT32, 0_i16, 0_i16)
    values_buf = Parquet::Encodings::PlainEncoder(Int32).encode_all([1_i32, 2_i32, 3_i32, 4_i32, 5_i32])
    page = Page::DataPage.new(values_buf, 5_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, nil)

    reader = ColumnReaderImpl(Int32).new(descr, TestPageReader.new([page] of Parquet::Column::Page))
    reader.skip_records(2).should eq(2)

    values = [] of Int32
    reader.read_records(3, nil, nil, values).should eq({3, 3, 3})
    values.should eq([3_i32, 4_i32, 5_i32])
  end

  it "dispatches via get_column_reader/get_typed_column_reader" do
    descr = reader_desc(PType::INT32, 0_i16, 0_i16)
    values_buf = Parquet::Encodings::PlainEncoder(Int32).encode_all([9_i32])
    page = Page::DataPage.new(values_buf, 1_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, nil)

    generic = Parquet::Column.get_column_reader(descr, TestPageReader.new([page] of Parquet::Column::Page))
    generic.should be_a(ColumnReader)
    typed = Parquet::Column.get_typed_column_reader(Int32, generic)
    values = [] of Int32
    typed.read_records(1, nil, nil, values).should eq({1, 1, 1})
    values.should eq([9_i32])

    expect_raises(Parquet::ParquetError, "Failed to convert column reader") do
      Parquet::Column.get_typed_column_reader(Int64, generic)
    end
  end
end
