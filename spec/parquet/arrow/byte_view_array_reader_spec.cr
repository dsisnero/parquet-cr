require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/byte_view_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `Utf8View`/`BinaryView` byte array reader.
#
# Divergence (documented in `plans/parity.md`): values are decoded through the
# existing `ColumnValueDecoderImpl(ByteArray)` and converted into a byte-view
# array at `consume_batch` time (via arrow-cr's view builders), instead of
# upstream's `ByteViewArrayColumnValueDecoder` writing `ViewBuffer` views while
# decoding.
private alias PArrow = Parquet::Arrow
private alias PageUtil = ParquetTestPageUtil
private alias ARUtil = ParquetTestArrayReaderUtil

private VIEW_UTF8_COLUMN     = "message test_schema { REQUIRED BINARY leaf (STRING); }"
private VIEW_BINARY_COLUMN   = "message test_schema { REQUIRED BINARY leaf; }"
private VIEW_NULLABLE_COLUMN = "message test_schema { OPTIONAL BINARY leaf (STRING); }"

private def view_pages(values : Array(String), encoding : Parquet::Encoding,
                       def_levels : Array(Int16)? = nil,
                       message_type : String = VIEW_UTF8_COLUMN) : Array(Parquet::Column::Page)
  desc = PageUtil.column_descriptor(message_type)
  bytes = values.map { |value| Parquet::ByteArray.new(value) }
  num_values = def_levels.try(&.size) || values.size
  builder = PageUtil::DataPageBuilderImpl.new(desc, num_values.to_u32, true)
  builder.add_def_levels(desc.max_def_level, def_levels.not_nil!) if def_levels
  builder.add_values(encoding, bytes)
  [builder.consume]
end

private def view_reader(pages : Array(Parquet::Column::Page),
                        arrow_type : Arrow::DataType? = nil,
                        message_type : String = VIEW_UTF8_COLUMN) : PArrow::ArrayReader
  desc = PageUtil.column_descriptor(message_type)
  PArrow.make_byte_view_array_reader(
    PageUtil::InMemoryPageIterator.new([pages]), desc, arrow_type, 1024, nil
  )
end

describe Parquet::Arrow::ByteViewArrayReader do
  it "defaults to Utf8View for UTF-8 columns" do
    reader = view_reader(view_pages(%w[a], Parquet::Encoding::PLAIN))
    reader.data_type.should eq(Arrow::DataType.utf8_view)
  end

  it "defaults to BinaryView for plain binary columns" do
    reader = view_reader(
      view_pages(%w[a], Parquet::Encoding::PLAIN, message_type: VIEW_BINARY_COLUMN),
      message_type: VIEW_BINARY_COLUMN
    )
    reader.data_type.should eq(Arrow::DataType.binary_view)
  end

  it "reads plain values, inlining short ones and referencing long blocks" do
    values = ["hello", "world", "large payload over 12 bytes", "b"]
    reader = view_reader(view_pages(values, Parquet::Encoding::PLAIN))

    reader.read_records(values.size).should eq(values.size)
    array = reader.consume_batch.as(Arrow::StringViewArray)
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(values)
  end

  it "reads dictionary-encoded values" do
    values = ["aa", "bb", "aa", "large payload over 12 bytes", "aa"]
    dict_bytes, index_bytes = ARUtil.encode_dictionary(values.map { |v| Parquet::ByteArray.new(v) })
    desc = PageUtil.column_descriptor(VIEW_UTF8_COLUMN)
    builder = PageUtil::DataPageBuilderImpl.new(desc, values.size.to_u32, true)
    builder.add_indices(index_bytes)
    pages = [
      Parquet::Column::Page::DictionaryPage.new(dict_bytes, 3_u32, Parquet::Encoding::PLAIN, false),
      builder.consume,
    ]

    reader = view_reader(pages)
    reader.read_records(values.size).should eq(values.size)
    array = reader.consume_batch.as(Arrow::StringViewArray)
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(values)
  end

  it "reads delta byte array encoded values" do
    values = %w[apple application apply banana band bandana]
    reader = view_reader(view_pages(values, Parquet::Encoding::DELTA_BYTE_ARRAY))

    reader.read_records(values.size).should eq(values.size)
    array = reader.consume_batch.as(Arrow::StringViewArray)
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(values)
  end

  it "applies null padding" do
    values = %w[one three]
    reader = view_reader(
      view_pages(values, Parquet::Encoding::PLAIN, def_levels: [1_i16, 0_i16, 1_i16],
        message_type: VIEW_NULLABLE_COLUMN),
      message_type: VIEW_NULLABLE_COLUMN
    )

    reader.read_records(3).should eq(3)
    array = reader.consume_batch.as(Arrow::StringViewArray)
    array.len.should eq(3)
    array.null_count.should eq(1)
    array.null?(1).should be_true
    array.value(0).should eq("one")
    array.value(2).should eq("three")
  end

  it "reads binary views" do
    values = %w[hello world]
    reader = view_reader(
      view_pages(values, Parquet::Encoding::PLAIN, message_type: VIEW_BINARY_COLUMN),
      Arrow::DataType.binary_view,
      VIEW_BINARY_COLUMN
    )

    reader.read_records(2).should eq(2)
    array = reader.consume_batch.as(Arrow::BinaryViewArray)
    Array(Bytes?).new(array.len) { |i| array.value(i) }.should eq(values.map(&.to_slice))
  end

  it "skips records" do
    reader = view_reader(view_pages(%w[a b c d], Parquet::Encoding::PLAIN))
    reader.skip_records(2).should eq(2)
    reader.read_records(2).should eq(2)
    array = reader.consume_batch.as(Arrow::StringViewArray)
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(%w[c d])
  end

  it "rejects unsupported arrow types" do
    expect_raises(Parquet::ParquetError, /invalid data type/) do
      view_reader(view_pages(%w[a], Parquet::Encoding::PLAIN), Arrow::DataType.int32)
    end
  end
end
