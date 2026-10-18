require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/byte_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# variable-length `ByteArrayReader` (UTF-8 / binary, small and large offsets).
#
# Divergence (documented in `plans/parity.md`): values are decoded through the
# existing `ColumnValueDecoderImpl(ByteArray)` and copied into the arrow offset
# buffer at `consume_batch` time, instead of upstream's dedicated
# `ByteArrayColumnValueDecoder` writing straight into an `OffsetBuffer`.
private alias PArrow = Parquet::Arrow
private alias PageUtil = ParquetTestPageUtil
private alias ARUtil = ParquetTestArrayReaderUtil

private BYTE_ARRAY_COLUMN          = "message test_schema { REQUIRED BINARY leaf (STRING); }"
private NULLABLE_BYTE_ARRAY_COLUMN = "message test_schema { OPTIONAL BINARY leaf (STRING); }"

private def byte_array_pages(values : Array(String), encoding : Parquet::Encoding,
                             def_levels : Array(Int16)? = nil,
                             dictionary : Bytes? = nil,
                             message_type : String = BYTE_ARRAY_COLUMN) : Array(Parquet::Column::Page)
  desc = PageUtil.column_descriptor(message_type)
  pages = [] of Parquet::Column::Page

  if dict = dictionary
    pages << Parquet::Column::Page::DictionaryPage.new(dict, values.size.to_u32, Parquet::Encoding::PLAIN, false)
  end

  bytes = values.map { |value| Parquet::ByteArray.new(value) }
  num_values = def_levels.try(&.size) || values.size
  builder = PageUtil::DataPageBuilderImpl.new(desc, num_values.to_u32, true)
  builder.add_def_levels(desc.max_def_level, def_levels.not_nil!) if def_levels
  builder.add_values(encoding, bytes)
  pages << builder.consume
  pages
end

private def byte_array_reader(pages : Array(Parquet::Column::Page),
                              arrow_type : Arrow::DataType? = nil,
                              message_type : String = BYTE_ARRAY_COLUMN) : PArrow::ArrayReader
  desc = PageUtil.column_descriptor(message_type)
  PArrow.make_byte_array_reader(
    PageUtil::InMemoryPageIterator.new([pages]), desc, arrow_type, 1024, nil
  )
end

describe Parquet::Arrow::ByteArrayReader do
  it "reads plain UTF-8 columns" do
    reader = byte_array_reader(byte_array_pages(%w[hello world cupcakes], Parquet::Encoding::PLAIN))

    reader.data_type.should eq(Arrow::DataType.utf8)
    reader.read_records(3).should eq(3)

    array = reader.consume_batch.as(Arrow::GenericStringArray(Int32))
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(["hello", "world", "cupcakes"])
  end

  it "reads dictionary-encoded columns" do
    values = %w[aa bb aa cc aa]
    dict_bytes, index_bytes = ARUtil.encode_dictionary(values.map { |v| Parquet::ByteArray.new(v) })
    desc = PageUtil.column_descriptor(BYTE_ARRAY_COLUMN)
    builder = PageUtil::DataPageBuilderImpl.new(desc, values.size.to_u32, true)
    builder.add_indices(index_bytes)
    pages = [
      Parquet::Column::Page::DictionaryPage.new(dict_bytes, 3_u32, Parquet::Encoding::PLAIN, false),
      builder.consume,
    ]

    reader = byte_array_reader(pages)
    reader.read_records(values.size).should eq(values.size)
    array = reader.consume_batch.as(Arrow::GenericStringArray(Int32))
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(values)
  end

  it "reads delta byte array encoded columns" do
    values = %w[apple application apply banana band bandana]
    pages = byte_array_pages(values, Parquet::Encoding::DELTA_BYTE_ARRAY)

    reader = byte_array_reader(pages)
    reader.read_records(values.size).should eq(values.size)
    array = reader.consume_batch.as(Arrow::GenericStringArray(Int32))
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(values)
  end

  it "reads nullable columns with a null bitmap" do
    values = %w[one three]
    reader = byte_array_reader(byte_array_pages(values, Parquet::Encoding::PLAIN,
      def_levels: [1_i16, 0_i16, 1_i16], message_type: NULLABLE_BYTE_ARRAY_COLUMN
    ), message_type: NULLABLE_BYTE_ARRAY_COLUMN)
    reader.read_records(3).should eq(3)

    array = reader.consume_batch.as(Arrow::GenericStringArray(Int32))
    array.len.should eq(3)
    array.null_count.should eq(1)
    array.null?(1).should be_true
    array.value(0).should eq("one")
    array.value(2).should eq("three")
  end

  it "supports large offsets" do
    values = %w[hello world]
    message_type = "message test_schema { REQUIRED BINARY leaf (STRING); }"
    reader = byte_array_reader(
      byte_array_pages(values, Parquet::Encoding::PLAIN, message_type: message_type),
      Arrow::DataType.large_utf8,
      message_type
    )

    reader.data_type.should eq(Arrow::DataType.large_utf8)
    reader.read_records(2).should eq(2)
    array = reader.consume_batch.as(Arrow::GenericStringArray(Int64))
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(values)
  end

  it "skips records" do
    reader = byte_array_reader(byte_array_pages(%w[a b c d], Parquet::Encoding::PLAIN))
    reader.skip_records(2).should eq(2)
    reader.read_records(2).should eq(2)
    array = reader.consume_batch.as(Arrow::GenericStringArray(Int32))
    Array(String?).new(array.len) { |i| array.value(i) }.should eq(%w[c d])
  end
end
