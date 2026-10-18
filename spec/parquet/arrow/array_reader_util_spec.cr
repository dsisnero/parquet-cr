require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/test_util.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the byte-array encoding
# helpers, the `make_int32_page_reader` builder and `EmptyPageIterator`.
private alias Util = ParquetTestArrayReaderUtil

describe ParquetTestArrayReaderUtil do
  it "builds a UTF-8 column descriptor" do
    column = Util.utf8_column
    column.converted_type.should eq(Parquet::ConvertedType::UTF8)
    column.max_def_level.should eq(1)
    column.max_rep_level.should eq(0)
  end

  it "encodes byte arrays in every encoding" do
    data = ["a", "bb", "ccc"].map { |value| Parquet::ByteArray.new(value) }
    pages, dictionary = Util.byte_array_all_encodings(data)

    pages.map(&.first).should eq([
      Parquet::Encoding::PLAIN,
      Parquet::Encoding::DELTA_BYTE_ARRAY,
      Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
      Parquet::Encoding::PLAIN_DICTIONARY,
      Parquet::Encoding::RLE_DICTIONARY,
    ])
    pages.each { |(_, bytes)| bytes.size.should be > 0 }
    dictionary.size.should be > 0
  end

  it "builds an int32 array reader with full padding" do
    reader = Util.make_int32_page_reader(
      values: [1, 2, 3],
      def_levels: [1_i16, 0_i16, 1_i16, 1_i16],
      rep_levels: [] of Int16,
      max_def_level: 1,
      max_rep_level: 0,
      padding_threshold: nil
    )

    reader.read_records(4).should eq(4)
    array = reader.consume_batch.as(Arrow::PrimitiveArray(Int32))
    array.len.should eq(4)
    array.null_count.should eq(1)
    array.null?(1).should be_true
  end

  it "builds an int32 array reader with selective padding" do
    # A list child: def 3 is a value, def 1 is a null/empty parent entry that
    # selective padding (threshold 2) drops entirely.
    reader = Util.make_int32_page_reader(
      values: [10, 20],
      def_levels: [3_i16, 1_i16, 3_i16],
      rep_levels: [0_i16, 1_i16, 1_i16],
      max_def_level: 3,
      max_rep_level: 1,
      padding_threshold: 2
    )

    reader.read_records(3).should eq(1)
    array = reader.consume_batch.as(Arrow::PrimitiveArray(Int32))
    array.len.should eq(2)
    array.null_count.should eq(0)
    [array.values[0], array.values[1]].should eq([10, 20])
  end

  it "yields no pages from the empty page iterator" do
    iterator = Util::EmptyPageIterator.new
    iterator.next.should be(Iterator::Stop::INSTANCE)
  end
end
