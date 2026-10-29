require "../../spec_helper"

# Source-derived characterizations of fixed_len_byte_array.rs, arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b). Upstream's buffer reservation and
# Decimal256-list tests are ported alongside integrated encoding tests.
private module FixedByteSpecs
  alias PA = Parquet::Arrow
  alias E = Parquet::Encoding
  alias Page = Parquet::Column::Page

  def self.desc(width : Int32 = 4)
    ParquetTestPageUtil.column_descriptor("message root { REQUIRED FIXED_LEN_BYTE_ARRAY (#{width}) leaf; }")
  end

  def self.page(data : Bytes, encoding : E, count : Int32 = 1) : Page
    Page::DataPageV2.new(data, count.to_u32, encoding, 0_u32, count.to_u32, 0_u32, 0_u32, false)
  end

  def self.reader(pages : Array(Page), width : Int32 = 4, type : Arrow::DataType = Arrow::DataType.fixed_size_binary(4))
    PA.make_fixed_len_byte_array_reader(ParquetTestPageUtil::InMemoryPageIterator.new([pages.map(&.as(Page))]), desc(width), type, 2, nil)
  end

  def self.delta(values : Array(String)) : Bytes
    encoder = Parquet::Encodings::DeltaByteArrayEncoder(Parquet::ByteArray).new
    encoder.put(values.map { |v| Parquet::ByteArray.new(v) })
    encoder.flush_buffer
  end

  def self.split(values : Array(Bytes)) : Bytes
    Bytes.new(values.size * values.first.size) { |i| values[i % values.size][i // values.size] }
  end
end

private alias FB = FixedByteSpecs

describe "Arrow fixed-length byte decoding" do
  [Arrow::DataType.fixed_size_binary(4), Arrow::DataType.decimal32(9, 0), Arrow::DataType.decimal64(18, 0)].each do |type|
    it "rejects wrong-width delta values before converting #{type}" do
      reader = FB.reader([FB.page(FB.delta(["ab"]), FB::E::DELTA_BYTE_ARRAY)], type: type)
      expect_raises(Parquet::GeneralError, "encountered array with incorrect length, got 2 expected 4") { reader.read_records(1) }
    end
  end

  it "skips delta values without validating their width, preserving prefix state" do
    reader = FB.reader([FB.page(FB.delta(["ab", "abcd"]), FB::E::DELTA_BYTE_ARRAY, 2)])
    reader.skip_records(1).should eq 1
    reader.read_records(1).should eq 1
    reader.consume_batch.as(Arrow::FixedSizeBinaryArray).value(0).should eq "abcd".to_slice
  end

  it "reports short dictionary bytes during dictionary installation" do
    pages = [FB::Page::DictionaryPage.new("ab".to_slice, 1_u32, FB::E::PLAIN, false).as(FB::Page), FB.page(Bytes[0, 2, 0], FB::E::RLE_DICTIONARY)]
    reader = FB.reader(pages)
    expect_raises(Parquet::GeneralError, "too few bytes in dictionary page, expected 4 got 2") { reader.read_records(1) }
  end

  it "replaces dictionary pages between data pages" do
    pages = [] of FB::Page
    ["abcd", "wxyz"].each do |value|
      pages << FB::Page::DictionaryPage.new(value.to_slice, 1_u32, FB::E::PLAIN, false)
      pages << FB.page(Bytes[0, 2, 0], FB::E::RLE_DICTIONARY)
    end
    reader = FB.reader(pages)
    reader.read_records(2).should eq 2
    array = reader.consume_batch.as(Arrow::FixedSizeBinaryArray)
    array.value(0).should eq "abcd".to_slice
    array.value(1).should eq "wxyz".to_slice
  end

  it "uses full byte-stream-split values and ignores trailing bytes like upstream" do
    data = Bytes[97, 119, 98, 120, 99, 121, 100, 122, 255]
    reader = FB.reader([FB.page(data, FB::E::BYTE_STREAM_SPLIT, 2)])
    reader.read_records(2).should eq 2
    array = reader.consume_batch.as(Arrow::FixedSizeBinaryArray)
    array.value(0).should eq "abcd".to_slice
    array.value(1).should eq "wxyz".to_slice
  end

  it "rejects unsupported encodings with the pinned General error" do
    [FB::E::RLE, FB::E::DELTA_LENGTH_BYTE_ARRAY, FB::E::DELTA_BINARY_PACKED].each do |encoding|
      reader = FB.reader([FB.page(Bytes.empty, encoding)])
      expect_raises(Parquet::GeneralError, "unsupported encoding for fixed length byte array: #{encoding}") { reader.read_records(1) }
    end
  end

  [1, 2, 3, 4, 8, 16, 32].each do |width|
    it "reads/skips byte-stream-split width #{width} across batches" do
      values = Array(Bytes).new(5) { |i| Bytes.new(width) { |j| (i * 10 + j).to_u8 } }
      reader = FB.reader([FB.page(FB.split(values), FB::E::BYTE_STREAM_SPLIT, 5)], width, Arrow::DataType.fixed_size_binary(width))
      reader.read_records(1).should eq 1
      reader.consume_batch.as(Arrow::FixedSizeBinaryArray).value(0).should eq values[0]
      reader.skip_records(2).should eq 2
      reader.read_records(2).should eq 2
      array = reader.consume_batch.as(Arrow::FixedSizeBinaryArray)
      array.value(0).should eq values[3]
      array.value(1).should eq values[4]
      reader.read_records(1).should eq 0
    end
  end

  it "ports test_decimal_list with the exact offsets, validity and batch slices" do
    decimal_type = Arrow::DataType.decimal256(76, 10)
    decimals = Arrow::PrimitiveArray(Arrow::I256).from_values((1..8).map { |v| Arrow::I256.new(v) }).with_data_type(decimal_type)
    offsets = Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 0, 1, 3, 3, 4, 5, 8]))
    list = Arrow::GenericListArray(Int32).new(Arrow::Field.new("item", decimal_type, false), offsets, decimals,
      Arrow::NullBuffer.from_slice([true, true, true, false, true, false, true]))
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("list", list.data_type, true)]))
    written = Arrow::RecordBatch.try_new(schema, [list.as(Arrow::ArrayBase)])
    sink = IO::Memory.new
    writer = FB::PA::ArrowWriter.try_new(sink, written.schema)
    writer.write(written)
    writer.close
    file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(sink.to_slice))
    read = FB::PA::ParquetRecordBatchReaderBuilder.try_new(file).with_batch_size(3).build.to_a
    read.size.should eq 3
    read[0].should eq written.slice(0, 3)
    read[1].should eq written.slice(3, 3)
    read[2].should eq written.slice(6, 1)
  end

  it "reads nullable fixed bytes in V1/V2 files over pages, groups and small batches" do
    expected = ["abcd", nil, "wxyz", "abcd", nil, "lmno"] of String?
    versions = [Parquet::File::Properties::WriterVersion::PARQUET_1_0, Parquet::File::Properties::WriterVersion::PARQUET_2_0]
    encodings = [FB::E::PLAIN, FB::E::RLE_DICTIONARY, FB::E::DELTA_BYTE_ARRAY, FB::E::BYTE_STREAM_SPLIT]
    versions.each do |version|
      encodings.each do |encoding|
        props = Parquet::File::Properties::WriterProperties.builder.set_writer_version(version)
          .set_write_batch_size(2).set_data_page_row_count_limit(2)
          .set_dictionary_enabled(encoding == FB::E::RLE_DICTIONARY)
          .set_encoding(encoding == FB::E::RLE_DICTIONARY ? FB::E::PLAIN : encoding).build
        sink = IO::Memory.new
        schema = Parquet::Schema.parse_message_type("message root { OPTIONAL FIXED_LEN_BYTE_ARRAY (4) leaf; }")
        writer = Parquet::File::SerializedFileWriter.new(sink, schema, props)
        [expected, [nil, nil] of String?, expected.reverse].each do |rows|
          group = writer.next_row_group
          column = group.next_column || raise "missing column"
          column.typed(Parquet::FixedLenByteArray).write_batch(rows.compact.map { |v| Parquet::FixedLenByteArray.new(Parquet::ByteArray.new(v)) },
            rows.map { |v| v ? 1_i16 : 0_i16 }, nil)
          column.close
          group.close
        end
        writer.close
        file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(sink.to_slice))
        reader = FB::PA::ParquetRecordBatchReaderBuilder.try_new(file).with_batch_size(3).build
        actual = [] of String?
        while batch = reader.next_batch
          batch.num_rows.should be <= 3
          array = batch.column(0).as(Arrow::FixedSizeBinaryArray)
          actual.concat(Array(String?).new(array.len) { |i| array.null?(i) ? nil : String.new(array.value(i)) })
        end
        actual.should eq expected + [nil, nil] + expected.reverse
      end
    end
  end

  it "applies the same delta-width contract to floating and interval conversions" do
    [{2, Arrow::DataType.float16}, {12, Arrow::DataType.interval(Arrow::IntervalUnit::YearMonth)},
     {16, Arrow::DataType.decimal128(25, 0)}, {32, Arrow::DataType.decimal256(60, 0)}].each do |width, type|
      reader = FB.reader([FB.page(FB.delta(["x"]), FB::E::DELTA_BYTE_ARRAY)], width, type)
      expect_raises(Parquet::GeneralError, "encountered array with incorrect length, got 1 expected #{width}") { reader.read_records(1) }
    end
  end
end
