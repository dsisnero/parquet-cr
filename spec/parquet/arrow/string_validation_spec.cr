require "../../spec_helper"

# Ported from arrow-rs 60.0.0 tests/arrow_reader/invalid_utf8.rs and
# src/arrow/arrow_reader/mod.rs::test_read_non_utf8_binary_as_utf8.
# Write raw BYTE_ARRAY values with the low-level writer instead of constructing
# invalid Arrow arrays with Rust unsafe new_unchecked. The on-disk contract and
# all six upstream malformed-value cases are preserved.
private module StringValidationSpecs
  alias PA = Parquet::Arrow

  def self.encodings
    [Parquet::Encoding::PLAIN, Parquet::Encoding::RLE_DICTIONARY,
     Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, Parquet::Encoding::DELTA_BYTE_ARRAY]
  end

  def self.types
    [Arrow::DataType.utf8, Arrow::DataType.large_utf8, Arrow::DataType.utf8_view,
     Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8),
     Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8_view)]
  end

  def self.invalid_cases : Array(Array(Bytes?))
    first = Bytes[0xa0, 0xa1, 0x20, 0x20]
    later = Bytes[0x20, 0x20, 0x20, 0xa0, 0xa1, 0x20, 0x20]
    long = "ThisStringIsCertainlyLongerThan12Bytes"
    [
      [nil, "   ".to_slice, nil, first],
      [nil, "   ".to_slice, nil, (long + String.new(first)).to_slice],
      [nil, "   ".to_slice, nil, later],
      [nil, "   ".to_slice, nil, (long + String.new(later)).to_slice],
      [nil, "   ".to_slice, nil, (long * 10 + String.new(later)).to_slice],
      [nil, "   ".to_slice, later, nil, (long * 10).to_slice, "   ".to_slice],
    ] of Array(Bytes?)
  end

  def self.file(values : Array(Bytes?), encoding : Parquet::Encoding, annotated : Bool = true) : Bytes
    schema = Parquet::Schema.parse_message_type("message root { OPTIONAL BINARY leaf #{annotated ? "(STRING)" : ""}; }")
    props = Parquet::File::Properties::WriterProperties.builder
      .set_dictionary_enabled(encoding == Parquet::Encoding::RLE_DICTIONARY)
      .set_encoding(encoding == Parquet::Encoding::RLE_DICTIONARY ? Parquet::Encoding::PLAIN : encoding).build
    sink = IO::Memory.new
    writer = Parquet::File::SerializedFileWriter.new(sink, schema, props)
    group = writer.next_row_group
    column = group.next_column || raise "missing column"
    column.typed(Parquet::ByteArray).write_batch(
      values.compact_map { |v| v ? Parquet::ByteArray.new(v) : nil },
      values.map { |v| v ? 1_i16 : 0_i16 }, nil)
    column.close
    group.close
    writer.close
    sink.to_slice.dup
  end

  def self.reader(bytes : Bytes, type : Arrow::DataType, batch_size : Int32 = 1024)
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("leaf", type, true)]))
    file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(bytes))
    PA::ParquetRecordBatchReaderBuilder.try_new_with_options(file, PA::ArrowReaderOptions.new.with_schema(schema))
      .with_batch_size(batch_size).build
  end

  def self.page_reader(values : Array(Bytes), encoding : Parquet::Encoding, type : Arrow::DataType,
                       annotated : Bool = true) : PA::ArrayReader
    desc = ParquetTestPageUtil.column_descriptor("message root { REQUIRED BINARY leaf #{annotated ? "(STRING)" : ""}; }")
    builder = ParquetTestPageUtil::DataPageBuilderImpl.new(desc, values.size.to_u32, true)
    raw = values.map { |v| Parquet::ByteArray.new(v) }
    column_pages = [] of Parquet::Column::Page
    if encoding == Parquet::Encoding::RLE_DICTIONARY || encoding == Parquet::Encoding::PLAIN_DICTIONARY
      dictionary, indices = ParquetTestArrayReaderUtil.encode_dictionary(raw)
      column_pages << Parquet::Column::Page::DictionaryPage.new(dictionary, raw.uniq.size.to_u32, Parquet::Encoding::PLAIN, false)
      builder.add_indices(indices)
    else
      builder.add_values(encoding, raw)
    end
    page = builder.consume
    if encoding == Parquet::Encoding::PLAIN_DICTIONARY
      # add_indices emits RLE_DICTIONARY; explicitly exercise the legacy tag.
      page = Parquet::Column::Page::DataPage.new(page.buffer, page.num_values, encoding, Parquet::Encoding::RLE, Parquet::Encoding::RLE)
    end
    column_pages << page
    pages = ParquetTestPageUtil::InMemoryPageIterator.new([column_pages])
    if type.kind.dictionary?
      PA.make_byte_array_dictionary_reader(pages, desc, type, 2, nil)
    elsif type.kind.utf8_view? || type.kind.binary_view?
      PA.make_byte_view_array_reader(pages, desc, type, 2, nil)
    else
      PA.make_byte_array_reader(pages, desc, type, 2, nil)
    end
  end

  def self.strings(array : Arrow::ArrayBase) : Array(String?)
    case array
    when Arrow::GenericStringArray(Int32), Arrow::GenericStringArray(Int64), Arrow::StringViewArray
      Array(String?).new(array.len) { |i| array.null?(i) ? nil : array.value(i) }
    when Arrow::DictionaryArray(Int32)
      values = strings(array.values)
      Array(String?).new(array.len) { |i| array.null?(i) ? nil : values[array.keys.value(i)] }
    else
      raise "unexpected string array #{array.data_type}"
    end
  end
end

private alias SV = StringValidationSpecs

describe "Arrow string decoding validation" do
  SV.types.each do |type|
    it "rejects all six upstream malformed cases for #{type} across four encodings" do
      SV.invalid_cases.each do |values|
        SV.encodings.each do |encoding|
          reader = SV.reader(SV.file(values, encoding), type)
          expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.next_batch }
        end
      end
    end
  end

  [Arrow::DataType.utf8, Arrow::DataType.large_utf8, Arrow::DataType.utf8_view,
   Arrow::DataType.binary, Arrow::DataType.large_binary, Arrow::DataType.binary_view].each do |type|
    it "validates annotated #{type} in read_records before consume_batch" do
      [Parquet::Encoding::PLAIN, Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, Parquet::Encoding::DELTA_BYTE_ARRAY].each do |encoding|
        reader = SV.page_reader(["valid".to_slice, Bytes[0xff]], encoding, type)
        reader.read_records(1).should eq 1
        reader.consume_batch.len.should eq 1
        expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.read_records(1) }
      end
    end

    it "skips malformed non-dictionary #{type} values without validating them" do
      [Parquet::Encoding::PLAIN, Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, Parquet::Encoding::DELTA_BYTE_ARRAY].each do |encoding|
        reader = SV.page_reader([Bytes[0xff], "valid".to_slice], encoding, type)
        reader.skip_records(1).should eq 1
        reader.read_records(1).should eq 1
        reader.consume_batch.len.should eq 1
      end
    end
  end

  it "rejects split codepoints even when concatenated bytes form valid UTF-8" do
    SV.types.each do |type|
      SV.encodings.each do |encoding|
        reader = SV.reader(SV.file(([Bytes[0xc3], Bytes[0xa9]] of Bytes?), encoding), type)
        expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.next_batch }
      end
    end
  end

  it "preserves arbitrary unannotated binary bytes" do
    [Arrow::DataType.binary, Arrow::DataType.large_binary, Arrow::DataType.binary_view].each do |type|
      SV.encodings.each do |encoding|
        bytes = Bytes[0xde, 0x00, 0xff]
        reader = SV.reader(SV.file(([bytes, nil, Bytes.empty] of Bytes?), encoding, false), type)
        batch = reader.next_batch || raise "missing batch"
        batch.num_rows.should eq 3
        array = batch.column(0)
        array.null?(1).should be_true
        case array
        when Arrow::GenericBinaryArray(Int32), Arrow::GenericBinaryArray(Int64), Arrow::BinaryViewArray
          array.value(0).should eq bytes
          array.value(2).should eq Bytes.empty
        else
          fail "unexpected binary array"
        end
      end
    end
  end

  it "rejects non-UTF-8 binary projected as a string (upstream schema override test)" do
    [Arrow::DataType.utf8, Arrow::DataType.large_utf8, Arrow::DataType.utf8_view].each do |type|
      reader = SV.reader(SV.file(([Bytes[0xde, 0, 0xff]] of Bytes?), Parquet::Encoding::PLAIN, false), type)
      expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.next_batch }
    end
  end

  it "validates even unused annotated dictionary entries before reading a batch" do
    desc = ParquetTestPageUtil.column_descriptor("message root { REQUIRED BINARY leaf (STRING); }")
    values = [Parquet::ByteArray.new("valid"), Parquet::ByteArray.new(Bytes[0x20, 0xff])]
    dictionary, indices = ParquetTestArrayReaderUtil.encode_dictionary(values)
    targets = SV.types + [Arrow::DataType.binary, Arrow::DataType.binary_view]
    targets.each do |type|
      builder = ParquetTestPageUtil::DataPageBuilderImpl.new(desc, 1_u32, true)
      builder.add_indices(indices)
      pages = ParquetTestPageUtil::InMemoryPageIterator.new([[
        Parquet::Column::Page::DictionaryPage.new(dictionary, 2_u32, Parquet::Encoding::PLAIN, false).as(Parquet::Column::Page), builder.consume,
      ]])
      reader = if type.kind.dictionary?
                 SV::PA.make_byte_array_dictionary_reader(pages, desc, type, 2, nil)
               elsif type.kind.utf8_view? || type.kind.binary_view?
                 SV::PA.make_byte_view_array_reader(pages, desc, type, 2, nil)
               else
                 SV::PA.make_byte_array_reader(pages, desc, type, 2, nil)
               end
      expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.read_records(1) }
    end
  end

  it "preserves valid multibyte, empty, long and null strings across batch boundaries" do
    values = [nil, "", "é", "😎", "ThisStringIsCertainlyLongerThan12Bytes" * 10, nil, "終"] of String?
    SV.types.each do |type|
      SV.encodings.each do |encoding|
        reader = SV.reader(SV.file(values.map { |v| v.try(&.to_slice) }, encoding), type, 2)
        actual = [] of String?
        while batch = reader.next_batch
          batch.num_rows.should be <= 2
          batch.column(0).data_type.should eq type
          actual.concat(SV.strings(batch.column(0)))
        end
        actual.should eq values
      end
    end
  end

  it "rejects invalid unannotated binary projected to dictionary strings" do
    [Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8),
     Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8_view)].each do |type|
      SV.encodings.each do |encoding|
        reader = SV.reader(SV.file(([Bytes[0x20, 0xff]] of Bytes?), encoding, false), type)
        expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.next_batch }
      end
    end
  end

  it "rejects the exact upstream invalid_utf8 file fixture" do
    bytes = Bytes[
      80, 65, 82, 49, 21, 6, 21, 22, 21, 22, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 4, 21, 0, 18,
      28, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111, 0, 0, 0, 3, 1,
      5, 0, 0, 0, 104, 101, 255, 108, 111, 38, 110, 28, 21, 12, 25, 37, 6, 0, 25, 24, 2, 99, 49,
      21, 0, 22, 2, 22, 102, 22, 102, 38, 8, 60, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5,
      104, 101, 255, 108, 111, 0, 0, 0, 21, 4, 25, 44, 72, 4, 114, 111, 111, 116, 21, 2, 0, 21,
      12, 37, 2, 24, 2, 99, 49, 37, 0, 76, 28, 0, 0, 0, 22, 2, 25, 28, 25, 28, 38, 110, 28, 21,
      12, 25, 37, 6, 0, 25, 24, 2, 99, 49, 21, 0, 22, 2, 22, 102, 22, 102, 38, 8, 60, 54, 0, 40,
      5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111, 0, 0, 0, 22, 102, 22, 2, 0, 40,
      44, 65, 114, 114, 111, 119, 50, 32, 45, 32, 78, 97, 116, 105, 118, 101, 32, 82, 117, 115,
      116, 32, 105, 109, 112, 108, 101, 109, 101, 110, 116, 97, 116, 105, 111, 110, 32, 111, 102,
      32, 65, 114, 114, 111, 119, 0, 130, 0, 0, 0, 80, 65, 82, 49,
    ]
    file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(bytes))
    reader = SV::PA::ParquetRecordBatchReaderBuilder.try_new(file).with_batch_size(10).build
    expect_raises(Parquet::GeneralError, /invalid utf-8 sequence/) { reader.next_batch }
  end

  [Arrow::DataType.binary, Arrow::DataType.large_binary, Arrow::DataType.binary_view].each do |type|
    it "validates all six malformed cases in annotated lower-level #{type} readers" do
      SV.invalid_cases.each do |values|
        (SV.encodings + [Parquet::Encoding::PLAIN_DICTIONARY]).each do |encoding|
          reader = SV.page_reader(values.compact, encoding, type)
          expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.read_records(values.compact.size) }
        end
      end
    end
  end

  it "reports corruption in a later batch after returning valid rows" do
    SV.types.each do |type|
      [Parquet::Encoding::PLAIN, Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, Parquet::Encoding::DELTA_BYTE_ARRAY].each do |encoding|
        reader = SV.reader(SV.file(SV.invalid_cases.last, encoding), type, 2)
        first = reader.next_batch || raise "missing first batch"
        SV.strings(first.column(0)).should eq [nil, "   "]
        expect_raises(Parquet::GeneralError, /encountered non UTF-8 data/) { reader.next_batch }
      end
    end
  end
end
