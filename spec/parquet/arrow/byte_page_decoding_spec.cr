require "../../spec_helper"

# Upstream byte_array.rs::test_byte_array_decoder{,_skip,_nulls} and
# byte_view_array.rs::test_byte_array_string_view_decoder, arrow-rs 60.0.0.
# Malformed-payload cases characterize the checked-in decoder implementations.
private module BytePageSpecs
  alias PA = Parquet::Arrow
  alias E = Parquet::Encoding

  def self.values(strings : Array(String))
    strings.map { |s| Parquet::ByteArray.new(s) }
  end

  def self.decoder
    PA::ByteArrayValueDecoder.new(ParquetTestArrayReaderUtil.utf8_column)
  end

  def self.lengths(values : Array(Int32), data : Bytes = Bytes.empty) : Bytes
    encoder = Parquet::Encodings::DeltaBitPackEncoder(Int32).new
    encoder.put(values)
    io = IO::Memory.new
    io.write(encoder.flush_buffer)
    io.write(data)
    io.to_slice.dup
  end

  def self.reader(data : Bytes, encoding : E, type : Arrow::DataType, count : Int32 = 1)
    desc = ParquetTestPageUtil.column_descriptor("message root { REQUIRED BINARY leaf (STRING); }")
    page = Parquet::Column::Page::DataPageV2.new(data, count.to_u32, encoding, 0_u32, count.to_u32, 0_u32, 0_u32, false)
    pages = ParquetTestPageUtil::InMemoryPageIterator.new([[page.as(Parquet::Column::Page)]])
    if type.kind.dictionary?
      PA.make_byte_array_dictionary_reader(pages, desc, type, 2, nil)
    elsif type.kind.utf8_view?
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
      raise "unexpected array #{array.data_type}"
    end
  end

  def self.types
    [Arrow::DataType.utf8, Arrow::DataType.large_utf8, Arrow::DataType.utf8_view,
     Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8),
     Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8_view)]
  end
end

private alias BP = BytePageSpecs

describe "Arrow variable-byte page decoding" do
  it "ports upstream incremental reads and exhaustion for every byte-array encoding" do
    values = BP.values(["hello", "world", "large payload over 12 bytes", "b"])
    pages, dictionary = ParquetTestArrayReaderUtil.byte_array_all_encodings(values)
    decoder = BP.decoder
    decoder.set_dict(dictionary, 4_u32, BP::E::RLE_DICTIONARY, false)
    pages.each do |encoding, data|
      decoder.set_data(encoding, data, 4, 4)
      output = [] of Parquet::ByteArray
      decoder.read(output, 1).should eq 1
      decoder.read(output, 1).should eq 1
      decoder.read(output, 2).should eq 2
      decoder.read(output, 4).should eq 0
      output.map(&.data).should eq values.map(&.data)
    end
  end

  it "ports upstream read-skip-read and bounded exhaustion across all encodings" do
    pages, dictionary = ParquetTestArrayReaderUtil.byte_array_all_encodings(BP.values(%w[hello world a b]))
    decoder = BP.decoder
    decoder.set_dict(dictionary, 4_u32, BP::E::RLE_DICTIONARY, false)
    pages.each do |encoding, data|
      decoder.set_data(encoding, data, 4, 4)
      output = [] of Parquet::ByteArray
      decoder.read(output, 0).should eq 0
      decoder.skip_values(0).should eq 0
      decoder.read(output, 1).should eq 1
      decoder.skip_values(1).should eq 1
      decoder.skip_values(1).should eq 1
      decoder.read(output, 1).should eq 1
      decoder.read(output, 4).should eq 0
      decoder.skip_values(4).should eq 0
      output.map { |v| String.new(v.data) }.should eq %w[hello b]
    end
  end

  it "ports upstream all-null/empty dictionary read and skip with V1 count hints" do
    pages, dictionary = ParquetTestArrayReaderUtil.byte_array_all_encodings([] of Parquet::ByteArray)
    decoder = BP.decoder
    decoder.set_dict(dictionary, 4_u32, BP::E::RLE_DICTIONARY, false)
    pages.each do |encoding, data|
      decoder.set_data(encoding, data, 4, nil)
      output = [] of Parquet::ByteArray
      decoder.read(output, 1024).should eq 0
      output.should be_empty
      decoder.set_data(encoding, data, 4, nil)
      decoder.skip_values(1024).should eq 0
    end
  end

  it "can replace dictionary pages and switch encodings without stale state" do
    decoder = BP.decoder
    [%w[first first other], %w[second other second]].each do |strings|
      values = BP.values(strings)
      dictionary, data = ParquetTestArrayReaderUtil.encode_dictionary(values)
      decoder.set_dict(dictionary, 2_u32, BP::E::PLAIN, false)
      decoder.set_data(BP::E::RLE_DICTIONARY, data, 3, 3)
      output = [] of Parquet::ByteArray
      decoder.read(output, 3).should eq 3
      output.map { |v| String.new(v.data) }.should eq strings
      plain = ParquetTestArrayReaderUtil.encode_byte_array(BP::E::PLAIN, BP.values(["plain"]))
      decoder.set_data(BP::E::PLAIN, plain, 1, 1)
      output.clear
      decoder.read(output, 1).should eq 1
      String.new(output.first.data).should eq "plain"
    end
  end

  it "defers missing dictionaries to read/skip with the upstream error" do
    [false, true].each do |skip|
      decoder = BP.decoder
      decoder.set_data(BP::E::RLE_DICTIONARY, Bytes[0], 1, 1)
      expect_raises(Parquet::GeneralError, "missing dictionary page for column") do
        skip ? decoder.skip_values(1) : decoder.read([] of Parquet::ByteArray, 1)
      end
    end
  end

  it "rejects negative delta lengths when the page is installed" do
    decoder = BP.decoder
    expect_raises(Parquet::GeneralError, "negative delta length byte array length") do
      decoder.set_data(BP::E::DELTA_LENGTH_BYTE_ARRAY, BP.lengths([-1]), 1, 1)
    end
  end

  it "rejects insufficient delta payloads before read or skip" do
    decoder = BP.decoder
    expect_raises(Parquet::GeneralError, "Insufficient delta length byte array bytes") do
      decoder.set_data(BP::E::DELTA_LENGTH_BYTE_ARRAY, BP.lengths([4], Bytes[97]), 1, 1)
    end
  end

  it "uses embedded delta counts rather than V1 level upper bounds" do
    [BP::E::DELTA_LENGTH_BYTE_ARRAY, BP::E::DELTA_BYTE_ARRAY].each do |encoding|
      decoder = BP.decoder
      data = ParquetTestArrayReaderUtil.encode_byte_array(encoding, BP.values(%w[a ab abc]))
      decoder.set_data(encoding, data, 100, nil)
      output = [] of Parquet::ByteArray
      decoder.read(output, 100).should eq 3
      decoder.read(output, 100).should eq 0
      output.map { |v| String.new(v.data) }.should eq %w[a ab abc]
    end
  end

  it "rejects inconsistent delta prefix/suffix counts" do
    decoder = BP.decoder
    data = BP.lengths([0, 0], BP.lengths([1], Bytes[97]))
    expect_raises(Parquet::GeneralError, "inconsistent DELTA_BYTE_ARRAY lengths, prefixes: 2, suffixes: 1") do
      decoder.set_data(BP::E::DELTA_BYTE_ARRAY, data, 2, 2)
    end
  end

  it "rejects an invalid dictionary key with the byte-array error" do
    decoder = BP.decoder
    dictionary, _ = ParquetTestArrayReaderUtil.encode_dictionary(BP.values(["a"]))
    decoder.set_dict(dictionary, 1_u32, BP::E::PLAIN, false)
    # bit width 1, RLE run of one key 1, outside the one-entry dictionary.
    decoder.set_data(BP::E::RLE_DICTIONARY, Bytes[1, 2, 1], 1, 1)
    expect_raises(Parquet::GeneralError, "dictionary key beyond bounds of dictionary: 0..1") do
      decoder.read([] of Parquet::ByteArray, 1)
    end
  end

  it "rejects unsupported variable-byte encodings with a General error" do
    [BP::E::RLE, BP::E::DELTA_BINARY_PACKED, BP::E::BYTE_STREAM_SPLIT].each do |encoding|
      expect_raises(Parquet::GeneralError, "unsupported encoding for byte array: #{encoding}") do
        BP.decoder.set_data(encoding, Bytes.empty, 1, 1)
      end
    end
  end

  BP.types.each do |type|
    it "rejects negative and insufficient delta pages through #{type} readers" do
      [[-1], [4]].each do |lengths|
        data = BP.lengths(lengths, Bytes[97])
        [false, true].each do |skip|
          # A whole-page skip bypasses decoding upstream; request a partial
          # skip so this test exercises page installation and value decoding.
          reader = BP.reader(data, BP::E::DELTA_LENGTH_BYTE_ARRAY, type, 2)
          message = lengths.first < 0 ? "negative delta length byte array length" : "Insufficient delta length byte array bytes"
          expect_raises(Parquet::GeneralError, message) { skip ? reader.skip_records(1) : reader.read_records(1) }
        end
      end
    end
  end

  [Arrow::DataType.utf8, Arrow::DataType.large_utf8, Arrow::DataType.utf8_view].each do |type|
    it "reports truncated plain prefixes and payloads through #{type} readers" do
      [Bytes[1, 0], Bytes[2, 0, 0, 0, 97]].each do |data|
        reader = BP.reader(data, BP::E::PLAIN, type)
        expect_raises(Parquet::EOFError, "eof decoding byte array") { reader.read_records(1) }
      end
    end
  end

  it "matches pinned prefix truncation for large and negative prefix lengths on read and skip" do
    # Rust casts negative i32 to usize, then Vec::truncate clamps to the
    # previous value's length. Crystal negative Range ends are relative.
    [100, -1].each do |prefix|
      [false, true].each do |skip|
        decoder = BP.decoder
        decoder.set_data(BP::E::DELTA_BYTE_ARRAY, BP.lengths([0, prefix], BP.lengths([1, 1], Bytes[97, 98])), 2, 2)
        output = [] of Parquet::ByteArray
        if skip
          decoder.skip_values(1).should eq 1
          decoder.read(output, 1).should eq 1
          output.map { |v| String.new(v.data) }.should eq ["ab"]
        else
          decoder.read(output, 2).should eq 2
          output.map { |v| String.new(v.data) }.should eq ["a", "ab"]
        end
      end
    end
  end

  it "allows whole-page skips to bypass malformed value payloads as upstream does" do
    BP.types.each do |type|
      reader = BP.reader(BP.lengths([-1]), BP::E::DELTA_LENGTH_BYTE_ARRAY, type)
      reader.skip_records(1).should eq 1
      reader.read_records(1).should eq 0
    end
  end

  it "preserves the distinct plain offset and view exhaustion contracts" do
    data = ParquetTestArrayReaderUtil.encode_byte_array(BP::E::PLAIN, BP.values(["a"]))
    offset = BP.decoder
    offset.set_data(BP::E::PLAIN, data, 2, nil)
    values = [] of Parquet::ByteArray
    offset.read(values, 2).should eq 2 # pinned offset over-report on corrupt count
    values.map { |v| String.new(v.data) }.should eq ["a"]
    view = BP::PA::ByteViewArrayValueDecoder.new(ParquetTestArrayReaderUtil.utf8_column)
    view.set_data(BP::E::PLAIN, data, 2, nil)
    expect_raises(Parquet::EOFError, "eof decoding byte array") { view.read([] of Parquet::ByteArray, 2) }
  end

  it "returns no decoder set errors before installing a data page" do
    expect_raises(Parquet::GeneralError, "no decoder set") { BP.decoder.read([] of Parquet::ByteArray, 1) }
    expect_raises(Parquet::GeneralError, "no decoder set") { BP.decoder.skip_values(1) }
  end

  it "rejects empty and invalid-width dictionary index pages during installation" do
    expect_raises(Parquet::GeneralError, "dictionary index page is empty") { BP.decoder.set_data(BP::E::RLE_DICTIONARY, Bytes.empty, 1, 1) }
    expect_raises(Parquet::GeneralError, "Invalid or corrupted RLE bit width 33") { BP.decoder.set_data(BP::E::RLE_DICTIONARY, Bytes[33], 1, 1) }
  end

  it "reads nullable V1/V2 files across page, row-group and batch boundaries" do
    expected = [nil, "hello", "", nil, "long value greater than twelve bytes", "é", "hello", nil] of String?
    versions = [Parquet::File::Properties::WriterVersion::PARQUET_1_0, Parquet::File::Properties::WriterVersion::PARQUET_2_0]
    encodings = [BP::E::PLAIN, BP::E::RLE_DICTIONARY, BP::E::DELTA_LENGTH_BYTE_ARRAY, BP::E::DELTA_BYTE_ARRAY]
    versions.each do |version|
      encodings.each do |encoding|
        props = Parquet::File::Properties::WriterProperties.builder.set_writer_version(version)
          .set_write_batch_size(2).set_data_page_row_count_limit(2)
          .set_dictionary_enabled(encoding == BP::E::RLE_DICTIONARY)
          .set_encoding(encoding == BP::E::RLE_DICTIONARY ? BP::E::PLAIN : encoding).build
        sink = IO::Memory.new
        schema = Parquet::Schema.parse_message_type("message root { OPTIONAL BINARY leaf (STRING); }")
        writer = Parquet::File::SerializedFileWriter.new(sink, schema, props)
        [expected, [nil, nil] of String?, expected.reverse].each do |rows|
          group = writer.next_row_group
          column = group.next_column || raise "missing column"
          column.typed(Parquet::ByteArray).write_batch(rows.compact.map { |v| Parquet::ByteArray.new(v) }, rows.map { |v| v ? 1_i16 : 0_i16 }, nil)
          column.close
          group.close
        end
        writer.close
        BP.types.each do |type|
          file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(sink.to_slice))
          supplied = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("leaf", type, true)]))
          reader = BP::PA::ParquetRecordBatchReaderBuilder.try_new_with_options(file, BP::PA::ArrowReaderOptions.new.with_schema(supplied))
            .with_batch_size(3).build
          actual = [] of String?
          while batch = reader.next_batch
            batch.num_rows.should be <= 3
            batch.column(0).data_type.should eq type
            actual.concat(BP.strings(batch.column(0)))
          end
          actual.should eq expected + [nil, nil] + expected.reverse
        end
      end
    end
  end
end
