require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/byte_array_dictionary.rs` test
# module plus characterization specs for the reader itself
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream only unit-tests `DictionaryDecoder`/`DictionaryBuffer` in this module
# (the reader is covered end-to-end by `arrow_reader`'s
# `test_dictionary_preservation`); the reader-level examples here drive
# `make_byte_array_dictionary_reader` directly because the user-facing
# dictionary arrow schema path (`with_schema`) is not ported yet.
#
# Divergences (see `plans/parity.md`): the fallback (non-dictionary encoding)
# path decodes through `ColumnValueDecoderImpl(ByteArray)` instead of upstream's
# `ByteArrayDecoder`; arrays are inspected through their dictionary keys and
# values instead of `cast(.., Utf8)` (arrow-cr has no `compute::cast`).
private alias PArrow = Parquet::Arrow
private alias PageUtil = ParquetTestPageUtil
private alias ARUtil = ParquetTestArrayReaderUtil
private alias PDict = Parquet::Arrow::Buffer::DictionaryBuffer
private alias PKind = Parquet::Arrow::Buffer::DictionaryBuffer::Kind

private BYTE_ARRAY_COLUMN = "message test_schema { REQUIRED BINARY leaf (STRING); }"

private def mask_from(bools : Array(Bool)) : Bytes
  bytes = Bytes.new((bools.size + 7) // 8, 0_u8)
  bools.each_with_index { |value, i| bytes[i // 8] |= (1_u8 << (i % 8)) if value }
  bytes
end

private def null_buffer(mask : Bytes, len : Int32) : Arrow::NullBuffer
  Arrow::NullBuffer.new(Arrow::BooleanBuffer.from_bits(mask, 0, len))
end

private def utf8_dictionary : Arrow::DataType
  Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8)
end

private def byte_array_values(values : Array(String)) : Array(Parquet::ByteArray)
  values.map { |value| Parquet::ByteArray.new(value) }
end

# The values of a `Dictionary(K, Utf8)` array, looked up through its keys.
private def dictionary_strings(array : Arrow::ArrayBase) : Array(String?)
  dict = array.as(Arrow::DictionaryArray(Int32))
  values = dict.values.as(Arrow::GenericStringArray(Int32))
  keys = dict.keys
  Array(String?).new(array.len) do |i|
    dict.valid?(i) ? values.value(keys.value(i)) : nil
  end
end

# The decoded dictionary of a `Dictionary(K, Utf8)` array.
private def dictionary_values(array : Arrow::ArrayBase) : Array(String?)
  values = array.as(Arrow::DictionaryArray(Int32)).values.as(Arrow::GenericStringArray(Int32))
  Array(String?).new(values.len) { |i| values.value(i) }
end

# The keys of a `Dictionary(Int32, _)` array.
private def dictionary_keys(array : Arrow::ArrayBase) : Array(Int32)
  keys = array.as(Arrow::DictionaryArray(Int32)).keys
  Array(Int32).new(keys.len) { |i| keys.value(i) }
end

# A `DictionaryDecoder` over the ported UTF-8 column descriptor.
private def dictionary_decoder : PArrow::DictionaryDecoder(Int32, Int32)
  PArrow::DictionaryDecoder(Int32, Int32).new(ARUtil.utf8_column)
end

# A dictionary page plus a dictionary-indexed data page for `values`.
private def dictionary_index_pages(values : Array(String)) : Array(Parquet::Column::Page)
  desc = PageUtil.column_descriptor(BYTE_ARRAY_COLUMN)
  bytes = byte_array_values(values)
  dict_bytes, index_bytes = ARUtil.encode_dictionary(bytes)

  builder = PageUtil::DataPageBuilderImpl.new(desc, values.size.to_u32, true)
  builder.add_indices(index_bytes)

  [
    Parquet::Column::Page::DictionaryPage.new(
      dict_bytes, values.uniq.size.to_u32, Parquet::Encoding::PLAIN, false
    ).as(Parquet::Column::Page),
    builder.consume,
  ]
end

# A single PLAIN encoded data page for `values`.
private def plain_pages(values : Array(String)) : Array(Parquet::Column::Page)
  desc = PageUtil.column_descriptor(BYTE_ARRAY_COLUMN)
  builder = PageUtil::DataPageBuilderImpl.new(desc, values.size.to_u32, true)
  builder.add_values(Parquet::Encoding::PLAIN, byte_array_values(values))
  [builder.consume]
end

# Builds a byte array dictionary reader over the given page readers.
private def dictionary_reader(page_batches : Array(Array(Parquet::Column::Page)),
                              arrow_type : Arrow::DataType) : PArrow::ArrayReader
  desc = PageUtil.column_descriptor(BYTE_ARRAY_COLUMN)
  PArrow.make_byte_array_dictionary_reader(
    PageUtil::InMemoryPageIterator.new(page_batches), desc, arrow_type, 1024, nil
  )
end

describe Parquet::Arrow::DictionaryDecoder do
  # Upstream `test_dictionary_preservation`.
  it "preserves the dictionary while reading and padding nulls" do
    data = ["0", "1", "0", "1", "2", "1", "2"]
    dict, encoded = ARUtil.encode_dictionary(byte_array_values(data))

    decoder = dictionary_decoder
    decoder.set_dict(dict, 3_u32, Parquet::Encoding::RLE_DICTIONARY, false)
    decoder.set_data(Parquet::Encoding::RLE_DICTIONARY, encoded, 14, data.size)

    output = PDict(Int32, Int32).with_capacity(0)
    decoder.read(output, 3).should eq(3)

    valid = [false, false, true, true, false, true]
    output.pad_nulls(0, 3, valid.size, mask_from(valid))
    output.kind.should eq(PKind::Dict)

    decoder.read(output, 4).should eq(4)

    valid += [false, false, true, true, false, true, true, false]
    valid_mask = mask_from(valid)
    output.pad_nulls(6, 4, 8, valid_mask)
    output.kind.should eq(PKind::Dict)

    array = output.into_array(null_buffer(valid_mask, 14), utf8_dictionary)
    array.data_type.should eq(utf8_dictionary)
    dictionary_strings(array).should eq([
      nil,
      nil,
      "0",
      "1",
      nil,
      "0",
      nil,
      nil,
      "1",
      "2",
      nil,
      "1",
      "2",
      nil,
    ])
  end

  # Upstream `test_dictionary_preservation_skip`.
  it "preserves the dictionary across skips" do
    data = ["0", "1", "0", "1", "2", "1", "2"]
    dict, encoded = ARUtil.encode_dictionary(byte_array_values(data))

    decoder = dictionary_decoder
    decoder.set_dict(dict, 3_u32, Parquet::Encoding::RLE_DICTIONARY, false)
    decoder.set_data(Parquet::Encoding::RLE_DICTIONARY, encoded, 7, data.size)

    output = PDict(Int32, Int32).with_capacity(0)

    # Read two, skip one.
    decoder.read(output, 2).should eq(2)
    decoder.skip_values(1).should eq(1)
    output.kind.should eq(PKind::Dict)

    # Read two, skip one.
    decoder.read(output, 2).should eq(2)
    decoder.skip_values(1).should eq(1)

    # Read one and test a skip at the end.
    decoder.read(output, 1).should eq(1)
    decoder.skip_values(4).should eq(0)

    valid = [true, true, true, true, true]
    valid_mask = mask_from(valid)
    output.pad_nulls(0, 5, 5, valid_mask)
    output.kind.should eq(PKind::Dict)

    array = output.into_array(null_buffer(valid_mask, 5), utf8_dictionary)
    array.data_type.should eq(utf8_dictionary)
    dictionary_strings(array).should eq(["0", "1", "1", "2", "2"])
  end

  # Upstream `test_dictionary_fallback`.
  it "falls back to decoding values and recomputing a dictionary" do
    data = ["hello", "world", "a", "b"]
    pages, encoded_dictionary = ARUtil.byte_array_all_encodings(byte_array_values(data))
    num_encodings = pages.size

    decoder = dictionary_decoder
    decoder.set_dict(encoded_dictionary, 4_u32, Parquet::Encoding::RLE_DICTIONARY, false)

    # Read all pages into a single buffer.
    output = PDict(Int32, Int32).with_capacity(0)
    pages.each do |encoding, page|
      decoder.set_data(encoding, page, 4, 4)
      decoder.read(output, 1024).should eq(4)
    end

    array = output.into_array(nil, utf8_dictionary)
    array.data_type.should eq(utf8_dictionary)
    dictionary_strings(array).should eq(data * num_encodings)
  end

  # Upstream `test_dictionary_skip_fallback`.
  it "skips values when falling back to non-dictionary decoding" do
    data = ["hello", "world", "a", "b"]
    pages, encoded_dictionary = ARUtil.byte_array_all_encodings(byte_array_values(data))
    num_encodings = pages.size

    decoder = dictionary_decoder
    decoder.set_dict(encoded_dictionary, 4_u32, Parquet::Encoding::RLE_DICTIONARY, false)

    output = PDict(Int32, Int32).with_capacity(0)
    pages.each do |encoding, page|
      decoder.set_data(encoding, page, 4, 4)
      decoder.skip_values(2)
      decoder.read(output, 1024).should eq(2)
    end

    array = output.into_array(nil, utf8_dictionary)
    array.data_type.should eq(utf8_dictionary)
    dictionary_strings(array).should eq(data[2..] * num_encodings)
  end

  # Upstream `test_too_large_dictionary`.
  it "rejects dictionaries that do not fit the key type" do
    data = Array(String).new(128) { |i| i.to_s }
    dictionary, _ = ARUtil.encode_dictionary(byte_array_values(data))

    decoder = PArrow::DictionaryDecoder(Int8, Int32).new(ARUtil.utf8_column)
    err = expect_raises(Parquet::GeneralError) do
      decoder.set_dict(dictionary, 128_u32, Parquet::Encoding::RLE_DICTIONARY, false)
    end
    err.message.to_s.should contain("dictionary too large for index type")

    decoder = PArrow::DictionaryDecoder(Int16, Int32).new(ARUtil.utf8_column)
    decoder.set_dict(dictionary, 128_u32, Parquet::Encoding::RLE_DICTIONARY, false)
  end

  # Upstream `test_nulls`.
  it "returns no values for an empty dictionary" do
    pages, encoded_dictionary = ARUtil.byte_array_all_encodings([] of Parquet::ByteArray)

    decoder = dictionary_decoder
    decoder.set_dict(encoded_dictionary, 4_u32, Parquet::Encoding::PLAIN_DICTIONARY, false)

    pages.each do |encoding, page|
      output = PDict(Int32, Int32).with_capacity(0)
      decoder.set_data(encoding, page, 8, nil)
      decoder.read(output, 1024).should eq(0)

      output.pad_nulls(0, 0, 8, Bytes[0])
      array = output.into_array(null_buffer(Bytes[0], 8), utf8_dictionary)

      array.len.should eq(8)
      array.null_count.should eq(8)
      array.logical_null_count.should eq(8)
    end

    pages.each do |encoding, page|
      output = PDict(Int32, Int32).with_capacity(0)
      decoder.set_data(encoding, page, 8, nil)
      decoder.skip_values(1024).should eq(0)

      output.pad_nulls(0, 0, 8, Bytes[0])
      array = output.into_array(null_buffer(Bytes[0], 8), utf8_dictionary)

      array.len.should eq(8)
      array.null_count.should eq(8)
      array.logical_null_count.should eq(8)
    end
  end

  # Upstream `test_dictionary_decoder_empty_data`.
  it "rejects an empty dictionary index page" do
    decoder = dictionary_decoder
    err = expect_raises(Parquet::GeneralError) do
      decoder.set_data(Parquet::Encoding::RLE_DICTIONARY, Bytes.empty, 0, nil)
    end
    err.to_s.should eq("Parquet error: dictionary index page is empty")
  end

  # Upstream `test_dictionary_decoder_invalid_bit_width`.
  it "rejects an invalid RLE bit width" do
    decoder = dictionary_decoder
    err = expect_raises(Parquet::GeneralError) do
      decoder.set_data(Parquet::Encoding::RLE_DICTIONARY, Bytes[33_u8, 0_u8, 0_u8, 0_u8], 1, nil)
    end
    err.to_s.should eq("Parquet error: Invalid or corrupted RLE bit width 33. Max allowed is 32")
  end
end

describe Parquet::Arrow::ByteArrayDictionaryReader do
  it "preserves the dictionary encoding of a single page" do
    values = %w[hello a b d c a b]
    reader = dictionary_reader([dictionary_index_pages(values)], utf8_dictionary)

    reader.data_type.should eq(utf8_dictionary)
    reader.read_records(values.size).should eq(values.size)

    array = reader.consume_batch
    array.data_type.should eq(utf8_dictionary)
    dictionary_values(array).should eq(%w[hello a b d c])
    dictionary_keys(array).should eq([0, 1, 2, 3, 4, 1, 2])
    dictionary_strings(array).should eq(values)

    # Consuming again yields an empty array of the reader's data type.
    empty = reader.consume_batch
    empty.len.should eq(0)
    empty.data_type.should eq(utf8_dictionary)
  end

  it "computes a fresh dictionary for non-dictionary pages" do
    values = %w[hello world hello a]
    reader = dictionary_reader([plain_pages(values)], utf8_dictionary)

    reader.read_records(values.size).should eq(values.size)

    array = reader.consume_batch
    dictionary_values(array).should eq(%w[hello world a])
    dictionary_keys(array).should eq([0, 1, 0, 2])
    dictionary_strings(array).should eq(values)
  end

  it "spills the dictionary when a read spans column chunks" do
    reader = dictionary_reader(
      [dictionary_index_pages(%w[aa bb cc]), dictionary_index_pages(%w[cc bb])],
      utf8_dictionary
    )

    reader.read_records(5).should eq(5)

    # The second column chunk has a different dictionary, so the encoding cannot
    # be preserved: the values are spilled and a new dictionary is computed.
    array = reader.consume_batch
    dictionary_values(array).should eq(%w[aa bb cc])
    dictionary_keys(array).should eq([0, 1, 2, 2, 1])
    dictionary_strings(array).should eq(%w[aa bb cc cc bb])
  end

  it "skips records" do
    reader = dictionary_reader([dictionary_index_pages(%w[hello a b d c])], utf8_dictionary)

    reader.skip_records(2).should eq(2)
    reader.read_records(3).should eq(3)

    array = reader.consume_batch
    dictionary_strings(array).should eq(%w[b d c])
  end

  it "converts the dictionary values to string view arrays" do
    view_type = Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8_view)
    values = %w[hello world hello]
    reader = dictionary_reader([dictionary_index_pages(values)], view_type)

    reader.data_type.should eq(view_type)
    reader.read_records(values.size).should eq(values.size)

    array = reader.consume_batch
    array.data_type.should eq(view_type)

    view_values = array.as(Arrow::DictionaryArray(Int32)).values.as(Arrow::StringViewArray)
    Array(String).new(view_values.len) { |i| view_values.value(i) }
      .should eq(%w[hello world])

    dict = array.as(Arrow::DictionaryArray(Int32))
    keys = dict.keys
    strings = Array(String).new(array.len) { |i| view_values.value(keys.value(i)) }
    strings.should eq(values)
  end

  it "rejects non-dictionary data types" do
    desc = PageUtil.column_descriptor(BYTE_ARRAY_COLUMN)
    pages = PageUtil::InMemoryPageIterator.new([plain_pages(%w[a])])

    err = expect_raises(Parquet::GeneralError) do
      PArrow.make_byte_array_dictionary_reader(pages, desc, Arrow::DataType.utf8, 1024, nil)
    end
    err.message.to_s.should contain(
      "invalid non-dictionary data type for byte array dictionary reader - Utf8"
    )
  end
end
