# Ported from `parquet/src/arrow/array_reader/test_util.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Test-only helpers for the arrow array reader tests: byte-array encodings in
# every supported encoding, a full `PrimitiveArrayReader(Int32)` builder over a
# single data page, and an empty page iterator.
#
# Divergences: `byte_array_all_encodings` takes `Array(Parquet::ByteArray)` (the
# Rust version accepts `impl Into<ByteArray>`); `get_encoder`/`DictEncoder` are
# the ported registries; the `padding_threshold`/level parameters are keyword
# arguments for readability.
module ParquetTestArrayReaderUtil
  # Returns a descriptor for a UTF-8 column (`max_def_level` 1).
  def self.utf8_column : Parquet::Schema::ColumnDescriptor
    type = Parquet::Schema::Type.primitive_type_builder("col", Parquet::Type::BYTE_ARRAY)
      .with_converted_type(Parquet::ConvertedType::UTF8)
      .build
    Parquet::Schema::ColumnDescriptor.new(
      type, 1_i16, 0_i16, Parquet::Schema::ColumnPath.from([] of String)
    )
  end

  # Encodes `data` with the provided encoding.
  def self.encode_byte_array(encoding : Parquet::Encoding, data : Array(Parquet::ByteArray)) : Bytes
    encoder = Parquet::Encodings.get_encoder(Parquet::ByteArray, encoding, utf8_column)
    encoder.put(data)
    encoder.flush_buffer
  end

  # Returns the encoded dictionary and value data.
  def self.encode_dictionary(data : Array(Parquet::ByteArray)) : Tuple(Bytes, Bytes)
    dict_encoder = Parquet::Encodings::DictEncoder(Parquet::ByteArray).new(utf8_column)
    dict_encoder.put(data)
    encoded_rle = dict_encoder.flush_buffer
    encoded_dictionary = dict_encoder.write_dict
    {encoded_dictionary, encoded_rle}
  end

  # Encodes `data` in all the possible encodings, returning the encoded data per
  # encoding plus the encoded dictionary.
  def self.byte_array_all_encodings(
    data : Array(Parquet::ByteArray),
  ) : Tuple(Array(Tuple(Parquet::Encoding, Bytes)), Bytes)
    encoded_dictionary, encoded_rle = encode_dictionary(data)

    pages = [
      {Parquet::Encoding::PLAIN, encode_byte_array(Parquet::Encoding::PLAIN, data)},
      {Parquet::Encoding::DELTA_BYTE_ARRAY, encode_byte_array(Parquet::Encoding::DELTA_BYTE_ARRAY, data)},
      {Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, encode_byte_array(Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, data)},
      {Parquet::Encoding::PLAIN_DICTIONARY, encoded_rle},
      {Parquet::Encoding::RLE_DICTIONARY, encoded_rle},
    ]
    {pages, encoded_dictionary}
  end

  # Builds a real `PrimitiveArrayReader(Int32)` from raw non-null values and
  # definition/repetition levels, exercising the production record reader
  # (including selective padding when `padding_threshold` is set).
  #
  # `values` must contain only the non-null values (`def_levels[i] ==
  # max_def_level`), in order.
  def self.make_int32_page_reader(
    values : Array(Int32),
    def_levels : Array(Int16),
    rep_levels : Array(Int16),
    max_def_level : Int16,
    max_rep_level : Int16,
    padding_threshold : Int16?,
  ) : Parquet::Arrow::ArrayReader
    leaf_type = Parquet::Schema::Type.primitive_type_builder("leaf", Parquet::Type::INT32).build
    desc = Parquet::Schema::ColumnDescriptor.new(
      leaf_type, max_def_level, max_rep_level, Parquet::Schema::ColumnPath.from([] of String)
    )

    builder = ParquetTestPageUtil::DataPageBuilderImpl.new(desc, def_levels.size.to_u32, true)
    builder.add_rep_levels(max_rep_level, rep_levels) if max_rep_level > 0
    builder.add_def_levels(max_def_level, def_levels) if max_def_level > 0
    builder.add_values(Parquet::Encoding::PLAIN, values)

    pages = ParquetTestPageUtil::InMemoryPageIterator.new([[builder.consume]])
    Parquet::Arrow::PrimitiveArrayReader(Int32).new(
      pages, desc, nil, 1024, padding_threshold
    ).as(Parquet::Arrow::ArrayReader)
  end

  # An iterator over testing reading empty columns.
  class EmptyPageIterator
    include Parquet::Column::PageIterator

    def next : Parquet::Column::PageReader | Iterator::Stop
      Iterator::Stop::INSTANCE
    end
  end
end
