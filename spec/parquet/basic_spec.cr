require "../spec_helper"

# Ported from `parquet/src/basic.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Tests for `LogicalType`, `Compression`, `SortOrder`, `ColumnOrder`,
# `EdgeInterpolationAlgorithm`, and `EncodingMask` are ported with those
# features (see `plans/parity.md`).
describe Parquet do
  # Upstream `test_display_type`.
  it "displays Type" do
    Parquet::Type::BOOLEAN.to_s.should eq("BOOLEAN")
    Parquet::Type::INT32.to_s.should eq("INT32")
    Parquet::Type::INT64.to_s.should eq("INT64")
    Parquet::Type::INT96.to_s.should eq("INT96")
    Parquet::Type::FLOAT.to_s.should eq("FLOAT")
    Parquet::Type::DOUBLE.to_s.should eq("DOUBLE")
    Parquet::Type::BYTE_ARRAY.to_s.should eq("BYTE_ARRAY")
    Parquet::Type::FIXED_LEN_BYTE_ARRAY.to_s.should eq("FIXED_LEN_BYTE_ARRAY")
  end

  # Upstream `test_from_string_into_type`.
  it "parses Type from string" do
    Parquet::Type.from_str("BOOLEAN").should eq(Parquet::Type::BOOLEAN)
    Parquet::Type.from_str("INT32").should eq(Parquet::Type::INT32)
    Parquet::Type.from_str("INT64").should eq(Parquet::Type::INT64)
    Parquet::Type.from_str("INT96").should eq(Parquet::Type::INT96)
    Parquet::Type.from_str("FLOAT").should eq(Parquet::Type::FLOAT)
    Parquet::Type.from_str("DOUBLE").should eq(Parquet::Type::DOUBLE)
    Parquet::Type.from_str("BYTE_ARRAY").should eq(Parquet::Type::BYTE_ARRAY)
    Parquet::Type.from_str("BINARY").should eq(Parquet::Type::BYTE_ARRAY)
    Parquet::Type.from_str("FIXED_LEN_BYTE_ARRAY")
      .should eq(Parquet::Type::FIXED_LEN_BYTE_ARRAY)
  end

  # Upstream `test_converted_type_roundtrip`.
  it "round-trips ConvertedType through thrift" do
    [
      Parquet::ConvertedType::UTF8,
      Parquet::ConvertedType::MAP,
      Parquet::ConvertedType::MAP_KEY_VALUE,
      Parquet::ConvertedType::LIST,
      Parquet::ConvertedType::ENUM,
      Parquet::ConvertedType::DECIMAL,
      Parquet::ConvertedType::DATE,
      Parquet::ConvertedType::TIME_MILLIS,
      Parquet::ConvertedType::TIME_MICROS,
      Parquet::ConvertedType::TIMESTAMP_MILLIS,
      Parquet::ConvertedType::TIMESTAMP_MICROS,
      Parquet::ConvertedType::UINT_8,
      Parquet::ConvertedType::UINT_16,
      Parquet::ConvertedType::UINT_32,
      Parquet::ConvertedType::UINT_64,
      Parquet::ConvertedType::INT_8,
      Parquet::ConvertedType::INT_16,
      Parquet::ConvertedType::INT_32,
      Parquet::ConvertedType::INT_64,
      Parquet::ConvertedType::JSON,
      Parquet::ConvertedType::BSON,
      Parquet::ConvertedType::INTERVAL,
    ].each do |value|
      ThriftRoundTrip.roundtrip(value).should eq(value)
    end
  end

  # Upstream `test_read_invalid_converted_type`.
  it "errors on an invalid ConvertedType" do
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(Bytes[0x7e_u8])
    error = expect_raises(Parquet::ParquetError) do
      Parquet::ConvertedType.read_thrift(prot)
    end
    error.to_s.should eq("Parquet error: Unexpected ConvertedType 63")
  end

  # Upstream `test_display_converted_type`.
  it "displays ConvertedType" do
    Parquet::ConvertedType::NONE.to_s.should eq("NONE")
    Parquet::ConvertedType::UTF8.to_s.should eq("UTF8")
    Parquet::ConvertedType::MAP.to_s.should eq("MAP")
    Parquet::ConvertedType::MAP_KEY_VALUE.to_s.should eq("MAP_KEY_VALUE")
    Parquet::ConvertedType::LIST.to_s.should eq("LIST")
    Parquet::ConvertedType::ENUM.to_s.should eq("ENUM")
    Parquet::ConvertedType::DECIMAL.to_s.should eq("DECIMAL")
    Parquet::ConvertedType::DATE.to_s.should eq("DATE")
    Parquet::ConvertedType::TIME_MILLIS.to_s.should eq("TIME_MILLIS")
    Parquet::ConvertedType::TIME_MICROS.to_s.should eq("TIME_MICROS")
    Parquet::ConvertedType::TIMESTAMP_MILLIS.to_s.should eq("TIMESTAMP_MILLIS")
    Parquet::ConvertedType::TIMESTAMP_MICROS.to_s.should eq("TIMESTAMP_MICROS")
    Parquet::ConvertedType::UINT_8.to_s.should eq("UINT_8")
    Parquet::ConvertedType::UINT_16.to_s.should eq("UINT_16")
    Parquet::ConvertedType::UINT_32.to_s.should eq("UINT_32")
    Parquet::ConvertedType::UINT_64.to_s.should eq("UINT_64")
    Parquet::ConvertedType::INT_8.to_s.should eq("INT_8")
    Parquet::ConvertedType::INT_16.to_s.should eq("INT_16")
    Parquet::ConvertedType::INT_32.to_s.should eq("INT_32")
    Parquet::ConvertedType::INT_64.to_s.should eq("INT_64")
    Parquet::ConvertedType::JSON.to_s.should eq("JSON")
    Parquet::ConvertedType::BSON.to_s.should eq("BSON")
    Parquet::ConvertedType::INTERVAL.to_s.should eq("INTERVAL")
  end

  # Upstream `test_from_string_into_converted_type`.
  it "parses ConvertedType from string" do
    [
      Parquet::ConvertedType::NONE,
      Parquet::ConvertedType::UTF8,
      Parquet::ConvertedType::MAP,
      Parquet::ConvertedType::MAP_KEY_VALUE,
      Parquet::ConvertedType::LIST,
      Parquet::ConvertedType::ENUM,
      Parquet::ConvertedType::DECIMAL,
      Parquet::ConvertedType::DATE,
      Parquet::ConvertedType::TIME_MILLIS,
      Parquet::ConvertedType::TIME_MICROS,
      Parquet::ConvertedType::TIMESTAMP_MILLIS,
      Parquet::ConvertedType::TIMESTAMP_MICROS,
      Parquet::ConvertedType::UINT_8,
      Parquet::ConvertedType::UINT_16,
      Parquet::ConvertedType::UINT_32,
      Parquet::ConvertedType::UINT_64,
      Parquet::ConvertedType::INT_8,
      Parquet::ConvertedType::INT_16,
      Parquet::ConvertedType::INT_32,
      Parquet::ConvertedType::INT_64,
      Parquet::ConvertedType::JSON,
      Parquet::ConvertedType::BSON,
      Parquet::ConvertedType::INTERVAL,
    ].each do |value|
      Parquet::ConvertedType.from_str(value.to_s).should eq(value)
    end
  end

  # Upstream `test_display_repetition`.
  it "displays Repetition" do
    Parquet::Repetition::REQUIRED.to_s.should eq("REQUIRED")
    Parquet::Repetition::OPTIONAL.to_s.should eq("OPTIONAL")
    Parquet::Repetition::REPEATED.to_s.should eq("REPEATED")
  end

  # Upstream `test_from_string_into_repetition`.
  it "parses Repetition from string" do
    Parquet::Repetition.from_str("REQUIRED").should eq(Parquet::Repetition::REQUIRED)
    Parquet::Repetition.from_str("OPTIONAL").should eq(Parquet::Repetition::OPTIONAL)
    Parquet::Repetition.from_str("REPEATED").should eq(Parquet::Repetition::REPEATED)
    expect_raises(Parquet::ParquetError, "Invalid parquet repetition NOPE") do
      Parquet::Repetition.from_str("NOPE")
    end
  end

  # Upstream `test_display_encoding`.
  it "displays Encoding" do
    Parquet::Encoding::PLAIN.to_s.should eq("PLAIN")
    Parquet::Encoding::PLAIN_DICTIONARY.to_s.should eq("PLAIN_DICTIONARY")
    Parquet::Encoding::RLE.to_s.should eq("RLE")
    Parquet::Encoding::BIT_PACKED.to_s.should eq("BIT_PACKED")
    Parquet::Encoding::DELTA_BINARY_PACKED.to_s.should eq("DELTA_BINARY_PACKED")
    Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY.to_s.should eq("DELTA_LENGTH_BYTE_ARRAY")
    Parquet::Encoding::DELTA_BYTE_ARRAY.to_s.should eq("DELTA_BYTE_ARRAY")
    Parquet::Encoding::RLE_DICTIONARY.to_s.should eq("RLE_DICTIONARY")
    Parquet::Encoding::ALP.to_s.should eq("ALP")
  end

  # Upstream `test_display_page_type`.
  it "displays PageType" do
    Parquet::PageType::DATA_PAGE.to_s.should eq("DATA_PAGE")
    Parquet::PageType::INDEX_PAGE.to_s.should eq("INDEX_PAGE")
    Parquet::PageType::DICTIONARY_PAGE.to_s.should eq("DICTIONARY_PAGE")
    Parquet::PageType::DATA_PAGE_V2.to_s.should eq("DATA_PAGE_V2")
  end

  # Upstream `test_display_boundary_order`.
  it "displays BoundaryOrder" do
    Parquet::BoundaryOrder::ASCENDING.to_s.should eq("ASCENDING")
    Parquet::BoundaryOrder::DESCENDING.to_s.should eq("DESCENDING")
    Parquet::BoundaryOrder::UNORDERED.to_s.should eq("UNORDERED")
  end
end
