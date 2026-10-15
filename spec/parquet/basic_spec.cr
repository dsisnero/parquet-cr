require "../spec_helper"

alias PLogicalType = Parquet::LogicalType
alias PConvertedType = Parquet::ConvertedType
alias PEdgeAlgo = Parquet::EdgeInterpolationAlgorithm
alias PTimeUnit = Parquet::TimeUnit
alias PPhysicalType = Parquet::Type
alias PColumnOrder = Parquet::ColumnOrder
alias PSortOrder = Parquet::SortOrder

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

  # Upstream `test_display_edge_algo`.
  it "displays EdgeInterpolationAlgorithm" do
    Parquet::EdgeInterpolationAlgorithm::SPHERICAL.to_s.should eq("SPHERICAL")
    Parquet::EdgeInterpolationAlgorithm::VINCENTY.to_s.should eq("VINCENTY")
    Parquet::EdgeInterpolationAlgorithm::THOMAS.to_s.should eq("THOMAS")
    Parquet::EdgeInterpolationAlgorithm::ANDOYER.to_s.should eq("ANDOYER")
    Parquet::EdgeInterpolationAlgorithm::KARNEY.to_s.should eq("KARNEY")
  end

  # Upstream `test_from_str_edge_algo`.
  it "parses EdgeInterpolationAlgorithm from string (case-insensitive)" do
    Parquet::EdgeInterpolationAlgorithm.from_str("spHErical")
      .should eq(Parquet::EdgeInterpolationAlgorithm::SPHERICAL)
    Parquet::EdgeInterpolationAlgorithm.from_str("vinceNTY")
      .should eq(Parquet::EdgeInterpolationAlgorithm::VINCENTY)
    Parquet::EdgeInterpolationAlgorithm.from_str("tHOmas")
      .should eq(Parquet::EdgeInterpolationAlgorithm::THOMAS)
    Parquet::EdgeInterpolationAlgorithm.from_str("anDOYEr")
      .should eq(Parquet::EdgeInterpolationAlgorithm::ANDOYER)
    Parquet::EdgeInterpolationAlgorithm.from_str("kaRNey")
      .should eq(Parquet::EdgeInterpolationAlgorithm::KARNEY)
    expect_raises(Parquet::ParquetError) do
      Parquet::EdgeInterpolationAlgorithm.from_str("does not exist")
    end
  end

  # Upstream `test_logical_to_converted_type`.
  it "converts LogicalType to ConvertedType" do
    PConvertedType.from(nil).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType.decimal(5, 20)).should eq(PConvertedType::DECIMAL)
    PConvertedType.from(PLogicalType::Bson.new).should eq(PConvertedType::BSON)
    PConvertedType.from(PLogicalType::Json.new).should eq(PConvertedType::JSON)
    PConvertedType.from(PLogicalType::String.new).should eq(PConvertedType::UTF8)
    PConvertedType.from(PLogicalType::Date.new).should eq(PConvertedType::DATE)
    PConvertedType.from(PLogicalType.time(true, PTimeUnit::MILLIS)).should eq(PConvertedType::TIME_MILLIS)
    PConvertedType.from(PLogicalType.time(true, PTimeUnit::MICROS)).should eq(PConvertedType::TIME_MICROS)
    PConvertedType.from(PLogicalType.time(false, PTimeUnit::NANOS)).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType.timestamp(true, PTimeUnit::MILLIS)).should eq(PConvertedType::TIMESTAMP_MILLIS)
    PConvertedType.from(PLogicalType.timestamp(false, PTimeUnit::MICROS)).should eq(PConvertedType::TIMESTAMP_MICROS)
    PConvertedType.from(PLogicalType.timestamp(false, PTimeUnit::NANOS)).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType.integer(8, false)).should eq(PConvertedType::UINT_8)
    PConvertedType.from(PLogicalType.integer(8, true)).should eq(PConvertedType::INT_8)
    PConvertedType.from(PLogicalType.integer(16, false)).should eq(PConvertedType::UINT_16)
    PConvertedType.from(PLogicalType.integer(16, true)).should eq(PConvertedType::INT_16)
    PConvertedType.from(PLogicalType.integer(32, false)).should eq(PConvertedType::UINT_32)
    PConvertedType.from(PLogicalType.integer(32, true)).should eq(PConvertedType::INT_32)
    PConvertedType.from(PLogicalType.integer(64, false)).should eq(PConvertedType::UINT_64)
    PConvertedType.from(PLogicalType.integer(64, true)).should eq(PConvertedType::INT_64)
    PConvertedType.from(PLogicalType::List.new).should eq(PConvertedType::LIST)
    PConvertedType.from(PLogicalType::Map.new).should eq(PConvertedType::MAP)
    PConvertedType.from(PLogicalType::Uuid.new).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType::Enum.new).should eq(PConvertedType::ENUM)
    PConvertedType.from(PLogicalType::Float16.new).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType.variant(nil)).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType.geometry(nil)).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType.geography(nil, PEdgeAlgo::SPHERICAL)).should eq(PConvertedType::NONE)
    PConvertedType.from(PLogicalType::Unknown.new).should eq(PConvertedType::NONE)
  end

  # Upstream `test_logical_type_roundtrip`.
  it "round-trips LogicalType through thrift" do
    values = [
      PLogicalType::String.new,
      PLogicalType::Map.new,
      PLogicalType::List.new,
      PLogicalType::Enum.new,
      PLogicalType.decimal(0, 20),
      PLogicalType::Date.new,
      PLogicalType.time(true, PTimeUnit::MICROS),
      PLogicalType.time(false, PTimeUnit::MILLIS),
      PLogicalType.time(false, PTimeUnit::NANOS),
      PLogicalType.timestamp(false, PTimeUnit::MICROS),
      PLogicalType.timestamp(true, PTimeUnit::MILLIS),
      PLogicalType.timestamp(true, PTimeUnit::NANOS),
      PLogicalType.integer(8, true),
      PLogicalType.integer(16, false),
      PLogicalType.integer(32, true),
      PLogicalType.integer(64, false),
      PLogicalType::Json.new,
      PLogicalType::Bson.new,
      PLogicalType::Uuid.new,
      PLogicalType::Float16.new,
      PLogicalType.variant(1),
      PLogicalType.variant(nil),
      PLogicalType.geometry("foo"),
      PLogicalType.geometry(nil),
      PLogicalType.geography("foo", PEdgeAlgo::ANDOYER),
      PLogicalType.geography(nil, PEdgeAlgo::KARNEY),
      PLogicalType.geography("foo", PEdgeAlgo::SPHERICAL),
      PLogicalType.geography(nil, PEdgeAlgo::SPHERICAL),
    ]

    values.each do |value|
      ThriftRoundTrip.roundtrip(value).should eq(value)
    end
  end

  # Upstream `test_display_sort_order`.
  it "displays SortOrder" do
    PSortOrder::SIGNED.to_s.should eq("SIGNED")
    PSortOrder::UNSIGNED.to_s.should eq("UNSIGNED")
    PSortOrder::UNDEFINED.to_s.should eq("UNDEFINED")
    PSortOrder::TOTAL_ORDER.to_s.should eq("TOTAL_ORDER")
    PSortOrder::INT96_TIMESTAMP.to_s.should eq("INT96_TIMESTAMP")
  end

  # Upstream `test_display_column_order`.
  it "displays ColumnOrder" do
    PColumnOrder.type_defined_order(PSortOrder::SIGNED).to_s.should eq("TYPE_DEFINED_ORDER(SIGNED)")
    PColumnOrder.type_defined_order(PSortOrder::UNSIGNED).to_s.should eq("TYPE_DEFINED_ORDER(UNSIGNED)")
    PColumnOrder.type_defined_order(PSortOrder::UNDEFINED).to_s.should eq("TYPE_DEFINED_ORDER(UNDEFINED)")
    PColumnOrder::IEEE_754_TOTAL_ORDER.to_s.should eq("IEEE_754_TOTAL_ORDER")
    PColumnOrder::INT96_TIMESTAMP_ORDER.to_s.should eq("INT96_TIMESTAMP_ORDER")
    PColumnOrder::UNDEFINED.to_s.should eq("UNDEFINED")
  end

  # Upstream `test_column_order_roundtrip`.
  # `SortOrder::SIGNED` is the default on read.
  it "round-trips ColumnOrder through thrift" do
    value = PColumnOrder.type_defined_order(PSortOrder::SIGNED)
    ThriftRoundTrip.roundtrip(value).should eq(value)
  end

  # Upstream `test_column_order_get_logical_type_sort_order`.
  it "computes sort order from logical type" do
    unsigned = [
      PLogicalType::String.new, PLogicalType::Json.new, PLogicalType::Bson.new,
      PLogicalType::Enum.new, PLogicalType::Uuid.new,
      PLogicalType.integer(8, false), PLogicalType.integer(16, false),
      PLogicalType.integer(32, false), PLogicalType.integer(64, false),
    ]
    unsigned.each do |t|
      PColumnOrder.column_order_for_type(t, PConvertedType::NONE, PPhysicalType::BYTE_ARRAY)
        .sort_order.should eq(PSortOrder::UNSIGNED)
    end

    signed = [
      PLogicalType.integer(8, true), PLogicalType.integer(16, true),
      PLogicalType.integer(32, true), PLogicalType.integer(64, true),
      PLogicalType.decimal(20, 4), PLogicalType::Date.new,
      PLogicalType.time(false, PTimeUnit::MILLIS), PLogicalType.time(false, PTimeUnit::MICROS),
      PLogicalType.time(true, PTimeUnit::NANOS),
      PLogicalType.timestamp(false, PTimeUnit::MILLIS),
      PLogicalType.timestamp(false, PTimeUnit::MICROS),
      PLogicalType.timestamp(true, PTimeUnit::NANOS),
    ]
    signed.each do |t|
      PColumnOrder.column_order_for_type(t, PConvertedType::NONE, PPhysicalType::BYTE_ARRAY)
        .sort_order.should eq(PSortOrder::SIGNED)
    end

    PColumnOrder.column_order_for_type(PLogicalType::Float16.new, PConvertedType::NONE, PPhysicalType::BYTE_ARRAY)
      .sort_order.should eq(PSortOrder::TOTAL_ORDER)

    undefined = [
      PLogicalType::List.new, PLogicalType::Map.new, PLogicalType.variant(nil),
      PLogicalType.geometry(nil), PLogicalType.geography(nil, PEdgeAlgo::SPHERICAL),
    ]
    undefined.each do |t|
      PColumnOrder.column_order_for_type(t, PConvertedType::NONE, PPhysicalType::BYTE_ARRAY)
        .sort_order.should eq(PSortOrder::UNDEFINED)
    end
  end

  # Upstream `test_column_order_get_converted_type_sort_order`.
  it "computes sort order from converted type" do
    check = ->(type_ : PConvertedType, expected : PSortOrder) do
      PColumnOrder.column_order_for_type(nil, type_, PPhysicalType::BYTE_ARRAY).sort_order.should eq(expected)
    end

    [PConvertedType::UTF8, PConvertedType::JSON, PConvertedType::BSON, PConvertedType::ENUM,
     PConvertedType::UINT_8, PConvertedType::UINT_16, PConvertedType::UINT_32, PConvertedType::UINT_64]
      .each { |t| check.call(t, PSortOrder::UNSIGNED) }

    [PConvertedType::INT_8, PConvertedType::INT_16, PConvertedType::INT_32, PConvertedType::INT_64,
     PConvertedType::DECIMAL, PConvertedType::DATE, PConvertedType::TIME_MILLIS, PConvertedType::TIME_MICROS,
     PConvertedType::TIMESTAMP_MILLIS, PConvertedType::TIMESTAMP_MICROS]
      .each { |t| check.call(t, PSortOrder::SIGNED) }

    [PConvertedType::LIST, PConvertedType::MAP, PConvertedType::MAP_KEY_VALUE, PConvertedType::INTERVAL]
      .each { |t| check.call(t, PSortOrder::UNDEFINED) }

    check.call(PConvertedType::NONE, PSortOrder::UNSIGNED)
  end

  # Upstream `test_column_order_get_default_sort_order`.
  it "returns the default sort order for a physical type" do
    PColumnOrder.get_default_sort_order(PPhysicalType::BOOLEAN, true).should eq(PSortOrder::UNSIGNED)
    PColumnOrder.get_default_sort_order(PPhysicalType::INT32, true).should eq(PSortOrder::SIGNED)
    PColumnOrder.get_default_sort_order(PPhysicalType::INT64, true).should eq(PSortOrder::SIGNED)
    PColumnOrder.get_default_sort_order(PPhysicalType::INT96, true).should eq(PSortOrder::UNDEFINED)
    PColumnOrder.get_default_sort_order(PPhysicalType::INT96, false).should eq(PSortOrder::INT96_TIMESTAMP)
    PColumnOrder.get_default_sort_order(PPhysicalType::FLOAT, false).should eq(PSortOrder::TOTAL_ORDER)
    PColumnOrder.get_default_sort_order(PPhysicalType::DOUBLE, false).should eq(PSortOrder::TOTAL_ORDER)
    PColumnOrder.get_default_sort_order(PPhysicalType::FLOAT, true).should eq(PSortOrder::SIGNED)
    PColumnOrder.get_default_sort_order(PPhysicalType::DOUBLE, true).should eq(PSortOrder::SIGNED)
    PColumnOrder.get_default_sort_order(PPhysicalType::BYTE_ARRAY, true).should eq(PSortOrder::UNSIGNED)
    PColumnOrder.get_default_sort_order(PPhysicalType::FIXED_LEN_BYTE_ARRAY, true).should eq(PSortOrder::UNSIGNED)
  end

  # Upstream `test_column_order_sort_order`.
  it "returns the sort order for a column order" do
    PColumnOrder.type_defined_order(PSortOrder::SIGNED).sort_order.should eq(PSortOrder::SIGNED)
    PColumnOrder.type_defined_order(PSortOrder::UNSIGNED).sort_order.should eq(PSortOrder::UNSIGNED)
    PColumnOrder.type_defined_order(PSortOrder::UNDEFINED).sort_order.should eq(PSortOrder::UNDEFINED)
    PColumnOrder::IEEE_754_TOTAL_ORDER.sort_order.should eq(PSortOrder::TOTAL_ORDER)
    PColumnOrder::INT96_TIMESTAMP_ORDER.sort_order.should eq(PSortOrder::INT96_TIMESTAMP)
    PColumnOrder::UNDEFINED.sort_order.should eq(PSortOrder::SIGNED)
  end

  # Upstream `test_parse_encoding`.
  it "parses Encoding from string" do
    Parquet::Encoding.from_str("PLAIN").should eq(Parquet::Encoding::PLAIN)
    Parquet::Encoding.from_str("PLAIN_DICTIONARY").should eq(Parquet::Encoding::PLAIN_DICTIONARY)
    Parquet::Encoding.from_str("RLE").should eq(Parquet::Encoding::RLE)
    Parquet::Encoding.from_str("BIT_PACKED").should eq(Parquet::Encoding::BIT_PACKED)
    Parquet::Encoding.from_str("DELTA_BINARY_PACKED").should eq(Parquet::Encoding::DELTA_BINARY_PACKED)
    Parquet::Encoding.from_str("DELTA_LENGTH_BYTE_ARRAY").should eq(Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY)
    Parquet::Encoding.from_str("DELTA_BYTE_ARRAY").should eq(Parquet::Encoding::DELTA_BYTE_ARRAY)
    Parquet::Encoding.from_str("RLE_DICTIONARY").should eq(Parquet::Encoding::RLE_DICTIONARY)
    Parquet::Encoding.from_str("BYTE_STREAM_SPLIT").should eq(Parquet::Encoding::BYTE_STREAM_SPLIT)
    Parquet::Encoding.from_str("alp").should eq(Parquet::Encoding::ALP)
    Parquet::Encoding.from_str("byte_stream_split").should eq(Parquet::Encoding::BYTE_STREAM_SPLIT)

    expect_raises(Parquet::ParquetError, "unknown encoding: plain_xxx") do
      Parquet::Encoding.from_str("plain_xxx")
    end
  end
end
