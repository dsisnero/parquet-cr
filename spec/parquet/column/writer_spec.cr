require "../../spec_helper"

# Ported from `parquet/src/column/writer/mod.rs` (`test_compare_greater_byte_array_decimals`)
# and `parquet/src/column/writer/encoder.rs` (caracterization)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias Writer = Parquet::Column::Writer
private alias CVE = Parquet::Column::Writer::ColumnValueEncoderImpl
private alias WP = Parquet::File::Properties::WriterProperties
private alias RCP = Parquet::File::Properties::ResolvedColumnProperties
private alias ES = Parquet::File::Properties::EnabledStatistics
private alias Enc = Parquet::Encoding
private alias Comp = Parquet::Compression
private alias PPhys = Parquet::Type
private alias WV = Parquet::File::Properties::WriterVersion

private def writer_desc(physical : PPhys, type_length : Int32 = -1) : Parquet::Schema::ColumnDescriptor
  ty = Parquet::Schema::Type.primitive_type_builder("col", physical).with_length(type_length).build
  Parquet::Schema::ColumnDescriptor.new(ty, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new("col"))
end

private def resolved(
  encoding : Enc? = nil,
  dictionary_enabled : Bool = false,
  statistics_enabled : ES = ES::Page,
  bloom : Parquet::File::Properties::BloomFilterProperties? = nil,
) : RCP
  RCP.new(encoding, Comp::UNCOMPRESSED, dictionary_enabled, statistics_enabled, false, 1024 * 1024, 1024 * 1024, 1.0, bloom)
end

private def bytes : Bytes
  Bytes.empty
end

private def bytes(*values : UInt8) : Bytes
  array = values.to_a
  Bytes.new(array.to_unsafe, array.size).dup
end

describe Parquet::Column::Writer do
  # Upstream `test_compare_greater_byte_array_decimals`.
  it "compares byte-array decimals" do
    Writer.compare_greater_byte_array_decimals(bytes(), bytes()).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(1_u8), bytes()).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(), bytes(1_u8)).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(1_u8), bytes(0_u8)).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(1_u8), bytes(1_u8)).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(1_u8, 0_u8), bytes(0_u8)).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(0_u8, 1_u8), bytes(1_u8, 0_u8)).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(255_u8, 35_u8, 0_u8, 0_u8), bytes(0_u8)).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(0_u8), bytes(255_u8, 35_u8, 0_u8, 0_u8)).should be_true

    Writer.compare_greater_byte_array_decimals(bytes(0_u8, 128_u8, 0_u8), bytes(0_u8, 255_u8)).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(0_u8, 255_u8), bytes(0_u8, 128_u8, 0_u8)).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(128_u8), bytes(255_u8, 127_u8)).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(255_u8, 127_u8), bytes(128_u8)).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(128_u8), bytes(255_u8, 0_u8)).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(0_u8, 10_u8), bytes(5_u8)).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(10_u8), bytes(0_u8, 5_u8)).should be_true
    Writer.compare_greater_byte_array_decimals(bytes(255_u8, 128_u8), bytes(128_u8)).should be_false
    Writer.compare_greater_byte_array_decimals(bytes(128_u8), bytes(255_u8, 128_u8)).should be_false
  end

  it "selects fallback encodings by version" do
    v1 = WP.builder.set_writer_version(WV::PARQUET_1_0).build
    v2 = WP.builder.set_writer_version(WV::PARQUET_2_0).build

    Writer.fallback_encoding(PPhys::BOOLEAN, v2).should eq(Enc::RLE)
    Writer.fallback_encoding(PPhys::INT32, v2).should eq(Enc::DELTA_BINARY_PACKED)
    Writer.fallback_encoding(PPhys::INT64, v2).should eq(Enc::DELTA_BINARY_PACKED)
    Writer.fallback_encoding(PPhys::BYTE_ARRAY, v2).should eq(Enc::DELTA_BYTE_ARRAY)
    Writer.fallback_encoding(PPhys::FIXED_LEN_BYTE_ARRAY, v2).should eq(Enc::DELTA_BYTE_ARRAY)
    Writer.fallback_encoding(PPhys::INT32, v1).should eq(Enc::PLAIN)
    Writer.fallback_encoding(PPhys::INT96, v2).should eq(Enc::PLAIN)
  end

  it "reports dictionary support" do
    v1 = WP.builder.set_writer_version(WV::PARQUET_1_0).build
    v2 = WP.builder.set_writer_version(WV::PARQUET_2_0).build

    Writer.has_dictionary_support(PPhys::BOOLEAN, v1).should be_false
    Writer.has_dictionary_support(PPhys::BOOLEAN, v2).should be_false
    Writer.has_dictionary_support(PPhys::FIXED_LEN_BYTE_ARRAY, v1).should be_false
    Writer.has_dictionary_support(PPhys::FIXED_LEN_BYTE_ARRAY, v2).should be_true
    Writer.has_dictionary_support(PPhys::INT32, v1).should be_true
  end

  it "computes min/max excluding NaNs" do
    info = writer_desc(PPhys::DOUBLE).get_basic_info
    mm = Writer.get_min_max(info, [Float64::NAN, 1.0, 2.0, Float64::NAN, -3.0])
    mm.should_not be_nil
    mm = mm.not_nil!
    mm[0].should eq(-3.0)
    mm[1].should eq(2.0)
    mm[2].should eq(2_u64)
  end
end

describe Parquet::Column::Writer::ColumnValueEncoderImpl do
  it "encodes PLAIN values with statistics" do
    descr = writer_desc(PPhys::INT32)
    encoder = CVE(Int32).try_new(descr, WP.default, resolved(statistics_enabled: ES::Page))

    encoder.write([1_i32, 2_i32, 3_i32], 0, 3)
    encoder.num_values.should eq(3)
    encoder.has_dictionary.should be_false

    page = encoder.flush_data_page
    page.encoding.should eq(Enc::PLAIN)
    page.num_values.should eq(3)
    page.min_value.should eq(1)
    page.max_value.should eq(3)

    decoder = Parquet::Encodings::PlainDecoder(Int32).new(-1)
    decoder.set_data(page.buf, 3)
    output = Array(Int32).new(3, 0)
    decoder.get(output).should eq(3)
    output.should eq([1_i32, 2_i32, 3_i32])
  end

  it "encodes via a dictionary and flushes both pages" do
    descr = writer_desc(PPhys::INT32)
    encoder = CVE(Int32).try_new(descr, WP.default, resolved(dictionary_enabled: true))
    encoder.has_dictionary.should be_true

    encoder.write([10_i32, 20_i32, 10_i32, 30_i32], 0, 4)
    data = encoder.flush_data_page
    data.encoding.should eq(Enc::RLE_DICTIONARY)
    dict = encoder.flush_dict_page
    dict.should_not be_nil
    dict = dict.not_nil!
    dict.num_values.should eq(3)

    dictionary = Parquet::Encodings::PlainDecoder(Int32).new(-1)
    dictionary.set_data(dict.buf, dict.num_values)
    decoder = Parquet::Encodings::DictDecoder(Int32).new
    decoder.set_dict(dictionary)
    decoder.set_data(data.buf, 4)
    output = Array(Int32).new(4, 0)
    decoder.get(output).should eq(4)
    output.should eq([10_i32, 20_i32, 10_i32, 30_i32])
  end

  it "counts values within a byte budget" do
    Writer.count_values_within_byte_budget(Int32, [1_i32, 2_i32, 3_i32, 4_i32], 0, 4, 8).should eq(2)
    Writer.count_values_within_byte_budget(Int32, [1_i32, 2_i32], 0, 2, 1024).should eq(2)
  end

  it "gathers values by index" do
    descr = writer_desc(PPhys::INT32)
    encoder = CVE(Int32).try_new(descr, WP.default, resolved)
    encoder.write_gather([10_i32, 20_i32, 30_i32, 40_i32], [3, 1])
    encoder.num_values.should eq(2)
    page = encoder.flush_data_page
    decoder = Parquet::Encodings::PlainDecoder(Int32).new(-1)
    decoder.set_data(page.buf, 2)
    output = Array(Int32).new(2, 0)
    decoder.get(output).should eq(2)
    output.should eq([40_i32, 20_i32])
  end
end
