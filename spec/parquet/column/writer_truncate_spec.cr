require "../../spec_helper"

# Ported from `parquet/src/column/writer/mod.rs`
# (`test_increment`, `test_increment_utf8`, `test_truncate_utf8`,
# `test_increment_max_binary_chars`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias Writer = Parquet::Column::Writer

private def u8arr(*values : UInt8) : Array(UInt8)
  values.to_a
end

private def as_bytes(values : Array(UInt8)) : Bytes
  Slice.new(values.to_unsafe, values.size).dup
end

describe Parquet::Column::Writer do
  # Upstream `test_increment`.
  it "increments binary data" do
    Writer.increment(u8arr(0_u8, 0_u8, 0_u8)).should eq(u8arr(0_u8, 0_u8, 1_u8))
    Writer.increment(u8arr(0_u8, 255_u8, 255_u8)).should eq(u8arr(1_u8, 0_u8, 0_u8))
    Writer.increment(u8arr(255_u8, 255_u8, 255_u8)).should be_nil
  end

  # Upstream `test_increment_max_binary_chars`.
  it "increments binary data at max values" do
    Writer.increment(u8arr(0xFF_u8, 0xFE_u8, 0xFD_u8, 0xFF_u8, 0xFF_u8)).should eq(
      u8arr(0xFF_u8, 0xFE_u8, 0xFE_u8, 0x00_u8, 0x00_u8)
    )
    Writer.increment(u8arr(0xFF_u8, 0xFF_u8, 0xFF_u8)).should be_nil
  end

  # Upstream `test_increment_utf8`.
  it "increments UTF-8 strings" do
    check = ->(original : String, expected : String) do
      result = Writer.increment_utf8(original)
      result.should_not be_nil
      bytes = result.not_nil!
      String.new(bytes).should eq(expected)
      (String.new(bytes) > original).should be_true

      greater = Parquet::ByteArray.new(bytes)
      original_ba = Parquet::ByteArray.new(original)
      (greater > original_ba).should be_true
    end

    check.call("hello", "hellp")
    check.call("a\u{7f}", "b")
    Writer.increment_utf8("\u{7f}\u{7f}").should be_nil
    check.call("❤️🧡💛💚💙💜", "❤️🧡💛💚💙💝")
    check.call("éééé", "éééê")
    check.call("\u{ff}\u{ff}", "\u{ff}\u{100}")
    check.call("a\u{7ff}", "b")
    Writer.increment_utf8("\u{7ff}\u{7ff}").should be_nil
    check.call("ࠀࠀ", "ࠀࠁ")
    check.call("a\u{ffff}", "b")
    Writer.increment_utf8("\u{ffff}\u{ffff}").should be_nil
    check.call("𐀀𐀀", "𐀀𐀁")
    check.call("a\u{10ffff}", "b")
    Writer.increment_utf8("\u{10ffff}\u{10ffff}").should be_nil
    check.call("a\u{D7FF}", "b")
  end

  # Upstream `test_truncate_utf8`.
  it "truncates UTF-8 statistics" do
    data = "❤️🧡💛💚💙💜"

    r = Writer.truncate_utf8(data, data.bytesize)
    r.should_not be_nil
    r = r.not_nil!
    r.size.should eq(data.bytesize)
    r.should eq(data.to_slice)

    r = Writer.truncate_utf8(data, 13)
    r.should_not be_nil
    r = r.not_nil!
    r.size.should eq(10)
    r.should eq("❤️🧡".to_slice)

    Writer.truncate_utf8("\u{0836}", 1).should be_nil

    r = Writer.truncate_and_increment_utf8("yyyyyyyyy", 8)
    r.should_not be_nil
    r.not_nil!.should eq("yyyyyyyz".to_slice)

    r = Writer.truncate_and_increment_utf8("ééééé", 7)
    r.should_not be_nil
    r.not_nil!.should eq("ééê".to_slice)

    r = Writer.truncate_and_increment_utf8("\u{ff}\u{ff}\u{ff}\u{ff}\u{ff}", 8)
    r.should_not be_nil
    r.not_nil!.should eq("\u{ff}\u{ff}\u{ff}\u{100}".to_slice)

    Writer.truncate_and_increment_utf8("߿߿߿߿߿", 8).should be_nil

    r = Writer.truncate_and_increment_utf8("ࠀࠀࠀࠀ", 8)
    r.should_not be_nil
    r.not_nil!.should eq("ࠀࠁ".to_slice)

    Writer.truncate_and_increment_utf8("\u{ffff}\u{ffff}\u{ffff}", 8).should be_nil

    r = Writer.truncate_and_increment_utf8("𐀀𐀀𐀀𐀀", 9)
    r.should_not be_nil
    r.not_nil!.should eq("𐀀𐀁".to_slice)

    Writer.truncate_and_increment_utf8("\u{10ffff}\u{10ffff}", 8).should be_nil
  end

  it "reports whether a column's values can be truncated" do
    byte_array = make_truncate_desc(PType::BYTE_ARRAY)
    flba = make_truncate_desc(PType::FIXED_LEN_BYTE_ARRAY, type_length: 4)
    flba_f16 = make_truncate_desc(PType::FIXED_LEN_BYTE_ARRAY, type_length: 2, logical: LogicalType::Float16.new)
    int32 = make_truncate_desc(PType::INT32)

    Writer.can_truncate_value(byte_array).should be_true
    Writer.can_truncate_value(flba).should be_true
    Writer.can_truncate_value(flba_f16).should be_false
    Writer.can_truncate_value(int32).should be_false
  end

  it "detects UTF-8 columns" do
    Writer.is_utf8(make_truncate_desc(PType::BYTE_ARRAY, logical: LogicalType::String.new)).should be_true
    Writer.is_utf8(make_truncate_desc(PType::BYTE_ARRAY, converted: PCT::UTF8)).should be_true
    Writer.is_utf8(make_truncate_desc(PType::BYTE_ARRAY)).should be_false
  end

  it "truncates min and max values" do
    utf8 = make_truncate_desc(PType::BYTE_ARRAY, logical: LogicalType::String.new)
    binary = make_truncate_desc(PType::BYTE_ARRAY)

    min, truncated = Writer.truncate_min_value(utf8, 7, "ééééé".to_slice)
    truncated.should be_true
    min.should eq("ééé".to_slice)

    min, truncated = Writer.truncate_min_value(binary, 4, as_bytes(u8arr(1_u8, 2, 3, 4, 5, 6)))
    truncated.should be_true
    min.should eq(as_bytes(u8arr(1_u8, 2, 3, 4)))

    max, truncated = Writer.truncate_max_value(utf8, 8, "yyyyyyyyy".to_slice)
    truncated.should be_true
    max.should eq("yyyyyyyz".to_slice)

    invalid = as_bytes(Array(UInt8).new(32, 128_u8))
    max, truncated = Writer.truncate_max_value(utf8, 8, invalid)
    truncated.should be_true
    max.should eq(as_bytes(u8arr(128_u8, 128, 128, 128, 128, 128, 128, 129)))
  end
end

private alias PType = Parquet::Type
private alias PCT = Parquet::ConvertedType
private alias LogicalType = Parquet::LogicalType

private def make_truncate_desc(
  physical : PType,
  type_length : Int32 = -1,
  logical : LogicalType? = nil,
  converted : PCT = PCT::NONE,
) : Parquet::Schema::ColumnDescriptor
  builder = Parquet::Schema::Type.primitive_type_builder("col", physical).with_length(type_length)
  builder = builder.with_logical_type(logical) if logical
  builder = builder.with_converted_type(converted) unless converted == PCT::NONE
  Parquet::Schema::ColumnDescriptor.new(builder.build, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new("col"))
end
