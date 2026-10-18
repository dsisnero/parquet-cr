require "../../spec_helper"

# Ported from `parquet/src/arrow/buffer/offset_buffer.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): `OffsetBuffer` (the byte
# array value buffer) and its `ValuesBuffer` null padding.
private alias POffset = Parquet::Arrow::Buffer::OffsetBuffer

private def mask_from(bools : Array(Bool)) : Bytes
  bytes = Bytes.new((bools.size + 7) // 8, 0_u8)
  bools.each_with_index { |value, i| bytes[i // 8] |= (1_u8 << (i % 8)) if value }
  bytes
end

private def null_buffer(mask : Bytes, len : Int32) : Arrow::NullBuffer
  Arrow::NullBuffer.new(Arrow::BooleanBuffer.from_bits(mask, 0, len))
end

private def strings_of(array : Arrow::ArrayBase) : Array(String?)
  data = array.to_data
  offsets = data.buffers[0]
  values = data.buffers[1]
  nulls = data.nulls
  Array(String?).new(array.len) do |i|
    if nulls && !nulls.valid?(i)
      nil
    else
      start = Arrow::ScalarBuffer(Int32).new(offsets, 0, array.len + 1)[i]
      finish = Arrow::ScalarBuffer(Int32).new(offsets, 0, array.len + 1)[i + 1]
      String.new(values.as_slice[start, finish - start])
    end
  end
end

describe Parquet::Arrow::Buffer::OffsetBuffer do
  # Upstream `test_offset_buffer_empty`.
  it "builds an empty array" do
    buffer = POffset(Int32).with_capacity(0)
    buffer.len.should eq(0)
    buffer.empty?.should be_true

    array = buffer.into_array(nil, Arrow::DataType.utf8)
    array.len.should eq(0)
  end

  # Upstream `test_offset_buffer_append`.
  it "appends values and dictionary values" do
    buffer = POffset(Int64).with_capacity(0)
    buffer.try_push("hello".to_slice, true)
    buffer.try_push("bar".to_slice, true)
    buffer.extend_from_dictionary([1_i64, 3_i64, 0_i64, 2_i64], [0_i64, 2_i64, 4_i64, 5_i64, 6_i64], "abcdef".to_slice)
    buffer.len.should eq(6)

    array = buffer.into_array(nil, Arrow::DataType.large_utf8)
    strings = array.as(Arrow::GenericStringArray(Int64))
    Array(String?).new(strings.len) { |i| strings.value(i) }.should eq(["hello", "bar", "cd", "f", "ab", "e"])
  end

  # Upstream `test_offset_buffer`.
  it "converts to an array" do
    buffer = POffset(Int32).with_capacity(0)
    ["hello", "world", "cupcakes", "a", "b", "c"].each do |value|
      buffer.try_push(value.to_slice, false)
    end

    array = buffer.into_array(nil, Arrow::DataType.utf8)
    strings_of(array).should eq(["hello", "world", "cupcakes", "a", "b", "c"])
  end

  # Upstream `test_offset_buffer_pad_nulls` (leading and trailing nulls).
  it "pads nulls into the offset buffer" do
    buffer = POffset(Int32).with_capacity(0)
    values = ["a", "b", "c", "def", "gh"]
    values.each { |value| buffer.try_push(value.to_slice, false) }

    valid = [true, false, false, true, false, true, false, true, true, false, false]
    valid_mask = mask_from(valid)

    buffer.pad_nulls(1, values.size - 1, valid.size - 1, valid_mask)

    array = buffer.into_array(null_buffer(valid_mask, valid.size), Arrow::DataType.utf8)
    strings_of(array).should eq([
      "a", nil, nil, "b", nil, "c", nil, "def", "gh", nil, nil,
    ])
  end

  # Upstream `test_utf8_validation`.
  it "validates UTF-8 boundaries" do
    valid_2_byte = Bytes[0b11001000, 0b10001000]
    valid_3_byte = Bytes[0b11101000, 0b10001000, 0b10001000]
    valid_4_byte = Bytes[0b11110010, 0b10101000, 0b10101001, 0b10100101]

    buffer = POffset(Int32).with_capacity(0)
    buffer.try_push(valid_2_byte, true)
    buffer.try_push(valid_3_byte, true)
    buffer.try_push(valid_4_byte, true)

    # Cannot append a string starting with an incomplete codepoint.
    [valid_2_byte[1..], valid_3_byte[1..], valid_3_byte[2..],
     valid_4_byte[1..], valid_4_byte[2..], valid_4_byte[3..]].each do |partial|
      expect_raises(Parquet::GeneralError, "encountered non UTF-8 data") do
        buffer.try_push(partial, true)
      end
    end

    # Can append data containing an incomplete codepoint.
    buffer.try_push(Bytes[0b01111111, 0b10111111], true)
    buffer.len.should eq(4)
    buffer.values.size.should eq(11)

    buffer.try_push(valid_3_byte, true)

    # The whole buffer is invalid (broken codepoint), but the tail is valid.
    expect_raises(Parquet::GeneralError) { buffer.check_valid_utf8(0) }
    buffer.check_valid_utf8(11)
    expect_raises(Parquet::GeneralError) { buffer.check_valid_utf8(12) }
  end

  # Upstream `test_pad_nulls_corrupt_input_returns_err`.
  it "rejects corrupt input while padding nulls" do
    buffer = POffset(Int32).with_capacity(0)
    buffer.try_push("a".to_slice, false)

    expect_raises(Parquet::GeneralError, "inconsistent offsets") do
      buffer.pad_nulls(0, 3, 3, mask_from([true, false, false]))
    end

    buffer = POffset(Int32).with_capacity(0)
    ["a", "b", "c"].each { |value| buffer.try_push(value.to_slice, false) }

    expect_raises(Parquet::GeneralError, "corrupt level data") do
      buffer.pad_nulls(0, 3, 3, mask_from([true, false, false]))
    end
  end

  # Upstream `test_pad_nulls_empty`.
  it "pads an empty buffer to all nulls" do
    buffer = POffset(Int32).with_capacity(0)
    valid_mask = mask_from(Array(Bool).new(9, false))
    buffer.pad_nulls(0, 0, 9, valid_mask)

    array = buffer.into_array(null_buffer(valid_mask, 9), Arrow::DataType.utf8)
    array.len.should eq(9)
    strings_of(array).should eq(Array(String?).new(9, nil))
  end
end
