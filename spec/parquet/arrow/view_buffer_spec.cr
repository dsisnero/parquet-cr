require "../../spec_helper"

# Ported from `parquet/src/arrow/buffer/view_buffer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): the byte-view `ValuesBuffer` used by
# the byte-view array reader.
private alias PView = Parquet::Arrow::Buffer::ViewBuffer

private def make_view(data : Bytes, block_id : UInt32, offset : UInt32) : UInt128
  if data.size <= Arrow::MAX_INLINE_VIEW_LEN
    # Inline view: length in the low 32 bits, then up to 12 payload bytes.
    value = data.size.to_u128
    data.each_with_index do |byte, i|
      value |= byte.to_u128 << (32 + i * 8)
    end
    value
  else
    Arrow::ByteView.new(data.size.to_u32, data[0, 4])
      .with_buffer_index(block_id)
      .with_offset(offset)
      .as_u128
  end
end

private def null_buffer(mask : Bytes, len : Int32) : Arrow::NullBuffer
  Arrow::NullBuffer.new(Arrow::BooleanBuffer.from_bits(mask, 0, len))
end

private def mask_of(bools : Array(Bool)) : Bytes
  bytes = Bytes.new((bools.size + 7) // 8, 0_u8)
  bools.each_with_index { |value, i| bytes[i // 8] |= (1_u8 << (i % 8)) if value }
  bytes
end

describe Parquet::Arrow::Buffer::ViewBuffer do
  # Upstream `test_view_buffer_empty`.
  it "builds an empty view array" do
    buffer = PView.with_capacity(0)
    buffer.empty?.should be_true

    array = buffer.into_array(nil, Arrow::DataType.utf8_view).as(Arrow::StringViewArray)
    array.len.should eq(0)
  end

  # Upstream `test_view_buffer_append_view`.
  it "appends blocks and views" do
    buffer = PView.with_capacity(0)
    data = "0123456789long string to test string view".to_slice
    block_id = buffer.append_block(Arrow::Buffer.from_slice_ref(data))

    buffer.views << make_view(data[0, 1], block_id, 0_u32)
    buffer.views << make_view(data[1, 9], block_id, 1_u32)
    buffer.views << make_view(data[10, 31], block_id, 10_u32)

    array = buffer.into_array(nil, Arrow::DataType.utf8_view).as(Arrow::StringViewArray)
    Array(String?).new(array.len) { |i| array.value(i) }.should eq([
      "0",
      "123456789",
      "long string to test string view",
    ])
  end

  # Upstream `test_view_buffer_pad_null`.
  it "pads nulls by shifting views" do
    buffer = PView.with_capacity(0)
    data = "0123456789long string to test string view".to_slice
    block_id = buffer.append_block(Arrow::Buffer.from_slice_ref(data))

    buffer.views << make_view(data[0, 1], block_id, 0_u32)
    buffer.views << make_view(data[1, 9], block_id, 1_u32)
    buffer.views << make_view(data[10, 31], block_id, 10_u32)

    valid = [true, false, false, true, false, false, true]
    valid_mask = mask_of(valid)

    buffer.pad_nulls(1, 2, valid.size - 1, valid_mask)

    array = buffer.into_array(null_buffer(valid_mask, valid.size), Arrow::DataType.utf8_view)
      .as(Arrow::StringViewArray)
    Array(String?).new(array.len) { |i| array.null?(i) ? nil : array.value(i) }.should eq([
      "0",
      nil,
      nil,
      "123456789",
      nil,
      nil,
      "long string to test string view",
    ])
  end
end
