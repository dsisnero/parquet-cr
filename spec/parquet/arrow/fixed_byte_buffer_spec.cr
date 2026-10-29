require "../../spec_helper"

# fixed_len_byte_array.rs::test_pending_reservation_tracks_max_capacity,
# followed by characterizations of its ValueDecoder and byte buffer contract.
private alias FixedA = Parquet::Arrow
private alias FixedE = Parquet::Encoding

private def fixed_native_desc(width : Int32 = 4)
  ParquetTestPageUtil.column_descriptor("message root { REQUIRED FIXED_LEN_BYTE_ARRAY (#{width}) leaf; }")
end

describe "Arrow fixed-width value buffer and decoder" do
  it "ports pending reservation maximum tracking" do
    buffer = FixedA::FixedLenByteArrayBuffer.with_capacity(0)
    buffer.reserve_exact(8)
    buffer.reserve_exact(4)
    buffer.values_capacity.should eq 8
    buffer.reserve_exact(12)
    buffer.values_capacity.should eq 12
  end

  [2, 4, 8, 32].each do |width|
    it "pads width #{width} bytes in reverse while retaining earlier batches" do
      decoder = FixedA::FixedLenByteArrayValueDecoder.new(fixed_native_desc(width))
      data = Bytes.new(3 * width) { |i| (65 + i // width).to_u8 }
      decoder.set_data(FixedE::PLAIN, data, 3, 3)
      buffer = FixedA::FixedLenByteArrayBuffer.with_capacity(3)
      decoder.read(buffer, 2).should eq 2
      buffer.byte_length.should eq width
      buffer.values_capacity.should be_nil
      buffer.pad_nulls(0, 2, 4, Bytes[0b00001010])
      buffer.buffer.size.should eq 4 * width
      buffer.values[1].as_bytes.should eq data[0, width]
      buffer.values[3].as_bytes.should eq data[width, width]
      decoder.read(buffer, 1).should eq 1
      buffer.pad_nulls(4, 1, 3, Bytes[0b01001010])
      buffer.buffer.size.should eq 7 * width
      buffer.values[1].as_bytes.should eq data[0, width]
      buffer.values[3].as_bytes.should eq data[width, width]
      buffer.values[6].as_bytes.should eq data[2 * width, width]
    end
  end

  it "rejects inconsistent null-padding byte counts" do
    decoder = FixedA::FixedLenByteArrayValueDecoder.new(fixed_native_desc)
    decoder.set_data(FixedE::PLAIN, "abcd".to_slice, 1, 1)
    buffer = FixedA::FixedLenByteArrayBuffer.with_capacity(0)
    decoder.read(buffer, 1).should eq 1
    expect_raises(Parquet::GeneralError, "found inconsistent buffer length while padding nulls: expected 8 bytes, got 4") do
      buffer.pad_nulls(0, 2, 3, Bytes[3])
    end
  end

  it "bounds plain reads/skips by complete payload values instead of count hints" do
    decoder = FixedA::FixedLenByteArrayValueDecoder.new(fixed_native_desc)
    decoder.set_data(FixedE::PLAIN, "abcdwxyz!".to_slice, 100, nil)
    buffer = FixedA::FixedLenByteArrayBuffer.with_capacity(0)
    decoder.read(buffer, 0).should eq 0
    decoder.read(buffer, 1).should eq 1
    decoder.skip_values(100).should eq 1
    decoder.read(buffer, 100).should eq 0
    decoder.skip_values(1).should eq 0
    buffer.values.first.as_bytes.should eq "abcd".to_slice
  end

  it "caps padded dictionary indices at the page count" do
    decoder = FixedA::FixedLenByteArrayValueDecoder.new(fixed_native_desc)
    decoder.set_dict("abcd".to_slice, 1_u32, FixedE::PLAIN, false)
    decoder.set_data(FixedE::RLE_DICTIONARY, Bytes[0, 16, 0], 3, 3)
    buffer = FixedA::FixedLenByteArrayBuffer.with_capacity(0)
    decoder.read(buffer, 1).should eq 1
    decoder.skip_values(1).should eq 1
    decoder.read(buffer, 100).should eq 1
    decoder.read(buffer, 1).should eq 0
    buffer.values.map(&.as_bytes).should eq ["abcd".to_slice, "abcd".to_slice]
  end

  it "returns no values for empty dictionaries but skips their index stream" do
    decoder = FixedA::FixedLenByteArrayValueDecoder.new(fixed_native_desc)
    decoder.set_dict(Bytes.empty, 0_u32, FixedE::PLAIN, false)
    decoder.set_data(FixedE::RLE_DICTIONARY, Bytes[0, 2, 0], 1, 1)
    buffer = FixedA::FixedLenByteArrayBuffer.with_capacity(0)
    decoder.read(buffer, 1).should eq 0
    decoder.skip_values(1).should eq 1
  end

  it "rejects unsupported dictionary encodings with the pinned NYI error" do
    decoder = FixedA::FixedLenByteArrayValueDecoder.new(fixed_native_desc)
    expect_raises(Parquet::NYIError, "Invalid/Unsupported encoding type for dictionary: DELTA_BYTE_ARRAY") do
      decoder.set_dict(Bytes.empty, 0_u32, FixedE::DELTA_BYTE_ARRAY, false)
    end
  end
end
