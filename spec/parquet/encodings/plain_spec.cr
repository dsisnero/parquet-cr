require "../../spec_helper"

# Ported from `parquet/src/encodings/decoding.rs` `mod tests` (PLAIN section) and
# the plain cases of `parquet/src/encodings/encoding/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: the Rust `DataType`/`Decoder` trait dispatch is
# `PlainDecoder(T)`/`PlainEncoder(T)` with `T.class` here.

private def plain_default(type : T.class) : T forall T
  {% if T == Bool %}
    false
  {% elsif T == Int32 %}
    0
  {% elsif T == Int64 %}
    0_i64
  {% elsif T == Float32 %}
    0.0_f32
  {% elsif T == Float64 %}
    0.0_f64
  {% elsif T == Parquet::Int96 %}
    Parquet::Int96.new
  {% elsif T == Parquet::ByteArray %}
    Parquet::ByteArray.new
  {% elsif T == Parquet::FixedLenByteArray %}
    Parquet::FixedLenByteArray.new(Parquet::ByteArray.new)
  {% else %}
    {% raise "no default" %}
  {% end %}
end

private def plain_decode(type : T.class, data : Bytes, num_values : Int32, type_length : Int32, buffer : Array(T), expected : Array(T)) : Nil forall T
  decoder = PlainDecoder(T).new(type_length)
  decoder.set_data(data, num_values)
  decoder.get(buffer)
  decoder.values_left.should eq(0)
  buffer.should eq(expected)
end

private def plain_skip(type : T.class, data : Bytes, num_values : Int32, skip : Int32, type_length : Int32, expected : Array(T)) : Nil forall T
  decoder = PlainDecoder(T).new(type_length)
  decoder.set_data(data, num_values)
  skipped = decoder.skip(skip)

  if skip >= num_values
    skipped.should eq(num_values)
    buffer = Array(T).new(1) { plain_default(T) }
    decoder.get(buffer).should eq(0)
  else
    skipped.should eq(skip)
    buffer = Array(T).new(num_values - skip) { plain_default(T) }
    decoder.get(buffer).should eq(num_values - skip)
    decoder.values_left.should eq(0)
    buffer.should eq(expected)
  end
end

describe Parquet::Encodings::PlainDecoder do
  it "decodes int32" do
    data = [42, 18, 52]
    bytes = PlainEncoder(Int32).encode_all(data)
    buffer = [0, 0, 0]
    plain_decode(Int32, bytes, 3, -1, buffer, data)
  end

  it "skips int32" do
    data = [42, 18, 52]
    bytes = PlainEncoder(Int32).encode_all(data)
    plain_skip(Int32, bytes, 3, 1, -1, [18, 52])
    plain_skip(Int32, bytes, 3, 5, -1, [] of Int32)
  end

  it "decodes int32 with spacing" do
    data = [42, 18, 52]
    bytes = PlainEncoder(Int32).encode_all(data)
    buffer = [0, 0, 0, 0, 0, 0, 0, 0]
    valid_bits = Bytes[0b01001010_u8]

    decoder = PlainDecoder(Int32).new(-1)
    decoder.set_data(bytes, 3)
    decoder.get_spaced(buffer, 5, valid_bits).should eq(8)
    decoder.values_left.should eq(0)
    buffer.should eq([0, 42, 0, 18, 0, 0, 52, 0])
  end

  it "decodes and skips int64" do
    data = [42_i64, 18_i64, 52_i64]
    bytes = PlainEncoder(Int64).encode_all(data)
    plain_decode(Int64, bytes, 3, -1, [0_i64, 0_i64, 0_i64], data)
    plain_skip(Int64, bytes, 3, 2, -1, [52_i64])
    plain_skip(Int64, bytes, 3, 3, -1, [] of Int64)
  end

  it "decodes and skips float" do
    data = [Math::PI.to_f32, 2.414_f32, 12.51_f32]
    bytes = PlainEncoder(Float32).encode_all(data)
    plain_decode(Float32, bytes, 3, -1, [0.0_f32, 0.0_f32, 0.0_f32], data)
    plain_skip(Float32, bytes, 3, 1, -1, [2.414_f32, 12.51_f32])
    plain_skip(Float32, bytes, 3, 4, -1, [] of Float32)
  end

  it "decodes and skips double" do
    data = [Math::PI, 2.414, 12.51]
    bytes = PlainEncoder(Float64).encode_all(data)
    plain_decode(Float64, bytes, 3, -1, [0.0, 0.0, 0.0], data)
    plain_skip(Float64, bytes, 3, 1, -1, [2.414, 12.51])
    plain_skip(Float64, bytes, 3, 5, -1, [] of Float64)
  end

  it "decodes and skips int96" do
    data = [Parquet::Int96.from([11_u32, 22_u32, 33_u32]), Parquet::Int96.from([44_u32, 55_u32, 66_u32]),
            Parquet::Int96.from([10_u32, 20_u32, 30_u32]), Parquet::Int96.from([40_u32, 50_u32, 60_u32])]
    bytes = PlainEncoder(Parquet::Int96).encode_all(data)
    plain_decode(Parquet::Int96, bytes, 4, -1, Array(Parquet::Int96).new(4) { Parquet::Int96.new }, data)
    plain_skip(Parquet::Int96, bytes, 4, 2, -1, [data[2], data[3]])
    plain_skip(Parquet::Int96, bytes, 4, 8, -1, [] of Parquet::Int96)
  end

  it "decodes and skips bool" do
    data = [false, true, false, false, true, false, true, true, false, true]
    bytes = PlainEncoder(Bool).encode_all(data)
    plain_decode(Bool, bytes, 10, -1, Array(Bool).new(10, false), data)
    plain_skip(Bool, bytes, 10, 5, -1, [false, true, true, false, true])
    plain_skip(Bool, bytes, 10, 20, -1, [] of Bool)
  end

  it "decodes and skips byte array" do
    data = [Parquet::ByteArray.new("hello".to_slice), Parquet::ByteArray.new("parquet".to_slice)]
    bytes = PlainEncoder(Parquet::ByteArray).encode_all(data)
    plain_decode(Parquet::ByteArray, bytes, 2, -1, [Parquet::ByteArray.new, Parquet::ByteArray.new], data)
    plain_skip(Parquet::ByteArray, bytes, 2, 1, -1, [data[1]])
    plain_skip(Parquet::ByteArray, bytes, 2, 2, -1, [] of Parquet::ByteArray)
  end

  it "decodes and skips fixed len byte array" do
    data = [Parquet::FixedLenByteArray.new("bird"), Parquet::FixedLenByteArray.new("come"), Parquet::FixedLenByteArray.new("flow")]
    bytes = PlainEncoder(Parquet::FixedLenByteArray).encode_all(data)
    plain_decode(Parquet::FixedLenByteArray, bytes, 3, 4, Array(Parquet::FixedLenByteArray).new(3) { Parquet::FixedLenByteArray.new(Parquet::ByteArray.new) }, data)
    plain_skip(Parquet::FixedLenByteArray, bytes, 3, 1, 4, [data[1], data[2]])
    plain_skip(Parquet::FixedLenByteArray, bytes, 3, 6, 4, [] of Parquet::FixedLenByteArray)
  end
end

describe Parquet::Encodings::PlainEncoder do
  it "round-trips a primitive batch" do
    values = [1, 2, 3, 4, 5]
    encoder = PlainEncoder(Int32).new
    encoder.put(values)
    encoder.flush_buffer.should eq(Bytes[
      1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0,
    ])
    encoder.encoding.should eq(Parquet::Encoding::PLAIN)
  end

  it "encodes bools as a packed bit stream" do
    encoder = PlainEncoder(Bool).new
    encoder.put([true, false, true, true, false, false, false, true])
    encoder.flush_buffer.should eq(Bytes[0b10001101_u8])
  end

  it "encodes byte arrays with a length prefix" do
    encoder = PlainEncoder(Parquet::ByteArray).new
    encoder.put([Parquet::ByteArray.new("ab".to_slice)])
    encoder.flush_buffer.should eq(Bytes[2, 0, 0, 0, 'a'.ord.to_u8, 'b'.ord.to_u8])
  end
end
