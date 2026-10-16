require "../../spec_helper"

# Ported from `parquet/src/encodings/encoding/dict_encoder.rs`,
# `parquet/src/encodings/encoding/mod.rs` (DictEncoder-related tests), and
# `parquet/src/encodings/decoding.rs` (DictDecoder)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: `estimated_memory_size` accounts for element counts rather than
# Rust's retained capacity (`Array` exposes no capacity), so the two upstream
# tests that assert on `capacity()` are characterized with length semantics.

private def make_col_desc(physical : PPhys, type_length : Int32 = -1) : SColumnDescriptor
  ty = ST.primitive_type_builder("col", physical).with_length(type_length).build
  SColumnDescriptor.new(ty, 0_i16, 0_i16, SColumnPath.new([] of String))
end

private def dict_roundtrip(desc : SColumnDescriptor, values : Array(T)) : Nil forall T
  encoder = Parquet::Encodings::DictEncoder(T).new(desc)
  encoder.put(values)
  data = encoder.flush_buffer

  decoder = Parquet::Encodings::DictDecoder(T).new
  dict_decoder = PlainDecoder(T).new(desc.type_length)
  dict_decoder.set_data(encoder.write_dict, encoder.num_entries)
  decoder.set_dict(dict_decoder)

  result = Array(T).new(values.size) { Parquet::Encodings::DictDecoder(T).default_value(T) }
  decoder.set_data(data, values.size)
  read = decoder.get(result)
  read.should eq(values.size)
  result.should eq(values)
end

describe Parquet::Encodings::DictEncoder do
  # Upstream `test_dict_encoded_size`.
  it "reports the dictionary encoded size" do
    # Only 2 variations of values 1 byte each.
    enc = Parquet::Encodings::DictEncoder(Bool).new(make_col_desc(PPhys::BOOLEAN))
    enc.put([true, false, true, false, true])
    enc.dict_encoded_size.should eq(2)

    enc = Parquet::Encodings::DictEncoder(Int32).new(make_col_desc(PPhys::INT32))
    enc.put([1_i32, 2_i32, 3_i32, 4_i32, 5_i32])
    enc.dict_encoded_size.should eq(20)

    enc = Parquet::Encodings::DictEncoder(Int64).new(make_col_desc(PPhys::INT64))
    enc.put([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
    enc.dict_encoded_size.should eq(40)

    enc = Parquet::Encodings::DictEncoder(Float32).new(make_col_desc(PPhys::FLOAT))
    enc.put([1.0_f32, 2.0_f32, 3.0_f32, 4.0_f32, 5.0_f32])
    enc.dict_encoded_size.should eq(20)

    enc = Parquet::Encodings::DictEncoder(Float64).new(make_col_desc(PPhys::DOUBLE))
    enc.put([1.0_f64, 2.0_f64, 3.0_f64, 4.0_f64, 5.0_f64])
    enc.dict_encoded_size.should eq(40)

    # INT96: len + reference.
    enc = Parquet::Encodings::DictEncoder(PInt96).new(make_col_desc(PPhys::INT96))
    enc.put([PInt96.from([1_u32, 2_u32, 3_u32]), PInt96.from([2_u32, 3_u32, 4_u32])])
    enc.dict_encoded_size.should eq(24)

    enc = Parquet::Encodings::DictEncoder(PByteArray).new(make_col_desc(PPhys::BYTE_ARRAY))
    enc.put([PByteArray.new("abcd"), PByteArray.new("efj")])
    enc.dict_encoded_size.should eq(15)

    enc = Parquet::Encodings::DictEncoder(PFixedLenByteArray).new(make_col_desc(PPhys::FIXED_LEN_BYTE_ARRAY, 2))
    enc.put([PFixedLenByteArray.new("ab"), PFixedLenByteArray.new("bc")])
    enc.dict_encoded_size.should eq(4)
  end

  # Upstream `test_estimated_memory_size_primitive_with_duplicates`.
  it "accounts for dictionary storage and buffered indices" do
    encoder = Parquet::Encodings::DictEncoder(Int32).new(make_col_desc(PPhys::INT32))
    empty_size = encoder.estimated_memory_size

    encoder.put([1_i32, 2_i32, 3_i32, 1_i32, 2_i32, 3_i32, 1_i32, 2_i32, 3_i32])
    size = encoder.estimated_memory_size

    dict_entry_size = 3 * sizeof(Int32)
    indices_size = 9 * sizeof(UInt64)
    size.should be >= empty_size + dict_entry_size
    size.should be >= empty_size + dict_entry_size + indices_size
  end

  # Upstream `test_estimated_memory_size_primitive_all_distinct`.
  it "accounts for all-distinct primitive dictionary entries" do
    encoder = Parquet::Encodings::DictEncoder(Int32).new(make_col_desc(PPhys::INT32))
    empty_size = encoder.estimated_memory_size

    vals = Array(Int32).new(100) { |i| i.to_i32 }
    encoder.put(vals)
    size = encoder.estimated_memory_size

    dict_entry_size = 100 * sizeof(Int32)
    indices_size = 100 * sizeof(UInt64)
    size.should be >= empty_size + dict_entry_size
    size.should be >= empty_size + dict_entry_size + indices_size
  end

  # Upstream `test_estimated_memory_size_byte_array_with_duplicates`.
  it "accounts for byte-array dictionary heap bytes" do
    encoder = Parquet::Encodings::DictEncoder(PByteArray).new(make_col_desc(PPhys::BYTE_ARRAY))
    empty_size = encoder.estimated_memory_size

    vals = ["foo", "bar", "baz", "foo", "bar", "baz", "foo", "bar", "baz"].map { |s| PByteArray.new(s) }
    encoder.put(vals)
    size = encoder.estimated_memory_size

    dict_entry_size = 3 * sizeof(PByteArray) + 3 * 3
    indices_size = 9 * sizeof(UInt64)
    size.should be >= empty_size + dict_entry_size
    size.should be >= empty_size + dict_entry_size + indices_size
  end

  # Upstream `test_estimated_memory_size_byte_array_all_distinct`.
  it "accounts for all-distinct byte-array heap bytes" do
    encoder = Parquet::Encodings::DictEncoder(PByteArray).new(make_col_desc(PPhys::BYTE_ARRAY))
    empty_size = encoder.estimated_memory_size

    values = (0...100).map { |i| PByteArray.new(i.to_s) }
    bytes_total = values.sum(&.len)
    encoder.put(values)
    size = encoder.estimated_memory_size

    dict_entry_size = 100 * sizeof(PByteArray) + bytes_total
    indices_size = 100 * sizeof(UInt64)
    size.should be >= empty_size + dict_entry_size
    size.should be >= empty_size + dict_entry_size + indices_size
  end

  # Upstream `test_estimated_memory_size_fixed_len_byte_array_with_duplicates`.
  it "accounts for fixed-len byte-array dictionary storage" do
    encoder = Parquet::Encodings::DictEncoder(PFixedLenByteArray).new(
      make_col_desc(PPhys::FIXED_LEN_BYTE_ARRAY, 3)
    )
    empty_size = encoder.estimated_memory_size

    vals = ["foo", "bar", "baz", "foo", "bar", "baz", "foo", "bar", "baz"].map do |s|
      PFixedLenByteArray.new(s)
    end
    encoder.put(vals)
    size = encoder.estimated_memory_size

    dict_entry_size = 3 * sizeof(PFixedLenByteArray) + 3 * 3
    indices_size = 9 * sizeof(UInt64)
    size.should be >= empty_size + dict_entry_size
    size.should be >= empty_size + dict_entry_size + indices_size
  end

  # Upstream `test_estimated_memory_size_fixed_len_byte_array_all_distinct`.
  it "accounts for all-distinct fixed-len byte-array storage" do
    encoder = Parquet::Encodings::DictEncoder(PFixedLenByteArray).new(
      make_col_desc(PPhys::FIXED_LEN_BYTE_ARRAY, 3)
    )
    empty_size = encoder.estimated_memory_size

    values = (0...100).map { |i| PFixedLenByteArray.new([0_u8, 0_u8, i.to_u8!]) }
    encoder.put(values)
    size = encoder.estimated_memory_size

    dict_entry_size = 100 * sizeof(PFixedLenByteArray) + 100 * 3
    indices_size = 100 * sizeof(UInt64)
    size.should be >= empty_size + dict_entry_size
    size.should be >= empty_size + dict_entry_size + indices_size
  end

  # Upstream `test_estimated_memory_size_includes_interner_dedup_table`.
  it "includes the preallocated interner dedup table" do
    encoder = Parquet::Encodings::DictEncoder(Int32).new(make_col_desc(PPhys::INT32))
    encoder.estimated_memory_size.should be > 0
  end

  # Upstream `test_estimated_memory_size_accounts_for_indices_capacity`
  # (characterized with length semantics: flushed indices are dropped, a
  # re-interned value only adds one buffered index).
  it "accounts for buffered indices after a flush" do
    encoder = Parquet::Encodings::DictEncoder(Int32).new(make_col_desc(PPhys::INT32))
    encoder.put(Array(Int32).new(64, 0_i32))
    encoder.flush_buffer
    flushed_size = encoder.estimated_memory_size

    encoder.put([0_i32])
    encoder.estimated_memory_size.should eq(flushed_size + sizeof(UInt64))
  end

  # Upstream `test_estimated_memory_size_accounts_for_uniques_capacity`.
  it "grows with the number of unique values" do
    encoder = Parquet::Encodings::DictEncoder(Int32).new(make_col_desc(PPhys::INT32))

    encoder.put(Array(Int32).new(64) { |i| i.to_i32 })
    encoder.flush_buffer
    size1 = encoder.estimated_memory_size

    encoder.put(Array(Int32).new(64) { |i| (i + 64).to_i32 })
    encoder.flush_buffer
    size2 = encoder.estimated_memory_size

    min_uniques_bytes = 64 * sizeof(Int32)
    size2.should be >= size1 + min_uniques_bytes
  end

  # Upstream `test_estimated_data_encoded_size` (RLE_DICTIONARY case).
  it "estimates the data page size" do
    encoder = Parquet::Encodings::DictEncoder(Int32).new(make_col_desc(PPhys::INT32))
    encoder.estimated_data_encoded_size.should eq(0)

    encoder.put([123_i32, 1024_i32])
    encoder.estimated_data_encoded_size.should eq(2)

    encoder.flush_buffer
    encoder.estimated_data_encoded_size.should eq(0)
  end

  # Upstream `test_bool` / `test_i32` / `test_byte_array` dictionary paths
  # (`test_dict_internal`).
  it "round-trips through the dictionary decoder" do
    dict_roundtrip(make_col_desc(PPhys::BOOLEAN), [true, false, true, true, false])
    dict_roundtrip(make_col_desc(PPhys::INT32), [1_i32, 2_i32, 3_i32, 1_i32, 2_i32, 3_i32])
    dict_roundtrip(make_col_desc(PPhys::INT64), [1_i64, 2_i64, 3_i64, 1_i64, 2_i64, 3_i64])
    dict_roundtrip(make_col_desc(PPhys::FLOAT), [1.5_f32, 2.5_f32, 1.5_f32])
    dict_roundtrip(make_col_desc(PPhys::DOUBLE), [1.5_f64, 2.5_f64, 1.5_f64])
    dict_roundtrip(
      make_col_desc(PPhys::INT96),
      [PInt96.from([1_u32, 2_u32, 3_u32]), PInt96.from([2_u32, 3_u32, 4_u32]), PInt96.from([1_u32, 2_u32, 3_u32])]
    )
    dict_roundtrip(
      make_col_desc(PPhys::BYTE_ARRAY),
      [PByteArray.new("foo"), PByteArray.new("bar"), PByteArray.new("foo")]
    )
    dict_roundtrip(
      make_col_desc(PPhys::FIXED_LEN_BYTE_ARRAY, 3),
      [PFixedLenByteArray.new("abc"), PFixedLenByteArray.new("def"), PFixedLenByteArray.new("abc")]
    )
  end

  it "re-encodes after a flush" do
    desc = make_col_desc(PPhys::INT32)
    encoder = Parquet::Encodings::DictEncoder(Int32).new(desc)
    encoder.put([1_i32, 2_i32, 3_i32])
    first = encoder.flush_buffer

    encoder.put([3_i32, 2_i32, 1_i32])
    second = encoder.flush_buffer

    decoder = Parquet::Encodings::DictDecoder(Int32).new
    dict_decoder = PlainDecoder(Int32).new(desc.type_length)
    dict_decoder.set_data(encoder.write_dict, encoder.num_entries)
    decoder.set_dict(dict_decoder)

    output = Array(Int32).new(3, 0_i32)
    decoder.set_data(first, 3)
    decoder.get(output)
    output.should eq([1, 2, 3])

    output2 = Array(Int32).new(3, 0_i32)
    decoder.set_data(second, 3)
    decoder.get(output2)
    output2.should eq([3, 2, 1])
  end
end

describe Parquet::Encodings::DictDecoder do
  it "rejects an invalid bit width" do
    decoder = Parquet::Encodings::DictDecoder(Int32).new
    expect_raises(Parquet::ParquetError, "Invalid or corrupted RLE bit width") do
      decoder.set_data(Bytes[33_u8, 0_u8], 1)
    end
  end

  it "rejects empty data" do
    decoder = Parquet::Encodings::DictDecoder(Int32).new
    expect_raises(Parquet::ParquetError, "Not enough bytes to decode bit_width") do
      decoder.set_data(Bytes.empty, 1)
    end
  end
end
