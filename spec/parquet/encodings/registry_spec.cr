require "../../spec_helper"

private alias Enc = Parquet::Encoding

# Ported from `test_get_encoders` (`parquet/src/encodings/encoding/mod.rs`) and
# `test_get_decoders` (`parquet/src/encodings/decoding.rs`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: upstream's fallible `get_encoder`/`get_decoder` return a `Result`;
# Crystal raises. The tests therefore capture the raised `ParquetError` and
# compare its message to the upstream error, and otherwise assert the selected
# codec's `encoding`.

private def registry_desc(physical : PPhys, type_length : Int32) : SColumnDescriptor
  ty = ST.primitive_type_builder("t", physical).with_length(type_length).build
  SColumnDescriptor.new(ty, 0_i16, 0_i16, SColumnPath.new([] of String))
end

private def check_encoder(type : T.class, type_length : Int32, encoding : Enc, expected_error : String?) forall T
  desc = registry_desc(Parquet::Encodings.physical_type(T), type_length)
  if expected_error
    ex = expect_raises(Parquet::ParquetError) { Parquet::Encodings.get_encoder(T, encoding, desc) }
    ex.to_s.should eq(expected_error)
  else
    Parquet::Encodings.get_encoder(T, encoding, desc).encoding.should eq(encoding)
  end
end

private def check_decoder(type : T.class, type_length : Int32, encoding : Enc, expected_error : String?) forall T
  desc = registry_desc(Parquet::Encodings.physical_type(T), type_length)
  if expected_error
    ex = expect_raises(Parquet::ParquetError) { Parquet::Encodings.get_decoder(T, desc, encoding) }
    ex.to_s.should eq(expected_error)
  else
    Parquet::Encodings.get_decoder(T, desc, encoding).encoding.should eq(encoding)
  end
end

describe Parquet::Encodings do
  # Upstream `test_get_encoders`.
  it "selects encoders by encoding" do
    check_encoder(Int32, 0, Enc::PLAIN, nil)
    check_encoder(Int32, 0, Enc::DELTA_BINARY_PACKED, nil)
    check_encoder(Int32, 0, Enc::DELTA_LENGTH_BYTE_ARRAY, nil)
    check_encoder(Int32, 0, Enc::DELTA_BYTE_ARRAY, nil)
    check_encoder(Bool, 0, Enc::RLE, nil)

    cannot_initialize = general_err!("Cannot initialize this encoding through this function").to_s
    check_encoder(Int32, 0, Enc::RLE_DICTIONARY, cannot_initialize)
    check_encoder(Int32, 0, Enc::PLAIN_DICTIONARY, cannot_initialize)
    check_encoder(
      Int32,
      0,
      Enc::ALP,
      general_err!("Encoding ALP only supports FLOAT and DOUBLE, got INT32").to_s
    )
    check_encoder(
      Int32,
      0,
      Enc::BIT_PACKED,
      nyi_err!("Encoding BIT_PACKED is not supported").to_s
    )
  end

  # Upstream `test_get_decoders`.
  it "selects decoders by encoding" do
    check_decoder(Int32, 0, Enc::PLAIN, nil)
    check_decoder(Int32, 0, Enc::DELTA_BINARY_PACKED, nil)
    check_decoder(Parquet::ByteArray, 0, Enc::DELTA_LENGTH_BYTE_ARRAY, nil)
    check_decoder(Parquet::ByteArray, 0, Enc::DELTA_BYTE_ARRAY, nil)
    check_decoder(Bool, 0, Enc::RLE, nil)
    check_decoder(Float32, 0, Enc::ALP, nil)
    check_decoder(Float64, 0, Enc::ALP, nil)

    cannot_initialize = general_err!("Cannot initialize this encoding through this function").to_s
    check_decoder(Int32, 0, Enc::RLE_DICTIONARY, cannot_initialize)
    check_decoder(Int32, 0, Enc::PLAIN_DICTIONARY, cannot_initialize)
    check_decoder(
      Int32,
      0,
      Enc::DELTA_LENGTH_BYTE_ARRAY,
      general_err!("Encoding DELTA_LENGTH_BYTE_ARRAY is not supported for type").to_s
    )
    check_decoder(
      Int32,
      0,
      Enc::DELTA_BYTE_ARRAY,
      general_err!("Encoding DELTA_BYTE_ARRAY is not supported for type").to_s
    )
    check_decoder(
      Int32,
      0,
      Enc::BIT_PACKED,
      nyi_err!("Encoding BIT_PACKED is not supported").to_s
    )
  end
end
