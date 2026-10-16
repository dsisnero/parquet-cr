require "../../../spec_helper"

# Characterization specs for the encryption Thrift IDL
# (`parquet/src/file/metadata/thrift/encryption.rs`, apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no dedicated tests for these structs (they are exercised through
# the encrypted-footer reader); these characterize compact-protocol round-trips.

private alias EA = Parquet::File::Metadata::EncryptionAlgorithm
private alias GCM = Parquet::File::Metadata::AesGcmV1
private alias GCMCTR = Parquet::File::Metadata::AesGcmCtrV1
private alias FCM = Parquet::File::Metadata::FileCryptoMetaData

describe Parquet::File::Metadata::EncryptionAlgorithm do
  it "round-trips AES_GCM_V1" do
    empty = GCM.new(nil, nil, nil)
    ThriftRoundTrip.roundtrip(EA.aes_gcm_v1(empty)).should eq(EA.aes_gcm_v1(empty))

    full = GCM.new("prefix".to_slice, Bytes.new(16) { 2_u8 }, true)
    ThriftRoundTrip.roundtrip(EA.aes_gcm_v1(full)).should eq(EA.aes_gcm_v1(full))
  end

  it "round-trips AES_GCM_CTR_V1" do
    empty = GCMCTR.new(nil, nil, nil)
    ThriftRoundTrip.roundtrip(EA.aes_gcm_ctr_v1(empty)).should eq(EA.aes_gcm_ctr_v1(empty))

    full = GCMCTR.new("prefix".to_slice, Bytes.new(8) { 3_u8 }, false)
    ThriftRoundTrip.roundtrip(EA.aes_gcm_ctr_v1(full)).should eq(EA.aes_gcm_ctr_v1(full))
  end

  it "rejects an empty union" do
    expect_raises(Parquet::ParquetError) do
      ThriftRoundTrip.read(Bytes[0x00], EA)
    end
  end
end

describe Parquet::File::Metadata::FileCryptoMetaData do
  it "round-trips encrypted-footer metadata" do
    gcm = GCM.new("prefix".to_slice, nil, true)
    metadata = FCM.new(EA.aes_gcm_v1(gcm), "key".to_slice)
    ThriftRoundTrip.roundtrip(metadata).should eq(metadata)

    gcm_ctr = GCMCTR.new(nil, Bytes.new(8) { 3_u8 }, nil)
    metadata = FCM.new(EA.aes_gcm_ctr_v1(gcm_ctr), nil)
    ThriftRoundTrip.roundtrip(metadata).should eq(metadata)
  end
end
