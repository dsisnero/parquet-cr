require "../../../spec_helper"

# Characterization specs for the encrypted-footer reader helpers in
# `parquet/src/file/metadata/thrift/encryption.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for these helpers (they are exercised through the
# encrypted file writer/reader); these characterize `get_file_decryptor`,
# `row_group_from_encrypted_thrift`, and `parquet_metadata_with_encryption`.

private alias MD = Parquet::File::Metadata
private alias EA = MD::EncryptionAlgorithm
private alias GCM = MD::AesGcmV1
private alias GCMCTR = MD::AesGcmCtrV1
private alias FCM = MD::FileCryptoMetaData
private alias CCM = Parquet::File::ColumnCryptoMetaData
private alias EWCK = Parquet::File::EncryptionWithColumnKey
private alias E = Parquet::Encryption
private alias ThriftWriter = Parquet::Thrift::ThriftCompactOutputProtocol

private FOOTER_KEY = Bytes.new(16) { |i| i.to_u8 }
private COLUMN_KEY = Bytes.new(16) { 9_u8 }

private def serialize_column_metadata(column : MD::ColumnChunkMetaData) : Bytes
  io = IO::Memory.new
  MD.serialize_column_meta_data(column, ThriftWriter.new(io))
  io.to_slice.dup
end

private def minimal_column(descr, num_values : Int64 = 10_i64) : MD::ColumnChunkMetaData
  MD::ColumnChunkMetaData.builder(descr)
    .set_encodings_mask(ParquetTestHelpers.enc_mask(Parquet::Encoding::PLAIN))
    .set_num_values(num_values)
    .set_compression_codec(Parquet::CompressionCodec::UNCOMPRESSED)
    .set_total_compressed_size(20_i64)
    .set_total_uncompressed_size(30_i64)
    .set_data_page_offset(40_i64)
    .build
end

describe "Parquet::File::Metadata encrypted-footer reader" do
  it "builds a file decryptor from the algorithm" do
    props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    aad_unique = Bytes.new(8) { 7_u8 }
    algorithm = EA.aes_gcm_v1(GCM.new("pfx".to_slice, aad_unique, false))

    decryptor = MD.get_file_decryptor(algorithm, nil, props)
    decryptor.file_aad.should eq("pfx".to_slice + aad_unique)
  end

  it "requires the AAD unique file id" do
    props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    algorithm = EA.aes_gcm_v1(GCM.new(nil, nil, nil))
    expect_raises(Parquet::ParquetError, /AAD unique file identifier is not set/) do
      MD.get_file_decryptor(algorithm, nil, props)
    end
  end

  it "rejects AES_GCM_CTR_V1" do
    props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    algorithm = EA.aes_gcm_ctr_v1(GCMCTR.new(nil, Bytes.new(8), nil))
    expect_raises(Parquet::NYIError, /AES_GCM_CTR_V1/) do
      MD.get_file_decryptor(algorithm, nil, props)
    end
  end

  it "decrypts encrypted column metadata" do
    descr = ParquetTestHelpers.test_schema_descr
    aad_unique = Bytes.new(8) { 6_u8 }
    props = E::FileDecryptionProperties.builder(FOOTER_KEY).with_column_key("a", COLUMN_KEY).build
    decryptor = E::FileDecryptor.new(props, nil, aad_unique, Bytes.empty)

    column = minimal_column(descr.column(0))
    aad = E.create_module_aad(aad_unique, E::ModuleType::ColumnMetaData, 0, 0, nil)
    column.encrypted_column_metadata = E::GcmBlockEncryptor.new(COLUMN_KEY)
      .encrypt(serialize_column_metadata(column), aad)
    column.column_crypto_metadata = CCM.encryption_with_column_key(EWCK.new(["a"], nil))

    row_group = MD::RowGroupMetaData.new(descr)
    row_group.columns = [column, minimal_column(descr.column(1))]
    row_group.num_rows = 1_i64
    row_group.total_byte_size = 20_i64
    row_group.ordinal = 0

    result = MD.row_group_from_encrypted_thrift(row_group, decryptor, nil)
    result.columns[0].num_values.should eq(10)
    result.columns[0].data_page_offset.should eq(40)
    result.columns[0].encrypted_column_metadata.should be_nil
  end

  it "keeps columns whose key is missing" do
    descr = ParquetTestHelpers.test_schema_descr
    aad_unique = Bytes.new(8) { 6_u8 }
    props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    decryptor = E::FileDecryptor.new(props, nil, aad_unique, Bytes.empty)

    column = minimal_column(descr.column(0))
    aad = E.create_module_aad(aad_unique, E::ModuleType::ColumnMetaData, 0, 0, nil)
    column.encrypted_column_metadata = E::GcmBlockEncryptor.new(COLUMN_KEY)
      .encrypt(serialize_column_metadata(column), aad)
    column.column_crypto_metadata = CCM.encryption_with_column_key(EWCK.new(["a"], nil))

    row_group = MD::RowGroupMetaData.new(descr)
    row_group.columns = [column, minimal_column(descr.column(1))]
    row_group.num_rows = 1_i64
    row_group.total_byte_size = 20_i64
    row_group.ordinal = 0

    result = MD.row_group_from_encrypted_thrift(row_group, decryptor, nil)
    result.columns[0].encrypted_column_metadata.should_not be_nil
  end

  it "decrypts an encrypted footer" do
    descr = ParquetTestHelpers.test_schema_descr
    column = minimal_column(descr.column(0), num_values: 1_i64)
    row_group = MD::RowGroupMetaData.new(descr)
    row_group.columns = [column, minimal_column(descr.column(1))]
    row_group.num_rows = 1_i64
    row_group.total_byte_size = 20_i64
    row_group.ordinal = 0

    file_metadata = MD::FileMetaData.new(1, 1_i64, nil, nil, descr, nil)
    io = IO::Memory.new
    MD.write_file_metadata(file_metadata, [row_group], ThriftWriter.new(io), true)
    plaintext = io.to_slice.dup

    aad_unique = Bytes.new(8) { 7_u8 }
    props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    aad_footer = E.create_footer_aad(aad_unique)
    encrypted = E::GcmBlockEncryptor.new(FOOTER_KEY).encrypt(plaintext, aad_footer)

    crypto = FCM.new(EA.aes_gcm_v1(GCM.new(nil, aad_unique, false)), nil)
    header = IO::Memory.new
    crypto.write_thrift(ThriftWriter.new(header))
    buf = header.to_slice + encrypted

    result = MD.parquet_metadata_with_encryption(props, true, buf, nil)
    result.num_row_groups.should eq(1)
    result.row_group(0).columns[0].num_values.should eq(1)
  end

  it "requires decryption properties for encrypted footers" do
    expect_raises(Parquet::ParquetError, /decryption properties were not provided/) do
      MD.parquet_metadata_with_encryption(nil, true, Bytes[0x00], nil)
    end
  end

  it "requires a stored or provided AAD prefix" do
    aad_unique = Bytes.new(8) { 7_u8 }
    crypto = FCM.new(EA.aes_gcm_v1(GCM.new(nil, aad_unique, true)), nil)
    io = IO::Memory.new
    crypto.write_thrift(ThriftWriter.new(io))

    props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    expect_raises(Parquet::ParquetError, /AAD prefix/) do
      MD.parquet_metadata_with_encryption(props, true, io.to_slice, nil)
    end
  end
end
