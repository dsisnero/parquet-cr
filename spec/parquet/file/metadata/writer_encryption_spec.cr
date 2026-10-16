require "../../../spec_helper"

# Characterization specs for the encryption-aware metadata writer
# (`parquet/src/file/metadata/writer.rs`, apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for the encrypted footer writer; these round-trip
# the written metadata back through the encrypted-footer reader.

private alias MD = Parquet::File::Metadata
private alias CCM = Parquet::File::ColumnCryptoMetaData
private alias EWCK = Parquet::File::EncryptionWithColumnKey
private alias E = Parquet::Encryption
private alias TrackedWrite = Parquet::File::TrackedWrite

private FOOTER_KEY   = Bytes.new(16) { |i| i.to_u8 }
private COLUMN_KEY_A = Bytes.new(16) { 1_u8 }
private COLUMN_KEY_B = Bytes.new(16) { 2_u8 }

private def minimal_column(descr) : MD::ColumnChunkMetaData
  MD::ColumnChunkMetaData.builder(descr)
    .set_encodings_mask(ParquetTestHelpers.enc_mask(Parquet::Encoding::PLAIN))
    .set_num_values(10_i64)
    .set_compression_codec(Parquet::CompressionCodec::UNCOMPRESSED)
    .set_total_compressed_size(20_i64)
    .set_total_uncompressed_size(30_i64)
    .set_data_page_offset(40_i64)
    .build
end

private def sample_metadata(descr, columns : Array(MD::ColumnChunkMetaData)) : MD::ParquetMetaData
  row_group = MD::RowGroupMetaData.new(descr)
  row_group.columns = columns
  row_group.num_rows = 1_i64
  row_group.total_byte_size = 40_i64
  row_group.ordinal = 0
  file_metadata = MD::FileMetaData.new(1, 1_i64, nil, nil, descr, nil)
  MD::ParquetMetaData.new(file_metadata, [row_group])
end

# Writes `metadata` with `encryptor` and returns `{raw_bytes, footer_bytes}`.
private def write_encrypted(metadata, encryptor) : Tuple(Bytes, Bytes)
  io = IO::Memory.new
  writer = MD::ParquetMetaDataWriter.new_with_tracked(TrackedWrite.new(io), metadata)
  writer.with_file_encryptor(encryptor)
  writer.finish
  bytes = io.to_slice.dup
  len = IO::ByteFormat::LittleEndian.decode(UInt32, bytes[bytes.size - 8, 4]).to_i32
  footer = bytes[bytes.size - 8 - len, len]
  {bytes, footer}
end

describe "Parquet::File::Metadata encrypted metadata writer" do
  it "writes an encrypted footer" do
    descr = ParquetTestHelpers.test_schema_descr
    metadata = sample_metadata(descr, [minimal_column(descr.column(0)), minimal_column(descr.column(1))])

    encrypt_props = E::FileEncryptionProperties.builder(FOOTER_KEY).build
    encryptor = E::FileEncryptor.new(encrypt_props)
    bytes, footer = write_encrypted(metadata, encryptor)

    bytes[bytes.size - 4, 4].should eq(Parquet::File::PARQUET_MAGIC_ENCRYPTED_FOOTER)

    decrypt_props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    result = MD.parquet_metadata_with_encryption(decrypt_props, true, footer, nil)
    result.num_row_groups.should eq(1)
    result.row_group(0).columns[0].num_values.should eq(10)
    result.row_group(0).columns[1].num_values.should eq(10)
  end

  it "encrypts column metadata with column keys" do
    descr = ParquetTestHelpers.test_schema_descr
    column_a = minimal_column(descr.column(0))
    column_a.column_crypto_metadata = CCM.encryption_with_column_key(EWCK.new(["a"], nil))
    column_b = minimal_column(descr.column(1))
    column_b.column_crypto_metadata = CCM.encryption_with_column_key(EWCK.new(["b"], nil))
    metadata = sample_metadata(descr, [column_a, column_b])

    encrypt_props = E::FileEncryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build
    bytes, footer = write_encrypted(metadata, E::FileEncryptor.new(encrypt_props))

    decrypt_props = E::FileDecryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build
    result = MD.parquet_metadata_with_encryption(decrypt_props, true, footer, nil)
    result.row_group(0).columns[0].num_values.should eq(10)
    result.row_group(0).columns[0].encrypted_column_metadata.should be_nil
    result.row_group(0).columns[1].num_values.should eq(10)
    bytes[bytes.size - 4, 4].should eq(Parquet::File::PARQUET_MAGIC_ENCRYPTED_FOOTER)
  end

  it "writes a signed plaintext footer" do
    descr = ParquetTestHelpers.test_schema_descr
    metadata = sample_metadata(descr, [minimal_column(descr.column(0)), minimal_column(descr.column(1))])

    encrypt_props = E::FileEncryptionProperties.builder(FOOTER_KEY)
      .with_plaintext_footer(true)
      .build
    encryptor = E::FileEncryptor.new(encrypt_props)
    bytes, footer = write_encrypted(metadata, encryptor)

    bytes[bytes.size - 4, 4].should eq(Parquet::File::PARQUET_MAGIC)

    decrypt_props = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    result = MD.parquet_metadata_with_encryption(decrypt_props, false, footer, nil)
    result.num_row_groups.should eq(1)
    result.row_group(0).columns[0].num_values.should eq(10)
  end
end
