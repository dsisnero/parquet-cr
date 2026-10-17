require "../../spec_helper"

# Characterization specs for the encrypted-metadata reader integration
# (`ParquetMetaDataReader`/`SerializedFileReader` decryption via
# `parquet_metadata_with_encryption`), matching
# `parquet/src/file/metadata/reader.rs` /
# `parquet/src/file/metadata/push_decoder.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias MD = Parquet::File::Metadata
private alias FWriter = Parquet::File::SerializedFileWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias E = Parquet::Encryption
private alias CCM = Parquet::File::ColumnCryptoMetaData
private alias BytesCR = Parquet::File::BytesChunkReader
private alias ReadOptionsBuilder = Parquet::File::ReadOptionsBuilder
private alias SerializedFileReader = Parquet::File::SerializedFileReader

private FOOTER_KEY   = Bytes.new(16) { |i| i.to_u8 }
private COLUMN_KEY_A = Bytes.new(16) { 1_u8 }
private COLUMN_KEY_B = Bytes.new(16) { 2_u8 }

private def two_column_schema : ST
  a = ST.primitive_type_builder("a", PType::INT32).with_repetition(PRep::REQUIRED).build.as(ST)
  b = ST.primitive_type_builder("b", PType::INT32).with_repetition(PRep::REQUIRED).build.as(ST)
  ST.group_type_builder("schema").with_fields([a, b]).build
end

private def write_encrypted_file(encryption : E::FileEncryptionProperties) : Bytes
  io = IO::Memory.new
  props = WP.builder.set_file_encryption_properties(encryption).build
  writer = FWriter.new(io, two_column_schema, props)
  row_group = writer.next_row_group

  column_writer = row_group.next_column.not_nil!
  column_writer.typed(Int32).write_batch([1, 2, 3], nil, nil)
  column_writer.close

  column_writer = row_group.next_column.not_nil!
  column_writer.typed(Int32).write_batch([4, 5, 6], nil, nil)
  column_writer.close

  row_group.close
  writer.close
  io.to_slice
end

describe "Parquet::File reader encryption" do
  it "decrypts an encrypted-footer metadata through ParquetMetaDataReader" do
    bytes = write_encrypted_file(E::FileEncryptionProperties.builder(FOOTER_KEY).build)
    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY).build

    metadata = MD::ParquetMetaDataReader.new
      .with_file_decryption_properties(decrypt)
      .parse_and_finish(BytesCR.new(bytes))

    metadata.num_row_groups.should eq(1)
    metadata.row_group(0).columns[0].num_values.should eq(3)
    metadata.row_group(0).columns[0].crypto_metadata.should eq(CCM::ENCRYPTION_WITH_FOOTER_KEY)
  end

  it "decrypts column metadata encrypted with column keys" do
    encryption = E::FileEncryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build
    bytes = write_encrypted_file(encryption)
    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build

    metadata = MD::ParquetMetaDataReader.new
      .with_file_decryption_properties(decrypt)
      .parse_and_finish(BytesCR.new(bytes))

    column_a = metadata.row_group(0).columns[0]
    column_a.num_values.should eq(3)
    column_a.encrypted_column_metadata.should be_nil
    column_a.crypto_metadata.not_nil!
      .encryption_with_column_key!.path_in_schema.should eq(["a"])
  end

  it "reads encrypted metadata through SerializedFileReader options" do
    bytes = write_encrypted_file(E::FileEncryptionProperties.builder(FOOTER_KEY).build)
    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY).build

    options = ReadOptionsBuilder.new.with_file_decryption_properties(decrypt).build
    reader = SerializedFileReader.new_with_options(BytesCR.new(bytes), options)

    reader.num_row_groups.should eq(1)
    reader.get_row_group(0).metadata.columns[0].num_values.should eq(3)
  end

  it "fails to parse an encrypted footer without decryption properties" do
    bytes = write_encrypted_file(E::FileEncryptionProperties.builder(FOOTER_KEY).build)
    expect_raises(Parquet::ParquetError) do
      MD::ParquetMetaDataReader.new.parse_and_finish(BytesCR.new(bytes))
    end
  end
end
