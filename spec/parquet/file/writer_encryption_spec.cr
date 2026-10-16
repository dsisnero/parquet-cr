require "../../spec_helper"

# Characterization specs for exposing file encryption through the public writer
# API (`WriterProperties.file_encryption_properties` + `SerializedFileWriter`),
# matching `parquet/src/column/writer/mod.rs` /
# `parquet/src/file/writer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for this wiring (it is exercised through the
# encrypted round-trip integration tests); these write a file with the public
# writer and read its footer metadata back with `parquet_metadata_with_encryption`.

private alias MD = Parquet::File::Metadata
private alias FWriter = Parquet::File::SerializedFileWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias E = Parquet::Encryption
private alias CCM = Parquet::File::ColumnCryptoMetaData

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
  metadata = writer.close
  metadata.row_group(0).columns[0].crypto_metadata.should_not be_nil
  io.to_slice
end

private def footer_of(bytes : Bytes) : Bytes
  size = bytes.size
  metadata_len = IO::ByteFormat::LittleEndian.decode(UInt32, bytes[size - 8, 4]).to_i32
  bytes[size - 8 - metadata_len, metadata_len]
end

describe "Parquet::File writer encryption" do
  it "writes a uniformly encrypted file" do
    encryption = E::FileEncryptionProperties.builder(FOOTER_KEY).build
    bytes = write_encrypted_file(encryption)

    bytes[bytes.size - 4, 4].should eq(Parquet::File::PARQUET_MAGIC_ENCRYPTED_FOOTER)
    bytes[0, 4].should eq(Parquet::File::PARQUET_MAGIC)

    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    result = MD.parquet_metadata_with_encryption(decrypt, true, footer_of(bytes), nil)
    result.row_group(0).columns[0].num_values.should eq(3)
    result.row_group(0).columns[0].crypto_metadata.should eq(CCM::ENCRYPTION_WITH_FOOTER_KEY)
    result.row_group(0).columns[1].num_values.should eq(3)
  end

  it "writes a file with per-column keys" do
    encryption = E::FileEncryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build
    bytes = write_encrypted_file(encryption)

    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build
    result = MD.parquet_metadata_with_encryption(decrypt, true, footer_of(bytes), nil)

    column_a = result.row_group(0).columns[0]
    column_a.num_values.should eq(3)
    column_a.encrypted_column_metadata.should be_nil
    column_a.crypto_metadata.not_nil!
      .encryption_with_column_key!.path_in_schema.should eq(["a"])
  end

  it "writes a signed plaintext footer" do
    encryption = E::FileEncryptionProperties.builder(FOOTER_KEY).with_plaintext_footer(true).build
    bytes = write_encrypted_file(encryption)

    bytes[bytes.size - 4, 4].should eq(Parquet::File::PARQUET_MAGIC)

    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    result = MD.parquet_metadata_with_encryption(decrypt, false, footer_of(bytes), nil)
    result.num_row_groups.should eq(1)
    result.row_group(0).columns[0].num_values.should eq(3)
  end
end
