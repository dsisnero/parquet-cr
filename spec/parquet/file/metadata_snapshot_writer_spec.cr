require "../../spec_helper"
require "../../support/metadata_snapshot_helpers"

# Characterizes writer.rs's borrowed-input clone and unencrypted return paths;
# upstream has no writer.rs unit tests. Pin: arrow-rs 60.0.0 ef1fa157.
private alias SW = MetadataSnapshotSpecs

describe "Reusable metadata footer writes" do
  it "writes indexes at independent positions without changing the borrowed snapshot" do
    metadata = SW.metadata
    first = SW::MD::ParquetMetaDataReader.decode_metadata(SW.footer(SW.write(metadata)))
    second = SW::MD::ParquetMetaDataReader.decode_metadata(SW.footer(SW.write(metadata, 50)))
    a = first.row_group(0).column(0).offset_index_offset || raise "missing first index"
    b = second.row_group(0).column(0).offset_index_offset || raise "missing second index"
    (b - a).should eq 50
    metadata.row_group(0).column(0).offset_index_offset.should be_nil
    metadata.row_group(0).column(0).offset_index_length.should be_nil
  end

  it "does not poison a snapshot when writing an encrypted footer" do
    metadata = SW.metadata
    column = metadata.row_group(0).column(0)
    column.column_crypto_metadata = SW.crypto
    key = Bytes.new(16, 1_u8)
    encryption = SW::E::FileEncryptionProperties.builder(key).with_column_key("leaf", key).build
    encryptor = SW::E::FileEncryptor.new(encryption)
    encrypted = SW.write(metadata, encryptor: encryptor)
    decrypt = SW::E::FileDecryptionProperties.builder(key).with_column_key("leaf", key).build
    decoded = SW::MD.parquet_metadata_with_encryption(decrypt, true, SW.footer(encrypted), nil)
    decoded.row_group(0).column(0).num_values.should eq 2
    column.encrypted_column_metadata.should be_nil
    column.plaintext_footer_mode.should be_false
    column.offset_index_offset.should be_nil
    plain = SW::MD::ParquetMetaDataReader.decode_metadata(SW.footer(SW.write(metadata)))
    plain.row_group(0).column(0).statistics.should eq column.statistics
  end

  it "returns a separate unencrypted metadata graph when applying column encryption" do
    metadata = SW.metadata
    metadata.row_group(0).column(0).column_crypto_metadata = SW.crypto
    key = Bytes.new(16, 1_u8)
    encryptor = SW::E::FileEncryptor.new(SW::E::FileEncryptionProperties.builder(key).with_column_key("leaf", key).build)
    encrypted, clear = SW::MD::MetadataObjectWriter.new(encryptor).apply_row_group_encryption(metadata.row_groups)
    encrypted[0].column(0).encrypted_column_metadata.should_not be_nil
    original = clear || raise "missing unencrypted metadata"
    original[0].column(0).encrypted_column_metadata.should be_nil
    original[0].column(0).statistics.should eq encrypted[0].column(0).statistics
  end

  it "returns plaintext statistics from a real encrypted file for snapshot reuse" do
    [false, true].each do |plaintext_footer|
      key = Bytes.new(16, 1_u8)
      encryption = SW::E::FileEncryptionProperties.builder(key).with_column_key("leaf", key)
        .with_plaintext_footer(plaintext_footer).build
      properties = Parquet::File::Properties::WriterProperties.builder.set_file_encryption_properties(encryption).build
      schema = Parquet::Schema.parse_message_type("message root { REQUIRED INT32 leaf; }")
      sink = IO::Memory.new
      writer = Parquet::File::SerializedFileWriter.new(sink, schema, properties)
      group = writer.next_row_group
      column = group.next_column || raise "missing column writer"
      column.typed(Int32).write_batch([1, 2, 3], nil, nil)
      column.close
      group.close
      metadata = writer.close
      snapshot = metadata.clone
      clear_column = snapshot.row_group(0).column(0)
      clear_column.encrypted_column_metadata.should be_nil
      clear_column.plaintext_footer_mode.should be_false
      expected = Parquet::Statistics.new(Parquet::ValueStatistics(Int32).new(1, 3, nil, 0_u64, false).with_backwards_compatible_min_max(true))
      clear_column.statistics.should eq expected
      decrypt = SW::E::FileDecryptionProperties.builder(key).with_column_key("leaf", key).build
      decoded = SW::MD.parquet_metadata_with_encryption(decrypt, !plaintext_footer, SW.footer(sink.to_slice), nil)
      # from_thrift_page_stats reconstructs compatibility from old_format;
      # modern min_value/max_value select false even when legacy fields exist.
      wire_statistics = Parquet::Statistics.int32(1, 3, nil, 0_u64, false)
      decoded.row_group(0).column(0).statistics.should eq wire_statistics
      # A new footer can be written from the returned snapshot without keys.
      snapshot.file_metadata.encryption_algorithm = nil
      snapshot.file_metadata.footer_signing_key_metadata = nil
      plain = SW::MD::ParquetMetaDataReader.decode_metadata(SW.footer(SW.write(snapshot)))
      plain.row_group(0).column(0).statistics.should eq wire_statistics
    end
  end
end
