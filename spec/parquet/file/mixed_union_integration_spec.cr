require "../../spec_helper"

# Freeze existing production wire/API contracts before using the generator.
# Source: column_crypto_metadata.rs, metadata/thrift/encryption.rs and encrypted
# file paths at arrow-rs 60.0.0 ef1fa157977633f0ba21aa921f9ef3d5c669235b.
private alias MixedM = Parquet::File::Metadata
private alias MixedC = Parquet::File::ColumnCryptoMetaData
private alias MixedE = Parquet::Encryption

private def mixed_protocols(bytes : Bytes, &)
  yield Parquet::Thrift::ThriftSliceInputProtocol.new(bytes)
  yield Parquet::Thrift::ThriftReadInputProtocol.new(IO::Memory.new(bytes))
end

describe "Generated production union integration" do
  it "preserves both column-crypto variants' exact bytes, constructors and payload accessor" do
    footer = MixedC::ENCRYPTION_WITH_FOOTER_KEY
    ThriftRoundTrip.encode(footer).should eq Bytes[0x1c, 0, 0]
    footer.kind.value.should eq 0
    expect_raises(Parquet::GeneralError, "ColumnCryptoMetaData is not ENCRYPTION_WITH_COLUMN_KEY") { footer.encryption_with_column_key! }
    value = MixedC.encryption_with_column_key(Parquet::File::EncryptionWithColumnKey.new(["a", "b"], Bytes[0, 255]))
    expected = Bytes[0x2c, 0x19, 0x28, 1, 97, 1, 98, 0x18, 2, 0, 255, 0, 0]
    ThriftRoundTrip.encode(value).should eq expected
    value.kind.value.should eq 1
    mixed_protocols(expected) do |prot|
      decoded = MixedC.read_thrift(prot)
      decoded.should eq value
      decoded.encryption_with_column_key!.path_in_schema.should eq ["a", "b"]
    end
  end

  it "preserves both AES algorithm variants' exact bytes and accessors" do
    gcm = MixedM::EncryptionAlgorithm.aes_gcm_v1(MixedM::AesGcmV1.new(Bytes[1, 2], Bytes[3], false))
    ctr = MixedM::EncryptionAlgorithm.aes_gcm_ctr_v1(MixedM::AesGcmCtrV1.new(Bytes[1, 2], Bytes[3], true))
    gcm.kind.value.should eq 0
    ctr.kind.value.should eq 1
    [{gcm, Bytes[0x1c, 0x18, 2, 1, 2, 0x18, 1, 3, 0x12, 0, 0]},
     {ctr, Bytes[0x2c, 0x18, 2, 1, 2, 0x18, 1, 3, 0x11, 0, 0]}].each do |value, expected|
      ThriftRoundTrip.encode(value).should eq expected
      mixed_protocols(expected) { |prot| MixedM::EncryptionAlgorithm.read_thrift(prot).should eq value }
    end
    gcm.aes_gcm_ctr_v1.should be_nil
    ctr.aes_gcm_v1.should be_nil
  end

  it "round trips nested FileCryptoMetaData through both protocol implementations" do
    value = MixedM::FileCryptoMetaData.new(MixedM::EncryptionAlgorithm.aes_gcm_v1(MixedM::AesGcmV1.new(Bytes[1], Bytes[2], true)), Bytes[9])
    mixed_protocols(ThriftRoundTrip.encode(value)) { |prot| MixedM::FileCryptoMetaData.read_thrift(prot).should eq value }
  end

  it "retains strict production union diagnostics" do
    mixed_protocols(Bytes[0]) do |prot|
      expect_raises(Parquet::GeneralError, "Received empty union from remote ColumnCryptoMetaData") { MixedC.read_thrift(prot) }
    end
    mixed_protocols(Bytes[0x3c, 0, 0]) do |prot|
      expect_raises(Parquet::GeneralError, "Unexpected EncryptionAlgorithm 3") { MixedM::EncryptionAlgorithm.read_thrift(prot) }
    end
    mixed_protocols(Bytes[0x1c, 0, 0x1c, 0]) do |prot|
      expect_raises(Parquet::GeneralError, "Received multiple fields for union from remote {}ColumnCryptoMetaData") { MixedC.read_thrift(prot) }
    end
  end

  it "writes and reads complete encrypted files with footer-key and column-key union metadata" do
    [false, true].each do |plaintext|
      [false, true].each do |column_key|
        footer_key = Bytes.new(16, 1_u8)
        key = Bytes.new(16, 2_u8)
        encryption = MixedE::FileEncryptionProperties.builder(footer_key).with_plaintext_footer(plaintext)
        encryption.with_column_key_and_metadata("a", key, Bytes[9]) if column_key
        props = Parquet::File::Properties::WriterProperties.builder.set_file_encryption_properties(encryption.build).build
        schema = Parquet::Schema.parse_message_type("message schema { required INT32 a; }")
        output = IO::Memory.new
        file = Parquet::File::SerializedFileWriter.new(output, schema, props)
        2.times do |i|
          group = file.next_row_group
          column = group.next_column || raise "column missing"
          column.typed(Parquet::Int32Type).write_batch([i * 3, i * 3 + 1, i * 3 + 2], nil, nil)
          column.close
          group.close
        end
        file.close
        decryption = MixedE::FileDecryptionProperties.builder(footer_key)
        decryption.with_column_key("a", key) if column_key
        options = Parquet::File::ReadOptionsBuilder.new.with_file_decryption_properties(decryption.build).build
        reader = Parquet::File::SerializedFileReader.new_with_options(Parquet::File::BytesChunkReader.new(output.to_slice), options)
        reader.metadata.file_metadata.num_rows.should eq 6
        values = [] of Int32
        2.times do |i|
          group = reader.get_row_group(i)
          crypto = group.metadata.column(0).crypto_metadata || raise "crypto metadata missing"
          if column_key
            crypto.encryption_with_column_key!.path_in_schema.should eq ["a"]
            crypto.encryption_with_column_key!.key_metadata.should eq Bytes[9]
          else
            crypto.should eq MixedC::ENCRYPTION_WITH_FOOTER_KEY
          end
          column = Parquet::Column.get_typed_column_reader(Parquet::Int32Type, group.get_column_reader(0))
          column.read_records(3, nil, nil, values).should eq({3, 3, 3})
        end
        values.should eq [0, 1, 2, 3, 4, 5]
      end
    end
  end
end
