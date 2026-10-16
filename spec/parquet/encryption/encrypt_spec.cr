require "../../spec_helper"

# Characterization specs for `parquet/src/encryption/encrypt.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for the encryption properties (they are exercised
# through the encrypted file writer/reader); these characterize the builder,
# `FileEncryptor`, and the thrift-encryption helpers.

private alias E = Parquet::Encryption
private alias FEP = Parquet::Encryption::FileEncryptionProperties
private alias FileEncryptor = Parquet::Encryption::FileEncryptor
private alias GcmBlockDecryptor = Parquet::Encryption::GcmBlockDecryptor
private alias SchemaDescriptor = Parquet::Schema::SchemaDescriptor
private alias SchemaType = Parquet::Schema::Type
private alias FileCryptoMetaData = Parquet::File::Metadata::FileCryptoMetaData

private FOOTER_KEY = Bytes.new(16) { |i| i.to_u8 }

private def sample_schema : SchemaDescriptor
  SchemaDescriptor.new(
    SchemaType.group_type_builder("schema")
      .with_fields([
        SchemaType.primitive_type_builder("x", Parquet::Type::INT32)
          .with_repetition(Parquet::Repetition::REQUIRED).build,
        SchemaType.primitive_type_builder("y", Parquet::Type::INT32)
          .with_repetition(Parquet::Repetition::REQUIRED).build,
      ] of SchemaType)
      .build
  )
end

private def crypto_metadata : FileCryptoMetaData
  gcm = Parquet::File::Metadata::AesGcmV1.new("prefix".to_slice, nil, true)
  FileCryptoMetaData.new(Parquet::File::Metadata::EncryptionAlgorithm.aes_gcm_v1(gcm), "key".to_slice)
end

describe Parquet::Encryption::FileEncryptionProperties do
  it "defaults to an encrypted footer with no column keys or AAD" do
    props = FEP.builder(FOOTER_KEY).build
    props.encrypt_footer.should be_true
    props.footer_key.should eq(FOOTER_KEY)
    props.footer_key_metadata.should be_nil
    props.aad_prefix.should be_nil
    props.store_aad_prefix.should be_false
    props.uniform_encryption?.should be_true
    props.column_keys.should eq({[] of String, [] of Bytes, [] of Bytes})
  end

  it "supports a plaintext footer and footer key metadata" do
    props = FEP.builder(FOOTER_KEY)
      .with_plaintext_footer(true)
      .with_footer_key_metadata("meta".to_slice)
      .build
    props.encrypt_footer.should be_false
    props.footer_key_metadata.should eq("meta".to_slice)
  end

  it "collects column keys and metadata" do
    key_x = Bytes.new(16) { 1_u8 }
    key_y = Bytes.new(16) { 2_u8 }
    props = FEP.builder(FOOTER_KEY)
      .with_column_key("x", key_x)
      .with_column_key_and_metadata("y", key_y, "ymeta".to_slice)
      .build

    props.uniform_encryption?.should be_false
    names, keys, metadata = props.column_keys
    names.should eq(["x", "y"])
    keys.should eq([key_x, key_y])
    metadata.should eq(["ymeta".to_slice])
    props.column_key_for("x").not_nil!.key.should eq(key_x)
  end

  it "sets multiple column keys and rejects count mismatches" do
    key_x = Bytes.new(16) { 1_u8 }
    key_y = Bytes.new(16) { 2_u8 }
    props = FEP.builder(FOOTER_KEY).with_column_keys(["x", "y"], [key_x, key_y]).build
    props.column_keys[0].should eq(["x", "y"])

    expect_raises(Parquet::ParquetError, /does not match the number of keys/) do
      FEP.builder(FOOTER_KEY).with_column_keys(["x"], [key_x, key_y])
    end
  end

  it "only stores the AAD prefix when one is set" do
    FEP.builder(FOOTER_KEY).with_aad_prefix_storage(true).build.store_aad_prefix.should be_false

    props = FEP.builder(FOOTER_KEY)
      .with_aad_prefix("file".to_slice)
      .with_aad_prefix_storage(true)
      .build
    props.aad_prefix.should eq("file".to_slice)
    props.store_aad_prefix.should be_true
  end

  it "validates encrypted column names against the schema" do
    schema = sample_schema
    FEP.builder(FOOTER_KEY).with_column_key("x", Bytes.new(16)).build
      .validate_encrypted_column_names(schema)

    props = FEP.builder(FOOTER_KEY).with_column_key("z", Bytes.new(16)).build
    expect_raises(Parquet::ParquetError, /were not found in the schema: z/) do
      props.validate_encrypted_column_names(schema)
    end
  end
end

describe Parquet::Encryption::FileEncryptor do
  it "encrypts every column with the footer key under uniform encryption" do
    encryptor = FileEncryptor.new(FEP.builder(FOOTER_KEY).build)
    encryptor.column_encrypted?("any").should be_true
    encryptor.column_encryptor("any").should be_a(E::BlockEncryptor)
    encryptor.aad_file_unique.size.should eq(8)
    encryptor.file_aad.should eq(encryptor.aad_file_unique)
  end

  it "prepends the AAD prefix to the file AAD" do
    props = FEP.builder(FOOTER_KEY).with_aad_prefix("file".to_slice).build
    encryptor = FileEncryptor.new(props)
    encryptor.file_aad.size.should eq(4 + 8)
    encryptor.file_aad[0, 4].should eq("file".to_slice)
    encryptor.file_aad[4, 8].should eq(encryptor.aad_file_unique)
  end

  it "only encrypts configured columns when column keys are set" do
    props = FEP.builder(FOOTER_KEY)
      .with_column_key("x", Bytes.new(16) { 1_u8 })
      .build
    encryptor = FileEncryptor.new(props)
    encryptor.column_encrypted?("x").should be_true
    encryptor.column_encrypted?("y").should be_false
    expect_raises(Parquet::ParquetError, /Column 'y' is not encrypted/) do
      encryptor.column_encryptor("y")
    end
  end

  it "produces working footer/column encryptors" do
    encryptor = FileEncryptor.new(FEP.builder(FOOTER_KEY).build)
    aad = E.create_footer_aad(encryptor.file_aad)
    block = encryptor.footer_encryptor.encrypt("payload".to_slice, aad)
    GcmBlockDecryptor.new(FOOTER_KEY).decrypt(block, aad).should eq("payload".to_slice)
  end
end

describe Parquet::Encryption do
  it "round-trips an encrypted thrift object" do
    encryptor = FileEncryptor.new(FEP.builder(FOOTER_KEY).build)
    aad = E.create_footer_aad(encryptor.file_aad)
    original = crypto_metadata

    encrypted = E.encrypt_thrift_object_to_vec(original, encryptor.footer_encryptor, aad)
    plaintext = GcmBlockDecryptor.new(FOOTER_KEY).decrypt(encrypted, aad)

    ThriftRoundTrip.read(plaintext, FileCryptoMetaData).should eq(original)
  end

  it "writes a signed plaintext thrift object" do
    encryptor = FileEncryptor.new(FEP.builder(FOOTER_KEY).build)
    aad = E.create_footer_aad(encryptor.file_aad)
    original = crypto_metadata
    plaintext = ThriftRoundTrip.encode(original)

    io = IO::Memory.new
    E.write_signed_plaintext_thrift_object(original, encryptor.footer_encryptor, io, aad)
    bytes = io.to_slice

    bytes[0, plaintext.size].should eq(plaintext)
    nonce = bytes[plaintext.size, E::NONCE_LEN]
    tag = bytes[plaintext.size + E::NONCE_LEN, E::TAG_LEN]
    frame = plaintext + nonce + Bytes.new(E::TAG_LEN)
    GcmBlockDecryptor.new(FOOTER_KEY).compute_plaintext_tag(aad, frame).should eq(tag)
  end

  it "builds column crypto metadata from the properties" do
    schema = sample_schema
    uniform = FEP.builder(FOOTER_KEY).build
    Parquet::Encryption.get_column_crypto_metadata(uniform, schema.columns[0])
      .should eq(Parquet::File::ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY)

    props = FEP.builder(FOOTER_KEY)
      .with_column_key_and_metadata("x", Bytes.new(16) { 1_u8 }, "xmeta".to_slice)
      .build
    metadata = Parquet::Encryption.get_column_crypto_metadata(props, schema.columns[0]).not_nil!
    metadata.kind.should eq(Parquet::File::ColumnCryptoMetaData::Kind::EncryptionWithColumnKey)
    metadata.encryption_with_column_key!.path_in_schema.should eq(["x"])
    metadata.encryption_with_column_key!.key_metadata.should eq("xmeta".to_slice)

    Parquet::Encryption.get_column_crypto_metadata(props, schema.columns[1]).should be_nil
  end
end
