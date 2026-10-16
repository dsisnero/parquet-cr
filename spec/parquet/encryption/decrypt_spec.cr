require "../../spec_helper"

# Characterization specs for `parquet/src/encryption/decrypt.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for the decryption properties (they are exercised
# through the encrypted file reader); these characterize the builder,
# `FileDecryptor`, `CryptoContext`, and `read_and_decrypt`.

private alias E = Parquet::Encryption
private alias FDP = Parquet::Encryption::FileDecryptionProperties
private alias FileDecryptor = Parquet::Encryption::FileDecryptor
private alias CryptoContext = Parquet::Encryption::CryptoContext
private alias ModuleType = Parquet::Encryption::ModuleType
private alias KeyRetriever = Parquet::Encryption::KeyRetriever
private alias FileEncryptionProperties = Parquet::Encryption::FileEncryptionProperties
private alias FileEncryptor = Parquet::Encryption::FileEncryptor
private alias CCM = Parquet::File::ColumnCryptoMetaData
private alias EWCK = Parquet::File::EncryptionWithColumnKey

private FOOTER_KEY = Bytes.new(16) { |i| i.to_u8 }
private COLUMN_KEY = Bytes.new(16) { 9_u8 }

private class MapKeyRetriever < Parquet::Encryption::KeyRetriever
  def initialize(@keys : Hash(String, Bytes))
  end

  def retrieve_key(key_metadata : Bytes) : Bytes
    key = @keys[String.new(key_metadata)]?
    raise Parquet::GeneralError.new("unknown key") if key.nil?
    key
  end
end

describe Parquet::Encryption::FileDecryptionProperties do
  it "builds with explicit keys" do
    props = FDP.builder(FOOTER_KEY).with_column_key("x", COLUMN_KEY).build
    props.footer_key(nil).should eq(FOOTER_KEY)
    props.column_key("x", nil).should eq(COLUMN_KEY)
    props.aad_prefix.should be_nil
    props.check_plaintext_footer_integrity.should be_true
    props.uses_key_retriever?.should be_false
    props.column_keys.should eq({["x"], [COLUMN_KEY]})
  end

  it "supports an AAD prefix and disabling signature verification" do
    props = FDP.builder(FOOTER_KEY)
      .with_aad_prefix("file".to_slice)
      .disable_footer_signature_verification
      .build
    props.aad_prefix.should eq("file".to_slice)
    props.check_plaintext_footer_integrity.should be_false
  end

  it "rejects column-key count mismatches and missing keys" do
    expect_raises(Parquet::ParquetError, /does not match the number of keys/) do
      FDP.builder(FOOTER_KEY).with_column_keys(["x"], [COLUMN_KEY, COLUMN_KEY])
    end

    expect_raises(Parquet::ParquetError, /No column decryption key set/) do
      FDP.builder(FOOTER_KEY).build.column_key("x", nil)
    end
  end

  it "retrieves keys through a key retriever" do
    retriever = MapKeyRetriever.new({"footer" => FOOTER_KEY, "col" => COLUMN_KEY})
    props = FDP.with_key_retriever(retriever).build
    props.uses_key_retriever?.should be_true
    props.footer_key("footer".to_slice).should eq(FOOTER_KEY)
    props.column_key("x", "col".to_slice).should eq(COLUMN_KEY)
    props.column_keys.should eq({[] of String, [] of Bytes})

    expect_raises(Parquet::GeneralError, /unknown key/) { props.footer_key("nope".to_slice) }
  end
end

describe Parquet::Encryption::FileDecryptor do
  it "derives the file AAD and a working footer decryptor" do
    aad_file_unique = Bytes.new(8) { 7_u8 }
    decryptor = FileDecryptor.new(FDP.builder(FOOTER_KEY).build, nil, aad_file_unique, "file".to_slice)
    decryptor.file_aad.should eq("file".to_slice + aad_file_unique)

    aad = E.create_footer_aad(decryptor.file_aad)
    block = E::GcmBlockEncryptor.new(FOOTER_KEY).encrypt("payload".to_slice, aad)
    decryptor.footer_decryptor.decrypt(block, aad).should eq("payload".to_slice)
  end

  it "reports an invalid footer key" do
    expect_raises(Parquet::GeneralError, /Invalid footer key/) do
      FileDecryptor.new(FDP.builder(Bytes.new(8)).build, nil, Bytes.new(8), Bytes.empty)
    end
  end

  it "verifies and rejects plaintext footer signatures" do
    encryptor = FileEncryptor.new(FileEncryptionProperties.builder(FOOTER_KEY).build)
    decryptor = FileDecryptor.new(
      FDP.builder(FOOTER_KEY).build, nil, encryptor.aad_file_unique, Bytes.empty
    )

    io = IO::Memory.new
    object = Parquet::File::Metadata::FileCryptoMetaData.new(
      Parquet::File::Metadata::EncryptionAlgorithm.aes_gcm_v1(
        Parquet::File::Metadata::AesGcmV1.new(nil, nil, nil)
      ), nil
    )
    E.write_signed_plaintext_thrift_object(
      object, encryptor.footer_encryptor, io, E.create_footer_aad(encryptor.file_aad)
    )
    footer = io.to_slice
    decryptor.verify_plaintext_footer_signature(footer)

    tampered = footer.dup
    tampered[0] ^= 0xFF_u8
    expect_raises(Parquet::GeneralError, /Footer signature verification failed/) do
      decryptor.verify_plaintext_footer_signature(tampered)
    end
  end
end

describe Parquet::Encryption::CryptoContext do
  it "uses the footer decryptor for footer-key columns" do
    decryptor = FileDecryptor.new(
      FDP.builder(FOOTER_KEY).build, nil, Bytes.new(8), Bytes.empty
    )
    context = CryptoContext.for_column(decryptor, CCM::ENCRYPTION_WITH_FOOTER_KEY, 0, 0)

    context.data_decryptor.should be(decryptor.footer_decryptor)
    context.create_column_index_aad.should eq(
      E.create_module_aad(decryptor.file_aad, ModuleType::ColumnIndex, 0, 0, nil)
    )
    context.create_offset_index_aad.should eq(
      E.create_module_aad(decryptor.file_aad, ModuleType::OffsetIndex, 0, 0, nil)
    )
    context.with_page_ordinal(1).create_page_aad.should eq(
      E.create_module_aad(decryptor.file_aad, ModuleType::DataPage, 0, 0, 1)
    )
    context.for_dictionary_page.create_page_header_aad.should eq(
      E.create_module_aad(decryptor.file_aad, ModuleType::DictionaryPageHeader, 0, 0, nil)
    )
  end

  it "uses the column decryptor for column-key columns" do
    props = FDP.builder(FOOTER_KEY).with_column_key("x", COLUMN_KEY).build
    decryptor = FileDecryptor.new(props, nil, Bytes.new(8), Bytes.empty)
    metadata = CCM.encryption_with_column_key(EWCK.new(["x"], nil))
    context = CryptoContext.for_column(decryptor, metadata, 2, 1)

    aad = context.with_page_ordinal(0).create_page_aad
    block = E::GcmBlockEncryptor.new(COLUMN_KEY).encrypt("data".to_slice, aad)
    context.data_decryptor.decrypt(block, aad).should eq("data".to_slice)
  end
end

describe Parquet::Encryption do
  it "reads and decrypts a length-prefixed encrypted block" do
    aad = E.create_footer_aad(Bytes.new(8))
    decryptor = E::GcmBlockDecryptor.new(FOOTER_KEY)
    block = E::GcmBlockEncryptor.new(FOOTER_KEY).encrypt("secret".to_slice, aad)

    E.read_and_decrypt(decryptor, IO::Memory.new(block), aad).should eq("secret".to_slice)
  end
end
