require "../../spec_helper"

# Ported from `parquet/src/column/page_encryption.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no unit tests for `PageEncryptor`; these characterize the
# page-data / page-header encryption and the writer wiring.

private alias PE = Parquet::Column::PageEncryptor
private alias CP = Parquet::Column::CompressedPage
private alias DPage = Parquet::Column::Page::DataPage
private alias PHeader = Parquet::Format::PageHeader
private alias E = Parquet::Encryption
private alias MD = Parquet::File::Metadata
private alias FWriter = Parquet::File::SerializedFileWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias BytesCR = Parquet::File::BytesChunkReader

private FOOTER_KEY = Bytes.new(16) { |i| i.to_u8 }
private COLUMN_KEY = Bytes.new(16) { 9_u8 }

private def data_page(data : Bytes) : CP
  page = DPage.new(
    data, 3_u32, Parquet::Encoding::PLAIN,
    Parquet::Encoding::RLE, Parquet::Encoding::RLE, nil
  )
  CP.new(page, data.size.to_i64)
end

private def one_column_schema : ST
  a = ST.primitive_type_builder("a", PType::INT32).with_repetition(PRep::REQUIRED).build.as(ST)
  ST.group_type_builder("schema").with_fields([a]).build
end

private def write_int32_file(encryption : E::FileEncryptionProperties?) : Bytes
  io = IO::Memory.new
  props = WP.builder.set_file_encryption_properties(encryption).build
  writer = FWriter.new(io, one_column_schema, props)
  row_group = writer.next_row_group
  column_writer = row_group.next_column.not_nil!
  column_writer.typed(Int32).write_batch([1, 2, 3], nil, nil)
  column_writer.close
  row_group.close
  writer.close
  io.to_slice
end

describe Parquet::Column::PageEncryptor do
  it "encrypts page data and page headers" do
    props = E::FileEncryptionProperties.builder(FOOTER_KEY).with_column_key("a", COLUMN_KEY).build
    file_encryptor = E::FileEncryptor.new(props)
    encryptor = PE.create_if_column_encrypted(file_encryptor, 0, 0, "a").not_nil!

    plaintext = "page-data".to_slice
    page = data_page(plaintext)
    encrypted = encryptor.encrypt_compressed_page(page)
    aad_data = E.create_module_aad(file_encryptor.file_aad, E::ModuleType::DataPage, 0, 0, 0)
    E::GcmBlockDecryptor.new(COLUMN_KEY).decrypt(encrypted.data, aad_data).should eq(plaintext)

    header = page.to_thrift_header
    io = IO::Memory.new
    encryptor.encrypt_page_header(header, io)
    aad_header = E.create_module_aad(file_encryptor.file_aad, E::ModuleType::DataPageHeader, 0, 0, 0)
    plaintext_header = E.read_and_decrypt(
      E::GcmBlockDecryptor.new(COLUMN_KEY), IO::Memory.new(io.to_slice), aad_header
    )
    ThriftRoundTrip.read(plaintext_header, PHeader).should eq(header)
  end

  it "advances the page index between data pages" do
    props = E::FileEncryptionProperties.builder(FOOTER_KEY).with_column_key("a", COLUMN_KEY).build
    file_encryptor = E::FileEncryptor.new(props)
    encryptor = PE.create_if_column_encrypted(file_encryptor, 1, 2, "a").not_nil!
    plaintext = "page".to_slice
    page = data_page(plaintext)

    first = encryptor.encrypt_compressed_page(page)
    encryptor.increment_page
    second = encryptor.encrypt_compressed_page(page)

    aad0 = E.create_module_aad(file_encryptor.file_aad, E::ModuleType::DataPage, 1, 2, 0)
    aad1 = E.create_module_aad(file_encryptor.file_aad, E::ModuleType::DataPage, 1, 2, 1)
    decryptor = E::GcmBlockDecryptor.new(COLUMN_KEY)
    decryptor.decrypt(first.data, aad0).should eq(plaintext)
    decryptor.decrypt(second.data, aad1).should eq(plaintext)
  end

  it "only creates an encryptor for encrypted columns" do
    props = E::FileEncryptionProperties.builder(FOOTER_KEY).with_column_key("a", COLUMN_KEY).build
    file_encryptor = E::FileEncryptor.new(props)

    PE.create_if_column_encrypted(file_encryptor, 0, 0, "b").should be_nil
    PE.create_if_column_encrypted(nil, 0, 0, "a").should be_nil
  end

  it "encrypts data pages written through the public writer" do
    plain_bytes = write_int32_file(nil)
    plain_metadata = MD::ParquetMetaDataReader.new.parse_and_finish(BytesCR.new(plain_bytes))

    encrypted_bytes = write_int32_file(E::FileEncryptionProperties.builder(FOOTER_KEY).build)
    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY).build
    encrypted_metadata = MD::ParquetMetaDataReader.new
      .with_file_decryption_properties(decrypt)
      .parse_and_finish(BytesCR.new(encrypted_bytes))

    encrypted_metadata.row_group(0).columns[0].total_compressed_size.should be >
                                                                            plain_metadata.row_group(0).columns[0].total_compressed_size
  end
end
