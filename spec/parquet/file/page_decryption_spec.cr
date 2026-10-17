require "../../spec_helper"

# Characterization specs for data-page decryption on read, matching
# `parquet/src/file/serialized_reader.rs` (`SerializedPageReader
# ::add_crypto_context` / `SerializedPageReaderContext::read_page_header` /
# `decrypt_page_data`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream exercises page decryption through the arrow reader; the core
# `SerializedRowGroupReader` does not attach a crypto context. These specs wire
# it through the core reader (see the parity note) so encrypted pages are
# reachable without an arrow layer.

private alias FWriter = Parquet::File::SerializedFileWriter
private alias FReader = Parquet::File::SerializedFileReader
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias E = Parquet::Encryption
private alias BytesCR = Parquet::File::BytesChunkReader
private alias ReadOptionsBuilder = Parquet::File::ReadOptionsBuilder
private alias Page = Parquet::Column::Page

private FOOTER_KEY   = Bytes.new(16) { |i| i.to_u8 }
private COLUMN_KEY_A = Bytes.new(16) { 1_u8 }
private COLUMN_KEY_B = Bytes.new(16) { 2_u8 }

private def two_column_schema : ST
  a = ST.primitive_type_builder("a", PType::INT32).with_repetition(PRep::REQUIRED).build.as(ST)
  b = ST.primitive_type_builder("b", PType::INT32).with_repetition(PRep::REQUIRED).build.as(ST)
  ST.group_type_builder("schema").with_fields([a, b]).build
end

private def write_int32_file(
  encryption : E::FileEncryptionProperties,
  values_a : Array(Int32),
  values_b : Array(Int32),
  dictionary : Bool = false,
  page_row_limit : Int32? = nil,
) : Bytes
  io = IO::Memory.new
  builder = WP.builder.set_file_encryption_properties(encryption)
  builder.set_dictionary_enabled(dictionary)
  builder.set_data_page_row_count_limit(page_row_limit) if limit = page_row_limit
  writer = FWriter.new(io, two_column_schema, builder.build)
  row_group = writer.next_row_group

  {values_a, values_b}.each do |values|
    column_writer = row_group.next_column.not_nil!
    values.each { |value| column_writer.typed(Int32).write_batch([value], nil, nil) }
    column_writer.close
  end

  row_group.close
  writer.close
  io.to_slice
end

private def read_int32_values(reader : FReader, column : Int32) : Array(Int32)
  page_reader = reader.get_row_group(0).get_column_page_reader(column)
  values = [] of Int32
  while page = page_reader.get_next_page
    next unless page.is_data_page?
    buffer = page.as(Page::DataPage).buf
    (buffer.size // 4).times do |i|
      values << IO::ByteFormat::LittleEndian.decode(Int32, buffer[i * 4, 4])
    end
  end
  values
end

private def read_with(bytes : Bytes, decrypt : E::FileDecryptionProperties) : FReader
  options = ReadOptionsBuilder.new.with_file_decryption_properties(decrypt).build
  FReader.new_with_options(BytesCR.new(bytes), options)
end

describe "Parquet::File page decryption" do
  it "decrypts data pages encrypted with the footer key" do
    bytes = write_int32_file(
      E::FileEncryptionProperties.builder(FOOTER_KEY).build,
      [1, 2, 3], [4, 5, 6]
    )
    reader = read_with(bytes, E::FileDecryptionProperties.builder(FOOTER_KEY).build)

    reader.metadata.file_decryptor.should_not be_nil
    read_int32_values(reader, 0).should eq([1, 2, 3])
    read_int32_values(reader, 1).should eq([4, 5, 6])
  end

  it "decrypts data pages encrypted with per-column keys" do
    encryption = E::FileEncryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build
    bytes = write_int32_file(encryption, [10, 20], [30, 40])
    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .with_column_key("b", COLUMN_KEY_B)
      .build
    reader = read_with(bytes, decrypt)

    read_int32_values(reader, 0).should eq([10, 20])
    read_int32_values(reader, 1).should eq([30, 40])
  end

  it "decrypts multiple data pages using advancing page ordinals" do
    bytes = write_int32_file(
      E::FileEncryptionProperties.builder(FOOTER_KEY).build,
      [1, 2, 3, 4, 5], [6, 7, 8, 9, 10],
      page_row_limit: 1
    )
    reader = read_with(bytes, E::FileDecryptionProperties.builder(FOOTER_KEY).build)

    page_reader = reader.get_row_group(0).get_column_page_reader(0)
    page_count = 0
    values = [] of Int32
    while page = page_reader.get_next_page
      next unless page.is_data_page?
      page_count += 1
      buffer = page.as(Page::DataPage).buf
      (buffer.size // 4).times do |i|
        values << IO::ByteFormat::LittleEndian.decode(Int32, buffer[i * 4, 4])
      end
    end

    page_count.should be > 1
    values.should eq([1, 2, 3, 4, 5])
  end

  it "decrypts dictionary pages" do
    bytes = write_int32_file(
      E::FileEncryptionProperties.builder(FOOTER_KEY).build,
      [1, 2, 3], [4, 5, 6], dictionary: true
    )
    reader = read_with(bytes, E::FileDecryptionProperties.builder(FOOTER_KEY).build)

    page_reader = reader.get_row_group(0).get_column_page_reader(0)
    dictionary_page = nil.as(Page::DictionaryPage?)
    saw_data_page = false
    while page = page_reader.get_next_page
      if page.is_dictionary_page?
        dictionary_page = page.as(Page::DictionaryPage)
      elsif page.is_data_page?
        saw_data_page = true
      end
    end

    dictionary_page.should_not be_nil
    page = dictionary_page.not_nil!
    page.num_values.should eq(3)
    buffer = page.buf
    Array.new(buffer.size // 4) do |i|
      IO::ByteFormat::LittleEndian.decode(Int32, buffer[i * 4, 4])
    end.should eq([1, 2, 3])
    saw_data_page.should be_true
  end

  it "decrypts pages in a plaintext-footer file with column keys" do
    encryption = E::FileEncryptionProperties.builder(FOOTER_KEY)
      .with_plaintext_footer(true)
      .with_column_key("a", COLUMN_KEY_A)
      .build
    bytes = write_int32_file(encryption, [7, 8], [9, 10])
    decrypt = E::FileDecryptionProperties.builder(FOOTER_KEY)
      .with_column_key("a", COLUMN_KEY_A)
      .build
    reader = read_with(bytes, decrypt)

    read_int32_values(reader, 0).should eq([7, 8])
    read_int32_values(reader, 1).should eq([9, 10])
  end

  it "fails to decrypt a corrupted data page" do
    bytes = write_int32_file(
      E::FileEncryptionProperties.builder(FOOTER_KEY).build,
      [1, 2, 3], [4, 5, 6]
    )

    metadata = read_with(bytes, E::FileDecryptionProperties.builder(FOOTER_KEY).build).metadata
    column = metadata.row_group(0).column(0)
    start = (column.dictionary_page_offset || column.data_page_offset).to_i32
    last = start + column.total_compressed_size - 1
    corrupted = bytes.dup
    corrupted[last] = (corrupted[last] ^ 0xFF_u8)

    reader = read_with(corrupted, E::FileDecryptionProperties.builder(FOOTER_KEY).build)

    expect_raises(Parquet::ParquetError) do
      page_reader = reader.get_row_group(0).get_column_page_reader(0)
      page_reader.get_next_page
    end
  end
end
