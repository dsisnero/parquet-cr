require "../../spec_helper"

# Ported from `parquet/src/file/metadata/mod.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe Parquet::File::Metadata::ColumnChunkMetaData do
  # Upstream `test_column_chunk_metadata_thrift_conversion`.
  it "round-trips column chunk metadata through thrift" do
    column_descr = ParquetTestHelpers.test_schema_descr.column(0)

    col_metadata = PMeta::ColumnChunkMetaData.builder(column_descr)
      .set_encodings_mask(ParquetTestHelpers.enc_mask(Parquet::Encoding::PLAIN, Parquet::Encoding::RLE))
      .set_file_path("file_path")
      .set_num_values(1000_i64)
      .set_compression_codec(Parquet::CompressionCodec::SNAPPY)
      .set_total_compressed_size(2000_i64)
      .set_total_uncompressed_size(3000_i64)
      .set_data_page_offset(4000_i64)
      .set_dictionary_page_offset(5000_i64)
      .set_page_encoding_stats([
        PMeta::PageEncodingStats.new(Parquet::PageType::DATA_PAGE, Parquet::Encoding::PLAIN, 3),
        PMeta::PageEncodingStats.new(Parquet::PageType::DATA_PAGE, Parquet::Encoding::RLE, 5),
      ])
      .set_bloom_filter_offset(6000_i64)
      .set_bloom_filter_length(25)
      .set_offset_index_offset(7000_i64)
      .set_offset_index_length(25)
      .set_column_index_offset(8000_i64)
      .set_column_index_length(25)
      .set_unencoded_byte_array_data_bytes(2000_i64)
      .set_repetition_level_histogram(PMeta::LevelHistogram.from([100_i64, 100_i64]))
      .set_definition_level_histogram(PMeta::LevelHistogram.from([0_i64, 200_i64]))
      .build

    io = IO::Memory.new
    writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
    col_metadata.write_thrift(writer)

    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(io.to_slice)
    result = PMeta.read_column_chunk(prot, column_descr, 0)

    expected = PMeta::ColumnChunkMetaData.builder(column_descr)
      .set_encodings_mask(ParquetTestHelpers.enc_mask(Parquet::Encoding::PLAIN, Parquet::Encoding::RLE))
      .set_file_path("file_path")
      .set_num_values(1000_i64)
      .set_compression_codec(Parquet::CompressionCodec::SNAPPY)
      .set_total_compressed_size(2000_i64)
      .set_total_uncompressed_size(3000_i64)
      .set_data_page_offset(4000_i64)
      .set_dictionary_page_offset(5000_i64)
      .set_page_encoding_stats_mask(ParquetTestHelpers.enc_mask(Parquet::Encoding::PLAIN, Parquet::Encoding::RLE))
      .set_bloom_filter_offset(6000_i64)
      .set_bloom_filter_length(25)
      .set_offset_index_offset(7000_i64)
      .set_offset_index_length(25)
      .set_column_index_offset(8000_i64)
      .set_column_index_length(25)
      .set_unencoded_byte_array_data_bytes(2000_i64)
      .set_repetition_level_histogram(PMeta::LevelHistogram.from([100_i64, 100_i64]))
      .set_definition_level_histogram(PMeta::LevelHistogram.from([0_i64, 200_i64]))
      .build

    result.should eq(expected)
  end

  # Characterization: column crypto metadata (ColumnChunk field 8) plus an
  # encrypted metadata blob (field 9). With the footer encrypted
  # (`plaintext_footer_mode` false) the plaintext meta_data (field 3) is skipped,
  # and the reader returns early once `encrypted_column_metadata` is seen.
  it "round-trips encrypted column chunk metadata" do
    column_descr = ParquetTestHelpers.test_schema_descr.column(0)
    crypto = Parquet::File::ColumnCryptoMetaData.encryption_with_column_key(
      Parquet::File::EncryptionWithColumnKey.new(["x"], Bytes[1, 2, 3])
    )

    col_metadata = PMeta::ColumnChunkMetaData.builder(column_descr)
      .set_file_path("file_path")
      .set_column_crypto_metadata(crypto)
      .set_encrypted_column_metadata(Bytes[9, 8, 7])
      .build

    io = IO::Memory.new
    col_metadata.write_thrift(Parquet::Thrift::ThriftCompactOutputProtocol.new(io))
    result = PMeta.read_column_chunk(
      Parquet::Thrift::ThriftSliceInputProtocol.new(io.to_slice), column_descr, 0
    )

    result.crypto_metadata.should eq(crypto)
    result.encrypted_column_metadata.should eq(Bytes[9, 8, 7])
    result.file_path.should eq("file_path")
    result.should eq(col_metadata)
  end

  # Characterization: when the footer is plaintext (`plaintext_footer_mode` true)
  # the plaintext meta_data is still written alongside the encrypted blob.
  it "writes plaintext meta_data in plaintext-footer mode" do
    column_descr = ParquetTestHelpers.test_schema_descr.column(0)
    crypto = Parquet::File::ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY

    col_metadata = PMeta::ColumnChunkMetaData.builder(column_descr)
      .set_encodings_mask(ParquetTestHelpers.enc_mask(Parquet::Encoding::PLAIN, Parquet::Encoding::RLE))
      .set_num_values(10_i64)
      .set_compression_codec(Parquet::CompressionCodec::SNAPPY)
      .set_total_compressed_size(20_i64)
      .set_total_uncompressed_size(30_i64)
      .set_data_page_offset(40_i64)
      .set_column_crypto_metadata(crypto)
      .set_encrypted_column_metadata(Bytes[1, 2])
      .set_plaintext_footer_mode(true)
      .build

    io = IO::Memory.new
    col_metadata.write_thrift(Parquet::Thrift::ThriftCompactOutputProtocol.new(io))
    result = PMeta.read_column_chunk(
      Parquet::Thrift::ThriftSliceInputProtocol.new(io.to_slice), column_descr, 0
    )

    result.num_values.should eq(10)
    result.data_page_offset.should eq(40)
    result.crypto_metadata.should eq(crypto)
    result.encrypted_column_metadata.should eq(Bytes[1, 2])
  end
end
