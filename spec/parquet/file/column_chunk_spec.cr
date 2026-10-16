require "../../spec_helper"

# Ported from `parquet/src/file/metadata/mod.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
alias PMeta = Parquet::File::Metadata

private def test_schema_descr : Parquet::Schema::SchemaDescriptor
  a = ST.primitive_type_builder("a", PPhys::INT32).build.as(ST)
  b = ST.primitive_type_builder("b", PPhys::INT32).build.as(ST)
  schema = ST.group_type_builder("schema").with_fields([a, b]).build.as(ST)
  SSchemaDescriptor.new(schema)
end

private def enc_mask(*encodings : Parquet::Encoding) : Parquet::EncodingMask
  Parquet::EncodingMask.new_from_encodings(encodings)
end

describe Parquet::File::Metadata::ColumnChunkMetaData do
  # Upstream `test_column_chunk_metadata_thrift_conversion`.
  it "round-trips column chunk metadata through thrift" do
    column_descr = test_schema_descr.column(0)

    col_metadata = PMeta::ColumnChunkMetaData.builder(column_descr)
      .set_encodings_mask(enc_mask(Parquet::Encoding::PLAIN, Parquet::Encoding::RLE))
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
      .set_encodings_mask(enc_mask(Parquet::Encoding::PLAIN, Parquet::Encoding::RLE))
      .set_file_path("file_path")
      .set_num_values(1000_i64)
      .set_compression_codec(Parquet::CompressionCodec::SNAPPY)
      .set_total_compressed_size(2000_i64)
      .set_total_uncompressed_size(3000_i64)
      .set_data_page_offset(4000_i64)
      .set_dictionary_page_offset(5000_i64)
      .set_page_encoding_stats_mask(enc_mask(Parquet::Encoding::PLAIN, Parquet::Encoding::RLE))
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
end
