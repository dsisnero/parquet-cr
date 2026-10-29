require "../../spec_helper"
require "../../support/metadata_snapshot_helpers"

# Clone is derived in upstream metadata/mod.rs. The mutation checks characterize
# Rust's owned-field vs Arc clone contract; the size/encoding fixture is derived
# from test_column_chunk_metadata_thrift_conversion_full_stats.
private alias SS = MetadataSnapshotSpecs

describe "Owned metadata snapshots" do
  it "clones a complete graph by value while sharing schema and page-index references" do
    source = SS.metadata
    snapshot = source.clone
    snapshot.should eq source
    snapshot.same?(source).should be_false
    snapshot.file_metadata.same?(source.file_metadata).should be_false
    snapshot.file_metadata.schema_descr.same?(source.file_metadata.schema_descr).should be_true
    snapshot.row_group(0).column(0).column_descr.same?(source.row_group(0).column(0).column_descr).should be_true
    snapshot.page_index.same?(source.page_index).should be_true
    snapshot.into_builder.take_row_groups.clear
    source.num_row_groups.should eq 1
    edited = source.clone.row_group(0).column(0).into_builder.set_num_values(77_i64).build
    edited.num_values.should eq 77
    source.row_group(0).column(0).num_values.should eq 2
  end

  it "owns binary statistics, histograms, encoding stats and metadata arrays independently" do
    source = SS.metadata
    snapshot = source.clone
    column = snapshot.row_group(0).column(0)
    stats = column.statistics || raise "missing stats"
    stats.min_bytes_opt.as(Bytes)[0] = 98_u8
    column.definition_level_histogram.as(SS::MD::LevelHistogram).values[1] = 99_i64
    column.encoding_stats.as(SS::MD::ParquetPageEncodingStats).full.as(Array(SS::MD::PageEncodingStats)).clear
    snapshot.file_metadata.key_value_metadata.as(Array(SS::MD::KeyValue)).clear
    snapshot.file_metadata.column_orders.as(Array(Parquet::ColumnOrder)).clear
    original = source.row_group(0).column(0)
    original.statistics.as(Parquet::Statistics).min_bytes_opt.should eq "a".to_slice
    original.definition_level_histogram.as(SS::MD::LevelHistogram).values.should eq [0_i64, 2_i64]
    original.encoding_stats.as(SS::MD::ParquetPageEncodingStats).full.as(Array(SS::MD::PageEncodingStats)).size.should eq 1
    source.file_metadata.key_value_metadata.as(Array(SS::MD::KeyValue)).size.should eq 1
    source.file_metadata.column_orders.as(Array(Parquet::ColumnOrder)).size.should eq 1
  end

  it "edits compression configurations without losing the metadata's codec contract" do
    source = SS.metadata.row_group(0).column(0)
    snapshot = source.clone.into_builder.set_compression(Parquet::Compression.zstd(7)).build
    snapshot.compression_codec.should eq Parquet::CompressionCodec::ZSTD
    snapshot.compression_config.should eq Parquet::Compression.zstd
    source.compression_codec.should eq Parquet::CompressionCodec::SNAPPY
  end

  it "preserves full encoding statistics across compact round trips" do
    source = SS.metadata
    snapshot = source.clone
    bytes = SS.write(snapshot)
    options = SS::MD::ParquetMetaDataOptions.new.with_encoding_stats_as_mask(false)
    decoded = SS::MD.parquet_metadata_from_bytes(SS.footer(bytes), options)
    decoded.row_group(0).column(0).encoding_stats.should eq source.row_group(0).column(0).encoding_stats
    decoded.row_group(0).column(0).statistics.should eq source.row_group(0).column(0).statistics
    decoded.row_group(0).column(0).compressed_size.should eq 2000
    decoded.row_group(0).column(0).uncompressed_size.should eq 3000
  end

  it "copies nested encryption buffers, sorting columns and geospatial types" do
    source = SS.metadata
    file = source.file_metadata
    file.encryption_algorithm = SS::MD::EncryptionAlgorithm.aes_gcm_ctr_v1(SS::MD::AesGcmCtrV1.new(Bytes[1], Bytes[2], true))
    file.footer_signing_key_metadata = Bytes[3]
    group = source.row_group(0)
    group.sorting_columns = [SS::MD::SortingColumn.new(0, false, true)]
    column = group.column(0)
    column.column_crypto_metadata = SS.crypto
    column.encrypted_column_metadata = Bytes[4]
    column.geo_statistics = Parquet::Geospatial::GeospatialStatistics.new(nil, [1, 2])
    column.repetition_level_histogram = SS::MD::LevelHistogram.from([2_i64])
    snapshot = source.clone
    copied = snapshot.row_group(0).column(0)
    snapshot.file_metadata.encryption_algorithm.as(SS::MD::EncryptionAlgorithm).aes_gcm_ctr_v1!.aad_prefix.as(Bytes)[0] = 99_u8
    snapshot.file_metadata.footer_signing_key_metadata.as(Bytes)[0] = 99_u8
    snapshot.row_group(0).sorting_columns.as(Array(SS::MD::SortingColumn)).clear
    crypto = copied.column_crypto_metadata.as(Parquet::File::ColumnCryptoMetaData).encryption_with_column_key!
    crypto.path_in_schema.clear
    crypto.key_metadata.as(Bytes)[0] = 99_u8
    copied.encrypted_column_metadata.as(Bytes)[0] = 99_u8
    copied.geo_statistics.as(Parquet::Geospatial::GeospatialStatistics).geospatial_types.as(Array(Int32)).clear
    copied.repetition_level_histogram.as(SS::MD::LevelHistogram).values[0] = 99_i64
    file.encryption_algorithm.as(SS::MD::EncryptionAlgorithm).aes_gcm_ctr_v1!.aad_prefix.should eq Bytes[1]
    file.footer_signing_key_metadata.should eq Bytes[3]
    group.sorting_columns.as(Array(SS::MD::SortingColumn)).size.should eq 1
    original_crypto = column.column_crypto_metadata.as(Parquet::File::ColumnCryptoMetaData).encryption_with_column_key!
    original_crypto.path_in_schema.should eq ["leaf"]
    original_crypto.key_metadata.should eq Bytes[7]
    column.encrypted_column_metadata.should eq Bytes[4]
    column.geo_statistics.as(Parquet::Geospatial::GeospatialStatistics).geospatial_types.should eq [1, 2]
    column.repetition_level_histogram.as(SS::MD::LevelHistogram).values.should eq [2_i64]
  end

  it "copies decryptor AAD while sharing its block decryptor and key properties" do
    source = SS.metadata
    properties = SS::E::FileDecryptionProperties.builder(Bytes.new(16, 1_u8)).build
    decryptor = SS::E::FileDecryptor.new(properties, nil, Bytes[2], Bytes[1])
    source.with_file_decryptor(decryptor)
    snapshot = source.clone
    snapshot.should eq source
    copied = snapshot.file_decryptor || raise "missing decryptor"
    copied.same?(decryptor).should be_false
    copied.footer_decryptor.same?(decryptor.footer_decryptor).should be_true
    copied.file_aad[0] = 99_u8
    decryptor.file_aad.should eq Bytes[1, 2]
    # Equality includes decryption properties; clone must not reconstruct them.
    copied.file_aad[0] = 1_u8
    copied.should eq decryptor
  end

  it "preserves all physical statistic variants and exactness flags without serialization" do
    values = [
      Parquet::Statistics.boolean(false, true, 2_u64, 1_u64, true),
      Parquet::Statistics.int32(-1, 2, nil, nil, false),
      Parquet::Statistics.int64(-3_i64, 4_i64, 2_u64, 0_u64, false),
      Parquet::Statistics.int96(Parquet::Int96.new, nil, nil, nil, true),
      Parquet::Statistics.float(1_f32, 2_f32, nil, 0_u64, false),
      Parquet::Statistics.new(Parquet::ValueStatistics(Float64).new(1.0, 2.0, 3_u64, 4_u64, false, 5_u64, false, false, true)),
      Parquet::Statistics.byte_array(Parquet::ByteArray.new("a"), nil, nil, nil, false),
      Parquet::Statistics.fixed_len_byte_array(Parquet::FixedLenByteArray.new("ab"), nil, nil, nil, false),
    ]
    values.each do |value|
      snapshot = value.clone
      snapshot.should eq value
      snapshot.same?(value).should be_false
    end
    fixed = values.last.clone
    fixed.min_bytes_opt.as(Bytes)[0] = 99_u8
    values.last.min_bytes_opt.should eq "ab".to_slice
    empty = Parquet::Statistics.byte_array(Parquet::ByteArray.new, nil, nil, nil, false)
    empty.clone.should eq empty
  end
end
