module MetadataSnapshotSpecs
  alias MD = Parquet::File::Metadata
  alias E = Parquet::Encryption

  def self.metadata : MD::ParquetMetaData
    schema = Parquet::Schema::SchemaDescriptor.new(Parquet::Schema.parse_message_type("message root { OPTIONAL BINARY leaf (STRING); }"))
    stats = Parquet::Statistics.byte_array(Parquet::ByteArray.new("a"), Parquet::ByteArray.new("z"), 2_u64, 0_u64, false)
    column = MD::ColumnChunkMetaData.builder(schema.column(0))
      .set_num_values(2_i64).set_compression_codec(Parquet::CompressionCodec::SNAPPY)
      .set_total_compressed_size(2000_i64).set_total_uncompressed_size(3000_i64).set_data_page_offset(4000_i64)
      .set_statistics(stats).set_page_encoding_stats([MD::PageEncodingStats.new(Parquet::PageType::DATA_PAGE, Parquet::Encoding::PLAIN, 3)])
      .set_definition_level_histogram(MD::LevelHistogram.from([0_i64, 2_i64])).build
    group = MD::RowGroupMetaData.builder(schema).set_num_rows(2_i64).set_column_metadata([column]).set_ordinal(0).build
    file = MD::FileMetaData.new(2, 2_i64, "snapshot", [MD::KeyValue.new("Foo", "bar")], schema, [Parquet::ColumnOrder::UNDEFINED])
    index = MD::PageIndexBuilder.new(1, 1)
    offsets = MD::OffsetIndexBuilder.new
    offsets.append_row_count(2_i64)
    offsets.append_offset_and_size(4000_i64, 2000)
    index.put_offset_index(offsets.build, 0, 0)
    MD::ParquetMetaData.new(file, [group], index.build)
  end

  def self.write(metadata : MD::ParquetMetaData, prefix : Int32 = 0, encryptor : E::FileEncryptor? = nil) : Bytes
    sink = IO::Memory.new
    tracked = Parquet::File::TrackedWrite.new(sink)
    tracked.write(Bytes.new(prefix))
    writer = MD::ParquetMetaDataWriter.new_with_tracked(tracked, metadata)
    writer.with_file_encryptor(encryptor) if encryptor
    writer.finish
    sink.to_slice.dup
  end

  def self.footer(bytes : Bytes) : Bytes
    length = IO::ByteFormat::LittleEndian.decode(UInt32, bytes[bytes.size - 8, 4]).to_i32
    bytes[bytes.size - 8 - length, length]
  end

  def self.crypto : Parquet::File::ColumnCryptoMetaData
    Parquet::File::ColumnCryptoMetaData.encryption_with_column_key(Parquet::File::EncryptionWithColumnKey.new(["leaf"], Bytes[7]))
  end
end
