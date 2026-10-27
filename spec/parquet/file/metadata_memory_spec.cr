require "../../spec_helper"
require "../../support/async_reader_helpers"

# Ports test_memory_size and test_memory_size_with_decryptor from metadata/mod.rs
# at arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b.
# Rust byte literals depend on Vec/Arc/enum layout. Exact native expectations
# below use Crystal class headers, array capacity and inline struct sizes.
private alias FootM = Parquet::File::Metadata
private alias FootP = Parquet::File::PageIndex
private alias FootS = Parquet::Schema

private def native_string_bytes(value : String) : Int64
  String::HEADER_SIZE.to_i64 + value.bytesize + 1
end

private def native_array_bytes(value : Array(T)) : Int64 forall T
  instance_sizeof(Array(T)).to_i64 + value.@capacity.to_i64 * sizeof(T)
end

private def footprint_fixture : FootM::ParquetMetaData
  fields = %w[a b].map do |name|
    FootS::Type.primitive_type_builder(name, Parquet::Type::INT32).build.as(FootS::Type)
  end
  schema = FootS::SchemaDescriptor.new(FootS::Type.group_type_builder("schema").with_fields(fields).build)
  columns = schema.columns.map do |column|
    FootM::ColumnChunkMetaData.builder(column)
      .set_statistics(Parquet::Statistics.new(Parquet::ValueStatistics(Int32).new(0, 100, nil, nil, false))).build
  end
  group = FootM::RowGroupMetaData.builder(schema).set_num_rows(1000).set_column_metadata(columns).build
  file = FootM::FileMetaData.new(2, 1000_i64, "test harness", [FootM::KeyValue.new("Foo", "bar")], schema,
    [Parquet::ColumnOrder::UNDEFINED, Parquet::ColumnOrder.type_defined_order(Parquet::SortOrder::UNSIGNED)])
  FootM::ParquetMetaData.new(file, [group])
end

private class OpaqueFootprintIndex < FootM::PageIndexProvider
  def column_indexes? : Bool
    false
  end

  def offset_indexes? : Bool
    false
  end

  def column_index(row_group_idx : Int32, column_idx : Int32) : FootP::ColumnIndexMetaData?
    raise "Memory accounting must not read custom providers"
  end

  def offset_index(row_group_idx : Int32, column_idx : Int32) : FootP::OffsetIndexMetaData?
    raise "Memory accounting must not read custom providers"
  end
end

describe "Parquet metadata memory accounting" do
  it "ports test_memory_size with exact native layout accounting" do
    metadata = footprint_fixture
    file = metadata.file_metadata
    schema = file.schema_descr
    root = schema.root_schema
    group = metadata.row_group(0)
    # Count schema once; leaf descriptors contribute paths, not primitive types.
    expected = instance_sizeof(FootM::ParquetMetaData).to_i64 + instance_sizeof(FootM::FileMetaData) +
               instance_sizeof(FootS::SchemaDescriptor) + instance_sizeof(FootS::GroupType) +
               native_string_bytes("schema") + native_array_bytes(root.get_fields) +
               2 * instance_sizeof(FootS::PrimitiveType) + 2 * native_string_bytes("a") +
               native_array_bytes(schema.columns) + 2 * instance_sizeof(FootS::ColumnDescriptor) +
               schema.columns.sum { |column| native_array_bytes(column.path.parts) + native_string_bytes(column.path.parts.first) } +
               native_array_bytes(schema.@leaf_to_base) + native_string_bytes("test harness") +
               native_array_bytes(file.key_value_metadata || [] of FootM::KeyValue) +
               native_string_bytes("Foo") + native_string_bytes("bar") +
               native_array_bytes(file.column_orders || [] of Parquet::ColumnOrder) +
               native_array_bytes(metadata.row_groups) + instance_sizeof(FootM::RowGroupMetaData) +
               native_array_bytes(group.columns) + 2 * instance_sizeof(FootM::ColumnChunkMetaData) +
               2 * instance_sizeof(Parquet::Statistics)
    metadata.memory_size.should eq expected

    builder = FootM::PageIndexBuilder.new(1, 1)
    column = FootM::ColumnIndexBuilder.new(Parquet::Type::BOOLEAN)
    column.append(false, Bytes[1], Bytes[2, 3], 4_i64, nil)
    column_index = column.build
    builder.put_column_index(column_index, 0, 0)
    offsets = FootM::OffsetIndexBuilder.new
    2.times do
      offsets.append_row_count(1_i64)
      offsets.append_offset_and_size(2_i64, 3)
      offsets.append_unencoded_byte_array_data_bytes(10_i64)
    end
    offset_index = offsets.build
    builder.put_offset_index(offset_index, 0, 0)
    index = builder.build
    indexed = FootM::ParquetMetaData.new(file, metadata.row_groups, index)
    # Independently total every native page-index buffer.
    primitive = column_index.inner.as(FootP::PrimitiveColumnIndex(Bool))
    common = primitive.column_index
    expected_index = instance_sizeof(FootM::PageIndex).to_i64 +
                     native_array_bytes(index.column_indexes || [] of Array(FootP::ColumnIndexMetaData?)) +
                     native_array_bytes((index.column_indexes || raise "column indexes missing").first) +
                     native_array_bytes(index.offset_indexes || [] of Array(FootP::OffsetIndexMetaData?)) +
                     native_array_bytes((index.offset_indexes || raise "offset indexes missing").first) +
                     native_array_bytes(common.null_pages) + native_array_bytes(common.null_counts || [] of Int64) +
                     native_array_bytes(primitive.min_values) + native_array_bytes(primitive.max_values) +
                     native_array_bytes(offset_index.page_locations) +
                     native_array_bytes(offset_index.unencoded_byte_array_data_bytes || [] of Int64)
    indexed.memory_size.should eq expected + expected_index
    indexed.memory_size.should be > metadata.memory_size
  end

  it "ports test_memory_size_with_decryptor, counting native keys and cipher state" do
    metadata = footprint_fixture
    props = Parquet::Encryption::FileDecryptionProperties.builder("0123456789012345".to_slice)
      .with_aad_prefix(Bytes.new(8, 2_u8)).with_column_key("a", "1234567890123450".to_slice).build
    decryptor = Parquet::Encryption::FileDecryptor.new(props, Bytes.new(8, 3_u8), Bytes.new(8, 1_u8), Bytes.new(8, 2_u8))
    encrypted = FootM::ParquetMetaData.new(metadata.file_metadata, metadata.row_groups, nil, decryptor)
    # Explicit properties, the footer cipher's retained key and composed AAD.
    keys = props.keys.explicit || raise "explicit keys missing"
    hash = keys.column_keys
    buckets = 1_i64 << hash.@indices_size_pow2
    hash_buffers = buckets // 2 * sizeof(Hash::Entry(String, Bytes)) +
                   (hash.@indices.null? ? 0_i64 : buckets * hash.@indices_bytesize)
    expected = instance_sizeof(Parquet::Encryption::FileDecryptor).to_i64 +
               instance_sizeof(Parquet::Encryption::FileDecryptionProperties) +
               instance_sizeof(Parquet::Encryption::GcmBlockDecryptor) +
               16 + 16 + 16 + 8 + instance_sizeof(Hash(String, Bytes)) +
               hash_buffers + native_string_bytes("a") + 16
    encrypted.memory_size.should eq metadata.memory_size + expected
    encrypted.memory_size.should be > metadata.memory_size
  end

  it "does not recount schema and column descriptors for additional row groups" do
    metadata = footprint_fixture
    group = metadata.row_group(0)
    repeated = FootM::ParquetMetaData.new(metadata.file_metadata, [group, group])
    delta = native_array_bytes(repeated.row_groups) - native_array_bytes(metadata.row_groups) +
            instance_sizeof(FootM::RowGroupMetaData) + native_array_bytes(group.columns) +
            2 * instance_sizeof(FootM::ColumnChunkMetaData) + 2 * instance_sizeof(Parquet::Statistics)
    repeated.memory_size.should eq metadata.memory_size + delta
  end

  it "counts retained array capacity, nested strings and optional scalar values" do
    values = Array(Int64).new(40)
    values << 1_i64
    Parquet::MemorySize.retained(values).should eq native_array_bytes(values)
    strings = ["abc", "defg"]
    Parquet::MemorySize.retained(strings).should eq native_array_bytes(strings) + native_string_bytes("abc") + native_string_bytes("defg")
    Parquet::MemorySize.retained(nil).should eq 0
    Parquet::MemorySize.retained(123_i64).should eq 0
    Parquet::MemorySize.retained(Parquet::Encoding::PLAIN).should eq 0
  end

  it "counts byte statistics payloads without charging inline scalar bounds" do
    Parquet::ValueStatistics(Int32).new(1, 5, nil, nil, false).heap_size.should eq 0
    Parquet::ValueStatistics(Parquet::ByteArray).new(Parquet::ByteArray.new(Bytes[1, 2]), Parquet::ByteArray.new(Bytes[3, 4, 5]), nil, nil, false).heap_size.should eq 5
    Parquet::ValueStatistics(Parquet::FixedLenByteArray).new(Parquet::FixedLenByteArray.new(Parquet::ByteArray.new(Bytes[1, 2])), nil, nil, nil, false).heap_size.should eq 2
    Parquet::Int96.new.heap_size.should eq 0
  end

  it "counts byte indexes, NaN counts and level histograms" do
    common = FootP::ColumnIndex.new([false], Parquet::BoundaryOrder::UNORDERED, [0_i64], [1_i64], [2_i64], [3_i64])
    index = FootP::ByteArrayColumnIndex.new(common, Bytes[1, 2], [0, 2], Bytes[3, 4, 5], [0, 3])
    expected = native_array_bytes(common.null_pages) +
               4 * native_array_bytes([0_i64]) +
               native_array_bytes(index.min_offsets) + native_array_bytes(index.max_offsets) + 5
    index.heap_size.should eq expected
    FootM::LevelHistogram.from([1_i64, 2_i64]).heap_size.should eq native_array_bytes([1_i64, 2_i64])
  end

  it "preserves upstream's exclusion of opaque custom index providers" do
    metadata = footprint_fixture
    custom = FootM::ParquetMetaData.new(metadata.file_metadata, metadata.row_groups, OpaqueFootprintIndex.new)
    custom.memory_size.should eq metadata.memory_size
  end

  it "accounts for parsed file metadata and loaded page indexes without mutating either" do
    bytes = AsyncReaderSpecs.fixture
    input = Parquet::File::BytesChunkReader.new(bytes)
    metadata = FootM::ParquetMetaDataReader.new.parse_and_finish(input)
    indexed = FootM::ParquetMetaDataReader.new.with_page_index_policy(FootM::PageIndexPolicy::Required).parse_and_finish(input)
    indexed.memory_size.should be > metadata.memory_size
    before = indexed.memory_size
    indexed.file_metadata.num_rows.should eq 12
    indexed.memory_size.should eq before
    indexed.row_groups.map(&.num_rows).should eq [4, 4, 4]
  end
end

private class FootprintKeyRetriever < Parquet::Encryption::KeyRetriever
  def retrieve_key(key_metadata : Bytes) : Bytes
    Bytes.new(16, 1_u8)
  end
end

describe "Native metadata allocations" do
  it "counts logical-type class storage and retained CRS text" do
    logical = Parquet::LogicalType::String.new
    field = FootS::Type.primitive_type_builder("text", Parquet::Type::BYTE_ARRAY).with_logical_type(logical).build
    field.heap_size.should eq native_string_bytes("text") + instance_sizeof(Parquet::LogicalType::String)
    geometry = Parquet::LogicalType::Geometry.new(Parquet::GeometryType.new("EPSG:4326"))
    field = FootS::Type.primitive_type_builder("shape", Parquet::Type::BYTE_ARRAY).with_logical_type(geometry).build
    field.heap_size.should eq native_string_bytes("shape") + instance_sizeof(Parquet::LogicalType::Geometry) + native_string_bytes("EPSG:4326")
  end

  it "counts file and column encryption payloads, histograms and geospatial types" do
    metadata = footprint_fixture
    before = metadata.memory_size
    file = metadata.file_metadata
    file.encryption_algorithm = FootM::EncryptionAlgorithm.aes_gcm_v1(FootM::AesGcmV1.new(Bytes.new(8), Bytes.new(8), true))
    file.footer_signing_key_metadata = Bytes.new(7)
    metadata.memory_size.should eq before + 23
    column = metadata.row_group(0).column(0)
    before = metadata.memory_size
    crypto = Parquet::File::EncryptionWithColumnKey.new(["a"], Bytes.new(10))
    column.column_crypto_metadata = Parquet::File::ColumnCryptoMetaData.encryption_with_column_key(crypto)
    column.encrypted_column_metadata = Bytes.new(13)
    column.repetition_level_histogram = FootM::LevelHistogram.from([1_i64, 2_i64])
    column.definition_level_histogram = FootM::LevelHistogram.from([3_i64, 4_i64])
    types = [1, 2]
    column.geo_statistics = Parquet::Geospatial::GeospatialStatistics.new(nil, types)
    expected = native_array_bytes(crypto.path_in_schema) + native_string_bytes("a") + 10 + 13 +
               2 * native_array_bytes([1_i64, 2_i64]) + instance_sizeof(Parquet::Geospatial::GeospatialStatistics) +
               native_array_bytes(types)
    metadata.memory_size.should eq before + expected
  end

  it "excludes application-owned key retriever state" do
    keys = Parquet::Encryption::DecryptionKeys.via_retriever(FootprintKeyRetriever.new)
    keys.heap_size.should eq 0
  end

  it "tracks reserved hash capacity after entries are removed" do
    hash = Hash(String, Bytes).new
    Parquet::MemorySize.retained(hash).should eq instance_sizeof(Hash(String, Bytes))
    40.times { |i| hash[i.to_s] = Bytes.new(16) }
    hash.clear
    buckets = hash.@entries.null? ? 0_i64 : 1_i64 << hash.@indices_size_pow2
    buffers = buckets // 2 * sizeof(Hash::Entry(String, Bytes)) +
              (hash.@indices.null? ? 0_i64 : buckets * hash.@indices_bytesize)
    Parquet::MemorySize.retained(hash).should eq instance_sizeof(Hash(String, Bytes)) + buffers
  end
end
