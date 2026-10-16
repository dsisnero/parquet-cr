require "../../spec_helper"

# Ported from `parquet/src/file/metadata/writer.rs` and `file/writer.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no `writer.rs` unit tests; the footer path is exercised through
# `roundtrip_rg_ordinals` (ported in parquet_metadata_spec.cr). These
# characterize the page-index footer layout (column/offset index placement and
# the back-filled offsets in `ColumnChunkMetaData`).

private def writer_row_group(schema_descr : Parquet::Schema::SchemaDescriptor, num_rows : Int64) : PMeta::RowGroupMetaData
  columns = schema_descr.columns.map { |col| PMeta::ColumnChunkMetaData.builder(col).build }
  PMeta::RowGroupMetaData.builder(schema_descr)
    .set_num_rows(num_rows)
    .set_total_byte_size(100_i64)
    .set_column_metadata(columns)
    .build
end

private def writer_page_index : PMeta::PageIndex
  builder = PMeta::PageIndexBuilder.new(1, 2)
  column = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
  column.append(false, Parquet::FromBytes.to_le_slice(-123), Parquet::FromBytes.to_le_slice(234), 0_i64, nil)
  column.set_boundary_order(Parquet::BoundaryOrder::ASCENDING)
  builder.put_column_index(column.build, 0, 0)
  offset = PMeta::OffsetIndexBuilder.new
  offset.append_row_count(50_i64)
  offset.append_offset_and_size(1000_i64, 500)
  builder.put_offset_index(offset.build, 0, 0)
  builder.build
end

private def writer_metadata(with_index : Bool, key_value_metadata : Array(PMeta::KeyValue)? = nil) : PMeta::ParquetMetaData
  schema_descr = ParquetTestHelpers.test_schema_descr
  row_group = writer_row_group(schema_descr, 50_i64)
  file_metadata = PMeta::FileMetaData.new(1, 50_i64, "parquet-cr", key_value_metadata, schema_descr, nil)
  PMeta::ParquetMetaData.new(file_metadata, [row_group], with_index ? writer_page_index : nil)
end

private def write_metadata_bytes(metadata : PMeta::ParquetMetaData, write_path_in_schema : Bool = true) : Bytes
  buffer = IO::Memory.new
  PMeta::ParquetMetaDataWriter.new(buffer, metadata)
    .with_write_path_in_schema(write_path_in_schema)
    .finish
  buffer.to_slice.dup
end

private def decode_written_file_metadata(bytes : Bytes) : PMeta::ParquetMetaData
  metadata_len = IO::Memory.new(bytes[bytes.size - 8, 4]).read_bytes(UInt32, IO::ByteFormat::LittleEndian)
  start = bytes.size - 8 - metadata_len.to_i32
  PMeta::ParquetMetaDataReader.decode_metadata(bytes[start, metadata_len.to_i32])
end

describe Parquet::File::Metadata::ParquetMetaDataWriter do
  it "writes a footer without page indexes" do
    bytes = write_metadata_bytes(writer_metadata(false))

    bytes[bytes.size - 4, 4].should eq("PAR1".to_slice)
    decoded = decode_written_file_metadata(bytes)
    decoded.num_row_groups.should eq(1)
    decoded.row_group(0).num_rows.should eq(50_i64)
    decoded.file_metadata.created_by.should eq("parquet-cr")
    decoded.row_group(0).column(0).column_index_offset.should be_nil
    decoded.row_group(0).column(0).offset_index_offset.should be_nil
  end

  it "writes column and offset indexes and back-fills their offsets" do
    bytes = write_metadata_bytes(writer_metadata(true))
    decoded = decode_written_file_metadata(bytes)

    col0 = decoded.row_group(0).column(0)
    ci_offset = col0.column_index_offset.not_nil!
    ci_length = col0.column_index_length.not_nil!
    oi_offset = col0.offset_index_offset.not_nil!
    oi_length = col0.offset_index_length.not_nil!

    ci = PPI.decode_column_index(bytes[ci_offset.to_i32, ci_length], col0.column_type)
    ci.boundary_order.should eq(Parquet::BoundaryOrder::ASCENDING)
    ci.inner.as(PPI::PrimitiveColumnIndex(Int32)).min_value(0).should eq(-123)
    ci.inner.as(PPI::PrimitiveColumnIndex(Int32)).max_value(0).should eq(234)

    oi = PPI.decode_offset_index(bytes[oi_offset.to_i32, oi_length])
    oi.page_locations[0].should eq(PPI::PageLocation.new(1000_i64, 500, 0_i64))

    # the second column has no indexes
    col1 = decoded.row_group(0).column(1)
    col1.column_index_offset.should be_nil
    col1.offset_index_offset.should be_nil
  end

  it "writes column orders and key-value metadata" do
    kv = [PMeta::KeyValue.new("a", "b")]
    bytes = write_metadata_bytes(writer_metadata(false, kv))
    decoded = decode_written_file_metadata(bytes)

    decoded.file_metadata.column_orders.not_nil!.size.should eq(2)
    decoded.file_metadata.key_value_metadata.should eq(kv)
  end

  it "supports toggling path_in_schema" do
    bytes = write_metadata_bytes(writer_metadata(true), write_path_in_schema: false)
    decoded = decode_written_file_metadata(bytes)
    decoded.row_group(0).column(0).column_index_offset.should_not be_nil
  end
end
