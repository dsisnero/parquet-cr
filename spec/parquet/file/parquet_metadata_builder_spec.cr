require "../../spec_helper"

# Ported from `parquet/src/file/metadata/mod.rs` (`ParquetMetaData`,
# `ParquetMetaDataBuilder`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no direct unit tests for the builder/page-index plumbing (it is
# exercised through the reader/writer). These characterize the API against the
# same behavior.

private def builder_file_metadata : PMeta::FileMetaData
  schema_descr = ParquetTestHelpers.test_schema_descr
  PMeta::FileMetaData.new(1, 100_i64, "test", nil, schema_descr, nil)
end

private def builder_row_group(schema_descr : Parquet::Schema::SchemaDescriptor, num_rows : Int64) : PMeta::RowGroupMetaData
  columns = schema_descr.columns.map { |col| PMeta::ColumnChunkMetaData.builder(col).build }
  PMeta::RowGroupMetaData.builder(schema_descr)
    .set_num_rows(num_rows)
    .set_total_byte_size(1_i64)
    .set_column_metadata(columns)
    .build
end

private def builder_page_index : PMeta::PageIndex
  builder = PMeta::PageIndexBuilder.new(1, 2)
  column = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
  column.append(false, Parquet::FromBytes.to_le_slice(1), Parquet::FromBytes.to_le_slice(2), 0_i64, nil)
  builder.put_column_index(column.build, 0, 0)
  offset = PMeta::OffsetIndexBuilder.new
  offset.append_row_count(10_i64)
  offset.append_offset_and_size(4_i64, 100)
  builder.put_offset_index(offset.build, 0, 0)
  builder.build
end

describe Parquet::File::Metadata::ParquetMetaDataBuilder do
  it "builds metadata from file metadata and row groups" do
    file_metadata = builder_file_metadata
    row_group = builder_row_group(file_metadata.schema_descr, 100_i64)

    metadata = PMeta::ParquetMetaDataBuilder.new(file_metadata)
      .add_row_group(row_group)
      .build

    metadata.num_row_groups.should eq(1)
    metadata.row_group(0).num_rows.should eq(100_i64)
    metadata.page_index.should be_nil
  end

  it "sets and takes row groups" do
    file_metadata = builder_file_metadata
    rg1 = builder_row_group(file_metadata.schema_descr, 1_i64)
    rg2 = builder_row_group(file_metadata.schema_descr, 2_i64)

    builder = PMeta::ParquetMetaDataBuilder.new(file_metadata).set_row_groups([rg1, rg2])
    builder.row_groups.size.should eq(2)
    builder.take_row_groups.size.should eq(2)
    builder.row_groups.should be_empty
    builder.build.num_row_groups.should eq(0)
  end

  it "sets and takes the page index" do
    file_metadata = builder_file_metadata
    page_index = builder_page_index

    builder = PMeta::ParquetMetaDataBuilder.new(file_metadata).set_page_index(page_index)
    builder.page_index.should eq(page_index)
    builder.take_page_index.should eq(page_index)
    builder.page_index.should be_nil
  end

  it "creates a builder from existing metadata" do
    file_metadata = builder_file_metadata
    page_index = builder_page_index
    metadata = PMeta::ParquetMetaData.new(
      file_metadata, [builder_row_group(file_metadata.schema_descr, 3_i64)], page_index
    )

    builder = PMeta::ParquetMetaDataBuilder.new_from_metadata(metadata)
    builder.row_groups.size.should eq(1)
    builder.page_index.should eq(page_index)
    builder.build.should eq(metadata)
  end
end

describe Parquet::File::Metadata::ParquetMetaData do
  it "exposes the page index and a per-row-group view" do
    file_metadata = builder_file_metadata
    page_index = builder_page_index
    metadata = PMeta::ParquetMetaData.new(file_metadata, [] of PMeta::RowGroupMetaData, page_index)

    metadata.page_index.should eq(page_index)
    row_group_index = metadata.page_index_for_row_group(0)
    row_group_index.column_index(0).should_not be_nil
    row_group_index.offset_index(0).should_not be_nil
    row_group_index.num_data_pages(0).should eq(1)
  end

  it "converts to a builder" do
    file_metadata = builder_file_metadata
    metadata = PMeta::ParquetMetaData.new(file_metadata, [] of PMeta::RowGroupMetaData)

    metadata.into_builder.build.should eq(metadata)
  end

  it "compares page indexes by value" do
    file_metadata = builder_file_metadata
    a = PMeta::ParquetMetaData.new(file_metadata, [] of PMeta::RowGroupMetaData, builder_page_index)
    b = PMeta::ParquetMetaData.new(file_metadata, [] of PMeta::RowGroupMetaData, builder_page_index)
    c = PMeta::ParquetMetaData.new(file_metadata, [] of PMeta::RowGroupMetaData)

    a.should eq(b)
    a.should_not eq(c)
  end
end
