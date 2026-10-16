require "../../spec_helper"

# Ported from `parquet/src/file/metadata/mod.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe Parquet::File::Metadata::RowGroupMetaData do
  # Upstream `test_row_group_metadata_thrift_conversion`.
  it "round-trips row group metadata through thrift" do
    schema_descr = ParquetTestHelpers.test_schema_descr

    columns = schema_descr.columns.map do |ptr|
      PMeta::ColumnChunkMetaData.builder(ptr).build
    end

    row_group_meta = PMeta::RowGroupMetaData.builder(schema_descr)
      .set_num_rows(1000_i64)
      .set_total_byte_size(2000_i64)
      .set_column_metadata(columns)
      .set_ordinal(1)
      .build

    io = IO::Memory.new
    writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
    row_group_meta.write_thrift(writer)

    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(io.to_slice)
    result = PMeta.read_row_group(prot, schema_descr)
    result.should eq(row_group_meta)
  end

  # Upstream `test_row_group_metadata_thrift_conversion_empty`.
  it "rejects a row group with a column count mismatch" do
    schema_descr = ParquetTestHelpers.test_schema_descr
    expect_raises(Parquet::ParquetError, "Column length mismatch: 2 != 0") do
      PMeta::RowGroupMetaData.builder(schema_descr).build
    end
  end

  # Upstream `test_row_group_metadata_thrift_corrupted`.
  it "rejects a row group whose column count differs from the schema" do
    schema_descr_2cols = ParquetTestHelpers.test_schema_descr
    schema_descr_3cols = ParquetTestHelpers.three_column_schema_descr

    row_group_meta_2cols = PMeta::RowGroupMetaData.builder(schema_descr_2cols)
      .set_num_rows(1000_i64)
      .set_total_byte_size(2000_i64)
      .set_column_metadata([
        PMeta::ColumnChunkMetaData.builder(schema_descr_2cols.column(0)).build,
        PMeta::ColumnChunkMetaData.builder(schema_descr_2cols.column(1)).build,
      ])
      .set_ordinal(1)
      .build

    io = IO::Memory.new
    writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
    row_group_meta_2cols.write_thrift(writer)

    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(io.to_slice)
    expect_raises(Parquet::ParquetError, "Column count mismatch. Schema has 3 columns while Row Group has 2") do
      PMeta.read_row_group(prot, schema_descr_3cols)
    end
  end
end
