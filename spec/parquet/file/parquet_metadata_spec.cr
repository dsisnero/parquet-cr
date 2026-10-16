require "../../spec_helper"

# Ported from `parquet/src/file/metadata/thrift/mod.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

# Upstream `roundtrip_rg_ordinals` helper: builds row groups with the given
# ordinals, writes them to a Parquet footer, decodes, and returns the ordinals.
private def roundtrip_rg_ordinals(ordinals : Array(Int32?)) : Array(Int32?)
  field = ST.primitive_type_builder("c", PPhys::INT32).build.as(ST)
  schema = ST.group_type_builder("schema").with_fields([field]).build.as(ST)
  schema_descr = SSchemaDescriptor.new(schema)

  row_groups = ordinals.map do |ordinal|
    columns = schema_descr.columns.map { |col| PMeta::ColumnChunkMetaData.builder(col).build }
    builder = PMeta::RowGroupMetaData.builder(schema_descr)
      .set_num_rows(10_i64)
      .set_total_byte_size(100_i64)
      .set_column_metadata(columns)
    builder = builder.set_ordinal(ordinal) unless ordinal.nil?
    builder.build
  end

  file_metadata = PMeta::FileMetaData.new(
    1, (10 * ordinals.size).to_i64, nil, nil, schema_descr, nil,
  )
  metadata = PMeta::ParquetMetaData.new(file_metadata, row_groups)

  buffer = IO::Memory.new
  PMeta::ParquetMetaDataWriter.new(buffer, metadata).finish
  bytes = buffer.to_slice
  decoded = PMeta::ParquetMetaDataReader.decode_metadata(bytes[0, bytes.size - 8])
  decoded.row_groups.map(&.ordinal)
end

describe Parquet::File::Metadata::ParquetMetaData do
  # Upstream `ordinals_all_present_are_honored`.
  it "honors present row group ordinals" do
    roundtrip_rg_ordinals([5, 1, 3] of Int32?).should eq([5, 1, 3] of Int32?)
  end

  # Upstream `ordinals_none_present_are_sequentially_filled`.
  it "sequentially fills missing row group ordinals" do
    roundtrip_rg_ordinals([nil, nil, nil] of Int32?).should eq([0, 1, 2] of Int32?)
  end

  # Upstream `ordinals_mixed_decode_succeeds_untouched`.
  it "leaves mixed row group ordinals untouched" do
    roundtrip_rg_ordinals([0, nil, 2] of Int32?).should eq([0, nil, 2] of Int32?)
    roundtrip_rg_ordinals([nil, 1, 2] of Int32?).should eq([nil, 1, 2] of Int32?)
  end
end
