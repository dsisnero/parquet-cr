require "../../spec_helper"

# Characterizations of parquet/examples/custom_page_index.rs (arrow-rs 60.0.0,
# ef1fa157). The pinned example has no unit tests. Preserve its exact sample
# dataset, selective fetch pattern and Option<HashMap> presence semantics.
private alias OD = Parquet::File::Metadata::OnDemandPageIndexProvider
private alias OM = Parquet::File::Metadata
private alias OE = Parquet::Examples::CustomPageIndex

private def demand_metadata(bytes : Bytes, policy = OM::PageIndexPolicy::Skip) : OM::ParquetMetaData
  OM::ParquetMetaDataReader.new.with_page_index_policy(policy)
    .parse_and_finish(Parquet::File::BytesChunkReader.new(bytes))
end

describe "On-demand page indexes" do
  it "starts with no cached indexes and never fetches on lookup" do
    bytes = OE.create_sample_file
    provider = OD.new(demand_metadata(bytes), bytes)
    provider.column_indexes?.should be_false
    provider.offset_indexes?.should be_false
    provider.column_index(0, 0).should be_nil
    provider.offset_index(0, 0).should be_nil
    provider.column_index(50, 50).should be_nil
  end

  it "ports the selective row-group/column workflow through ParquetMetaData" do
    bytes = OE.create_sample_file
    metadata = demand_metadata(bytes)
    metadata.num_row_groups.should eq 3
    metadata.file_metadata.schema_descr.num_columns.should eq 6
    metadata.row_groups.map(&.num_rows).should eq [50_i64, 50_i64, 50_i64]
    provider = OD.new(metadata.clone, bytes)
    provider.fetch_column_index(0, 0)
    [0, 1, 4].each { |column| provider.fetch_offset_index(0, column) }
    selected = metadata.into_builder.set_page_index(provider).build
    full = demand_metadata(bytes, OM::PageIndexPolicy::Required).page_index || raise "missing full index"
    3.times do |group|
      6.times do |column|
        view = selected.page_index_for_row_group(group)
        view.column_index(column).should eq(group == 0 && column == 0 ? full.column_index(group, column) : nil)
        view.offset_index(column).should eq(group == 0 && [0, 1, 4].includes?(column) ? full.offset_index(group, column) : nil)
      end
    end
    provider.complete?.should be_true
    selected.clone.page_index.same?(provider).should be_true
    provider.num_data_pages(0, 0).should eq full.num_data_pages(0, 0)
    provider.page_locations(0, 4).should eq full.page_locations(0, 4)
  end

  it "caches both kinds and reuses the decoded arrays across repeated fetches" do
    bytes = OE.create_sample_file
    provider = OD.new(demand_metadata(bytes), bytes)
    provider.fetch_column_index(0, 0)
    provider.offset_indexes?.should be_false
    provider.fetch_offset_index(0, 0)
    column = provider.column_index(0, 0) || raise "missing column index"
    offsets = provider.offset_index(0, 0) || raise "missing offset index"
    provider.fetch_column_index(0, 0)
    provider.fetch_offset_index(0, 0)
    cached = provider.column_index(0, 0).as(OM::ColumnIndexMetaData).inner.as(Parquet::File::PageIndex::PrimitiveColumnIndex(Int32))
    original = column.inner.as(Parquet::File::PageIndex::PrimitiveColumnIndex(Int32))
    cached.min_values.same?(original.min_values).should be_true
    provider.offset_index(0, 0).as(OM::OffsetIndexMetaData).page_locations.same?(offsets.page_locations).should be_true
  end

  it "decodes every sample type across all groups identically to the dense provider" do
    bytes = OE.create_sample_file
    provider = OD.new(demand_metadata(bytes), bytes)
    full = demand_metadata(bytes, OM::PageIndexPolicy::Required).page_index || raise "missing full index"
    3.times do |group|
      6.times do |column|
        provider.fetch_offset_index(group, column)
        provider.fetch_column_index(group, column)
        provider.column_index(group, column).should eq full.column_index(group, column)
        provider.offset_index(group, column).should eq full.offset_index(group, column)
      end
    end
  end

  it "reports missing page-index metadata from the example printer" do
    bytes = OE.create_sample_file
    output = IO::Memory.new
    expect_raises(Parquet::GeneralError, "no page index") do
      OE.print_page_index(demand_metadata(bytes), 0, output)
    end
    output.to_s.should contain("No page index in metadata")
  end

  it "clones populated caches independently while keeping further fetches selective" do
    bytes = OE.create_sample_file
    provider = OD.new(demand_metadata(bytes), bytes)
    provider.fetch_column_index(0, 0)
    provider.fetch_offset_index(0, 0)
    provider.fetch_column_index(0, 2)
    provider.fetch_offset_index(0, 2)
    snapshot = provider.clone
    snapshot.column_index(0, 0).should eq provider.column_index(0, 0)
    snapshot.offset_index(0, 0).should eq provider.offset_index(0, 0)
    copied = snapshot.column_index(0, 0).as(OM::ColumnIndexMetaData).inner.as(Parquet::File::PageIndex::PrimitiveColumnIndex(Int32))
    original = provider.column_index(0, 0).as(OM::ColumnIndexMetaData).inner.as(Parquet::File::PageIndex::PrimitiveColumnIndex(Int32))
    copied.min_values[0] = 999
    original.min_values[0].should eq 0
    snapshot.offset_index(0, 0).as(OM::OffsetIndexMetaData).page_locations.clear
    provider.offset_index(0, 0).as(OM::OffsetIndexMetaData).page_locations.should_not be_empty
    text = snapshot.column_index(0, 2).as(OM::ColumnIndexMetaData).inner.as(Parquet::File::PageIndex::ByteArrayColumnIndex)
    original_text = provider.column_index(0, 2).as(OM::ColumnIndexMetaData).inner.as(Parquet::File::PageIndex::ByteArrayColumnIndex)
    text.min_bytes[0] = 99_u8
    original_text.min_bytes[0].should eq 'n'.ord.to_u8
    text.min_offsets.clear
    original_text.min_offsets.should_not be_empty
    copied.column_index.null_pages.clear
    original.column_index.null_pages.should_not be_empty
    snapshot.fetch_column_index(1, 4)
    provider.column_index(1, 4).should be_nil
    empty = OD.new(demand_metadata(bytes), bytes).clone
    empty.column_indexes?.should be_false
    empty.offset_indexes?.should be_false
  end

  it "keeps index-kind availability after requesting a chunk without that index" do
    bytes = OE.create_sample_file
    metadata = demand_metadata(bytes)
    column = metadata.row_group(0).column(0)
    column.column_index_offset = nil
    column.column_index_length = nil
    column.offset_index_offset = nil
    column.offset_index_length = nil
    provider = OD.new(metadata, bytes)
    provider.fetch_column_index(0, 0)
    provider.column_indexes?.should be_true
    provider.offset_indexes?.should be_false
    provider.column_index(0, 0).should be_nil
    provider.fetch_offset_index(0, 0)
    provider.offset_indexes?.should be_true
    provider.offset_index(0, 0).should be_nil
  end

  it "does not parse corrupt bytes until the corresponding index is requested" do
    bytes = OE.create_sample_file
    metadata = demand_metadata(bytes)
    column = metadata.row_group(0).column(1)
    column.column_index_offset = 0_i64
    column.column_index_length = 1
    provider = OD.new(metadata, bytes)
    provider.fetch_column_index(0, 0)
    expect_raises(Parquet::ParquetError) { provider.fetch_column_index(0, 1) }
    provider.column_index(0, 1).should be_nil
    provider.column_index(0, 0).should_not be_nil
    # Failed decodes do not occupy the cache slot; a corrected range can retry.
    original = demand_metadata(bytes).row_group(0).column(1)
    column.column_index_offset = original.column_index_offset
    column.column_index_length = original.column_index_length
    provider.fetch_column_index(0, 1)
    provider.column_index(0, 1).should_not be_nil
  end

  it "reports invalid index byte ranges without populating a cache entry" do
    bytes = OE.create_sample_file
    metadata = demand_metadata(bytes)
    metadata.row_group(0).column(0).offset_index_offset = bytes.size.to_i64
    provider = OD.new(metadata, bytes)
    expect_raises(Parquet::GeneralError, "Page index range exceeds file bytes") { provider.fetch_offset_index(0, 0) }
    provider.offset_index(0, 0).should be_nil
  end

  it "runs the complete standard-versus-selective example with its original sample values" do
    output = IO::Memory.new
    OE.run(output)
    output.to_s.should contain("Number of row groups: 3")
    output.to_s.should contain("Number of columns: 6")
    output.to_s.should contain("Column 4: has offset idx true, has column idx false")
    output.to_s.should contain("Column 5: has offset idx false, has column idx false")
    file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(OE.create_sample_file))
    batches = Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(file).with_batch_size(17).build.to_a
    batches.sum(&.num_rows).should eq 150
    offset = 0
    batches.each do |batch|
      values = (offset...offset + batch.num_rows).to_a
      batch.column(0).should eq Arrow::PrimitiveArray(Int32).from_values(values)
      batch.column(1).should eq Arrow::PrimitiveArray(Int32).from_values(values.map { |value| value * 2 })
      batch.column(3).should eq Arrow::PrimitiveArray(Int32).from_values(values.map { |value| value * 3 })
      batch.column(5).should eq Arrow::PrimitiveArray(Int32).from_values(values.map { |value| value * 4 })
      batch.column(2).should eq Arrow::StringArray.from_values(values.map { |value| "name#{value}" })
      batch.column(4).should eq Arrow::StringArray.from_values(values.map { |value| value.even? ? "even" : "odd" })
      offset += batch.num_rows
    end
  end
end
