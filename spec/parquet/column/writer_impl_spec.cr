require "../../spec_helper"

# Ported from `parquet/src/column/writer/mod.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Uses an in-memory `PageWriter` (upstream `get_test_page_writer`) and the
# `statistics_roundtrip`/`test_column_writer_check_metadata` patterns.

private alias Writer = Parquet::Column::Writer
private alias GW = Parquet::Column::Writer::GenericColumnWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias PType = Parquet::Type
private alias Enc = Parquet::Encoding

private class TestPageWriter < Parquet::Column::PageWriter
  getter pages = [] of Parquet::Column::CompressedPage
  getter bytes = 0_u64

  def write_page(page : Parquet::Column::CompressedPage) : Parquet::Column::PageWriteSpec
    spec = Parquet::Column::PageWriteSpec.new
    spec.page_type = page.page_type
    spec.uncompressed_size = page.uncompressed_size
    spec.compressed_size = page.compressed_size.to_i64
    spec.num_values = page.num_values
    spec.offset = @bytes
    spec.bytes_written = page.compressed_size.to_u64
    @bytes += page.compressed_size.to_u64
    @pages << page
    spec
  end

  def close : Nil
  end
end

private def writer_desc(physical : PType, max_def : Int16 = 0_i16, max_rep : Int16 = 0_i16, type_length : Int32 = -1) : Parquet::Schema::ColumnDescriptor
  ty = Parquet::Schema::Type.primitive_type_builder("col", physical).with_length(type_length).build
  Parquet::Schema::ColumnDescriptor.new(ty, max_def, max_rep, Parquet::Schema::ColumnPath.new("col"))
end

private def statistics_roundtrip(type : T.class, values : Array(T), descr : Parquet::Schema::ColumnDescriptor, props : WP = WP.default) : Parquet::Statistics? forall T
  page_writer = TestPageWriter.new
  writer = GW(T).new(descr, props, page_writer)
  writer.write_batch(values, nil, nil)
  writer.close.metadata.statistics
end

describe Parquet::Column::Writer::GenericColumnWriter do
  # Upstream `test_int32_statistics`.
  it "computes int32 chunk statistics" do
    descr = writer_desc(PType::INT32)
    stats = statistics_roundtrip(Int32, [1_i32, 2_i32, 3_i32, 4_i32, 5_i32], descr)
    stats.should_not be_nil
    value = stats.not_nil!.value.as(Parquet::ValueStatistics(Int32))
    value.min_opt.should eq(1)
    value.max_opt.should eq(5)
    value.null_count_opt.should eq(0_u64)
  end

  # Upstream `test_int64_statistics`.
  it "computes int64 chunk statistics" do
    descr = writer_desc(PType::INT64)
    stats = statistics_roundtrip(Int64, [3_i64, 1_i64, 2_i64], descr)
    value = stats.not_nil!.value.as(Parquet::ValueStatistics(Int64))
    value.min_opt.should eq(1_i64)
    value.max_opt.should eq(3_i64)
  end

  # Upstream `test_column_writer_check_metadata` (metadata shape).
  it "builds column metadata" do
    descr = writer_desc(PType::INT32)
    page_writer = TestPageWriter.new
    props = WP.builder.set_dictionary_enabled(false).build
    writer = GW(Int32).new(descr, props, page_writer)
    writer.write_batch([1_i32, 2_i32, 3_i32], nil, nil)
    result = writer.close
    result.rows_written.should eq(3_u64)
    result.metadata.num_values.should eq(3_i64)
    result.metadata.column_type.should eq(PType::INT32)
    page_writer.pages.size.should eq(1)
    page_writer.pages.first.page_type.should eq(Parquet::PageType::DATA_PAGE)
  end

  # Upstream `test_column_writer_dictionary_disabled_v1`.
  it "writes plainly when dictionary is disabled (v1)" do
    descr = writer_desc(PType::INT32)
    props = WP.builder.set_writer_version(Parquet::File::Properties::WriterVersion::PARQUET_1_0).set_dictionary_enabled(false).build
    page_writer = TestPageWriter.new
    writer = GW(Int32).new(descr, props, page_writer)
    writer.write_batch([10_i32, 20_i32, 10_i32], nil, nil)
    result = writer.close
    result.metadata.encodings.set?(Enc::PLAIN).should be_true
    result.column_index.should_not be_nil
  end
end
