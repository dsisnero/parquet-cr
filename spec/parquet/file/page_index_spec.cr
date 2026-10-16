require "../../spec_helper"

# Ported from `parquet/src/file/metadata/page_index.rs` and the
# `ColumnIndexBuilder`/`OffsetIndexBuilder` implementations in
# `parquet/src/file/metadata/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream `test_page_index_builder_partial_population`,
# `test_page_index_builder_empty_to_none`, and `test_rebuild_page_index` are
# ported directly. The builder value tests characterize `ColumnIndexBuilder` and
# `OffsetIndexBuilder`, which upstream covers through writer integration tests.

private def int32_bytes(value : Int32) : Bytes
  Parquet::FromBytes.to_le_slice(value)
end

describe Parquet::File::Metadata::ColumnIndexBuilder do
  it "builds a primitive column index" do
    builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    builder.append(false, int32_bytes(-123), int32_bytes(234), 0_i64, nil)
    builder.set_boundary_order(Parquet::BoundaryOrder::ASCENDING)

    meta = builder.build

    meta.kind.should eq(Parquet::Type::INT32)
    meta.boundary_order.should eq(Parquet::BoundaryOrder::ASCENDING)
    index = meta.inner.as(PPI::PrimitiveColumnIndex(Int32))
    index.min_value(0).should eq(-123)
    index.max_value(0).should eq(234)
    index.null_count(0).should eq(0_i64)
  end

  it "builds a byte array column index" do
    builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::BYTE_ARRAY)
    builder.append(false, "Hello".to_slice, "today".to_slice, 0_i64, nil)

    meta = builder.build

    meta.kind.should eq(Parquet::Type::BYTE_ARRAY)
    index = meta.inner.as(PPI::ByteArrayColumnIndex)
    index.min_value(0).should eq("Hello".to_slice)
    index.max_value(0).should eq("today".to_slice)
  end

  it "collects nan counts for floating point columns only" do
    float = PMeta::ColumnIndexBuilder.new(Parquet::Type::FLOAT)
    float.append(false, Parquet::FromBytes.to_le_slice(1.0_f32), Parquet::FromBytes.to_le_slice(2.0_f32), 0_i64, 3_i64)
    float.append(false, Parquet::FromBytes.to_le_slice(3.0_f32), Parquet::FromBytes.to_le_slice(4.0_f32), 0_i64, 4_i64)
    float_meta = float.build
    float_meta.inner.as(PPI::PrimitiveColumnIndex(Float32)).nan_count(0).should eq(3_i64)
    float_meta.inner.as(PPI::PrimitiveColumnIndex(Float32)).nan_count(1).should eq(4_i64)

    int = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    int.append(false, int32_bytes(1), int32_bytes(2), 0_i64, nil)
    int_meta = int.build
    int_meta.inner.as(PPI::PrimitiveColumnIndex(Int32)).nan_counts.should be_nil
  end

  it "concatenates page-level histograms" do
    builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    builder.append(false, int32_bytes(1), int32_bytes(2), 0_i64, nil)
    builder.append_histograms(
      Parquet::File::Metadata::LevelHistogram.new([1_i64, 2_i64]),
      Parquet::File::Metadata::LevelHistogram.new([7_i64, 8_i64]),
    )
    builder.append(false, int32_bytes(3), int32_bytes(4), 0_i64, nil)
    builder.append_histograms(
      Parquet::File::Metadata::LevelHistogram.new([3_i64, 4_i64]),
      Parquet::File::Metadata::LevelHistogram.new([9_i64, 10_i64]),
    )

    meta = builder.build
    index = meta.inner.as(PPI::PrimitiveColumnIndex(Int32))
    index.repetition_level_histogram(0).should eq([1_i64, 2_i64])
    index.repetition_level_histogram(1).should eq([3_i64, 4_i64])
    index.definition_level_histogram(1).should eq([9_i64, 10_i64])
  end

  it "ignores histograms once marked invalid" do
    builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    builder.append(false, int32_bytes(1), int32_bytes(2), 0_i64, nil)
    builder.to_invalid
    builder.valid?.should be_false
    builder.append_histograms(
      Parquet::File::Metadata::LevelHistogram.new([1_i64]),
      Parquet::File::Metadata::LevelHistogram.new([1_i64]),
    )

    meta = builder.build
    index = meta.inner.as(PPI::PrimitiveColumnIndex(Int32))
    index.repetition_level_histogram(0).should be_nil
    index.definition_level_histogram(0).should be_nil
  end
end

describe Parquet::File::Metadata::OffsetIndexBuilder do
  it "builds page locations and unencoded data bytes" do
    builder = PMeta::OffsetIndexBuilder.new
    builder.append_row_count(50_i64)
    builder.append_offset_and_size(1000_i64, 500)
    builder.append_row_count(25_i64)
    builder.append_offset_and_size(1500_i64, 250)
    builder.append_unencoded_byte_array_data_bytes(4000_i64)
    builder.append_unencoded_byte_array_data_bytes(2000_i64)

    index = builder.build

    index.page_locations.size.should eq(2)
    index.page_locations[0].should eq(PPI::PageLocation.new(1000_i64, 500, 0_i64))
    index.page_locations[1].should eq(PPI::PageLocation.new(1500_i64, 250, 50_i64))
    index.unencoded_byte_array_data_bytes.should eq([4000_i64, 2000_i64])
  end

  it "leaves unencoded data bytes nil when never appended" do
    builder = PMeta::OffsetIndexBuilder.new
    builder.append_row_count(10_i64)
    builder.append_offset_and_size(100_i64, 20)

    builder.build.unencoded_byte_array_data_bytes.should be_nil
  end
end

describe Parquet::File::Metadata::PageIndexBuilder do
  # Upstream `test_page_index_builder_partial_population`.
  it "builds a page index with only offset indexes populated" do
    builder = PMeta::PageIndexBuilder.new(1, 1)

    offset_builder = PMeta::OffsetIndexBuilder.new
    offset_builder.append_row_count(50_i64)
    offset_builder.append_offset_and_size(1000_i64, 500)
    builder.put_offset_index(offset_builder.build, 0, 0)

    page_index = builder.build

    page_index.column_indexes?.should be_false
    page_index.offset_indexes?.should be_true
  end

  # Upstream `test_page_index_builder_empty_to_none`.
  it "converts unpopulated indexes to none" do
    builder = PMeta::PageIndexBuilder.new(2, 2)

    page_index = builder.build

    page_index.column_indexes?.should be_false
    page_index.offset_indexes?.should be_false
  end

  # Upstream `test_rebuild_page_index`.
  it "rebuilds a partially populated page index" do
    builder = PMeta::PageIndexBuilder.new(1, 2)

    col_builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    col_builder.append(false, int32_bytes(1), int32_bytes(100), 10_i64, nil)
    col_idx = col_builder.build
    builder.put_column_index(col_idx, 0, 0)

    offset_builder = PMeta::OffsetIndexBuilder.new
    offset_builder.append_row_count(50_i64)
    offset_builder.append_offset_and_size(1000_i64, 500)
    offset_idx = offset_builder.build
    builder.put_offset_index(offset_idx, 0, 0)

    page_index = builder.build

    page_index.complete?.should be_true
    page_index.column_index(0, 0).should_not be_nil
    page_index.offset_index(0, 0).should_not be_nil
    page_index.column_index(0, 1).should be_nil
    page_index.offset_index(0, 1).should be_nil

    builder = PMeta::PageIndexBuilder.new_from(page_index)
    builder.put_column_index(col_idx, 0, 1)
    builder.put_offset_index(offset_idx, 0, 1)
    page_index = builder.build

    page_index.column_index(0, 0).should_not be_nil
    page_index.offset_index(0, 0).should_not be_nil
    page_index.column_index(0, 1).should_not be_nil
    page_index.offset_index(0, 1).should_not be_nil
  end

  it "allocates indexes on demand via allocate_*" do
    builder = PMeta::PageIndexBuilder.new(0, 0)
    builder.allocate_column_indexes(1, 1)
    builder.allocate_offset_indexes(1, 1)

    col_builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    col_builder.append(false, int32_bytes(1), int32_bytes(2), 0_i64, nil)
    builder.put_column_index(col_builder.build, 0, 0)
    offset_builder = PMeta::OffsetIndexBuilder.new
    offset_builder.append_row_count(1_i64)
    offset_builder.append_offset_and_size(4_i64, 100)
    builder.put_offset_index(offset_builder.build, 0, 0)

    page_index = builder.build
    page_index.column_indexes?.should be_true
    page_index.offset_indexes?.should be_true
  end

  it "ignores out-of-bounds puts" do
    builder = PMeta::PageIndexBuilder.new(1, 1)
    col_builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    col_builder.append(false, int32_bytes(1), int32_bytes(2), 0_i64, nil)
    builder.put_column_index(col_builder.build, 5, 5)

    builder.build.column_indexes?.should be_false
  end

  it "converts to a builder with into_builder" do
    builder = PMeta::PageIndexBuilder.new(1, 1)
    offset_builder = PMeta::OffsetIndexBuilder.new
    offset_builder.append_row_count(1_i64)
    offset_builder.append_offset_and_size(4_i64, 100)
    page_index = builder.tap(&.put_offset_index(offset_builder.build, 0, 0)).build

    rebuilt = page_index.into_builder.build
    rebuilt.offset_indexes?.should be_true
    rebuilt.offset_index(0, 0).should eq(page_index.offset_index(0, 0))
  end
end

describe Parquet::File::Metadata::RowGroupPageIndex do
  it "applies the row group index to a provider" do
    builder = PMeta::PageIndexBuilder.new(1, 2)
    col_builder = PMeta::ColumnIndexBuilder.new(Parquet::Type::INT32)
    col_builder.append(false, int32_bytes(1), int32_bytes(100), 0_i64, nil)
    builder.put_column_index(col_builder.build, 0, 0)
    offset_builder = PMeta::OffsetIndexBuilder.new
    offset_builder.append_row_count(50_i64)
    offset_builder.append_offset_and_size(1000_i64, 500)
    builder.put_offset_index(offset_builder.build, 0, 0)
    page_index = builder.build

    row_group = PMeta::RowGroupPageIndex.new(0, page_index)

    row_group.column_index(0).should_not be_nil
    row_group.offset_index(0).should_not be_nil
    row_group.column_index(1).should be_nil
    row_group.offset_index(1).should be_nil
    row_group.num_data_pages(0).should eq(1)
    row_group.page_locations(0).should eq([PPI::PageLocation.new(1000_i64, 500, 0_i64)])
  end

  it "returns nil accessors when no provider is present" do
    row_group = PMeta::RowGroupPageIndex.new(0, nil)
    row_group.column_index(0).should be_nil
    row_group.offset_index(0).should be_nil
    row_group.num_data_pages(0).should be_nil
    row_group.page_locations(0).should be_nil
  end
end
