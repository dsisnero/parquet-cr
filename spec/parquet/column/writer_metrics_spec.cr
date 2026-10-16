require "../../spec_helper"

# Ported from `parquet/src/column/writer/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `test_level_data_ref_value_count` is ported; `ColumnCloseResult#update_dictionary_location`
# is characterized.

private alias Writer = Parquet::Column::Writer
private alias LevelDataRef = Parquet::Column::Writer::LevelDataRef
private alias ColumnCloseResult = Parquet::Column::Writer::ColumnCloseResult
private alias PMeta = Parquet::File::Metadata

private def metrics_desc : Parquet::Schema::ColumnDescriptor
  ty = Parquet::Schema::Type.primitive_type_builder("col", Parquet::Type::INT32).with_length(-1).build
  Parquet::Schema::ColumnDescriptor.new(ty, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new("col"))
end

describe Parquet::Column::Writer::LevelDataRef do
  # Upstream `test_level_data_ref_value_count`.
  it "counts values for every column shape" do
    max_def = 2_i16
    LevelDataRef.absent.value_count(64, max_def).should eq(64)
    LevelDataRef.uniform(max_def, 40).value_count(40, max_def).should eq(40)
    LevelDataRef.uniform((max_def - 1).to_i16, 40).value_count(40, max_def).should eq(0)

    levels = [2_i16, 0_i16, 2_i16, 1_i16, 2_i16, 2_i16, 0_i16]
    LevelDataRef.materialized(levels).value_count(levels.size, max_def).should eq(4)
  end

  it "reports length, first and slices" do
    LevelDataRef.absent.len.should eq(0)
    LevelDataRef.absent.first.should be_nil

    LevelDataRef.uniform(3_i16, 5).len.should eq(5)
    LevelDataRef.uniform(3_i16, 5).first.should eq(3_i16)
    LevelDataRef.uniform(3_i16, 0).first.should be_nil
    LevelDataRef.uniform(3_i16, 5).slice(1, 2).len.should eq(2)

    levels = [1_i16, 2_i16, 3_i16]
    LevelDataRef.materialized(levels).first.should eq(1_i16)
    sliced = LevelDataRef.materialized(levels).slice(1, 2)
    sliced.len.should eq(2)
    sliced.first.should eq(2_i16)
  end
end

describe Parquet::Column::Writer::ColumnCloseResult do
  it "rewrites page offsets for a dictionary-first layout" do
    metadata = PMeta::ColumnChunkMetaData.builder(metrics_desc)
      .set_data_page_offset(100_i64)
      .build
    locations = [
      Parquet::File::PageIndex::PageLocation.new(100_i64, 20_i32, 0_i64),
      Parquet::File::PageIndex::PageLocation.new(120_i64, 25_i32, 4_i64),
    ]
    offset_index = PMeta::OffsetIndexMetaData.new(locations, nil)

    result = ColumnCloseResult.new(0_u64, 0_u64, metadata, nil, nil, offset_index)
    result.update_dictionary_location(16)

    result.metadata.dictionary_page_offset.should eq(0_i64)
    result.metadata.data_page_offset.should eq(16_i64)
    result.offset_index.not_nil!.page_locations[0].offset.should eq(16_i64)
    result.offset_index.not_nil!.page_locations[1].offset.should eq(36_i64)
  end

  it "leaves offsets unchanged with no dictionary page" do
    metadata = PMeta::ColumnChunkMetaData.builder(metrics_desc)
      .set_data_page_offset(100_i64)
      .build
    result = ColumnCloseResult.new(0_u64, 0_u64, metadata)
    result.update_dictionary_location(0)
    result.metadata.data_page_offset.should eq(100_i64)
    result.metadata.dictionary_page_offset.should be_nil
  end
end
