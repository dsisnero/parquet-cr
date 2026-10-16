require "../../spec_helper"

# Characterization tests for the `ByteBudgetChunker` ported from
# `parquet/src/column/writer/byte_budget_chunker.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
# The upstream module has no unit tests; these exercise `pick_sub_batch`.

private alias Writer = Parquet::Column::Writer
private alias Chunker = Parquet::Column::Writer::ByteBudgetChunker
private alias SubBatchStrategy = Parquet::Column::Writer::SubBatchStrategy
private alias LevelDataRef = Parquet::Column::Writer::LevelDataRef
private alias CVE = Parquet::Column::Writer::ColumnValueEncoderImpl
private alias RCP = Parquet::File::Properties::ResolvedColumnProperties
private alias PType = Parquet::Type
private alias PByteArray = Parquet::ByteArray

private def chunker_desc(physical : PType, max_def : Int16, type_length : Int32 = -1) : Parquet::Schema::ColumnDescriptor
  ty = Parquet::Schema::Type.primitive_type_builder("col", physical).with_length(type_length).build
  Parquet::Schema::ColumnDescriptor.new(ty, max_def, 0_i16, Parquet::Schema::ColumnPath.new("col"))
end

private def chunker_props(data_page_size_limit : Int32, dict_page_size_limit : Int32, dictionary_enabled : Bool) : RCP
  RCP.new(nil, Parquet::Compression::UNCOMPRESSED, dictionary_enabled, Parquet::File::Properties::EnabledStatistics::None, false, data_page_size_limit, dict_page_size_limit, 1.0, nil)
end

describe Parquet::Column::Writer::ByteBudgetChunker do
  it "short-circuits fixed-width columns that always fit" do
    descr = chunker_desc(PType::INT32, 0_i16)
    props = chunker_props(1024, 1024, false)
    chunker = Chunker.new(descr, props, 128)
    encoder = CVE(Int32).try_new(descr, Parquet::File::Properties::WriterProperties.default, props)

    chunker.pick_sub_batch(Int32, encoder, [1_i32, 2_i32], nil, LevelDataRef.absent, 0, 2).should be_nil
  end

  it "sub-batches large variable-width values" do
    descr = chunker_desc(PType::BYTE_ARRAY, 0_i16)
    props = chunker_props(8, 8, false)
    chunker = Chunker.new(descr, props, 128)
    encoder = CVE(PByteArray).try_new(descr, Parquet::File::Properties::WriterProperties.default, props)

    values = [PByteArray.new("aaaaaaaaaa"), PByteArray.new("bbbbbbbbbb")]
    strategy = chunker.pick_sub_batch(PByteArray, encoder, values, nil, LevelDataRef.absent, 0, 2)
    strategy.should_not be_nil
    strategy = strategy.not_nil!
    strategy.kind.should eq(SubBatchStrategy::Kind::Levels)
    strategy.size.should eq(1)
  end

  it "returns nil when the whole chunk fits the budget" do
    descr = chunker_desc(PType::BYTE_ARRAY, 0_i16)
    props = chunker_props(1024, 1024, false)
    chunker = Chunker.new(descr, props, 128)
    encoder = CVE(PByteArray).try_new(descr, Parquet::File::Properties::WriterProperties.default, props)

    values = [PByteArray.new("a"), PByteArray.new("b")]
    chunker.pick_sub_batch(PByteArray, encoder, values, nil, LevelDataRef.absent, 0, 2).should be_nil
  end
end
