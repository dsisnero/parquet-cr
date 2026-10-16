require "../../spec_helper"

# Ported from `parquet/src/file/properties.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: Rust `panic!`/`assert!` become Crystal exceptions
# (`expect_raises(Exception, "…")`); `Option<&BloomFilterProperties>` comparisons
# use `nil`/value directly; `WriterVersion`/`EnabledStatistics` `FromStr` errors
# are raised (not returned).

private alias WP = Parquet::File::Properties::WriterProperties
private alias BFP = Parquet::File::Properties::BloomFilterProperties
private alias CdcOptions = Parquet::File::Properties::CdcOptions
private alias ES = Parquet::File::Properties::EnabledStatistics
private alias WV = Parquet::File::Properties::WriterVersion
private alias ColPath = Parquet::Schema::ColumnPath
private alias KV = Parquet::File::Metadata::KeyValue
private alias Comp = Parquet::Compression
private alias Enc = Parquet::Encoding

private def default_stats : ES
  ES::Page
end

describe Parquet::File::Properties::WriterVersion do
  # Upstream `test_writer_version`.
  it "reports the numeric version" do
    WV::PARQUET_1_0.as_num.should eq(1)
    WV::PARQUET_2_0.as_num.should eq(2)
  end

  # Upstream `test_parse_writerversion`.
  it "parses from strings" do
    WV.from_str("PARQUET_1_0").should eq(WV::PARQUET_1_0)
    WV.from_str("PARQUET_2_0").should eq(WV::PARQUET_2_0)
    WV.from_str("parquet_1_0").should eq(WV::PARQUET_1_0)
    expect_raises(Exception, "Invalid writer version: PARQUET_-1_0") do
      WV.from_str("PARQUET_-1_0")
    end
  end
end

describe Parquet::File::Properties::EnabledStatistics do
  # Upstream `test_parse_enabledstatistics`.
  it "parses from strings" do
    ES.from_str("NONE").should eq(ES::None)
    ES.from_str("CHUNK").should eq(ES::Chunk)
    ES.from_str("PAGE").should eq(ES::Page)
    ES.from_str("none").should eq(ES::None)
    expect_raises(Exception, "Invalid statistics arg: ChunkAndPage") do
      ES.from_str("ChunkAndPage")
    end
  end

  it "defaults to page" do
    ES.default.should eq(ES::Page)
  end
end

describe Parquet::File::Properties::CdcOptions do
  # Upstream `test_cdc_options_equality`.
  it "compares by value" do
    opts = CdcOptions.default
    opts.should eq(CdcOptions.default)

    custom = CdcOptions.new(1024, 8192, 1)
    custom.should eq(custom)
    opts.should_not eq(custom)
  end
end

describe Parquet::File::Properties::BloomFilterProperties do
  # Upstream `test_bloom_filter_builder_default`.
  it "builds defaults" do
    props = BFP.builder.build
    props.fpp.should eq(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_FPP)
    props.ndv.should eq(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_NDV)
    props.should eq(BFP.default)
    BFP.builder.build.should eq(BFP.default)
  end

  it "builds explicit fpp" do
    props = BFP.builder.with_fpp(0.01).build
    props.fpp.should eq(0.01)
    props.ndv.should eq(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_NDV)
  end

  it "builds explicit ndv" do
    props = BFP.builder.with_max_ndv(1000_u64).build
    props.fpp.should eq(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_FPP)
    props.ndv.should eq(1000_u64)
  end

  it "validates fpp at build" do
    [0.0, 1.0, -0.5, 2.0].each do |wrong|
      expect_raises(Exception) { BFP.builder.with_fpp(wrong).build }
    end
  end

  it "validates fpp at try_build" do
    [0.0, 1.0, -0.5, 2.0].each do |wrong|
      expect_raises(Exception) { BFP.builder.with_fpp(wrong).try_build }
    end

    ok = BFP.builder.with_fpp(0.01).with_max_ndv(1000_u64).try_build
    ok.fpp.should eq(0.01)
    ok.ndv.should eq(1000_u64)
  end
end

describe Parquet::File::Properties::WriterProperties do
  # Upstream `test_writer_properties_default_settings`.
  it "has the documented defaults" do
    props = WP.default
    props.data_page_size_limit.should eq(Parquet::File::Properties::DEFAULT_PAGE_SIZE)
    props.dictionary_page_size_limit.should eq(Parquet::File::Properties::DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT)
    props.write_batch_size.should eq(Parquet::File::Properties::DEFAULT_WRITE_BATCH_SIZE)
    props.max_row_group_row_count.should eq(Parquet::File::Properties::DEFAULT_MAX_ROW_GROUP_ROW_COUNT)
    props.max_row_group_bytes.should be_nil
    props.bloom_filter_position.should eq(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_POSITION)
    props.writer_version.should eq(WV::PARQUET_1_0)
    props.created_by.should eq(Parquet::File::Properties::DEFAULT_CREATED_BY)
    props.key_value_metadata.should be_nil
    props.encoding(ColPath.from("col")).should be_nil
    props.compression(ColPath.from("col")).should eq(Comp::UNCOMPRESSED)
    props.dictionary_enabled(ColPath.from("col")).should eq(true)
    props.statistics_enabled(ColPath.from("col")).should eq(default_stats)
    props.bloom_filter_properties(ColPath.from("col")).should be_nil
  end

  # Upstream `test_writer_properties_dictionary_encoding`.
  it "keeps dictionary encodings fixed for both versions" do
    [WV::PARQUET_1_0, WV::PARQUET_2_0].each do |version|
      props = WP.builder.set_writer_version(version).build
      props.dictionary_page_encoding.should eq(Enc::PLAIN)
      props.dictionary_data_page_encoding.should eq(Enc::RLE_DICTIONARY)
    end
  end

  it "panics when plain dictionary is used as a fallback encoding" do
    expect_raises(Exception, "Dictionary encoding can not be used as fallback encoding") do
      WP.builder.set_encoding(Enc::PLAIN_DICTIONARY).build
    end
  end

  it "panics when rle dictionary is used as a fallback encoding" do
    expect_raises(Exception, "Dictionary encoding can not be used as fallback encoding") do
      WP.builder.set_encoding(Enc::RLE_DICTIONARY).build
    end
  end

  it "panics when dictionary is enabled and a column encoding is dictionary" do
    expect_raises(Exception, "Dictionary encoding can not be used as fallback encoding") do
      WP.builder.set_dictionary_enabled(true).set_column_encoding(ColPath.from("col"), Enc::RLE_DICTIONARY).build
    end
  end

  it "panics when dictionary is disabled and a column encoding is dictionary" do
    expect_raises(Exception, "Dictionary encoding can not be used as fallback encoding") do
      WP.builder.set_dictionary_enabled(false).set_column_encoding(ColPath.from("col"), Enc::RLE_DICTIONARY).build
    end
  end

  # Upstream `test_writer_properties_builder`.
  it "round-trips through the builder" do
    props = WP.builder
      .set_writer_version(WV::PARQUET_2_0)
      .set_data_page_size_limit(10)
      .set_dictionary_page_size_limit(20)
      .set_write_batch_size(30)
      .set_max_row_group_row_count(40)
      .set_created_by("default")
      .set_key_value_metadata([KV.new("key", "value")])
      .set_encoding(Enc::DELTA_BINARY_PACKED)
      .set_compression(Comp.gzip)
      .set_dictionary_enabled(false)
      .set_statistics_enabled(ES::None)
      .set_column_encoding(ColPath.from("col"), Enc::RLE)
      .set_column_compression(ColPath.from("col"), Comp::SNAPPY)
      .set_column_dictionary_enabled(ColPath.from("col"), true)
      .set_column_statistics_enabled(ColPath.from("col"), ES::Chunk)
      .set_column_bloom_filter_enabled(ColPath.from("col"), true)
      .set_column_bloom_filter_max_ndv(ColPath.from("col"), 100_u64)
      .set_column_bloom_filter_fpp(ColPath.from("col"), 0.1)
      .build

    check = ->(p : WP) do
      p.writer_version.should eq(WV::PARQUET_2_0)
      p.data_page_size_limit.should eq(10)
      p.dictionary_page_size_limit.should eq(20)
      p.write_batch_size.should eq(30)
      p.max_row_group_row_count.should eq(40)
      p.created_by.should eq("default")
      p.key_value_metadata.should eq([KV.new("key", "value")])

      p.encoding(ColPath.from("a")).should eq(Enc::DELTA_BINARY_PACKED)
      p.compression(ColPath.from("a")).should eq(Comp.gzip)
      p.dictionary_enabled(ColPath.from("a")).should eq(false)
      p.statistics_enabled(ColPath.from("a")).should eq(ES::None)

      p.encoding(ColPath.from("col")).should eq(Enc::RLE)
      p.compression(ColPath.from("col")).should eq(Comp::SNAPPY)
      p.dictionary_enabled(ColPath.from("col")).should eq(true)
      p.statistics_enabled(ColPath.from("col")).should eq(ES::Chunk)
      p.bloom_filter_properties(ColPath.from("col")).should eq(BFP.new(0.1, 100_u64))
    end

    check.call(props)
    check.call(props.into_builder.build)
  end

  # Upstream `test_writer_properties_builder_partial_defaults`.
  it "falls back to defaults for unset per-column settings" do
    props = WP.builder
      .set_encoding(Enc::DELTA_BINARY_PACKED)
      .set_compression(Comp.gzip)
      .set_bloom_filter_enabled(true)
      .set_column_encoding(ColPath.from("col"), Enc::RLE)
      .build

    props.encoding(ColPath.from("col")).should eq(Enc::RLE)
    props.compression(ColPath.from("col")).should eq(Comp.gzip)
    props.dictionary_enabled(ColPath.from("col")).should eq(Parquet::File::Properties::DEFAULT_DICTIONARY_ENABLED)
    props.bloom_filter_properties(ColPath.from("col")).should eq(
      BFP.new(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_FPP, Parquet::File::Properties::DEFAULT_BLOOM_FILTER_NDV)
    )
  end

  it "panics on zero row group row count" do
    expect_raises(Exception, "Cannot have a 0 max row group row count") do
      WP.builder.set_max_row_group_row_count(0)
    end
  end

  it "panics on zero row group bytes" do
    expect_raises(Exception, "Cannot have a 0 max row group bytes") do
      WP.builder.set_max_row_group_bytes(0)
    end
  end

  it "panics on zero write batch size" do
    expect_raises(Exception, "Cannot have a 0 write batch size") do
      WP.builder.set_write_batch_size(0)
    end
  end

  it "panics on zero data page row count limit" do
    expect_raises(Exception, "Cannot have a 0 data page row count limit") do
      WP.builder.set_data_page_row_count_limit(0)
    end
  end

  # Upstream `test_writer_properties_bloom_filter_ndv_fpp_set`.
  it "resolves bloom filter ndv/fpp" do
    WP.builder.build.bloom_filter_properties(ColPath.from("col")).should be_nil
    WP.builder.set_bloom_filter_max_ndv(100_u64).build
      .bloom_filter_properties(ColPath.from("col"))
      .should eq(BFP.new(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_FPP, 100_u64))
    WP.builder.set_bloom_filter_fpp(0.1).build
      .bloom_filter_properties(ColPath.from("col"))
      .should eq(BFP.new(0.1, Parquet::File::Properties::DEFAULT_BLOOM_FILTER_NDV))
  end

  # Upstream `test_writer_properties_column_data_page_v2_compression_ratio_threshold`.
  it "resolves the data page v2 compression ratio threshold" do
    props = WP.builder
      .set_data_page_v2_compression_ratio_threshold(0.5)
      .set_column_data_page_v2_compression_ratio_threshold(ColPath.from("col"), 0.1)
      .build

    props.data_page_v2_compression_ratio_threshold.should eq(0.5)
    props.column_data_page_v2_compression_ratio_threshold(ColPath.from("col")).should eq(0.1)
    props.column_data_page_v2_compression_ratio_threshold(ColPath.from("other")).should eq(0.5)
  end

  it "panics on an invalid data page v2 compression ratio threshold" do
    expect_raises(Exception, "data_page_v2_compression_ratio_threshold must be a positive finite number") do
      WP.builder.set_data_page_v2_compression_ratio_threshold(0.0).build
    end
  end

  # Upstream `test_writer_properties_deprecated_bloom_filter_ndv_setters_still_work`.
  it "supports the deprecated bloom filter ndv setters" do
    col = ColPath.from("col")
    props = WP.builder
      .set_bloom_filter_ndv(100_u64)
      .set_column_bloom_filter_ndv(col, 200_u64)
      .build
    props.bloom_filter_properties(ColPath.from("other")).should eq(
      BFP.new(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_FPP, 100_u64)
    )
    props.bloom_filter_properties(col).should eq(
      BFP.new(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_FPP, 200_u64)
    )
  end

  # Upstream `test_writer_properties_column_dictionary_page_size_limit`.
  it "resolves per-column dictionary page size limits" do
    props = WP.builder
      .set_dictionary_page_size_limit(100)
      .set_column_dictionary_page_size_limit(ColPath.from("col"), 10)
      .build

    props.dictionary_page_size_limit.should eq(100)
    props.column_dictionary_page_size_limit(ColPath.from("col")).should eq(10)
    props.column_dictionary_page_size_limit(ColPath.from("other")).should eq(100)
  end

  # Upstream `test_writer_properties_column_data_page_size_limit`.
  it "resolves per-column data page size limits" do
    props = WP.builder
      .set_data_page_size_limit(100)
      .set_column_data_page_size_limit(ColPath.from("col"), 10)
      .build

    props.data_page_size_limit.should eq(100)
    props.column_data_page_size_limit(ColPath.from("col")).should eq(10)
    props.column_data_page_size_limit(ColPath.from("other")).should eq(100)
  end

  # Upstream `test_column_specific_implicit_ndv_uses_row_group_size`.
  it "uses the row group size for implicit per-column ndv" do
    col = ColPath.from("col")
    props = WP.builder
      .set_max_row_group_row_count(7777)
      .set_column_bloom_filter_enabled(col, true)
      .build
    bf = props.bloom_filter_properties(col)
    bf.should_not be_nil
    bf = bf.not_nil!
    bf.ndv.should eq(7777_u64)
    bf.fpp.should eq(Parquet::File::Properties::DEFAULT_BLOOM_FILTER_FPP)
  end

  # Upstream `test_set_bloom_filter_properties_applied_globally`.
  it "applies bloom filter properties globally" do
    bf = BFP.builder.with_fpp(0.01).with_max_ndv(500_u64).build
    props = WP.builder.set_bloom_filter_properties(bf).build
    props.bloom_filter_properties(ColPath.from("a")).should eq(bf)
    props.bloom_filter_properties(ColPath.from("b")).should eq(bf)
  end

  # Upstream `test_set_column_bloom_filter_properties_overrides_global`.
  it "lets a per-column bloom filter override the global one" do
    global = BFP.builder.with_fpp(0.01).with_max_ndv(500_u64).build
    tailored = BFP.builder.with_fpp(0.02).with_max_ndv(1000_u64).build
    col = ColPath.from("col")
    props = WP.builder
      .set_bloom_filter_properties(global)
      .set_column_bloom_filter_properties(col, tailored)
      .build
    props.bloom_filter_properties(col).should eq(tailored)
    props.bloom_filter_properties(ColPath.from("other")).should eq(global)
  end

  # Upstream `test_set_bloom_filter_properties_preserve_explicit_ndv`.
  it "preserves an explicit ndv over the row-group fallback" do
    bf = BFP.builder.with_max_ndv(42_u64).build
    props = WP.builder
      .set_max_row_group_row_count(99_999)
      .set_bloom_filter_properties(bf)
      .build
    props.bloom_filter_properties(ColPath.from("col")).not_nil!.ndv.should eq(42_u64)
  end

  # Upstream `test_resolve_column_properties_matches_individual_accessors`.
  it "resolves all per-column settings in one pass" do
    overridden = ColPath.from("overridden")
    inherited = ColPath.from("inherited")

    props = WP.builder
      .set_encoding(Enc::DELTA_BINARY_PACKED)
      .set_compression(Comp::SNAPPY)
      .set_dictionary_enabled(false)
      .set_statistics_enabled(ES::Chunk)
      .set_write_page_header_statistics(false)
      .set_data_page_size_limit(1111)
      .set_dictionary_page_size_limit(2222)
      .set_data_page_v2_compression_ratio_threshold(0.25)
      .set_bloom_filter_enabled(true)
      .set_column_encoding(overridden, Enc::PLAIN)
      .set_column_compression(overridden, Comp::UNCOMPRESSED)
      .set_column_dictionary_enabled(overridden, true)
      .set_column_statistics_enabled(overridden, ES::Page)
      .set_column_write_page_header_statistics(overridden, true)
      .set_column_data_page_size_limit(overridden, 3333)
      .set_column_dictionary_page_size_limit(overridden, 4444)
      .set_column_data_page_v2_compression_ratio_threshold(overridden, 0.75)
      .set_column_bloom_filter_fpp(overridden, 0.5)
      .build

    bare = WP.builder.build

    [{props, overridden}, {props, inherited}, {bare, inherited}].each do |(p, col)|
      resolved = p.resolve_column_properties(col)
      resolved.encoding.should eq(p.encoding(col))
      resolved.compression.should eq(p.compression(col))
      resolved.dictionary_enabled.should eq(p.dictionary_enabled(col))
      resolved.statistics_enabled.should eq(p.statistics_enabled(col))
      resolved.write_page_header_statistics.should eq(p.write_page_header_statistics(col))
      resolved.data_page_size_limit.should eq(p.column_data_page_size_limit(col))
      resolved.dictionary_page_size_limit.should eq(p.column_dictionary_page_size_limit(col))
      resolved.data_page_v2_compression_ratio_threshold.should eq(p.column_data_page_v2_compression_ratio_threshold(col))
      resolved.bloom_filter_properties.should eq(p.bloom_filter_properties(col))
    end
  end
end

describe Parquet::File::Properties::ReaderProperties do
  # Upstream `test_reader_properties_default_settings`.
  it "has default codec options" do
    props = Parquet::File::Properties::ReaderProperties.builder.build
    codec_options = Parquet::CodecOptionsBuilder.default.set_backward_compatible_lz4(true).build
    props.codec_options.should eq(codec_options)
    props.read_bloom_filter.should eq(false)
  end

  # Upstream `test_reader_properties_builder`.
  it "builds with custom codec options" do
    props = Parquet::File::Properties::ReaderProperties.builder.set_backward_compatible_lz4(false).build
    codec_options = Parquet::CodecOptionsBuilder.default.set_backward_compatible_lz4(false).build
    props.codec_options.should eq(codec_options)
  end
end
