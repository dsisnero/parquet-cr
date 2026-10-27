require "../../spec_helper"

# Characterizations of parquet/src/bin/parquet-rewrite.rs (no upstream unit
# tests), plus the WriterProperties tests behind its options. arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b. Ported before implementation.
private alias Rewrite = Parquet::Bin::ParquetRewrite
private alias RP = Parquet::File::Properties

private def rewrite_args(*extra : String) : Rewrite::Args
  rewrite_args(extra.to_a)
end

private def rewrite_args(extra : Array(String)) : Rewrite::Args
  Rewrite::Args.parse(["-i", "input.parquet", "-o", "output.parquet"] + extra)
end

private def rewrite_args : Rewrite::Args
  rewrite_args([] of String)
end

private def rewrite_column : Parquet::Schema::ColumnPath
  Parquet::Schema::ColumnPath.new(["col"])
end

describe "Parquet rewrite arguments and properties" do
  it "requires input/output and preserves writer defaults when options are absent" do
    expect_raises(ArgumentError, "input") { Rewrite::Args.parse([] of String) }
    expect_raises(ArgumentError, "output") { Rewrite::Args.parse(["-i", "input"]) }
    args = rewrite_args
    args.input.should eq "input.parquet"
    args.output.should eq "output.parquet"
    props = args.writer_properties
    props.compression(rewrite_column).should eq Parquet::Compression::UNCOMPRESSED
    props.dictionary_enabled(rewrite_column).should be_true
    props.statistics_enabled(rewrite_column).should eq RP::EnabledStatistics::Page
    props.writer_version.should eq RP::WriterVersion::PARQUET_1_0
    props.max_row_group_row_count.should eq RP::DEFAULT_MAX_ROW_GROUP_ROW_COUNT
  end

  it "maps every compression spelling and its default level" do
    {
      "none" => Parquet::Compression::UNCOMPRESSED, "snappy" => Parquet::Compression::SNAPPY,
      "gzip" => Parquet::Compression.gzip, "lzo" => Parquet::Compression::LZO,
      "brotli" => Parquet::Compression.brotli, "lz4" => Parquet::Compression::LZ4,
      "zstd" => Parquet::Compression.zstd, "lz4-raw" => Parquet::Compression::LZ4_RAW,
    }.each do |name, compression|
      rewrite_args("--compression", name).writer_properties.compression(rewrite_column).should eq compression
    end
    [{"gzip", 9}, {"brotli", 11}, {"zstd", 22}].each do |name, level|
      rewrite_args("--compression", name, "--compression-level", level.to_s).writer_properties.compression(rewrite_column).level.should eq level
    end
  end

  it "validates selected codec levels but ignores an unused compression level" do
    [{"gzip", "10"}, {"brotli", "12"}, {"zstd", "23"}].each do |name, level|
      expect_raises(Parquet::GeneralError) { rewrite_args("--compression", name, "--compression-level", level).writer_properties }
    end
    rewrite_args("--compression-level", "999").writer_properties.compression(rewrite_column).should eq Parquet::Compression::UNCOMPRESSED
    rewrite_args("--compression", "snappy", "--compression-level", "999").writer_properties.compression(rewrite_column).should eq Parquet::Compression::SNAPPY
    # Rust casts the u32 level to i32 for Zstd, preserving its wrapping bits.
    rewrite_args("--compression", "zstd", "--compression-level", "4294967295").writer_properties.compression(rewrite_column).should eq Parquet::Compression.zstd(-1)
  end

  it "maps all fallback encodings and rejects dictionary encodings as fallbacks" do
    {"plain" => Parquet::Encoding::PLAIN, "rle" => Parquet::Encoding::RLE,
     "bit-packed" => Parquet::Encoding::BIT_PACKED, "delta-binary-packed" => Parquet::Encoding::DELTA_BINARY_PACKED,
     "delta-length-byte-array" => Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
     "delta-byte-array" => Parquet::Encoding::DELTA_BYTE_ARRAY,
     "byte-stream-split" => Parquet::Encoding::BYTE_STREAM_SPLIT, "alp" => Parquet::Encoding::ALP}.each do |name, encoding|
      rewrite_args("--encoding", name).writer_properties.encoding(rewrite_column).should eq encoding
    end
    ["plain-dictionary", "rle-dictionary"].each do |name|
      expect_raises(Exception, "Dictionary encoding can not be used") { rewrite_args("--encoding", name).writer_properties }
    end
  end

  it "applies explicit true and false values without treating false as absent" do
    ["true", "false"].each do |text|
      props = rewrite_args("--dictionary-enabled", text, "--write-path-in-schema", text, "--coerce-types", text).writer_properties
      props.dictionary_enabled(rewrite_column).should eq(text == "true")
      props.write_path_in_schema.should eq(text == "true")
      props.coerce_types.should eq(text == "true")
    end
  end

  it "applies size, batch and truncation options to the actual writer properties" do
    props = rewrite_args("--dictionary-page-size-limit", "8192", "--max-row-group-size", "37",
      "--data-page-row-count-limit", "11", "--data-page-size-limit", "4096",
      "--statistics-truncate-length", "8", "--column-index-truncate-length", "9", "--write-batch-size", "7").writer_properties
    props.dictionary_page_size_limit.should eq 8192
    props.max_row_group_row_count.should eq 37
    props.data_page_row_count_limit.should eq 11
    props.data_page_size_limit.should eq 4096
    props.statistics_truncate_length.should eq 8
    props.column_index_truncate_length.should eq 9
    props.write_batch_size.should eq 7
  end

  it "keeps page-header statistics precedence independent of argument order" do
    [true, false].each do |reverse|
      extra = ["--statistics-enabled", "none", "--write-page-header-statistics", "true"]
      extra = extra[2, 2] + extra[0, 2] if reverse
      props = rewrite_args(extra).writer_properties
      props.statistics_enabled(rewrite_column).should eq RP::EnabledStatistics::Page
      props.write_page_header_statistics(rewrite_column).should be_true
    end
    props = rewrite_args("--statistics-enabled", "none", "--write-page-header-statistics", "false").writer_properties
    props.statistics_enabled(rewrite_column).should eq RP::EnabledStatistics::None
    props.write_page_header_statistics(rewrite_column).should be_false
    ["none", "chunk", "page"].each do |name|
      rewrite_args("--statistics-enabled", name).writer_properties.statistics_enabled(rewrite_column).should eq RP::EnabledStatistics.from_str(name)
    end
  end

  it "applies bloom suboptions only under explicit enablement" do
    [nil, "false"].each do |enabled|
      extra = ["--bloom-filter-fpp", "2", "--bloom-filter-ndv", "20", "--bloom-filter-position", "end"]
      extra += ["--bloom-filter-enabled", enabled] if enabled
      props = rewrite_args(extra).writer_properties
      props.bloom_filter_properties(rewrite_column).should be_nil
      props.bloom_filter_position.should eq RP::BloomFilterPosition::AfterRowGroup
    end
    props = rewrite_args("--bloom-filter-enabled", "true", "--bloom-filter-fpp", "0.01", "--bloom-filter-ndv", "20", "--bloom-filter-position", "end").writer_properties
    bloom = props.bloom_filter_properties(rewrite_column) || raise "Missing bloom properties"
    bloom.fpp.should eq 0.01
    bloom.ndv.should eq 20
    props.bloom_filter_position.should eq RP::BloomFilterPosition::End
    rewrite_args("--bloom-filter-enabled", "true", "--bloom-filter-position", "after-row-group").writer_properties.bloom_filter_position.should eq RP::BloomFilterPosition::AfterRowGroup
    expect_raises(Exception) { rewrite_args("--bloom-filter-enabled", "true", "--bloom-filter-fpp", "2").writer_properties }
  end

  it "accepts only upstream writer versions 1.0 and 2.0" do
    rewrite_args("--writer-version", "1.0").writer_properties.writer_version.should eq RP::WriterVersion::PARQUET_1_0
    rewrite_args("--writer-version", "2.0").writer_properties.writer_version.should eq RP::WriterVersion::PARQUET_2_0
    expect_raises(ArgumentError) { rewrite_args("--writer-version", "parquet_2_0") }
  end

  it "rejects invalid syntax, enum spellings, typed values and repeated options" do
    [
      ["--unknown", "x"], ["--compression", "GZIP"], ["--compression", "lz4_raw"],
      ["--encoding", "delta_binary_packed"], ["--statistics-enabled", "PAGE"],
      ["--dictionary-enabled", "TRUE"], ["--dictionary-enabled"], ["positional"],
      ["--compression-level", "4294967296"], ["--compression-level=-1"],
      ["--max-row-group-size", "-1"], ["--write-batch-size", "1_000"],
      ["--bloom-filter-ndv", "18446744073709551616"], ["--bloom-filter-fpp", "not-a-number"],
      ["--compression", "gzip", "--compression", "snappy"],
      ["--input", "again"],
    ].each do |extra|
      expect_raises(ArgumentError) { rewrite_args(extra) }
    end
  end
end

describe "Parquet rewrite property errors" do
  it "preserves upstream zero-size validation in the property builder" do
    %w[max-row-group-size write-batch-size data-page-row-count-limit statistics-truncate-length column-index-truncate-length].each do |option|
      expect_raises(Exception, "Cannot have a 0") { rewrite_args("--#{option}", "0").writer_properties }
    end
  end
end
