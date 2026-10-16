require "../../spec_helper"

# Ported from `parquet/src/file/metadata/options.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream `test_options_default` is ported directly; the remaining cases
# characterize the policy plumbing. `test_provide_schema` needs
# `ParquetMetaDataPushDecoder` and is deferred to that feature.
describe Parquet::File::Metadata::ParquetMetaDataOptions do
  # Upstream `test_options_default`.
  it "defaults encoding_stats_as_mask to true and has no schema" do
    options = PMeta::ParquetMetaDataOptions.new
    options.encoding_stats_as_mask?.should be_true
    options.schema.should be_nil
  end

  it "tracks the schema, the mask flag, and the per-column policies" do
    options = PMeta::ParquetMetaDataOptions.new
    schema = ParquetTestHelpers.test_schema_descr

    options.set_schema(schema)
    options.schema.should eq(schema)

    options.set_encoding_stats_as_mask(false)
    options.encoding_stats_as_mask?.should be_false

    # Default is KeepAll for every policy.
    options.skip_encoding_stats(0).should be_false
    options.skip_column_stats(0).should be_false
    options.skip_size_stats(0).should be_false

    options.set_encoding_stats_policy(PMeta::ParquetStatisticsPolicy::SkipAll)
    options.skip_encoding_stats(0).should be_true
    options.skip_encoding_stats(5).should be_true

    options.set_column_stats_policy(PMeta::ParquetStatisticsPolicy.skip_except([1, 3]))
    options.skip_column_stats(0).should be_true
    options.skip_column_stats(1).should be_false
    options.skip_column_stats(3).should be_false

    options.set_size_stats_policy(PMeta::ParquetStatisticsPolicy.skip_except([] of Int32))
    options.skip_size_stats(0).should be_true
  end

  it "chains through the with_* methods" do
    schema = ParquetTestHelpers.test_schema_descr
    options = PMeta::ParquetMetaDataOptions.new
      .with_schema(schema)
      .with_encoding_stats_as_mask(false)
      .with_encoding_stats_policy(PMeta::ParquetStatisticsPolicy::SkipAll)
      .with_column_stats_policy(PMeta::ParquetStatisticsPolicy::SkipAll)
      .with_size_stats_policy(PMeta::ParquetStatisticsPolicy::SkipAll)

    options.schema.should eq(schema)
    options.encoding_stats_as_mask?.should be_false
    options.skip_encoding_stats(0).should be_true
    options.skip_column_stats(0).should be_true
    options.skip_size_stats(0).should be_true
  end
end

describe Parquet::File::Metadata::ParquetStatisticsPolicy do
  it "returns SkipAll from skip_except when the keep list is empty" do
    PMeta::ParquetStatisticsPolicy.skip_except([] of Int32).skip?(0).should be_true
  end

  it "skips only columns outside the keep set" do
    policy = PMeta::ParquetStatisticsPolicy.skip_except([0, 2])
    policy.skip?(0).should be_false
    policy.skip?(1).should be_true
    policy.skip?(2).should be_false

    PMeta::ParquetStatisticsPolicy::KeepAll.skip?(0).should be_false
    PMeta::ParquetStatisticsPolicy::SkipAll.skip?(0).should be_true
  end
end
