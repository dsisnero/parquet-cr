require "../spec_helper"

# Ported from `parquet-variant/src/builder/metadata.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `IndexSet` is an insertion-ordered `Array(String)` + index map;
# `Extend`/`FromIterator` are `#extend`/`.from_iter`; `is_sorted` → `sorted?`;
# `ArrowError` → `ParquetVariant::InvalidArgumentError`. The upstream
# `test_read_only_metadata_builder_fails_on_unknown_field` drove the builder
# through `ObjectBuilder` (not ported yet); it is characterized here directly on
# `ReadOnlyMetadataBuilder`.

private alias WritableMetadataBuilder = ParquetVariant::WritableMetadataBuilder
private alias ReadOnlyMetadataBuilder = ParquetVariant::ReadOnlyMetadataBuilder
private alias VariantMetadata = ParquetVariant::VariantMetadata

describe ParquetVariant::WritableMetadataBuilder do
  # Upstream `test_metadata_builder_from_iter`.
  it "builds from an iterable" do
    metadata = WritableMetadataBuilder.from_iter(["apple", "banana", "cherry"])
    metadata.num_field_names.should eq(3)
    metadata.field_name(0).should eq("apple")
    metadata.field_name(1).should eq("banana")
    metadata.field_name(2).should eq("cherry")
    metadata.sorted?.should be_true

    metadata = WritableMetadataBuilder.from_iter(["zebra", "apple", "banana"])
    metadata.num_field_names.should eq(3)
    metadata.field_name(0).should eq("zebra")
    metadata.field_name(1).should eq("apple")
    metadata.field_name(2).should eq("banana")
    metadata.sorted?.should be_false

    metadata = WritableMetadataBuilder.from_iter([] of String)
    metadata.num_field_names.should eq(0)
    metadata.sorted?.should be_false
  end

  # Upstream `test_metadata_builder_extend`.
  it "extends with field names" do
    metadata = WritableMetadataBuilder.new
    metadata.num_field_names.should eq(0)
    metadata.sorted?.should be_false

    metadata.extend(["apple", "cherry"])
    metadata.num_field_names.should eq(2)
    metadata.field_name(0).should eq("apple")
    metadata.field_name(1).should eq("cherry")
    metadata.sorted?.should be_true

    metadata.extend(["dinosaur", "monkey"])
    metadata.num_field_names.should eq(4)
    metadata.field_name(2).should eq("dinosaur")
    metadata.field_name(3).should eq("monkey")
    metadata.sorted?.should be_true

    initial_count = metadata.num_field_names
    metadata.extend(["apple", "monkey"])
    metadata.num_field_names.should eq(initial_count)
  end

  # Upstream `test_metadata_builder_extend_sort_order`.
  it "tracks sort order" do
    metadata = WritableMetadataBuilder.new

    metadata.extend(["middle"])
    metadata.sorted?.should be_true

    metadata.extend(["zebra"])
    metadata.sorted?.should be_true

    metadata.extend(["apple"])
    metadata.sorted?.should be_false
  end

  # Upstream `test_metadata_builder_from_iter_with_string_types`.
  it "builds from mixed string types" do
    WritableMetadataBuilder.from_iter(["a", "b", "c"]).num_field_names.should eq(3)
    WritableMetadataBuilder.from_iter(["a".to_s, "b".to_s, "c".to_s]).num_field_names.should eq(3)
  end

  # Extra characterization: finished bytes are readable by `VariantMetadata`.
  it "produces metadata readable by VariantMetadata" do
    builder = WritableMetadataBuilder.from_iter(["apple", "banana", "cherry"])
    builder.finish
    metadata = VariantMetadata.try_new(builder.into_inner)
    metadata.len.should eq(3)
    metadata.iter.to_a.should eq(["apple", "banana", "cherry"])
    metadata.sorted?.should be_true
  end
end

describe ParquetVariant::ReadOnlyMetadataBuilder do
  # Upstream `test_read_only_metadata_builder_fails_on_unknown_field` (adapted).
  it "resolves known fields and rejects unknown ones" do
    builder = WritableMetadataBuilder.new
    builder.upsert_field_name("known_field")
    builder.finish

    metadata = VariantMetadata.try_new(builder.into_inner)
    read_only = ReadOnlyMetadataBuilder.new(metadata)

    read_only.try_upsert_field_name("known_field").should eq(0_u32)
    error = expect_raises(ParquetVariant::InvalidArgumentError) do
      read_only.try_upsert_field_name("unknown_field")
    end
    error.message.not_nil!.should contain("Field name 'unknown_field' not found")
  end
end
