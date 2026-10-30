require "../../spec_helper"

# parquet/benches/parquet_round_trip.rs has no unit tests. Characterize its
# complete case matrix and file assertions before porting the harness.
private alias RB = Parquet::Benchmarks::RoundTrip

describe "Round-trip benchmark" do
  it "preserves source defaults and immutable spec updates" do
    spec = RB::ParquetFileSpec.new(RB::ColumnType.int32)
    spec.num_columns.should eq 10
    spec.num_row_groups.should eq 10
    spec.rows_per_row_group.should eq 10_000
    spec.rows_per_page.should eq 2_000
    spec.encoding.should eq Parquet::Encoding::PLAIN
    spec.use_dict?.should be_true
    changed = spec.with_num_columns(2).with_num_row_groups(3).with_rows_per_row_group(32)
      .with_rows_per_page(16).with_encoding(Parquet::Encoding::BYTE_STREAM_SPLIT).with_use_dict(false)
    changed.num_columns.should eq 2
    changed.num_row_groups.should eq 3
    changed.rows_per_row_group.should eq 32
    changed.rows_per_page.should eq 16
    changed.encoding.should eq Parquet::Encoding::BYTE_STREAM_SPLIT
    changed.use_dict?.should be_false
    spec.num_columns.should eq 10
    spec.use_dict?.should be_true
  end

  it "ports all 46 source case names and settings" do
    cases = RB.cases
    cases.size.should eq 46
    cases.map(&.name).uniq.size.should eq 46
    cases.map(&.name).should contain "int32 delta_binary"
    cases.map(&.name).should contain "int64 byte_stream_split"
    cases.map(&.name).should contain "f32 dict"
    cases.map(&.name).should contain "f64 plain"
    cases.map(&.name).should contain "String(100) delta_byte_array"
    cases.map(&.name).should contain "StringView(20) delta_length"
    cases.map(&.name).should contain "Binary(100) plain"
    cases.map(&.name).should contain "Fixed(16) byte_stream_split"
    cases.each do |item|
      item.spec.num_columns.should eq(item.spec.column_type.numeric? ? 10 : 5)
    end
  end

  it "generates deterministic, independently seeded nullable columns for every type" do
    RB.types.each do |type|
      schema = RB.schema(type, 2)
      schema.fields.to_a.map(&.name).should eq ["col_0", "col_1"]
      schema.fields.to_a.all?(&.nullable?).should be_true
      first = RB.create_batch(schema, type, 31, 2, 32)
      first.should eq RB.create_batch(schema, type, 31, 2, 32)
      first.should_not eq RB.create_batch(schema, type, 32, 2, 32)
      first.column(0).should_not eq first.column(1)
      all_null = RB.create_batch(schema, type, 31, 2, 5, null_density: 1.0)
      all_null.columns.each { |column| column.null_count.should eq 5 }
    end
  end

  it "round trips every source type/encoding combination with exact rows, groups and values" do
    RB.cases.each do |item|
      spec = RB.smoke_spec(item.spec)
      bytes = RB.file_from_spec(spec)
      metadata = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(bytes)).metadata
      metadata.num_row_groups.should eq 2
      metadata.num_rows.should eq 64
      RB.validate(bytes, spec, batch_size: 17).should eq 64
    end
  end

  it "resets reusable output for every timed write rather than appending complete files" do
    spec = RB.smoke_spec(RB.cases.first.spec)
    output = IO::Memory.new
    first = RB.file_from_spec(spec, output).dup
    second = RB.file_from_spec(spec, output)
    second.should eq first
    output.size.should eq first.size
    RB.validate(second, spec).should eq 64
  end

  it "rejects shapes that would cause zero progress or overshoot the source batch loop" do
    base = RB::ParquetFileSpec.new(RB::ColumnType.int32)
    expect_raises(ArgumentError) { RB.file_from_spec(base.with_rows_per_row_group(0)) }
    expect_raises(ArgumentError) { RB.file_from_spec(base.with_rows_per_row_group(101)) }
    expect_raises(ArgumentError) { RB.file_from_spec(base.with_num_columns(0)) }
    expect_raises(ArgumentError) { RB.file_from_spec(base.with_num_row_groups(0)) }
    expect_raises(ArgumentError) { RB.file_from_spec(base.with_rows_per_page(0)) }
  end

  it "reports separate cold and warm write/read measurements with validated throughput" do
    item = RB::Case.new("int32 plain", RB.smoke_spec(RB.cases[1].spec))
    output = IO::Memory.new
    results = RB.run([item], 3, output)
    results.map(&.operation).should eq ["write", "read"]
    results.each do |result|
      result.rows.should eq 64
      result.bytes.should be > 0
      result.cold_seconds.should be >= 0
      result.warm_seconds.size.should eq 3
      result.mean_seconds.should be > 0
      result.rows_per_second.should be > 0
    end
    output.to_s.lines.size.should eq 3
    output.to_s.should contain "cold_seconds,warm_seconds,rows_per_second"
    expect_raises(ArgumentError) { RB.run([item], 0, IO::Memory.new) }
  end
end
