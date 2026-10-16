require "../../spec_helper"

# Ported from `parquet/src/record/triplet.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: the SNAPPY-compressed fixtures (`nulls.snappy.parquet`,
# `nested_lists.snappy.parquet`, `nested_maps.snappy.parquet`) are not readable
# because the SNAPPY codec needs a Crystal shard; those cases are covered here
# with the uncompressed Impala fixtures instead.

private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias TIter = Parquet::Record::TripletIter
private alias Field = Parquet::Record::Field

private def open_reader(file_name : String) : FReader
  FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))
end

private def find_column(descr : Parquet::Schema::SchemaDescriptor, path : Array(String)) : Int32?
  target = Parquet::Schema::ColumnPath.new(path)
  descr.num_columns.times do |i|
    return i if descr.column(i).path == target
  end
  nil
end

private def test_triplet_column(
  descr : Parquet::Schema::ColumnDescriptor,
  reader : Parquet::Column::ColumnReader,
  batch_size : Int32,
  expected_values : Array(Field),
  expected_def_levels : Array(Int16),
  expected_rep_levels : Array(Int16),
) : Nil
  iter = TIter.new(descr, reader, batch_size)
  values = [] of Field
  def_levels = [] of Int16
  rep_levels = [] of Int16

  iter.max_def_level.should eq(descr.max_def_level)
  iter.max_rep_level.should eq(descr.max_rep_level)

  while iter.read_next
    iter.has_next?.should be_true
    values << iter.current_value unless iter.null?
    def_levels << iter.current_def_level
    rep_levels << iter.current_rep_level
  end

  values.should eq(expected_values)
  def_levels.should eq(expected_def_levels)
  rep_levels.should eq(expected_rep_levels)
end

private def test_column_in_file(
  file_name : String,
  batch_size : Int32,
  column_path : Array(String),
  expected_values : Array(Field),
  expected_def_levels : Array(Int16),
  expected_rep_levels : Array(Int16),
) : Nil
  reader = open_reader(file_name)
  schema = reader.metadata.file_metadata.schema_descr
  row_group_reader = reader.get_row_group(0)

  index = find_column(schema, column_path)
  index.should_not be_nil
  descr = schema.column(index.not_nil!)
  col_reader = row_group_reader.get_column_reader(index.not_nil!)
  test_triplet_column(descr, col_reader, batch_size, expected_values, expected_def_levels, expected_rep_levels)
end

private def test_triplet_iter(
  file_name : String,
  column_path : Array(String),
  expected_values : Array(Field),
  expected_def_levels : Array(Int16),
  expected_rep_levels : Array(Int16),
) : Nil
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 128, 256].each do |batch_size|
    test_column_in_file(file_name, batch_size, column_path, expected_values, expected_def_levels, expected_rep_levels)
  end
end

describe Parquet::Record::TripletIter do
  # Upstream `test_triplet_zero_batch_size`.
  it "rejects a zero batch size" do
    expect_raises(ArgumentError, "Expected positive batch size, found: 0") do
      test_column_in_file("nonnullable.impala.parquet", 0, ["ID"], [] of Field, [] of Int16, [] of Int16)
    end
  end

  # Upstream `test_triplet_required_column`.
  it "iterates a required column" do
    test_triplet_iter(
      "nonnullable.impala.parquet",
      ["ID"],
      [Field::Long.new(8_i64)] of Field,
      [0_i16],
      [0_i16],
    )
  end

  # Upstream `test_triplet_optional_column`.
  it "iterates an optional column" do
    test_triplet_iter(
      "nullable.impala.parquet",
      ["nested_struct", "A"],
      [Field::Int.new(1), Field::Int.new(7)] of Field,
      [2_i16, 1_i16, 1_i16, 1_i16, 1_i16, 0_i16, 2_i16],
      [0_i16, 0_i16, 0_i16, 0_i16, 0_i16, 0_i16, 0_i16],
    )
  end
end
