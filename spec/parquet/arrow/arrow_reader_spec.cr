require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `ParquetRecordBatchReader`/`ParquetRecordBatchReaderBuilder` batch-reading
# path.
#
# Divergences (see `plans/parity.md`): row selection, the offset/limit plans and
# `RowFilter` (see `row_filter_spec.cr`) are ported, but the plan is always
# selector-backed, there is no row-group selection, and statistics/reader
# metrics are not available yet. The supplied-schema tests that upstream writes
# through `ArrowWriter`
# (`write_parquet_from_iter`/`arrow::ArrowWriter`) are characterized with the
# ported `SerializedFileWriter` instead, since the arrow writer is not ported.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader
private alias FWriter = Parquet::File::SerializedFileWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition

private def serialized_reader(name : String) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(name)))
end

# Ported from upstream `generate_single_column_file_with_data` (the
# `arrow_reader` test helper): writes one OPTIONAL UTF8 `BYTE_ARRAY` column, one
# row group per `def_levels` entry, optionally embedding `field` as the
# `ARROW:schema` metadata.
private def write_utf8_file(
  values : Array(Array(String)),
  def_levels : Array(Array(Int16)),
  field : Arrow::Field? = nil,
  optional : Bool = true,
  dictionary : Bool = true,
) : Parquet::File::SerializedFileReader
  leaf = ST.primitive_type_builder("leaf", PType::BYTE_ARRAY)
    .with_repetition(optional ? PRep::OPTIONAL : PRep::REQUIRED)
    .with_converted_type(Parquet::ConvertedType::UTF8)
    .build.as(ST)
  schema = ST.group_type_builder("test_schema").with_fields([leaf]).build

  props = WP.builder.set_dictionary_enabled(dictionary).build
  if arrow_field = field
    PArrow.add_encoded_arrow_schema_to_metadata(
      Arrow::Schema.new(Arrow::Fields.new([arrow_field])), props
    )
  end

  io = IO::Memory.new
  writer = FWriter.new(io, schema, props)
  values.each_with_index do |column_values, idx|
    row_group = writer.next_row_group
    column_writer = row_group.next_column.not_nil!
    column_writer.typed(Parquet::ByteArray).write_batch(
      column_values.map { |value| Parquet::ByteArray.new(value) },
      optional ? def_levels[idx] : nil,
      nil,
    )
    column_writer.close
    row_group.close
  end
  writer.close
  Parquet::File::SerializedFileReader.new(BytesCR.new(io.to_slice))
end

# Writes a REQUIRED struct column (`nested`) holding one REQUIRED UTF8 leaf
# (`utf8_to_dict`), used for the nested supplied-schema test.
private def write_nested_utf8_file(values : Array(String)) : Parquet::File::SerializedFileReader
  leaf = ST.primitive_type_builder("utf8_to_dict", PType::BYTE_ARRAY)
    .with_repetition(PRep::REQUIRED)
    .with_converted_type(Parquet::ConvertedType::UTF8)
    .build.as(ST)
  nested = ST.group_type_builder("nested")
    .with_repetition(PRep::REQUIRED)
    .with_fields([leaf])
    .build.as(ST)
  schema = ST.group_type_builder("schema").with_fields([nested]).build

  io = IO::Memory.new
  writer = FWriter.new(io, schema, WP.builder.build)
  row_group = writer.next_row_group
  column_writer = row_group.next_column.not_nil!
  column_writer.typed(Parquet::ByteArray).write_batch(
    values.map { |value| Parquet::ByteArray.new(value) }, nil, nil
  )
  column_writer.close
  row_group.close
  writer.close
  Parquet::File::SerializedFileReader.new(BytesCR.new(io.to_slice))
end

# Writes a one-row file with the given REQUIRED columns (INT32/INT64 only), used
# for the supplied-schema validation tests.
private def write_int_file(columns : Array(Tuple(String, Parquet::Type))) : Parquet::File::SerializedFileReader
  fields = columns.map do |(name, physical)|
    ST.primitive_type_builder(name, physical).with_repetition(PRep::REQUIRED).build.as(ST)
  end
  schema = ST.group_type_builder("schema").with_fields(fields).build

  io = IO::Memory.new
  writer = FWriter.new(io, schema, WP.builder.build)
  row_group = writer.next_row_group
  columns.each do |(_name, physical)|
    column_writer = row_group.next_column.not_nil!
    case physical
    when PType::INT64
      column_writer.typed(Int64).write_batch([0_i64], nil, nil)
    when PType::INT32
      column_writer.typed(Int32).write_batch([0_i32], nil, nil)
    else
      raise ArgumentError.new("unsupported physical type #{physical}")
    end
    column_writer.close
  end
  row_group.close
  writer.close
  Parquet::File::SerializedFileReader.new(BytesCR.new(io.to_slice))
end

# The dictionary values of a `Dictionary(Int32, Utf8)` column (upstream compares
# the child `ArrayData` of the dictionary array).
private def dictionary_values_of(column : Arrow::ArrayBase) : Array(String?)
  values = column.as(Arrow::DictionaryArray(Int32)).values.as(Arrow::GenericStringArray(Int32))
  Array(String?).new(values.len) { |idx| values.value(idx) }
end

# A reader over the `id` column of `alltypes_plain.parquet` (ids 4, 5, 6, 7, 2,
# 3, 0, 1) — a REQUIRED INT32 leaf, so skips are well defined.
private def projected_id_reader(batch_size : Int32 = 1024) : PArrow::ParquetRecordBatchReaderBuilder
  reader_file = serialized_reader("alltypes_plain.parquet")
  mask = PArrow::ProjectionMask.leaves(reader_file.metadata.file_metadata.schema_descr, [0])
  PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file)
    .with_projection(mask)
    .with_batch_size(batch_size)
end

private def ids(batches : Array(Arrow::RecordBatch)) : Array(Int32)
  batches.flat_map do |batch|
    column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
    (0...column.len).map { |idx| column.value(idx).as(Int32) }
  end
end

private def read_batches(name : String, batch_size : Int32 = 1024,
                         projection : PArrow::ProjectionMask? = nil) : Array(Arrow::RecordBatch)
  builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(serialized_reader(name))
    .with_batch_size(batch_size)
  builder = builder.with_projection(projection.not_nil!) if projection
  builder.build.to_a
end

describe Parquet::Arrow::ParquetRecordBatchReader do
  it "reads a struct column of nulls" do
    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(serialized_reader("nulls.snappy.parquet"))

    builder.metadata.num_row_groups.should eq(1)
    builder.metadata.file_metadata.num_rows.should eq(8)
    builder.schema.fields.len.should eq(1)
    builder.parquet_schema.num_columns.should eq(1)

    reader = builder.build
    reader.schema.fields.len.should eq(1)

    batch = reader.next_batch.not_nil!
    batch.num_rows.should eq(8)
    batch.num_columns.should eq(1)
    batch.column(0).as(Arrow::StructArray).column(0)
      .as(Arrow::PrimitiveArray(Int32)).null_count.should eq(8)
    reader.next_batch.should be_nil
  end

  it "splits rows into batches of the requested size" do
    batches = read_batches("nulls.snappy.parquet", batch_size: 3)
    batches.map(&.num_rows).should eq([3, 3, 2])
  end

  it "clamps the batch size to the row count (upstream test_batch_size_overallocate)" do
    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(
      serialized_reader("alltypes_plain.parquet")
    )
    num_rows = builder.metadata.file_metadata.num_rows
    num_rows.should eq(8)

    # The port cannot project the INT96 column, so it selects the ten supported
    # leaves (upstream projects all columns).
    projection = PArrow::ProjectionMask.leaves(builder.parquet_schema, (0...10).to_a)
    reader = builder
      .with_batch_size(1024)
      .with_projection(projection)
      .build

    num_rows.should_not eq(1024)
    # The shared builder clamps eagerly, so `batch_size` reports the clamped
    # value before `build` (upstream `ArrowReaderBuilder::with_batch_size`).
    reader.read_plan.batch_size.should eq(num_rows.to_i32)
  end

  it "accepts a predicate cache size but ignores it (upstream ArrowReaderBuilder)" do
    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(
      serialized_reader("alltypes_plain.parquet")
    )
    builder.max_predicate_cache_size
      .should eq(PArrow::ArrowReaderBuilder::DEFAULT_MAX_PREDICATE_CACHE_SIZE)

    builder = builder.with_max_predicate_cache_size(0)
    builder.max_predicate_cache_size.should eq(0)

    # The synchronous reader never uses the predicate cache (arrow-rs#8000).
    projection = PArrow::ProjectionMask.leaves(builder.parquet_schema, (0...10).to_a)
    batches = builder.with_projection(projection).build.to_a
    batches.flat_map(&.num_rows).sum.should eq(8)
  end

  it "reads primitive columns with a projection mask" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file)
    # Keep this projection test focused on the first ten columns; rewrite specs
    # also cover the legacy INT96 timestamp column.
    mask = PArrow::ProjectionMask.leaves(
      reader_file.metadata.file_metadata.schema_descr, (0...10).to_a
    )

    batches = builder.with_projection(mask).build.to_a
    batch = batches.first
    batch.num_rows.should eq(8)
    batch.num_columns.should eq(10)

    ids = batch.column(0).as(Arrow::PrimitiveArray(Int32))
    Array(Int32?).new(3) { |idx| ids.value(idx) }.should eq([4, 5, 6])
    bools = batch.column(1).as(Arrow::BooleanArray)
    Array(Bool?).new(3) { |idx| bools.value(idx) }.should eq([true, false, true])
    batch.column(2).as(Arrow::PrimitiveArray(Int32)).value(1).should eq(1)
    batch.column(3).as(Arrow::PrimitiveArray(Int32)).value(1).should eq(1)
    batch.column(4).as(Arrow::PrimitiveArray(Int32)).value(1).should eq(1)
    batch.column(5).as(Arrow::PrimitiveArray(Int64)).value(1).should eq(10)
    batch.column(6).as(Arrow::PrimitiveArray(Float32)).value(1).should be_close(1.1_f32, 0.001)
    batch.column(7).as(Arrow::PrimitiveArray(Float64)).value(1).should be_close(10.1, 0.001)
    String.new(batch.column(9).as(Arrow::GenericBinaryArray(Int32)).value(1)).should eq("1")
  end

  it "reads list columns" do
    batches = read_batches("list_columns.parquet")
    batches.size.should eq(1)
    batch = batches.first
    batch.num_rows.should eq(3)

    ints = batch.column(0).as(Arrow::GenericListArray(Int32))
    ints.null_count.should eq(0)
    Array(Int64?).new(3) { |idx| ints.value(0).as(Arrow::PrimitiveArray(Int64)).value(idx) }
      .should eq([1, 2, 3])
    ints.value(1).as(Arrow::PrimitiveArray(Int64)).null?(0).should be_true
    ints.value(2).as(Arrow::PrimitiveArray(Int64)).value(0).should eq(4)

    strings = batch.column(1).as(Arrow::GenericListArray(Int32))
    strings.null?(1).should be_true
    first = strings.value(0).as(Arrow::GenericStringArray(Int32))
    Array(String?).new(3) { |idx| first.value(idx) }.should eq(%w[abc efg hij])
    last = strings.value(2).as(Arrow::GenericStringArray(Int32))
    last.null?(1).should be_true
    last.value(3).should eq("xyz")
  end

  it "reads nested list columns" do
    batches = read_batches("nested_lists.snappy.parquet")
    batch = batches.first
    batch.num_rows.should eq(3)
    batch.column(0).as(Arrow::GenericListArray(Int32)).len.should eq(3)
  end

  it "provides the convenience constructor" do
    reader = PArrow::ParquetRecordBatchReader.try_new(
      serialized_reader("nulls.snappy.parquet"), 4
    )
    batches = reader.to_a
    batches.map(&.num_rows).should eq([4, 4])
  end

  it "returns no batches for an empty projection" do
    reader_file = serialized_reader("nulls.snappy.parquet")
    mask = PArrow::ProjectionMask.none(reader_file.metadata.file_metadata.schema_descr.num_columns)
    batches = PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file)
      .with_projection(mask)
      .build
      .to_a

    # The empty reader still yields the row count, with no columns.
    batches.map(&.num_rows).should eq([8])
    batches.first.num_columns.should eq(0)
  end
end

describe "Parquet::Arrow::ParquetRecordBatchReader row plans" do
  it "applies an offset and a limit" do
    batches = projected_id_reader.with_offset(2).with_limit(3).build.to_a
    batches.map(&.num_rows).should eq([3])
    ids(batches).should eq([6, 7, 2])
  end

  it "applies a row selection" do
    selection = PArrow::RowSelection.new([
      PArrow::RowSelector.select(2),
      PArrow::RowSelector.skip(4),
      PArrow::RowSelector.select(2),
    ])
    batches = projected_id_reader.with_row_selection(selection).build.to_a
    ids(batches).should eq([4, 5, 0, 1])
  end

  it "splits selected rows into batches" do
    selection = PArrow::RowSelection.new([PArrow::RowSelector.select(3)])
    batches = projected_id_reader(batch_size: 2).with_row_selection(selection).build.to_a
    batches.map(&.num_rows).should eq([2, 1])
    ids(batches).should eq([4, 5, 6])
  end

  it "combines a selection with an offset and a limit" do
    selection = PArrow::RowSelection.new([PArrow::RowSelector.select(2), PArrow::RowSelector.skip(4), PArrow::RowSelector.select(2)])
    batches = projected_id_reader.with_row_selection(selection).with_offset(1).with_limit(2).build.to_a
    ids(batches).should eq([5, 0])
  end

  it "returns no batches when the plan selects nothing" do
    projected_id_reader.with_offset(8).build.to_a.should be_empty
    projected_id_reader.with_limit(0).build.to_a.should be_empty
    selection = PArrow::RowSelection.new([PArrow::RowSelector.skip(8)])
    projected_id_reader.with_row_selection(selection).build.to_a.should be_empty
  end
end

describe Parquet::Arrow::ArrowReaderMetadata do
  it "loads the arrow schema from the parquet metadata" do
    metadata = PArrow::ArrowReaderMetadata.load(serialized_reader("list_columns.parquet"))

    metadata.metadata.file_metadata.num_rows.should eq(3)
    metadata.schema.fields.len.should eq(2)
    metadata.schema.fields[0].data_type.kind.list?.should be_true
    metadata.schema.fields[1].data_type.kind.list?.should be_true
    metadata.fields.not_nil!.arrow_type.kind.struct?.should be_true
  end

  it "can skip the embedded arrow schema" do
    metadata = PArrow::ArrowReaderMetadata.try_new(
      serialized_reader("list_columns.parquet").metadata,
      PArrow::ArrowReaderOptions.new.with_skip_arrow_metadata(true),
    )
    metadata.schema.fields.len.should eq(2)
  end
end

describe "Parquet::Arrow::ArrowReaderOptions supplied schemas" do
  # Upstream `test_dictionary_preservation` (an end-to-end dictionary read where
  # the dictionary arrow field is embedded in the `ARROW:schema` metadata).
  it "preserves dictionary encoding across batches and row groups" do
    dict_type = Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8)
    reader_file = write_utf8_file(
      [["hello", "a", "b", "d"], ["c", "a", "b"]],
      [
        [1_i16, 0_i16, 0_i16, 1_i16, 0_i16, 0_i16, 1_i16, 1_i16],
        [0_i16, 0_i16, 1_i16, 1_i16, 0_i16, 0_i16, 1_i16, 0_i16, 0_i16],
      ],
      Arrow::Field.new("leaf", dict_type, true),
    )

    batches = PArrow::ParquetRecordBatchReader.try_new(reader_file, 3).to_a

    batches.size.should eq(6)
    batches.each { |batch| batch.num_columns.should eq(1) }

    row_counts = batches.map { |batch| {batch.num_rows, batch.column(0).null_count} }
    row_counts.should eq([{3, 2}, {3, 2}, {3, 1}, {3, 1}, {3, 2}, {2, 2}])

    dictionaries = batches.map { |batch| dictionary_values_of(batch.column(0)) }

    # First and second batch in the same row group -> same dictionary.
    dictionaries[0].should eq(dictionaries[1])
    # Third batch spans row groups -> computed dictionary.
    dictionaries[1].should_not eq(dictionaries[2])
    dictionaries[2].should_not eq(dictionaries[3])
    # Fourth, fifth and sixth from the same row group -> same dictionary.
    dictionaries[3].should eq(dictionaries[4])
    dictionaries[4].should eq(dictionaries[5])

    # The underlying values are preserved by the dictionary encoding.
    strings = batches.flat_map do |batch|
      dict = batch.column(0).as(Arrow::DictionaryArray(Int32))
      values = dict.values.as(Arrow::GenericStringArray(Int32))
      keys = dict.keys
      Array(String?).new(dict.len) do |idx|
        dict.valid?(idx) ? values.value(keys.value(idx)) : nil
      end
    end
    strings.should eq([
      "hello", nil, nil,
      "a", nil, nil,
      "b", "d",
      nil, nil, "c",
      "a", nil, nil,
      "b", nil, nil,
    ])
  end

  # Upstream `test_null_schema_inference`.
  it "infers the parquet schema when the arrow metadata is skipped" do
    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
      serialized_reader("null_list.parquet"),
      PArrow::ArrowReaderOptions.new.with_skip_arrow_metadata(true),
    )

    schema = builder.schema
    schema.fields.len.should eq(1)
    schema.fields[0].should eq(Arrow::Field.new(
      "emptylist",
      Arrow::DataType.list(Arrow::Field.new("item", Arrow::DataType.null, true)),
      true,
    ))
  end

  # Characterization of upstream's `with_schema` dictionary preservation (the
  # upstream test writes through `ArrowWriter`, which is not ported).
  it "reads a column as a dictionary with a supplied schema" do
    reader_file = write_utf8_file(
      [["Berlin", "Berlin", "Paris", "Berlin", "Paris"]],
      [[1_i16, 1_i16, 1_i16, 1_i16, 1_i16]],
    )
    dict_type = Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8)
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new("leaf", dict_type, true),
    ]))

    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
      reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
    )

    builder.schema.should eq(supplied)

    reader = builder.build
    reader.schema.should eq(supplied)

    batch = reader.next_batch.not_nil!
    batch.num_rows.should eq(5)
    batch.num_columns.should eq(1)
    batch.column(0).data_type.should eq(dict_type)
    dictionary_values_of(batch.column(0)).should eq(["Berlin", "Paris"])
  end

  # Characterization of upstream's `with_schema` nested dictionary preservation
  # (`test_with_schema`'s `nested.utf8_to_dict` assertions).
  it "reads a nested struct column as a dictionary with a supplied schema" do
    reader_file = write_nested_utf8_file(["a", "a", "a", "b"])
    dict_type = Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8)
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new(
        "nested",
        Arrow::DataType.struct(Arrow::Fields.new([
          Arrow::Field.new("utf8_to_dict", dict_type, false),
        ])),
        false,
      ),
    ]))

    reader = PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
      reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
    ).build

    batch = reader.next_batch.not_nil!
    batch.num_rows.should eq(4)
    nested = batch.column(0).as(Arrow::StructArray)
    nested_dict = nested.column(0).as(Arrow::DictionaryArray(Int32))

    dictionary_values_of(nested_dict).should eq(["a", "b"])
    keys = nested_dict.keys
    Array(Int32).new(keys.len) { |idx| keys.value(idx) }.should eq([0, 0, 0, 1])
  end

  # Upstream `test_schema_too_few_columns`.
  it "rejects a supplied schema with too few columns" do
    reader_file = write_int_file([{"int64", PType::INT64}, {"int32", PType::INT32}])
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new("int64", Arrow::DataType.int64, false),
    ]))

    err = expect_raises(Parquet::ArrowError) do
      PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
        reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
      )
    end
    err.to_s.should eq("Arrow: incompatible arrow schema, expected 2 struct fields got 1")
  end

  # Upstream `test_schema_too_many_columns`.
  it "rejects a supplied schema with too many columns" do
    reader_file = write_int_file([{"int64", PType::INT64}])
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new("int64", Arrow::DataType.int64, false),
      Arrow::Field.new("int32", Arrow::DataType.int32, false),
    ]))

    err = expect_raises(Parquet::ArrowError) do
      PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
        reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
      )
    end
    err.to_s.should eq("Arrow: incompatible arrow schema, expected 1 struct fields got 2")
  end

  # Upstream `test_schema_mismatched_column_names`.
  it "rejects a supplied schema with mismatched column names" do
    reader_file = write_int_file([{"int64", PType::INT64}])
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new("other", Arrow::DataType.int64, false),
    ]))

    err = expect_raises(Parquet::ArrowError) do
      PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
        reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
      )
    end
    err.to_s.should eq("Arrow: incompatible arrow schema, expected field named int64 got other")
  end

  # Upstream `test_schema_incompatible_columns`.
  it "rejects a supplied schema with incompatible column types" do
    reader_file = write_int_file([
      {"col1_invalid", PType::INT64},
      {"col2_valid", PType::INT32},
      {"col3_invalid", PType::INT64},
    ])
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new("col1_invalid", Arrow::DataType.int32, false),
      Arrow::Field.new("col2_valid", Arrow::DataType.int32, false),
      Arrow::Field.new("col3_invalid", Arrow::DataType.int32, false),
    ]))

    err = expect_raises(Parquet::ArrowError) do
      PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
        reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
      )
    end
    err.to_s.should eq(
      "Arrow: Incompatible supplied Arrow schema: " \
      "data type mismatch for field col1_invalid: requested Int32 but found Int64, " \
      "data type mismatch for field col3_invalid: requested Int32 but found Int64"
    )
  end

  # Upstream `test_schema_error_bad_types`.
  it "rejects a supplied schema with an incompatible type" do
    reader_file = write_utf8_file([["foo", "bar", "baz"]], [[] of Int16], optional: false)
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new("leaf", Arrow::DataType.int32, false),
    ]))

    err = expect_raises(Parquet::ArrowError) do
      PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
        reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
      )
    end
    err.to_s.should eq(
      "Arrow: Incompatible supplied Arrow schema: " \
      "data type mismatch for field leaf: requested Int32 but found Utf8"
    )
  end

  # Upstream `test_schema_error_bad_nullability`.
  it "rejects a supplied schema with an incompatible nullability" do
    reader_file = write_utf8_file([["foo", "bar", "baz"]], [[] of Int16], optional: false)
    supplied = Arrow::Schema.new(Arrow::Fields.new([
      Arrow::Field.new("leaf", Arrow::DataType.utf8, true),
    ]))

    err = expect_raises(Parquet::ArrowError) do
      PArrow::ParquetRecordBatchReaderBuilder.try_new_with_options(
        reader_file, PArrow::ArrowReaderOptions.new.with_schema(supplied)
      )
    end
    err.to_s.should eq(
      "Arrow: Incompatible supplied Arrow schema: " \
      "nullability mismatch for field leaf: expected true but found false"
    )
  end
end
