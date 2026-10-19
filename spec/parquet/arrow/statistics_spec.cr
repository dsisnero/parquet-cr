require "../../spec_helper"

# Ported from `parquet/tests/arrow_reader/statistics.rs` and the API of
# `parquet/src/arrow/arrow_reader/statistics.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `StatisticsConverter` extracting row-group and data-page statistics as Arrow
# arrays.
#
# The upstream tests write their fixtures with `ArrowWriter`
# (`make_test_file_rg`/`build_parquet_file`); the arrow writer is not ported, so
# the fixtures here are written with the ported `SerializedFileWriter` (one
# column, one row group per entry, `ARROW:schema` embedded so the arrow field is
# recovered) and read back with page indexes enabled. Everything else follows
# the upstream cases: the same expectations for minimums/maximums, null counts,
# row counts and min/max exactness, and the same data-page checks where a page
# row-count limit produces multiple pages.
#
# Divergences: `usize` counts are `Int32`, `StatisticsConverter#row_group_mins`
# and friends take `Enumerable(RowGroupMetaData)` (upstream takes any
# `IntoIterator`), `Char`/`Float16` support follows arrow-cr's `Arrow::Float16`
# (Crystal has no native half float), and tests upstream runs on both row groups
# and data pages run on row groups unless noted.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader
private alias FWriter = Parquet::File::SerializedFileWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias ES = Parquet::File::Properties::EnabledStatistics
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias StatisticsConverter = Parquet::Arrow::StatisticsConverter

# Writes one column (one row group per `row_groups` entry, `nil` = null) with
# `physical`, embedding `field` as the `ARROW:schema`, and reads it back with
# page indexes enabled.
private def write_stats_file(
  physical : PType,
  field : Arrow::Field,
  row_groups : Array(Array(T?)),
  *,
  optional : Bool = true,
  statistics : ES = ES::Page,
  page_row_count_limit : Int32? = nil,
  truncate_length : Int32? = nil,
  fixed_len : Int32? = nil,
  column_name : String = "col",
  embed_field : Bool = true,
  converted_type : Parquet::ConvertedType? = nil,
  logical_type : Parquet::LogicalType? = nil,
) : Parquet::File::SerializedFileReader forall T
  builder = ST.primitive_type_builder(column_name, physical)
  builder = builder.with_repetition(optional ? PRep::OPTIONAL : PRep::REQUIRED)
  builder = builder.with_length(fixed_len) if fixed_len
  builder = builder.with_converted_type(converted_type) if converted_type
  builder = builder.with_logical_type(logical_type) if logical_type
  leaf = builder.build.as(ST)
  schema = ST.group_type_builder("schema").with_fields([leaf]).build

  props_builder = WP.builder.set_statistics_enabled(statistics)
  props_builder.set_data_page_row_count_limit(page_row_count_limit) if page_row_count_limit
  props_builder.set_statistics_truncate_length(truncate_length) if truncate_length
  props = props_builder.build
  if embed_field
    PArrow.add_encoded_arrow_schema_to_metadata(
      Arrow::Schema.new(Arrow::Fields.new([field])), props
    )
  end

  io = IO::Memory.new
  writer = FWriter.new(io, schema, props)
  row_groups.each do |values|
    row_group = writer.next_row_group
    column_writer = row_group.next_column.not_nil!
    typed = column_writer.typed(T)
    def_levels = optional ? values.map { |value| value.nil? ? 0_i16 : 1_i16 } : nil.as(Array(Int16)?)
    if page_row_count_limit
      # Upstream's fixture helper sends one row per batch when a data-page row
      # count limit is set, which is what makes the writer split pages.
      values.each_with_index do |value, index|
        batch = value.nil? ? [] of T : [value.as(T)]
        levels = def_levels.try { |levels| [levels[index]] }
        typed.write_batch(batch, levels, nil)
      end
    elsif optional
      typed.write_batch(values.compact.map(&.as(T)), def_levels, nil)
    else
      typed.write_batch(values.map(&.as(T)), nil, nil)
    end
    column_writer.close
    row_group.close
  end
  writer.close

  Parquet::File::SerializedFileReader.new_with_options(
    BytesCR.new(io.to_slice),
    Parquet::File::ReadOptionsBuilder.new.with_page_index.build,
  )
end

# A 32-byte big-endian two's-complement 100 (zero-filled except the last byte).
private def low_32_bytes : Array(UInt8)
  bytes = Array(UInt8).new(32, 0_u8)
  bytes[31] = 0x64_u8
  bytes
end

private def converter_for(
  reader : Parquet::File::SerializedFileReader,
  field : Arrow::Field,
  column : String = "col",
) : StatisticsConverter
  PArrow::StatisticsConverter.try_new(
    column,
    Arrow::Schema.new(Arrow::Fields.new([field])),
    reader.metadata.file_metadata.schema_descr,
  )
end

private def u64(values : Array(UInt64?)) : Arrow::PrimitiveArray(UInt64)
  Arrow::PrimitiveArray(UInt64).from_options(values.map { |value| value.as(UInt64?) })
end

private def bools(values : Array(Bool?)) : Arrow::BooleanArray
  Arrow::BooleanArray.from_options(values.map { |value| value.as(Bool?) })
end

# Ported from the upstream `Test::run_checks` helper: checks the row-group
# statistics and, when `data_pages` is set, the data-page statistics.
private def check_stats(
  reader : Parquet::File::SerializedFileReader,
  field : Arrow::Field,
  expected_min : Arrow::ArrayBase,
  expected_max : Arrow::ArrayBase,
  *,
  nulls : Array(UInt64?),
  rows : Array(UInt64?)? = nil,
  min_exact : Array(Bool?)? = nil,
  max_exact : Array(Bool?)? = nil,
  row_groups : Bool = true,
  data_pages : Bool = false,
  no_rows_statistics : Bool = false,
  column : String = "col",
) : Nil
  converter = converter_for(reader, field, column)
  groups = reader.metadata.row_groups

  if row_groups
    converter.row_group_mins(groups).should eq(expected_min)
    converter.row_group_maxes(groups).should eq(expected_max)
    converter.row_group_null_counts(groups).should eq(u64(nulls))
    if value = rows
      converter.row_group_row_counts(groups).should eq(u64(value))
    elsif no_rows_statistics
      converter.row_group_row_counts(groups).should be_nil
    end
    if exact = max_exact
      converter.row_group_is_max_value_exact(groups).should eq(bools(exact))
    end
    if exact = min_exact
      converter.row_group_is_min_value_exact(groups).should eq(bools(exact))
    end
  end

  return unless data_pages

  page_index = reader.metadata.page_index.not_nil!("expected page indexes")
  indices = (0...groups.size).to_a
  converter.data_page_mins(page_index, indices).should eq(expected_min)
  converter.data_page_maxes(page_index, indices).should eq(expected_max)
  converter.data_page_null_counts(page_index, indices).should eq(u64(nulls))
  if value = rows
    converter.data_page_row_counts(page_index, groups, indices).should eq(u64(value))
  end
end

private def int64_field(name : String = "col") : Arrow::Field
  Arrow::Field.new(name, Arrow::DataType.int64, true)
end

# Ported from the upstream `Int64Case`: `[start, end)` non-null values followed
# by `null_values` nulls, in row groups of `row_per_group` rows.
private def int64_row_groups(
  start_value : Int64,
  end_value : Int64,
  null_values : Int32,
  row_per_group : Int32,
) : Array(Array(Int64?))
  values = (start_value...end_value).map { |value| value.as(Int64?) }
  null_values.times { values << nil }

  row_groups = [] of Array(Int64?)
  values.each_slice(row_per_group) { |slice| row_groups << slice }
  row_groups
end

describe Parquet::Arrow::StatisticsConverter do
  describe "row group statistics (INT64)" do
    it "extracts one row group without nulls (upstream test_one_row_group_without_null)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, int64_row_groups(4_i64, 7_i64, 0, 20))

      check_stats(reader, field,
        Arrow::PrimitiveArray(Int64).from_options([4_i64]),
        Arrow::PrimitiveArray(Int64).from_options([6_i64]),
        nulls: [0_u64],
        rows: [3_u64],
        min_exact: [true],
        max_exact: [true],
        data_pages: true)
    end

    it "extracts one row group with nulls and negatives (upstream test_one_row_group_with_null_and_negative)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, int64_row_groups(-1_i64, 5_i64, 2, 20))

      check_stats(reader, field,
        Arrow::PrimitiveArray(Int64).from_options([-1_i64]),
        Arrow::PrimitiveArray(Int64).from_options([4_i64]),
        nulls: [2_u64],
        rows: [8_u64],
        min_exact: [true],
        max_exact: [true],
        data_pages: true)
    end

    it "extracts two row groups with nulls (upstream test_two_row_group_with_null)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, int64_row_groups(4_i64, 17_i64, 2, 10))

      check_stats(reader, field,
        Arrow::PrimitiveArray(Int64).from_options([4_i64, 14_i64]),
        Arrow::PrimitiveArray(Int64).from_options([13_i64, 16_i64]),
        nulls: [0_u64, 2_u64],
        rows: [10_u64, 5_u64],
        min_exact: [true, true],
        max_exact: [true, true],
        data_pages: true)
    end

    it "extracts two row groups with all nulls in one (upstream test_two_row_groups_with_all_nulls_in_one)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, int64_row_groups(-2_i64, 2_i64, 4, 5))

      check_stats(reader, field,
        Arrow::PrimitiveArray(Int64).from_options([-2_i64, nil]),
        Arrow::PrimitiveArray(Int64).from_options([1_i64, nil]),
        nulls: [1_u64, 3_u64],
        rows: [5_u64, 3_u64],
        min_exact: [true, false],
        max_exact: [true, false],
        data_pages: true)
    end

    it "extracts multiple data pages with nulls and negatives (upstream test_multiple_data_pages_nulls_and_negatives)" do
      field = int64_field
      reader = write_stats_file(
        PType::INT64, field, int64_row_groups(-1_i64, 10_i64, 3, 20),
        page_row_count_limit: 4,
      )

      # page 0: [-1, 0, 1, 2]
      # page 1: [3, 4, 5, 6]
      # page 2: [7, 8, 9, null]
      # page 3: [null, null]
      check_stats(reader, field,
        Arrow::PrimitiveArray(Int64).from_options([-1_i64, 3_i64, 7_i64, nil]),
        Arrow::PrimitiveArray(Int64).from_options([2_i64, 6_i64, 9_i64, nil]),
        nulls: [0_u64, 0_u64, 1_u64, 2_u64],
        rows: [4_u64, 4_u64, 4_u64, 2_u64],
        row_groups: false,
        data_pages: true)
    end

    it "extracts an all-null data page (upstream test_data_page_stats_with_all_null_page)" do
      # A 4-row all-null column with a 4-row page limit gives one data page
      # whose statistics are present but null.
      field = int64_field
      reader = write_stats_file(
        PType::INT64, field, [[nil, nil, nil, nil] of Int64?],
        page_row_count_limit: 4,
      )

      check_stats(reader, field,
        Arrow::PrimitiveArray(Int64).new_null(1),
        Arrow::PrimitiveArray(Int64).new_null(1),
        nulls: [4_u64],
        rows: [4_u64],
        row_groups: false,
        data_pages: true)
    end
  end

  describe "primitive arrow type conversion" do
    it "extracts Int8/Int16/Int32/Int64 statistics (upstream test_int_8/16/32/64)" do
      i8 = Arrow::Field.new("col", Arrow::DataType.int8, true)
      reader = write_stats_file(PType::INT32, i8, [[-128_i32, 127_i32, nil], [0_i32]])
      check_stats(reader, i8,
        Arrow::PrimitiveArray(Int8).from_options([-128_i8, 0_i8]),
        Arrow::PrimitiveArray(Int8).from_options([127_i8, 0_i8]),
        nulls: [1_u64, 0_u64], data_pages: true)

      i16 = Arrow::Field.new("col", Arrow::DataType.int16, true)
      reader = write_stats_file(PType::INT32, i16, [[-32768_i32, 32767_i32], [5_i32]])
      check_stats(reader, i16,
        Arrow::PrimitiveArray(Int16).from_options([-32768_i16, 5_i16]),
        Arrow::PrimitiveArray(Int16).from_options([32767_i16, 5_i16]),
        nulls: [0_u64, 0_u64], data_pages: true)

      i32 = Arrow::Field.new("col", Arrow::DataType.int32, true)
      reader = write_stats_file(PType::INT32, i32, [[1_i32, 2_i32, nil], [-3_i32, 4_i32]])
      check_stats(reader, i32,
        Arrow::PrimitiveArray(Int32).from_options([1_i32, -3_i32]),
        Arrow::PrimitiveArray(Int32).from_options([2_i32, 4_i32]),
        nulls: [1_u64, 0_u64], data_pages: true)

      i64 = int64_field
      reader = write_stats_file(PType::INT64, i64, [[1_i64, 2_i64], [-3_i64, 4_i64]])
      check_stats(reader, i64,
        Arrow::PrimitiveArray(Int64).from_options([1_i64, -3_i64]),
        Arrow::PrimitiveArray(Int64).from_options([2_i64, 4_i64]),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts unsigned statistics with Rust's casts (upstream test_uint)" do
      u8 = Arrow::Field.new("col", Arrow::DataType.uint8, true)
      reader = write_stats_file(PType::INT32, u8, [[0_i32, 255_i32], [17_i32]],
        converted_type: Parquet::ConvertedType::UINT_8)
      check_stats(reader, u8,
        Arrow::PrimitiveArray(UInt8).from_options([0_u8, 17_u8]),
        Arrow::PrimitiveArray(UInt8).from_options([255_u8, 17_u8]),
        nulls: [0_u64, 0_u64], data_pages: true)

      u16 = Arrow::Field.new("col", Arrow::DataType.uint16, true)
      reader = write_stats_file(PType::INT32, u16, [[0_i32, 65535_i32], [17_i32]],
        converted_type: Parquet::ConvertedType::UINT_16)
      check_stats(reader, u16,
        Arrow::PrimitiveArray(UInt16).from_options([0_u16, 17_u16]),
        Arrow::PrimitiveArray(UInt16).from_options([65535_u16, 17_u16]),
        nulls: [0_u64, 0_u64], data_pages: true)

      u32 = Arrow::Field.new("col", Arrow::DataType.uint32, true)
      reader = write_stats_file(PType::INT32, u32, [[0_i32, -1_i32], [17_i32]],
        converted_type: Parquet::ConvertedType::UINT_32)
      check_stats(reader, u32,
        Arrow::PrimitiveArray(UInt32).from_options([0_u32, 17_u32]),
        Arrow::PrimitiveArray(UInt32).from_options([UInt32::MAX, 17_u32]),
        nulls: [0_u64, 0_u64], data_pages: true)

      u64 = Arrow::Field.new("col", Arrow::DataType.uint64, true)
      reader = write_stats_file(PType::INT64, u64, [[0_i64, -1_i64], [17_i64]],
        converted_type: Parquet::ConvertedType::UINT_64)
      check_stats(reader, u64,
        Arrow::PrimitiveArray(UInt64).from_options([0_u64, 17_u64]),
        Arrow::PrimitiveArray(UInt64).from_options([UInt64::MAX, 17_u64]),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts Float32/Float64 statistics (upstream test_float_32/64)" do
      f32 = Arrow::Field.new("col", Arrow::DataType.float32, true)
      reader = write_stats_file(PType::FLOAT, f32, [[-1.5_f32, 2.5_f32, nil], [0.0_f32]])
      check_stats(reader, f32,
        Arrow::PrimitiveArray(Float32).from_options([-1.5_f32, 0.0_f32]),
        Arrow::PrimitiveArray(Float32).from_options([2.5_f32, 0.0_f32]),
        nulls: [1_u64, 0_u64], data_pages: true)

      f64 = Arrow::Field.new("col", Arrow::DataType.float64, true)
      reader = write_stats_file(PType::DOUBLE, f64, [[-1.5_f64, 2.5_f64], [-0.0_f64]])
      check_stats(reader, f64,
        Arrow::PrimitiveArray(Float64).from_options([-1.5_f64, -0.0_f64]),
        Arrow::PrimitiveArray(Float64).from_options([2.5_f64, -0.0_f64]),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts Float16 statistics from 2-byte FLBA statistics (upstream test_float_16)" do
      field = Arrow::Field.new("col", Arrow::DataType.float16, true)
      # parquet stores FLOAT16 as little-endian 2-byte values; one value per row
      # group keeps min == max so the fixture is independent of the writer's
      # byte-wise ordering.
      one_and_a_half = Array(UInt8).new(2, 0_u8)
      one_and_a_half[1] = 0x3E_u8
      minus_two = Array(UInt8).new(2, 0_u8)
      minus_two[1] = 0xC0_u8
      reader = write_stats_file(PType::FIXED_LEN_BYTE_ARRAY, field, [
        [Parquet::FixedLenByteArray.new(one_and_a_half)],
        [Parquet::FixedLenByteArray.new(minus_two)],
      ], fixed_len: 2, truncate_length: 4096)

      one_and_a_half_f16 = Arrow::Float16.from_bits(0x3E00_u16)
      minus_two_f16 = Arrow::Float16.from_bits(0xC000_u16)
      check_stats(reader, field,
        Arrow::PrimitiveArray(Arrow::Float16).from_options([one_and_a_half_f16, minus_two_f16]).with_data_type(Arrow::DataType.float16),
        Arrow::PrimitiveArray(Arrow::Float16).from_options([one_and_a_half_f16, minus_two_f16]).with_data_type(Arrow::DataType.float16),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts Boolean statistics (upstream test_boolean)" do
      field = Arrow::Field.new("col", Arrow::DataType.boolean, true)
      reader = write_stats_file(PType::BOOLEAN, field, [[true, false, nil], [true]])

      check_stats(reader, field,
        Arrow::BooleanArray.from_options([false, true]),
        Arrow::BooleanArray.from_options([true, true]),
        nulls: [1_u64, 0_u64], data_pages: true)
    end

    it "extracts Binary statistics (upstream test_byte)" do
      field = Arrow::Field.new("col", Arrow::DataType.binary, true)
      values = [
        [Parquet::ByteArray.new("b"), Parquet::ByteArray.new("a"), nil],
        [Parquet::ByteArray.new("zzz")],
      ]
      reader = write_stats_file(PType::BYTE_ARRAY, field, values)

      check_stats(reader, field,
        Arrow::GenericBinaryArray(Int32).from_options(["a".to_slice, "zzz".to_slice]),
        Arrow::GenericBinaryArray(Int32).from_options(["b".to_slice, "zzz".to_slice]),
        nulls: [1_u64, 0_u64], data_pages: true)
    end

    it "extracts Utf8/LargeUtf8 statistics (upstream test_utf8)" do
      field = Arrow::Field.new("col", Arrow::DataType.utf8, true)
      values = [
        [Parquet::ByteArray.new("abc"), Parquet::ByteArray.new("def"), nil],
        [Parquet::ByteArray.new("aaa")],
      ]
      reader = write_stats_file(PType::BYTE_ARRAY, field, values)

      check_stats(reader, field,
        Arrow::GenericStringArray(Int32).from_options(["abc", "aaa"]),
        Arrow::GenericStringArray(Int32).from_options(["def", "aaa"]),
        nulls: [1_u64, 0_u64], data_pages: true)

      large = Arrow::Field.new("col", Arrow::DataType.large_utf8, true)
      reader = write_stats_file(PType::BYTE_ARRAY, large, values)
      check_stats(reader, large,
        Arrow::GenericStringArray(Int64).from_options(["abc", "aaa"]),
        Arrow::GenericStringArray(Int64).from_options(["def", "aaa"]),
        nulls: [1_u64, 0_u64], data_pages: true)
    end

    it "extracts Utf8View/BinaryView statistics (upstream test_utf8_view/test_binary_view)" do
      view = Arrow::Field.new("col", Arrow::DataType.utf8_view, true)
      reader = write_stats_file(PType::BYTE_ARRAY, view, [
        [Parquet::ByteArray.new("abc"), Parquet::ByteArray.new("def"), nil],
        [Parquet::ByteArray.new("aaa")],
      ])
      check_stats(reader, view,
        Arrow::StringViewArray.from_options(["abc", "aaa"]),
        Arrow::StringViewArray.from_options(["def", "aaa"]),
        nulls: [1_u64, 0_u64], data_pages: true)

      binary_view = Arrow::Field.new("col", Arrow::DataType.binary_view, true)
      reader = write_stats_file(PType::BYTE_ARRAY, binary_view, [
        [Parquet::ByteArray.new("b"), Parquet::ByteArray.new("a"), nil],
        [Parquet::ByteArray.new("zzz")],
      ])
      check_stats(reader, binary_view,
        Arrow::BinaryViewArray.from_options(["a".to_slice, "zzz".to_slice]),
        Arrow::BinaryViewArray.from_options(["b".to_slice, "zzz".to_slice]),
        nulls: [1_u64, 0_u64], data_pages: true)
    end

    it "extracts dictionary statistics as the value type (upstream test_dictionary)" do
      string_type = Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8)
      field = Arrow::Field.new("col", string_type, true)
      reader = write_stats_file(PType::BYTE_ARRAY, field, [
        [Parquet::ByteArray.new("abc"), Parquet::ByteArray.new("def"), nil],
        [Parquet::ByteArray.new("aaa"), Parquet::ByteArray.new("fffff")],
      ])

      check_stats(reader, field,
        Arrow::GenericStringArray(Int32).from_options(["abc", "aaa"]),
        Arrow::GenericStringArray(Int32).from_options(["def", "fffff"]),
        nulls: [1_u64, 0_u64], data_pages: true)

      int_type = Arrow::DataType.dictionary(Arrow::DataType.int8, Arrow::DataType.int64)
      int_field = Arrow::Field.new("col", int_type, true)
      reader = write_stats_file(PType::INT64, int_field, [
        [-100_i64, 0_i64, nil],
        [0_i64, 100_i64],
      ])
      check_stats(reader, int_field,
        Arrow::PrimitiveArray(Int64).from_options([-100_i64, 0_i64]),
        Arrow::PrimitiveArray(Int64).from_options([0_i64, 100_i64]),
        nulls: [1_u64, 0_u64], data_pages: true)
    end
  end

  describe "temporal arrow type conversion" do
    it "extracts Timestamp statistics for every unit and timezone (upstream test_timestamp)" do
      Arrow::TimeUnit.each do |unit|
        field = Arrow::Field.new("col", Arrow::DataType.timestamp(unit, "UTC"), true)
        reader = write_stats_file(PType::INT64, field, [[1_i64, 2_i64], [3_i64]])

        dt = Arrow::DataType.timestamp(unit, "UTC")
        check_stats(reader, field,
          Arrow::PrimitiveArray(Int64).from_options([1_i64, 3_i64]).with_data_type(dt),
          Arrow::PrimitiveArray(Int64).from_options([2_i64, 3_i64]).with_data_type(dt),
          nulls: [0_u64, 0_u64], data_pages: true)
      end
    end

    it "extracts Date32 and Date64 statistics (upstream test_dates_32/64)" do
      date32 = Arrow::Field.new("col", Arrow::DataType.date32, true)
      reader = write_stats_file(PType::INT32, date32, [[100_i32, 200_i32], [50_i32]])
      check_stats(reader, date32,
        Arrow::PrimitiveArray(Int32).from_options([100_i32, 50_i32]).with_data_type(Arrow::DataType.date32),
        Arrow::PrimitiveArray(Int32).from_options([200_i32, 50_i32]).with_data_type(Arrow::DataType.date32),
        nulls: [0_u64, 0_u64], data_pages: true)

      # Date64 from INT32 is scaled to milliseconds.
      date64_from_i32 = Arrow::Field.new("col", Arrow::DataType.date64, true)
      reader = write_stats_file(PType::INT32, date64_from_i32, [[1_i32, 2_i32], [3_i32]])
      millis = 24_i64 * 60 * 60 * 1000
      check_stats(reader, date64_from_i32,
        Arrow::PrimitiveArray(Int64).from_options([1_i64 * millis, 3_i64 * millis]).with_data_type(Arrow::DataType.date64),
        Arrow::PrimitiveArray(Int64).from_options([2_i64 * millis, 3_i64 * millis]).with_data_type(Arrow::DataType.date64),
        nulls: [0_u64, 0_u64], data_pages: true)

      date64 = Arrow::Field.new("col", Arrow::DataType.date64, true)
      reader = write_stats_file(PType::INT64, date64, [[100_i64, 200_i64], [50_i64]])
      check_stats(reader, date64,
        Arrow::PrimitiveArray(Int64).from_options([100_i64, 50_i64]).with_data_type(Arrow::DataType.date64),
        Arrow::PrimitiveArray(Int64).from_options([200_i64, 50_i64]).with_data_type(Arrow::DataType.date64),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts Time32/Time64 statistics (upstream test_time32_*/test_time64_*)" do
      Arrow::TimeUnit.each do |unit|
        case unit
        when Arrow::TimeUnit::Second, Arrow::TimeUnit::Millisecond
          field = Arrow::Field.new("col", Arrow::DataType.time32(unit), true)
          reader = write_stats_file(PType::INT32, field, [[1_i32, 2_i32], [3_i32]])
          dt = Arrow::DataType.time32(unit)
          check_stats(reader, field,
            Arrow::PrimitiveArray(Int32).from_options([1_i32, 3_i32]).with_data_type(dt),
            Arrow::PrimitiveArray(Int32).from_options([2_i32, 3_i32]).with_data_type(dt),
            nulls: [0_u64, 0_u64], data_pages: true)
        else
          field = Arrow::Field.new("col", Arrow::DataType.time64(unit), true)
          reader = write_stats_file(PType::INT64, field, [[1_i64, 2_i64], [3_i64]])
          dt = Arrow::DataType.time64(unit)
          check_stats(reader, field,
            Arrow::PrimitiveArray(Int64).from_options([1_i64, 3_i64]).with_data_type(dt),
            Arrow::PrimitiveArray(Int64).from_options([2_i64, 3_i64]).with_data_type(dt),
            nulls: [0_u64, 0_u64], data_pages: true)
        end
      end
    end
  end

  describe "decimal statistics" do
    it "extracts Decimal32 statistics from INT32 stats and byte-array stats (upstream test_decimal32)" do
      dt = Arrow::DataType.decimal32(9, 2)
      field = Arrow::Field.new("col", dt, true)
      reader = write_stats_file(PType::INT32, field, [[-100_i32, 100_i32, nil], [0_i32]])
      check_stats(reader, field,
        Arrow::PrimitiveArray(Int32).from_options([-100_i32, 0_i32]).with_data_type(dt),
        Arrow::PrimitiveArray(Int32).from_options([100_i32, 0_i32]).with_data_type(dt),
        nulls: [1_u64, 0_u64], data_pages: true)

      # FLBA statistics (parquet stores a 4-byte big-endian two's-complement
      # value). One value per row group keeps min == max, so the fixture does
      # not depend on the writer's byte-wise min/max ordering.
      flba = Arrow::DataType.decimal32(9, 2)
      field = Arrow::Field.new("col", flba, true)
      reader = write_stats_file(PType::FIXED_LEN_BYTE_ARRAY, field, [
        [Parquet::FixedLenByteArray.new([0xFF_u8, 0xFF_u8, 0xFF_u8, 0x9C_u8])],
        [Parquet::FixedLenByteArray.new([0x00_u8, 0x00_u8, 0x00_u8, 0x64_u8])],
      ], fixed_len: 4, truncate_length: 4096)
      check_stats(reader, field,
        Arrow::PrimitiveArray(Int32).from_options([-100_i32, 100_i32]).with_data_type(flba),
        Arrow::PrimitiveArray(Int32).from_options([-100_i32, 100_i32]).with_data_type(flba),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts Decimal64 statistics (upstream test_decimal64)" do
      dt = Arrow::DataType.decimal64(18, 4)
      field = Arrow::Field.new("col", dt, true)
      reader = write_stats_file(PType::INT64, field, [[-1_000_i64, 1_000_i64], [7_i64]])
      check_stats(reader, field,
        Arrow::PrimitiveArray(Int64).from_options([-1_000_i64, 7_i64]).with_data_type(dt),
        Arrow::PrimitiveArray(Int64).from_options([1_000_i64, 7_i64]).with_data_type(dt),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts Decimal128 statistics from INT32/INT64/FLBA stats (upstream test_decimal128)" do
      dt = Arrow::DataType.decimal128(38, 9)
      field = Arrow::Field.new("col", dt, true)
      reader = write_stats_file(PType::INT64, field, [[-1_i64, 1_i64], [0_i64]])
      check_stats(reader, field,
        Arrow::PrimitiveArray(Int128).from_options([-1_i128, 0_i128]).with_data_type(dt),
        Arrow::PrimitiveArray(Int128).from_options([1_i128, 0_i128]).with_data_type(dt),
        nulls: [0_u64, 0_u64], data_pages: true)

      # 128-bit FLBA statistics (big-endian two's complement, sign extended).
      high = Array(UInt8).new(16, 0xFF_u8)
      high[15] = 0x9C_u8
      low = Array(UInt8).new(16, 0_u8)
      low[15] = 0x64_u8
      reader = write_stats_file(PType::FIXED_LEN_BYTE_ARRAY, field, [
        [Parquet::FixedLenByteArray.new(high)],
        [Parquet::FixedLenByteArray.new(low)],
      ], fixed_len: 16, truncate_length: 4096)
      check_stats(reader, field,
        Arrow::PrimitiveArray(Int128).from_options([-100_i128, 100_i128]).with_data_type(dt),
        Arrow::PrimitiveArray(Int128).from_options([-100_i128, 100_i128]).with_data_type(dt),
        nulls: [0_u64, 0_u64], data_pages: true)
    end

    it "extracts Decimal256 statistics (upstream test_decimal_256)" do
      dt = Arrow::DataType.decimal256(76, 9)
      field = Arrow::Field.new("col", dt, true)
      reader = write_stats_file(PType::FIXED_LEN_BYTE_ARRAY, field, [
        [Parquet::FixedLenByteArray.new(low_32_bytes)],
      ], fixed_len: 32, truncate_length: 4096, embed_field: false)

      check_stats(reader, field,
        Arrow::PrimitiveArray(Arrow::I256).from_options([Arrow::I256.from_i128(100_i128)]).with_data_type(dt),
        Arrow::PrimitiveArray(Arrow::I256).from_options([Arrow::I256.from_i128(100_i128)]).with_data_type(dt),
        nulls: [0_u64], data_pages: true)
    end
  end

  describe "converter construction" do
    it "reports a missing column in the arrow schema (upstream test_column_not_found)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, [[1_i64]])

      expect_raises(Parquet::ArrowError, /Column 'missing' not found in schema for statistics conversion/) do
        PArrow::StatisticsConverter.try_new(
          "missing",
          Arrow::Schema.new(Arrow::Fields.new([field])),
          reader.metadata.file_metadata.schema_descr,
        )
      end
    end

    it "returns null arrays for a column missing from the parquet file (upstream test_column_non_existent)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, [[1_i64]])

      other = Arrow::Field.new("other", Arrow::DataType.int32, true)
      schema = Arrow::Schema.new(Arrow::Fields.new([field, other]))
      converter = PArrow::StatisticsConverter.try_new(
        "other", schema, reader.metadata.file_metadata.schema_descr
      )

      converter.parquet_column_index.should be_nil
      converter.arrow_field.should be(other)

      row_groups = reader.metadata.row_groups
      converter.row_group_mins(row_groups).should eq(Arrow::PrimitiveArray(Int32).new_null(1))
      converter.row_group_maxes(row_groups).should eq(Arrow::PrimitiveArray(Int32).new_null(1))
      converter.row_group_row_counts(row_groups).should be_nil
      converter.row_group_null_counts(row_groups).should eq(u64([nil]))
      converter.row_group_is_max_value_exact(row_groups).should eq(bools([nil]))
      converter.row_group_is_min_value_exact(row_groups).should eq(bools([nil]))
      converter.row_group_nan_counts(row_groups).should eq(u64([nil]))
      converter.row_group_distinct_counts(row_groups).should eq(u64([nil]))
    end

    it "resolves a nested field by leaf column index (upstream test_struct)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, [[1_i64, 2_i64], [3_i64]])

      converter = PArrow::StatisticsConverter.from_column_index(
        0, field, reader.metadata.file_metadata.schema_descr
      )
      converter.parquet_column_index.should eq(0)

      row_groups = reader.metadata.row_groups
      converter.row_group_mins(row_groups).should eq(Arrow::PrimitiveArray(Int64).from_options([1_i64, 3_i64]))
      converter.row_group_maxes(row_groups).should eq(Arrow::PrimitiveArray(Int64).from_options([2_i64, 3_i64]))

      expect_raises(Parquet::ArrowError, /Parquet column index 1 out of bounds, column count 1/) do
        PArrow::StatisticsConverter.from_column_index(
          1, field, reader.metadata.file_metadata.schema_descr
        )
      end
    end

    it "returns null statistics when the file has none (upstream test_missing_statistics)" do
      field = int64_field
      reader = write_stats_file(PType::INT64, field, [[1_i64, 2_i64], [nil]], statistics: ES::None)

      row_groups = reader.metadata.row_groups
      converter = converter_for(reader, field)
      converter.row_group_mins(row_groups).should eq(Arrow::PrimitiveArray(Int64).new_null(2))
      converter.row_group_maxes(row_groups).should eq(Arrow::PrimitiveArray(Int64).new_null(2))
      # Row counts still come from the row group metadata.
      converter.row_group_row_counts(row_groups).should eq(u64([2_u64, 1_u64]))
      converter.row_group_null_counts(row_groups).should eq(u64([nil, nil]))
    end

    it "treats missing null counts as unknown when asked (upstream missing_null_counts_as_zero)" do
      leaf = ST.primitive_type_builder("a", PType::INT32).build.as(ST)
      parquet_schema = Parquet::Schema::SchemaDescriptor.new(
        ST.group_type_builder("schema").with_fields([leaf]).build
      )

      # A boolean column "b" matched to the parquet INT32 leaf "a" whose
      # statistics have no null count.
      stats = Parquet::Statistics.boolean(nil, nil, nil, nil, false)
      column_chunk = Parquet::File::Metadata::ColumnChunkMetaData.builder(parquet_schema.column(0))
        .set_statistics(stats)
        .build
      row_group = Parquet::File::Metadata::RowGroupMetaData.builder(parquet_schema)
        .set_column_metadata([column_chunk])
        .build

      field = Arrow::Field.new("b", Arrow::DataType.boolean, true)
      converter = PArrow::StatisticsConverter.try_new(
        "b", Arrow::Schema.new(Arrow::Fields.new([field])), parquet_schema
      )

      # By default a missing null count is reported as 0 nulls.
      converter.row_group_null_counts([row_group]).should eq(u64([0_u64]))

      # Disabling the flag reports the unknown null count as null.
      strict = converter.with_missing_null_counts_as_zero(false)
      strict.row_group_null_counts([row_group]).should eq(u64([nil]))
    end

    it "reports min/max exactness for truncated statistics (upstream test_max_and_min_value_truncated)" do
      field = Arrow::Field.new("col", Arrow::DataType.utf8, true)
      reader = write_stats_file(PType::BYTE_ARRAY, field, [
        [Parquet::ByteArray.new("a" * 64 + "1"), Parquet::ByteArray.new("d" * 64 + "4")],
        [Parquet::ByteArray.new("e" * 64 + "5"), Parquet::ByteArray.new("i" * 64 + "6")],
        [Parquet::ByteArray.new("j"), Parquet::ByteArray.new("n" * 64 + "14")],
      ], truncate_length: 64)

      check_stats(reader, field,
        Arrow::GenericStringArray(Int32).from_options(["a" * 64, "e" * 64, "j"]),
        Arrow::GenericStringArray(Int32).from_options([
          "d" * 63 + "e",
          "i" * 63 + "j",
          "n" * 63 + "o",
        ]),
        nulls: [0_u64, 0_u64, 0_u64],
        rows: [2_u64, 2_u64, 2_u64],
        min_exact: [false, false, true],
        max_exact: [false, false, false])
    end

    it "reports NaN and distinct counts (upstream row_group_nan_counts/distinct_counts)" do
      field = Arrow::Field.new("col", Arrow::DataType.float64, true)
      reader = write_stats_file(PType::DOUBLE, field, [
        [1.0_f64, Float64::NAN, Float64::NAN],
        [2.0_f64, nil],
      ])
      row_groups = reader.metadata.row_groups
      converter = converter_for(reader, field)

      # NaN counts are written alongside the statistics when enabled.
      converter.row_group_nan_counts(row_groups).should eq(u64([2_u64, 0_u64]))

      # parquet-rs does not write distinct counts, so they are always unknown.
      converter.row_group_distinct_counts(row_groups).should eq(u64([nil, nil]))
    end
  end
end
