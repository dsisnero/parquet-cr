require "../../spec_helper"

# Ported from `parquet/src/record/reader.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: SNAPPY-compressed fixtures (`nulls.snappy.parquet`,
# `nested_maps.snappy.parquet`, `nested_lists.snappy.parquet`) are not readable
# (SNAPPY codec needs a Crystal shard); the projection cases here use the
# uncompressed `nonnullable.impala.parquet` schema instead.

private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias Row = Parquet::Record::Row
private alias Field = Parquet::Record::Field
private alias RTriplet = Parquet::Record

private def f(name : String, value : Field) : Tuple(String, Field)
  pair : Tuple(String, Field) = {name, value}
  pair
end

private def entry(key : Field, value : Field) : Tuple(Field, Field)
  pair : Tuple(Field, Field) = {key, value}
  pair
end

private def row(pairs : Array(Tuple(String, F))) : Row forall F
  Row.new(pairs)
end

private def list_of(values : Array(F)) : Field forall F
  Field::ListInternal.new(RTriplet.make_list(values))
end

private def group_of(pairs : Array(Tuple(String, F))) : Field forall F
  Field::Group.new(Row.new(pairs))
end

private def map_of(entries : Array(Tuple(F1, F2))) : Field forall F1, F2
  Field::MapInternal.new(RTriplet.make_map(entries))
end

private def open_reader(file_name : String) : FReader
  FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(file_name)))
end

private def test_file_reader_rows(file_name : String, schema : Parquet::Schema::Type?) : Array(Row)
  open_reader(file_name).get_row_iter(schema).to_a
end

describe Parquet::Record::RowIter do
  # Upstream `test_file_reader_rows_nonnullable`.
  it "reads non-nullable rows" do
    rows = test_file_reader_rows("nonnullable.impala.parquet", nil)
    empty_field = [] of Field
    expected = [
      row([
        f("ID", Field::Long.new(8)),
        f("Int_Array", list_of([Field::Int.new(-1)])),
        f("int_array_array", list_of([
          list_of([Field::Int.new(-1), Field::Int.new(-2)]),
          list_of(empty_field),
        ])),
        f("Int_Map", map_of([entry(Field::Str.new("k1"), Field::Int.new(-1))])),
        f("int_map_array", list_of([
          map_of([] of Tuple(Field, Field)),
          map_of([entry(Field::Str.new("k1"), Field::Int.new(1))]),
          map_of([] of Tuple(Field, Field)),
          map_of([] of Tuple(Field, Field)),
        ])),
        f("nested_Struct", group_of([
          f("a", Field::Int.new(-1)),
          f("B", list_of([Field::Int.new(-1)])),
          f("c", group_of([
            f("D", list_of([
              list_of([
                group_of([f("e", Field::Int.new(-1)), f("f", Field::Str.new("nonnullable"))]),
              ]),
            ])),
          ])),
          f("G", map_of([] of Tuple(Field, Field))),
        ])),
      ]),
    ]
    rows.should eq(expected)
  end

  # Upstream `test_file_reader_rows_nullable`.
  it "reads nullable rows" do
    rows = test_file_reader_rows("nullable.impala.parquet", nil)
    expected_first = row([
      f("id", Field::Long.new(1)),
      f("int_array", list_of([Field::Int.new(1), Field::Int.new(2), Field::Int.new(3)])),
      f("int_array_Array", list_of([
        list_of([Field::Int.new(1), Field::Int.new(2)]),
        list_of([Field::Int.new(3), Field::Int.new(4)]),
      ])),
      f("int_map", map_of([
        entry(Field::Str.new("k1"), Field::Int.new(1)),
        entry(Field::Str.new("k2"), Field::Int.new(100)),
      ])),
      f("int_Map_Array", list_of([map_of([entry(Field::Str.new("k1"), Field::Int.new(1))])])),
      f("nested_struct", group_of([
        f("A", Field::Int.new(1)),
        f("b", list_of([Field::Int.new(1)])),
        f("C", group_of([
          f("d", list_of([
            list_of([
              group_of([f("E", Field::Int.new(10)), f("F", Field::Str.new("aaa"))]),
              group_of([f("E", Field::Int.new(-10)), f("F", Field::Str.new("bbb"))]),
            ]),
            list_of([group_of([f("E", Field::Int.new(11)), f("F", Field::Str.new("c"))])]),
          ])),
        ])),
        f("g", map_of([
          entry(Field::Str.new("foo"), group_of([
            f("H", group_of([f("i", list_of([Field::Double.new(1.1)]))])),
          ])),
        ])),
      ])),
    ])
    rows.first.should eq(expected_first)
    rows.size.should eq(7)
  end

  # Upstream `test_file_reader_iter` / `test_file_reader_iter_projection`.
  it "iterates rows from a file and with a projection" do
    reader = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")))
    values = Parquet::Record::RowIter.from_file_into(reader).flat_map { |r| [r.get_int(0)] }.to_a
    values.should eq([4, 5, 6, 7, 2, 3, 0, 1])

    reader = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")))
    proj = Parquet::Schema.parse_message_type("message schema { OPTIONAL INT32 id; }")
    values = Parquet::Record::RowIter.from_file_into(reader).project(proj).flat_map { |r| [r.get_int(0)] }.to_a
    values.should eq([4, 5, 6, 7, 2, 3, 0, 1])
  end

  # Upstream `test_file_reader_iter_projection_err` (adapted).
  it "errors on an invalid projection" do
    proj = Parquet::Schema.parse_message_type(
      "message schema { REQUIRED INT32 key; REQUIRED BOOLEAN value; }"
    )
    reader = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("nonnullable.impala.parquet")))
    expect_raises(Parquet::GeneralError, "Root schema does not contain projection") do
      Parquet::Record::RowIter.from_file_into(reader).project(proj)
    end
  end

  # Upstream `test_iter_columns_in_row` / `test_into_columns_in_row`.
  it "iterates and extracts columns in a row" do
    r = row([f("c", Field::Double.new(1.0)), f("b", Field::Int.new(1))])
    result = r.get_column_iter.to_a
    result.map(&.[0]).should eq(["c", "b"])
    result.map(&.[1]).should eq([Field::Double.new(1.0), Field::Int.new(1)])

    r2 = row([f("a", Field::Str.new("My string")), f("b", Field::Int.new(1))])
    r2.into_columns.should eq([
      f("a", Field::Str.new("My string")),
      f("b", Field::Int.new(1)),
    ])
  end

  # Upstream `test_read_old_nested_list`.
  it "reads old nested lists" do
    rows = test_file_reader_rows("old_list_structure.parquet", nil)
    expected = [
      row([f("a", list_of([
        list_of([Field::Int.new(1), Field::Int.new(2)]),
        list_of([Field::Int.new(3), Field::Int.new(4)]),
      ]))]),
    ]
    rows.should eq(expected)
  end

  # Upstream `test_tree_reader_handle_repeated_fields_with_no_annotation`.
  it "reads repeated fields with no annotation" do
    rows = test_file_reader_rows("repeated_no_annotation.parquet", nil)
    rows.size.should eq(6)
    rows[0].should eq(row([f("id", Field::Int.new(1)), f("phoneNumbers", Field::Null.new)]))
    rows[5].should eq(row([
      f("id", Field::Int.new(6)),
      f("phoneNumbers", group_of([
        f("phone", list_of([
          group_of([f("number", Field::Long.new(1111111111)), f("kind", Field::Str.new("home"))]),
          group_of([f("number", Field::Long.new(2222222222)), f("kind", Field::Null.new)]),
          group_of([f("number", Field::Long.new(3333333333)), f("kind", Field::Str.new("mobile"))]),
        ])),
      ])),
    ]))
  end

  # Upstream `test_map_no_value`.
  it "reads maps with no value column" do
    rows = test_file_reader_rows("map_no_value.parquet", nil)
    rows.each do |r|
      cols = r.into_columns
      cols[1][1].should eq(cols[2][1])
    end
  end

  # Upstream `test_reader_iter_returns_error_when_num_records_exceeds_data`
  # (adapted to an uncompressed fixture).
  it "errors when the record count exceeds the data" do
    reader = open_reader("nonnullable.impala.parquet")
    row_group_reader = reader.get_row_group(0)
    actual_rows = row_group_reader.metadata.num_rows.to_i32
    tree = Parquet::Record::TreeBuilder.new.build(reader.metadata.file_metadata.schema_descr, row_group_reader)
    iter = Parquet::Record::ReaderIter.new(tree, actual_rows + 1)

    count = 0
    error = nil
    begin
      loop do
        value = iter.next
        break if value.is_a?(Iterator::Stop)

        count += 1
      end
    rescue ex
      error = ex
    end
    count.should eq(actual_rows)
    error.should_not be_nil
    error.not_nil!.message.not_nil!.includes?("Unexpected end of column data").should be_true
  end
end
