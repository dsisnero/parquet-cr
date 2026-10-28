require "../../spec_helper"

# DataType/make_type and typed conversion contracts from data_type.rs,
# column/{reader,writer/mod}.rs and column/mod.rs's read/write example.
# apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b.
private alias TypedP = Parquet

private def typed_physical_schema(physical : Parquet::Type, nullable = true) : Parquet::Schema::Type
  field = Parquet::Schema::Type.primitive_type_builder("col", physical)
    .with_repetition(nullable ? Parquet::Repetition::OPTIONAL : Parquet::Repetition::REQUIRED)
    .with_length(physical == Parquet::Type::FIXED_LEN_BYTE_ARRAY ? 2 : -1).build
  Parquet::Schema::Type.group_type_builder("schema").with_fields([field.as(Parquet::Schema::Type)]).build
end

private def typed_physical_values(type : Bool.class)
  [true, false, true]
end

private def typed_physical_values(type : Int32.class)
  [Int32::MIN, 0, Int32::MAX]
end

private def typed_physical_values(type : Int64.class)
  [Int64::MIN, 0_i64, Int64::MAX]
end

private def typed_physical_values(type : TypedP::Int96.class)
  [TypedP::Int96.new, TypedP::Int96.new(StaticArray[1_u32, 2_u32, 3_u32]), TypedP::Int96.new(StaticArray[UInt32::MAX, 0_u32, 7_u32])]
end

private def typed_physical_values(type : Float32.class)
  [-5.5_f32, 0_f32, Float32::MAX]
end

private def typed_physical_values(type : Float64.class)
  [-5.5, 0.0, Float64::MAX]
end

private def typed_physical_values(type : TypedP::ByteArray.class)
  [TypedP::ByteArray.new(""), TypedP::ByteArray.new("hello"), TypedP::ByteArray.new(Bytes[0, 255])]
end

private def typed_physical_values(type : TypedP::FixedLenByteArray.class)
  [TypedP::FixedLenByteArray.new("ab"), TypedP::FixedLenByteArray.new("cd"), TypedP::FixedLenByteArray.new(TypedP::ByteArray.new(Bytes[0, 255]))]
end

describe "Typed physical columns" do
  {% begin %}
  {% types = [
       {TypedP::BoolType, Bool, "TypedP::Type::BOOLEAN"},
       {TypedP::Int32Type, Int32, "TypedP::Type::INT32"},
       {TypedP::Int64Type, Int64, "TypedP::Type::INT64"},
       {TypedP::Int96Type, TypedP::Int96, "TypedP::Type::INT96"},
       {TypedP::FloatType, Float32, "TypedP::Type::FLOAT"},
       {TypedP::DoubleType, Float64, "TypedP::Type::DOUBLE"},
       {TypedP::ByteArrayType, TypedP::ByteArray, "TypedP::Type::BYTE_ARRAY"},
       {TypedP::FixedLenByteArrayType, TypedP::FixedLenByteArray, "TypedP::Type::FIXED_LEN_BYTE_ARRAY"},
     ] %}
  {% for entry, index in types %}
    it "exposes {{entry[0]}}'s physical/native type and representation size" do
      {{entry[0]}}::PHYSICAL_TYPE.should eq {{entry[2].id}}
      {{entry[0]}}.get_physical_type.should eq {{entry[2].id}}
      {{entry[0]}}.get_type_size.should eq sizeof({{entry[1]}})
      {{entry[0]}}::T.should eq {{entry[1]}}
      {{entry[0]}}.new.should be_a(TypedP::DataType)
    end

    it "round trips {{entry[0]}} nullable values across row groups and both page versions" do
      values = typed_physical_values({{entry[1]}})
      [TypedP::File::Properties::WriterVersion::PARQUET_1_0, TypedP::File::Properties::WriterVersion::PARQUET_2_0].each do |version|
        output = IO::Memory.new
        props = TypedP::File::Properties::WriterProperties.builder.set_writer_version(version).set_write_batch_size(2).build
        file = TypedP::File::SerializedFileWriter.new(output, typed_physical_schema({{entry[2].id}}), props)
        2.times do
          group = file.next_row_group
          column = group.next_column || raise "column missing"
          untyped = column.untyped
          writer = column.typed({{entry[0]}})
          writer.same?(column.typed({{entry[1]}})).should be_true
          {{entry[0]}}.get_column_writer(untyped).same?(writer).should be_true
          {{entry[0]}}.get_column_writer_ref(untyped).same?(writer).should be_true
          {{entry[0]}}.get_column_writer_mut(untyped).same?(writer).should be_true
          TypedP::Column::Writer.get_typed_column_writer({{entry[0]}}, untyped).same?(writer).should be_true
          TypedP::Column::Writer.get_typed_column_writer_ref({{entry[0]}}, untyped).same?(writer).should be_true
          TypedP::Column::Writer.get_typed_column_writer_mut({{entry[0]}}, untyped).same?(writer).should be_true
          TypedP::Column::Writer.get_typed_column_writer_ref({{entry[1]}}, untyped).same?(writer).should be_true
          TypedP::Column::Writer.get_typed_column_writer_mut({{entry[1]}}, untyped).same?(writer).should be_true
          writer.write_batch(values, [1_i16, 0_i16, 1_i16, 1_i16], nil).should eq 3
          column.close
          group.close.num_rows.should eq 4
        end
        file.close.file_metadata.num_rows.should eq 8
        reader = TypedP::File::SerializedFileReader.new(TypedP::File::BytesChunkReader.new(output.to_slice))
        reader.num_row_groups.should eq 2
        2.times do |group|
          untyped = reader.get_row_group(group).get_column_reader(0)
          typed = TypedP::Column.get_typed_column_reader({{entry[0]}}, untyped)
          native = TypedP::Column.get_typed_column_reader({{entry[1]}}, untyped)
          typed.same?(native).should be_true
          {{entry[0]}}.get_column_reader(untyped).same?(typed).should be_true
          actual, definitions = [] of {{entry[1]}}, [] of Int16
          typed.read_records(16, definitions, nil, actual).should eq({4, 3, 4})
          actual.should eq values
          definitions.should eq [1_i16, 0_i16, 1_i16, 1_i16]
          typed.read_records(1, nil, nil, [] of {{entry[1]}}).should eq({0, 0, 0})
        end
      end
    end

    it "preserves optional and raising mismatch contracts for {{entry[0]}} against every physical type" do
      output = IO::Memory.new
      file = TypedP::File::SerializedFileWriter.new(output, typed_physical_schema({{entry[2].id}}, false), TypedP::File::Properties::WriterProperties.builder.build)
      group = file.next_row_group
      column = group.next_column || raise "column missing"
      untyped = column.untyped
      {% for other, other_index in types %}
        {% if other_index != index %}
          {{other[0]}}.get_column_writer(untyped).should be_nil
          {{other[0]}}.get_column_writer_ref(untyped).should be_nil
          {{other[0]}}.get_column_writer_mut(untyped).should be_nil
          message = "Failed to convert column writer into a typed column writer for `#{{{other[2].id}}}` type"
          expect_raises(TypedP::GeneralError, message) { column.typed({{other[0]}}) }
          expect_raises(TypedP::GeneralError, message) { TypedP::Column::Writer.get_typed_column_writer({{other[0]}}, untyped) }
          expect_raises(TypedP::GeneralError, message) { TypedP::Column::Writer.get_typed_column_writer_ref({{other[0]}}, untyped) }
          expect_raises(TypedP::GeneralError, message) { TypedP::Column::Writer.get_typed_column_writer_mut({{other[0]}}, untyped) }
        {% end %}
      {% end %}
      column.typed({{entry[0]}}).write_batch(typed_physical_values({{entry[1]}}), nil, nil)
      column.close
      group.close
      file.close
      reader = TypedP::File::SerializedFileReader.new(TypedP::File::BytesChunkReader.new(output.to_slice))
      source = reader.get_row_group(0).get_column_reader(0)
      {% for other, other_index in types %}
        {% if other_index != index %}
          {{other[0]}}.get_column_reader(source).should be_nil
          expect_raises(TypedP::GeneralError, "Failed to convert column reader into a typed column reader for `#{{{other[2].id}}}` type") do
            TypedP::Column.get_typed_column_reader({{other[0]}}, source)
          end
        {% end %}
      {% end %}
      # Failed conversions must leave the original column usable.
      actual = [] of {{entry[1]}}
      TypedP::Column.get_typed_column_reader({{entry[0]}}, source).read_records(3, nil, nil, actual).should eq({3, 3, 3})
      actual.should eq typed_physical_values({{entry[1]}})
    end
  {% end %}

  {% end %}

  it "ports column/mod.rs's nested-list example with exact records, values and levels" do
    schema = TypedP::Schema.parse_message_type("message schema { optional group values (LIST) { repeated group list { optional INT32 element; } } }")
    output = IO::Memory.new
    file = TypedP::File::SerializedFileWriter.new(output, schema, TypedP::File::Properties::WriterProperties.builder.build)
    group = file.next_row_group
    column = group.next_column || raise "column missing"
    column.typed(TypedP::Int32Type).write_batch([1, 2, 3], [3_i16, 3_i16, 3_i16, 2_i16, 2_i16], [0_i16, 1_i16, 0_i16, 1_i16, 1_i16]).should eq 3
    column.close
    group.close
    file.close
    reader = TypedP::File::SerializedFileReader.new(TypedP::File::BytesChunkReader.new(output.to_slice))
    typed = TypedP::Column.get_typed_column_reader(TypedP::Int32Type, reader.get_row_group(0).get_column_reader(0))
    actual, definitions, repetitions = [] of Int32, [] of Int16, [] of Int16
    typed.read_records(8, definitions, repetitions, actual).should eq({2, 3, 5})
    actual.should eq [1, 2, 3]
    definitions.should eq [3_i16, 3_i16, 3_i16, 2_i16, 2_i16]
    repetitions.should eq [0_i16, 1_i16, 0_i16, 1_i16, 1_i16]
  end
end

private def typed_level_writer(max_def : Int16, max_rep : Int16)
  field = TypedP::Schema::Type.primitive_type_builder("col", TypedP::Type::INT32).build
  descr = TypedP::Schema::ColumnDescriptor.new(field, max_def, max_rep, TypedP::Schema::ColumnPath.new("col"))
  pages = TypedP::File::SerializedPageWriter.new(TypedP::File::TrackedWrite.new(IO::Memory.new))
  writer = TypedP::Column::Writer.get_column_writer(descr, TypedP::File::Properties::WriterProperties.builder.build, pages)
  TypedP::Column::Writer.get_typed_column_writer_mut(TypedP::Int32Type, writer)
end

describe "Upstream typed writer level errors" do
  it "ports test_column_writer_inconsistent_def_rep_length" do
    error = expect_raises(TypedP::GeneralError) do
      typed_level_writer(1_i16, 1_i16).write_batch([1, 2, 3, 4], [1_i16, 1_i16, 1_i16], [0_i16, 0_i16])
    end
    error.message.should eq "Parquet error: Inconsistent length of definition and repetition levels: 3 != 2"
  end

  it "ports test_column_writer_invalid_def_levels" do
    error = expect_raises(TypedP::GeneralError) { typed_level_writer(1_i16, 0_i16).write_batch([1, 2, 3, 4], nil, nil) }
    error.message.should eq "Parquet error: Definition levels are required, because max definition level = 1"
  end

  it "ports test_column_writer_invalid_rep_levels" do
    error = expect_raises(TypedP::GeneralError) { typed_level_writer(0_i16, 1_i16).write_batch([1, 2, 3, 4], nil, nil) }
    error.message.should eq "Parquet error: Repetition levels are required, because max repetition level = 1"
  end

  it "ports test_column_writer_not_enough_values_to_write" do
    error = expect_raises(TypedP::GeneralError) { typed_level_writer(1_i16, 0_i16).write_batch([1, 2], [1_i16, 1_i16, 1_i16, 1_i16], nil) }
    error.message.should eq "Parquet error: Expected to write 4 values, but have only 2"
  end
end
