require "../../spec_helper"

# Ported from `parquet/src/schema/types.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Descriptor tests (`ColumnDescriptor`, `SchemaDescriptor`, build-tree), thrift
# conversion, parser/printer, and `roundtrip_schema` tests are ported with their
# features (see `plans/parity.md`).
alias ST = Parquet::Schema::Type
alias SPrimitiveType = Parquet::Schema::PrimitiveType
alias PPhys = Parquet::Type
alias PLT = Parquet::LogicalType
alias PCT = Parquet::ConvertedType
alias PRep = Parquet::Repetition
alias PTU = Parquet::TimeUnit

private def new_group(name : String, repetition : PRep, types : Array(ST)) : ST
  ST.group_type_builder(name).with_repetition(repetition).with_fields(types).build.as(ST)
end

private def primitive(name : String, physical : PPhys) : ST
  ST.primitive_type_builder(name, physical).build.as(ST)
end

describe Parquet::Schema::Type do
  # Upstream `test_primitive_type`.
  it "builds primitive types and validates their inputs" do
    tp = ST.primitive_type_builder("foo", PPhys::INT32)
      .with_logical_type(PLT.integer(32, true))
      .with_id(0)
      .build

    tp.should be_a(SPrimitiveType)
    tp.primitive?.should be_true
    tp.group?.should be_false
    basic_info = tp.get_basic_info
    basic_info.repetition.should eq(PRep::OPTIONAL)
    basic_info.logical_type_ref.should eq(PLT.integer(32, true))
    basic_info.converted_type.should eq(PCT::INT_32)
    basic_info.id.should eq(0)
    tp.get_physical_type.should eq(PPhys::INT32)

    # Illegal inputs with logical type
    expect_raises(Parquet::ParquetError, "Cannot annotate Integer(IntType { bit_width: 8, is_signed: true }) from INT64 for field 'foo'") do
      ST.primitive_type_builder("foo", PPhys::INT64)
        .with_repetition(PRep::REPEATED)
        .with_logical_type(PLT.integer(8, true))
        .build
    end

    # Illegal inputs with converted type
    expect_raises(Parquet::ParquetError, "BSON cannot annotate field 'foo' because it is not a BYTE_ARRAY field") do
      ST.primitive_type_builder("foo", PPhys::INT64)
        .with_repetition(PRep::REPEATED)
        .with_converted_type(PCT::BSON)
        .build
    end

    expect_raises(Parquet::ParquetError, "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY") do
      ST.primitive_type_builder("foo", PPhys::INT96)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_precision(-1).with_scale(-1)
        .build
    end

    expect_raises(Parquet::ParquetError, "DECIMAL logical type scale 32 must match self.scale -1 for field 'foo'") do
      ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT.decimal(32, 12))
        .with_precision(-1).with_scale(-1)
        .build
    end

    expect_raises(Parquet::ParquetError, "Invalid DECIMAL precision: -1") do
      ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_precision(-1).with_scale(-1)
        .build
    end

    expect_raises(Parquet::ParquetError, "Invalid DECIMAL precision: 0") do
      ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_precision(0).with_scale(-1)
        .build
    end

    expect_raises(Parquet::ParquetError, "Invalid DECIMAL scale: -1") do
      ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_precision(1).with_scale(-1)
        .build
    end

    expect_raises(Parquet::ParquetError, "Invalid DECIMAL: scale (2) cannot be greater than precision (1)") do
      ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_precision(1).with_scale(2)
        .build
    end

    # It is OK if precision == scale
    ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
      .with_repetition(PRep::REQUIRED)
      .with_converted_type(PCT::DECIMAL)
      .with_precision(1).with_scale(1)
      .build.should be_a(SPrimitiveType)

    expect_raises(Parquet::ParquetError, "Cannot represent INT32 as DECIMAL with precision 18") do
      ST.primitive_type_builder("foo", PPhys::INT32)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_precision(18).with_scale(2)
        .build
    end

    expect_raises(Parquet::ParquetError, "Cannot represent INT64 as DECIMAL with precision 32") do
      ST.primitive_type_builder("foo", PPhys::INT64)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_precision(32).with_scale(2)
        .build
    end

    expect_raises(Parquet::ParquetError, "Cannot represent FIXED_LEN_BYTE_ARRAY as DECIMAL with length 5 and precision 12. The max precision can only be 11") do
      ST.primitive_type_builder("foo", PPhys::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_length(5).with_precision(12).with_scale(2)
        .build
    end

    expect_raises(Parquet::ParquetError, "UINT_8 cannot annotate field 'foo' because it is not a INT32 field") do
      ST.primitive_type_builder("foo", PPhys::INT64)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::UINT_8)
        .build
    end

    expect_raises(Parquet::ParquetError, "TIME_MICROS cannot annotate field 'foo' because it is not a INT64 field") do
      ST.primitive_type_builder("foo", PPhys::INT32)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::TIME_MICROS)
        .build
    end

    expect_raises(Parquet::ParquetError, "INTERVAL cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(12) field") do
      ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::INTERVAL)
        .build
    end

    expect_raises(Parquet::ParquetError, "INTERVAL cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(12) field") do
      ST.primitive_type_builder("foo", PPhys::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::INTERVAL)
        .with_length(1)
        .build
    end

    expect_raises(Parquet::ParquetError, "ENUM cannot annotate field 'foo' because it is not a BYTE_ARRAY field") do
      ST.primitive_type_builder("foo", PPhys::INT32)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::ENUM)
        .build
    end

    expect_raises(Parquet::ParquetError, "MAP cannot be applied to primitive field 'foo'") do
      ST.primitive_type_builder("foo", PPhys::INT32)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::MAP)
        .build
    end

    expect_raises(Parquet::ParquetError, "Invalid FIXED_LEN_BYTE_ARRAY length: -1 for field 'foo'") do
      ST.primitive_type_builder("foo", PPhys::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_converted_type(PCT::DECIMAL)
        .with_length(-1)
        .build
    end

    ST.primitive_type_builder("foo", PPhys::FIXED_LEN_BYTE_ARRAY)
      .with_repetition(PRep::REQUIRED)
      .with_logical_type(PLT::Float16.new)
      .with_length(2)
      .build.should be_a(SPrimitiveType)

    expect_raises(Parquet::ParquetError, "Cannot annotate Float16 from FLOAT for field 'foo'") do
      ST.primitive_type_builder("foo", PPhys::FLOAT)
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::Float16.new)
        .with_length(2)
        .build
    end

    expect_raises(Parquet::ParquetError, "FLOAT16 cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(2) field") do
      ST.primitive_type_builder("foo", PPhys::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::Float16.new)
        .with_length(4)
        .build
    end

    expect_raises(Parquet::ParquetError, "UUID cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(16) field") do
      ST.primitive_type_builder("foo", PPhys::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::Uuid.new)
        .with_length(15)
        .build
    end

    # Unknown logical types are ok
    ST.primitive_type_builder("foo", PPhys::BYTE_ARRAY)
      .with_logical_type(PLT::UnknownField.new(100_i16))
      .build.should be_a(SPrimitiveType)
  end

  # Upstream `test_group_type`.
  it "builds group types" do
    f1 = ST.primitive_type_builder("f1", PPhys::INT32)
      .with_converted_type(PCT::INT_32).with_id(0).build.as(ST)
    f2 = ST.primitive_type_builder("f2", PPhys::BYTE_ARRAY)
      .with_converted_type(PCT::UTF8).with_id(1).build.as(ST)

    tp = ST.group_type_builder("foo")
      .with_repetition(PRep::REPEATED)
      .with_logical_type(PLT::List.new)
      .with_fields([f1, f2])
      .with_id(1)
      .build

    basic_info = tp.get_basic_info
    tp.group?.should be_true
    tp.primitive?.should be_false
    basic_info.repetition.should eq(PRep::REPEATED)
    basic_info.logical_type_ref.should eq(PLT::List.new)
    basic_info.converted_type.should eq(PCT::LIST)
    basic_info.id.should eq(1)
    tp.get_fields.size.should eq(2)
    tp.get_fields[0].name.should eq("f1")
    tp.get_fields[1].name.should eq("f2")
  end

  # Upstream `test_get_physical_type_panic`.
  it "raises when requesting the physical type of a group" do
    list = ST.group_type_builder("records").with_repetition(PRep::REPEATED).build
    expect_raises(Parquet::ParquetError) { list.get_physical_type }
  end

  # Upstream `test_get_physical_type_primitive`.
  it "returns the physical type of a primitive" do
    primitive("f", PPhys::INT64).get_physical_type.should eq(PPhys::INT64)
    primitive("f", PPhys::BYTE_ARRAY).get_physical_type.should eq(PPhys::BYTE_ARRAY)
  end

  # Upstream `test_check_contains_primitive_primitive`.
  it "checks containment for primitive/primitive" do
    f1 = primitive("f", PPhys::INT32)
    f2 = primitive("f", PPhys::INT32)
    f1.check_contains(f2).should be_true

    f1 = ST.primitive_type_builder("f", PPhys::INT32).with_converted_type(PCT::UINT_8).build
    f2 = ST.primitive_type_builder("f", PPhys::INT32).with_converted_type(PCT::UINT_16).build
    f1.check_contains(f2).should be_true

    f1 = primitive("f1", PPhys::INT32)
    f2 = primitive("f2", PPhys::INT32)
    f1.check_contains(f2).should be_false

    f1 = primitive("f", PPhys::INT32)
    f2 = primitive("f", PPhys::INT64)
    f1.check_contains(f2).should be_false

    f1 = ST.primitive_type_builder("f", PPhys::INT32).with_repetition(PRep::REQUIRED).build
    f2 = ST.primitive_type_builder("f", PPhys::INT32).with_repetition(PRep::OPTIONAL).build
    f1.check_contains(f2).should be_false
  end

  # Upstream `test_check_contains_group_group`.
  it "checks containment for group/group" do
    f1 = ST.group_type_builder("f").build
    f2 = ST.group_type_builder("f").build
    f1.check_contains(f2).should be_true
    f1.optional?.should be_false

    f1 = new_group("f", PRep::REPEATED, [primitive("f1", PPhys::INT32), primitive("f2", PPhys::INT64)])
    f2 = new_group("f", PRep::REPEATED, [primitive("f1", PPhys::INT32), primitive("f2", PPhys::INT64)])
    f1.check_contains(f2).should be_true

    f1 = new_group("f", PRep::REPEATED, [primitive("f1", PPhys::INT32), primitive("f2", PPhys::INT64)])
    f2 = new_group("f", PRep::REPEATED, [primitive("f2", PPhys::INT64)])
    f1.check_contains(f2).should be_true

    f1 = ST.group_type_builder("f1").build
    f2 = ST.group_type_builder("f2").build
    f1.check_contains(f2).should be_false

    f1 = ST.group_type_builder("f").with_repetition(PRep::OPTIONAL).build
    f2 = ST.group_type_builder("f").with_repetition(PRep::REPEATED).build
    f1.check_contains(f2).should be_false

    f1 = new_group("f", PRep::REPEATED, [primitive("f1", PPhys::INT32), primitive("f2", PPhys::INT64)])
    f2 = new_group("f", PRep::REPEATED, [primitive("f1", PPhys::INT32), primitive("f2", PPhys::BOOLEAN)])
    f1.check_contains(f2).should be_false

    f1 = new_group("f", PRep::REPEATED, [primitive("f1", PPhys::INT32), primitive("f2", PPhys::INT64)])
    f2 = new_group("f", PRep::REPEATED, [primitive("f3", PPhys::INT32)])
    f1.check_contains(f2).should be_false
  end

  # Upstream `test_check_contains_group_primitive`.
  it "checks containment for group/primitive" do
    f1 = ST.group_type_builder("f").build
    f2 = primitive("f", PPhys::INT64)
    f1.check_contains(f2).should be_false
    f2.check_contains(f1).should be_false

    f1 = new_group("f", PRep::REPEATED, [primitive("f1", PPhys::INT32)])
    f2 = primitive("f1", PPhys::INT32)
    f1.check_contains(f2).should be_false
    f2.check_contains(f1).should be_false

    f1 = new_group("a", PRep::REPEATED, [
      new_group("b", PRep::REPEATED, [primitive("c", PPhys::INT32)]),
      primitive("d", PPhys::INT64),
      primitive("e", PPhys::BOOLEAN),
    ])
    f2 = new_group("a", PRep::REPEATED, [
      new_group("b", PRep::REPEATED, [primitive("c", PPhys::INT32)]),
    ])
    f1.check_contains(f2).should be_true
    f2.check_contains(f1).should be_false
  end

  # Upstream `test_file_logical_type_*` (except `roundtrip`).
  it "validates FILE logical type groups" do
    all_fields = [
      file_field("uri", PPhys::BYTE_ARRAY, PLT::String.new),
      file_field("offset", PPhys::INT64, nil),
      file_field("size", PPhys::INT64, nil),
      file_field("content_type", PPhys::BYTE_ARRAY, PLT::String.new),
      file_field("checksum", PPhys::BYTE_ARRAY, PLT::String.new),
      file_field("inline", PPhys::BYTE_ARRAY, nil),
    ]

    ok = ST.group_type_builder("file_field")
      .with_repetition(PRep::REQUIRED)
      .with_logical_type(PLT::File.new)
      .with_fields(all_fields)
      .build
    ok.get_fields.size.should eq(6)

    ST.group_type_builder("file_field")
      .with_repetition(PRep::REQUIRED)
      .with_logical_type(PLT::File.new)
      .with_fields([file_field("uri", PPhys::BYTE_ARRAY, PLT::String.new)])
      .build.get_basic_info.logical_type_ref.should eq(PLT::File.new)

    ST.group_type_builder("inline_file")
      .with_repetition(PRep::REQUIRED)
      .with_logical_type(PLT::File.new)
      .with_fields([file_field("inline", PPhys::BYTE_ARRAY, nil)])
      .build.should be_a(Parquet::Schema::GroupType)

    ST.group_type_builder("empty_file")
      .with_repetition(PRep::REQUIRED)
      .with_logical_type(PLT::File.new)
      .with_fields([] of ST)
      .build.should be_a(Parquet::Schema::GroupType)

    expect_raises(Parquet::ParquetError, "FILE type group 'bad_file' contains unrecognized field 'unknown_field'. Valid fields are: uri, offset, size, content_type, checksum, inline") do
      ST.group_type_builder("bad_file")
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::File.new)
        .with_fields([
          file_field("uri", PPhys::BYTE_ARRAY, PLT::String.new),
          file_field("unknown_field", PPhys::BYTE_ARRAY, nil),
        ])
        .build
    end

    expect_raises(Parquet::ParquetError, "FILE type field 'uri' must be OPTIONAL in group 'required_uri'") do
      uri_field = ST.primitive_type_builder("uri", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::String.new)
        .build.as(ST)
      ST.group_type_builder("required_uri")
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::File.new)
        .with_fields([uri_field])
        .build
    end

    expect_raises(Parquet::ParquetError, "FILE type field 'size' in group 'bad_size' must have physical type INT64") do
      ST.group_type_builder("bad_size")
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::File.new)
        .with_fields([file_field("size", PPhys::BYTE_ARRAY, nil)])
        .build
    end

    expect_raises(Parquet::ParquetError, "FILE type field 'uri' in group 'bad_uri' must have logical type Some(String)") do
      ST.group_type_builder("bad_uri")
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::File.new)
        .with_fields([file_field("uri", PPhys::BYTE_ARRAY, nil)])
        .build
    end

    expect_raises(Parquet::ParquetError) do
      ST.primitive_type_builder("bad", PPhys::BYTE_ARRAY)
        .with_repetition(PRep::REQUIRED)
        .with_logical_type(PLT::File.new)
        .build
    end
  end
end

private def file_field(name : String, physical : PPhys, logical : PLT?) : ST
  ST.primitive_type_builder(name, physical)
    .with_repetition(PRep::OPTIONAL)
    .with_logical_type(logical)
    .build.as(ST)
end
