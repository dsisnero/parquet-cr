require "../../spec_helper"

# Ported from `parquet/src/file/writer.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The upstream `test_roundtrip`/`test_file_roundtrip`/`test_bytes_roundtrip`
# helpers read values back with the record API (`RowIter`); that is deferred to
# the record chunk, so the value read-back here uses the already-ported
# `ColumnReader` + `read_records`.

private alias FWriter = Parquet::File::SerializedFileWriter
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias WP = Parquet::File::Properties::WriterProperties
private alias KV = Parquet::File::Metadata::KeyValue
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PComp = Parquet::Compression
private alias PRep = Parquet::Repetition
private alias COrder = Parquet::ColumnOrder
private alias SOrder = Parquet::SortOrder
private alias Enc = Parquet::Encoding
private alias PLogical = Parquet::LogicalType
private alias PConverted = Parquet::ConvertedType

private def primitive(name : String, physical : PType, repetition : PRep = PRep::OPTIONAL) : ST
  ST.primitive_type_builder(name, physical).with_repetition(repetition).build.as(ST)
end

private def schema_of(fields : Array(ST)) : ST
  ST.group_type_builder("schema").with_fields(fields).build
end

# Reads all INT32 values from the first column of row group `i`.
private def read_int32_column(io : IO, row_group : Int32) : Array(Int32)
  reader = FReader.new(BytesCR.new(io.to_slice))
  column_reader = Parquet::Column.get_typed_column_reader(
    Int32, reader.get_row_group(row_group).get_column_reader(0)
  )
  values = [] of Int32
  loop do
    buffer = [] of Int32
    records, _values_read, _levels = column_reader.read_records(64, nil, nil, buffer)
    break if records == 0

    values.concat(buffer)
  end
  values
end

private def roundtrip_i32(io : IO, data : Array(Array(Int32)), compression : PComp) : Parquet::File::Metadata::ParquetMetaData
  schema = schema_of([primitive("col1", PType::INT32, PRep::REQUIRED)])
  props = WP.builder.set_compression(compression).build
  file_writer = FWriter.new(io, schema, props)
  rows = 0_i64

  data.each_with_index do |subset, idx|
    row_group_file_offset = file_writer.buf.bytes_written
    row_group_writer = file_writer.next_row_group
    if writer = row_group_writer.next_column
      rows += writer.typed(Int32).write_batch(subset, nil, nil).to_i64
      writer.close
    end
    last_group = row_group_writer.close
    flushed = file_writer.flushed_row_groups
    flushed.size.should eq(idx + 1)
    last_group.ordinal.should eq(idx)
    last_group.file_offset.should eq(row_group_file_offset)
    flushed[idx].should eq(last_group)
  end
  file_metadata = file_writer.close

  reader = FReader.new(BytesCR.new(io.to_slice))
  reader.num_row_groups.should eq(data.size)
  reader.metadata.file_metadata.num_rows.should eq(rows)

  data.each_with_index do |item, i|
    row_group_reader = reader.get_row_group(i)
    row_group_size = row_group_reader.metadata.total_byte_size
    uncompressed_size = row_group_reader.metadata.columns.sum(&.uncompressed_size)
    row_group_size.should eq(uncompressed_size)
  end

  data.each_with_index do |item, i|
    read_int32_column(io, i).should eq(item)
  end

  file_metadata
end

describe Parquet::File::SerializedFileWriter do
  # Upstream `test_file_writer_empty_file`.
  it "writes an empty file" do
    io = IO::Memory.new
    schema = schema_of([primitive("col1", PType::INT32)])
    props = WP.default
    writer = FWriter.new(io, schema, props)
    writer.close

    reader = FReader.new(BytesCR.new(io.to_slice))
    reader.metadata.file_metadata.num_rows.should eq(0)
    reader.num_row_groups.should eq(0)
  end

  # Upstream `test_row_group_writer_error_not_all_columns_written`.
  it "errors when not all columns are written" do
    io = IO::Memory.new
    schema = schema_of([primitive("col1", PType::INT32)])
    props = WP.default
    writer = FWriter.new(io, schema, props)
    row_group_writer = writer.next_row_group
    expect_raises(Parquet::GeneralError, "Column length mismatch: 1 != 0") do
      row_group_writer.close
    end
  end

  # Upstream `test_row_group_writer_num_records_mismatch`.
  it "errors when row counts mismatch across columns" do
    io = IO::Memory.new
    schema = schema_of([
      primitive("col1", PType::INT32, PRep::REQUIRED),
      primitive("col2", PType::INT32, PRep::REQUIRED),
    ])
    writer = FWriter.new(io, schema, WP.default)
    row_group_writer = writer.next_row_group

    col_writer = row_group_writer.next_column.not_nil!
    col_writer.typed(Int32).write_batch([1, 2, 3], nil, nil)
    col_writer.close

    col_writer = row_group_writer.next_column.not_nil!
    col_writer.typed(Int32).write_batch([1, 2], nil, nil)
    expect_raises(Parquet::GeneralError, "Incorrect number of rows, expected 3 != 2 rows") do
      col_writer.close
    end
  end

  # Upstream `test_file_writer_with_metadata`.
  it "writes key-value metadata" do
    io = IO::Memory.new
    schema = schema_of([primitive("col1", PType::INT32)])
    props = WP.builder
      .set_key_value_metadata([KV.new("key", "value")])
      .build
    writer = FWriter.new(io, schema, props)
    writer.close

    reader = FReader.new(BytesCR.new(io.to_slice))
    reader.metadata.file_metadata.key_value_metadata.try(&.size).should eq(1)
  end

  # Upstream `test_file_writer_column_orders_populated`.
  it "populates column orders" do
    io = IO::Memory.new
    col3 = ST.primitive_type_builder("col3", PType::FIXED_LEN_BYTE_ARRAY)
      .with_logical_type(PLogical::Float16.new)
      .with_length(2)
      .build.as(ST)
    col4 = ST.primitive_type_builder("col4", PType::BYTE_ARRAY)
      .with_logical_type(PLogical::String.new)
      .build.as(ST)
    nested = ST.group_type_builder("nested")
      .with_repetition(PRep::REQUIRED)
      .with_fields([col3, col4] of ST)
      .build.as(ST)
    col2 = ST.primitive_type_builder("col2", PType::FIXED_LEN_BYTE_ARRAY)
      .with_converted_type(PConverted::INTERVAL)
      .with_length(12)
      .build.as(ST)
    schema = ST.group_type_builder("schema")
      .with_fields([
        primitive("col1", PType::INT32),
        col2,
        nested,
        primitive("col5", PType::FLOAT),
        primitive("col6", PType::DOUBLE),
      ] of ST)
      .build
    writer = FWriter.new(io, schema, WP.default)
    writer.close

    reader = FReader.new(BytesCR.new(io.to_slice))
    actual = reader.metadata.file_metadata.column_orders
    actual.should_not be_nil
    actual.not_nil!.should eq([
      COrder.type_defined_order(SOrder::SIGNED),
      COrder.type_defined_order(SOrder::UNDEFINED),
      COrder::IEEE_754_TOTAL_ORDER,
      COrder.type_defined_order(SOrder::UNSIGNED),
      COrder::IEEE_754_TOTAL_ORDER,
      COrder::IEEE_754_TOTAL_ORDER,
    ])
  end

  # Upstream `test_bytes_writer_empty_row_groups`.
  it "round-trips no row groups" do
    roundtrip_i32(IO::Memory.new, Array(Array(Int32)).new, PComp::UNCOMPRESSED)
  end

  # Upstream `test_bytes_writer_single_row_group`.
  it "round-trips a single row group" do
    roundtrip_i32(IO::Memory.new, [[1, 2, 3, 4, 5]], PComp::UNCOMPRESSED)
  end

  # Upstream `test_bytes_writer_multiple_row_groups`.
  it "round-trips multiple row groups" do
    roundtrip_i32(
      IO::Memory.new,
      [[1, 2, 3, 4, 5], [1, 2, 3], [1], [1, 2, 3, 4, 5, 6]],
      PComp::UNCOMPRESSED
    )
  end

  # Upstream `test_bytes_writer_single_row_group_compressed`.
  it "round-trips a single compressed row group" do
    roundtrip_i32(IO::Memory.new, [[1, 2, 3, 4, 5]], PComp::SNAPPY)
  end

  # Upstream `test_bytes_writer_multiple_row_groups_compressed`.
  it "round-trips multiple compressed row groups" do
    roundtrip_i32(
      IO::Memory.new,
      [[1, 2, 3, 4, 5], [1, 2, 3], [1], [1, 2, 3, 4, 5, 6]],
      PComp::SNAPPY
    )
  end

  # Characterization (upstream only exercises SNAPPY here): the LZ4 codecs are
  # wired through `create_codec`, so a written file must read back.
  it "round-trips row groups compressed with lz4" do
    roundtrip_i32(IO::Memory.new, [[1, 2, 3, 4, 5]], PComp::LZ4_RAW)
    roundtrip_i32(
      IO::Memory.new,
      [[1, 2, 3, 4, 5], [1, 2, 3], [1], [1, 2, 3, 4, 5, 6]],
      PComp::LZ4
    )
  end

  # Characterization: ZSTD and BROTLI are wired through `create_codec` as well.
  it "round-trips row groups compressed with zstd and brotli" do
    roundtrip_i32(IO::Memory.new, [[1, 2, 3, 4, 5]], PComp.zstd)
    roundtrip_i32(
      IO::Memory.new,
      [[1, 2, 3, 4, 5], [1, 2, 3], [1], [1, 2, 3, 4, 5, 6]],
      PComp.brotli
    )
  end

  # Upstream `test_column_offset_index_file`.
  it "writes column and offset indexes" do
    io = IO::Memory.new
    metadata = roundtrip_i32(io, [[1, 2, 3, 4, 5]], PComp::UNCOMPRESSED)
    metadata.row_groups.each do |row_group|
      row_group.columns.each do |column_chunk|
        column_chunk.column_index_offset.should_not be_nil
        column_chunk.column_index_length.should_not be_nil
        column_chunk.offset_index_offset.should_not be_nil
        column_chunk.offset_index_length.should_not be_nil
      end
    end
  end

  # Upstream `test_boolean_roundtrip`.
  it "round-trips boolean values" do
    io = IO::Memory.new
    schema = schema_of([primitive("col1", PType::BOOLEAN, PRep::REQUIRED)])
    props = WP.builder.set_compression(PComp::UNCOMPRESSED).build
    writer = FWriter.new(io, schema, props)
    values = (0...2049).map { |idx| idx % 2 == 0 }
    row_group_writer = writer.next_row_group
    col_writer = row_group_writer.next_column.not_nil!
    col_writer.typed(Bool).write_batch(values, nil, nil)
    col_writer.close
    row_group_writer.close
    writer.close

    reader = FReader.new(BytesCR.new(io.to_slice))
    column_reader = Parquet::Column.get_typed_column_reader(
      Bool, reader.get_row_group(0).get_column_reader(0)
    )
    read_back = [] of Bool
    loop do
      buffer = [] of Bool
      records, _values_read, _levels = column_reader.read_records(2049, nil, nil, buffer)
      break if records == 0

      read_back.concat(buffer)
    end
    read_back.should eq(values)
  end
end
