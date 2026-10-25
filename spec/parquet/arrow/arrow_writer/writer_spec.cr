require "../../../spec_helper"

# Upstream parquet/src/arrow/arrow_writer/mod.rs (arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b), with shared round-trip fixtures.
private alias PA = Parquet::Arrow
private alias DT = Arrow::DataType
private alias WP = Parquet::File::Properties::WriterProperties

private def writer_batch(array : Arrow::ArrayBase, nullable : Bool = true) : Arrow::RecordBatch
  field = Arrow::Field.new("a", array.data_type, nullable)
  Arrow::RecordBatch.try_new(Arrow::Schema.new(Arrow::Fields.from([field])), [array] of Arrow::ArrayBase)
end

private def read_writer_bytes(bytes : Bytes) : Array(Arrow::RecordBatch)
  file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(bytes))
  PA::ParquetRecordBatchReaderBuilder.try_new(file).build.to_a
end

private def roundtrip_writer(array : Arrow::ArrayBase, props : WP = WP.builder.build) : Nil
  batch = writer_batch(array)
  sink = IO::Memory.new
  writer = PA::ArrowWriter.try_new(sink, batch.schema, props)
  writer.write(batch)
  writer.close.file_metadata.num_rows.should eq array.len
  batches = read_writer_bytes(sink.to_slice)
  batches.sum(&.num_rows).should eq array.len
  offset = 0
  batches.each do |actual|
    actual.column(0).should eq array.slice(offset, actual.num_rows)
    offset += actual.num_rows
  end
end

describe "Arrow RecordBatch writer" do
  {% for type in [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64] %}
    it "round trips {{type}} and nulls" do
      roundtrip_writer(Arrow::PrimitiveArray({{type}}).from_nullable([{{type}}::MIN, nil, {{type}}.new(0), {{type}}::MAX] of {{type}}?))
    end
  {% end %}

  it "round trips booleans" do
    roundtrip_writer(Arrow::BooleanArray.from_nullable([true, nil, false]))
  end

  it "round trips strings through dictionary and plain encoding" do
    builder = Arrow::GenericStringBuilder(Int32).new
    ["hello", nil, "", "hello", "world"].each { |value| value ? builder.append_value(value) : builder.append_null }
    array = builder.finish
    [true, false].each { |dictionary| roundtrip_writer(array, WP.builder.set_dictionary_enabled(dictionary).build) }
  end

  it "splits a batch into row groups (test_row_group_limit_rows_only)" do
    batch = writer_batch(Arrow::PrimitiveArray(Int32).from_values((0...1000).to_a), false)
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, batch.schema, WP.builder.set_max_row_group_row_count(300).build)
    writer.write(batch)
    meta = writer.close
    meta.row_groups.map(&.num_rows).should eq [300, 300, 300, 100]
    read_writer_bytes(sink.to_slice).sum(&.num_rows).should eq 1000
  end

  it "tracks buffered rows and bytes and clears on flush (in_progress_accounting)" do
    batch = writer_batch(Arrow::PrimitiveArray(Int32).from_values([1, 2, 3, 4, 5]), false)
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, batch.schema)
    writer.in_progress_rows.should eq 0
    writer.in_progress_size.should eq 0
    writer.memory_size.should eq 0
    writer.bytes_written.should eq 4
    writer.write(batch)
    size = writer.in_progress_size
    memory = writer.memory_size
    size.should be > 0
    memory.should be >= size
    writer.write(batch)
    writer.in_progress_rows.should eq 10
    writer.in_progress_size.should be > size
    writer.memory_size.should be > memory
    writer.flush
    writer.in_progress_rows.should eq 0
    writer.memory_size.should eq 0
    writer.bytes_written.should be > 4
    writer.close
  end

  it "round trips nullable lists with null and empty elements" do
    array = Arrow::GenericListArray(Int32).from_iter_primitive(Int32, [[1, nil, 2], nil, [] of Int32?, ([3] of Int32?)] of Array(Int32?)?)
    roundtrip_writer(array)
  end

  it "rejects incompatible batches and writes after finish" do
    batch = writer_batch(Arrow::PrimitiveArray(Int32).from_values([1]))
    writer = PA::ArrowWriter.try_new(IO::Memory.new, batch.schema)
    other = writer_batch(Arrow::PrimitiveArray(Float64).from_values([1.0]))
    expect_raises(Parquet::ArrowError, "Incompatible type") { writer.write(other) }
    writer = PA::ArrowWriter.try_new(IO::Memory.new, batch.schema)
    writer.write(batch)
    writer.finish
    expect_raises(Parquet::GeneralError) { writer.write(batch) }
  end
end

# Sparse opaque handles reproduce upstream custom_page_store_is_byte_identical_to_default.
private class WriterRecordingFactory < Parquet::Column::PageStoreFactory
  property puts = 0
  property bytes_put = 0_i64
  property takes = 0
  getter contexts = [] of Parquet::Column::PageStoreArgs

  def create(args : Parquet::Column::PageStoreArgs) : Parquet::Column::PageStore
    @contexts << args
    WriterRecordingStore.new(self)
  end
end

private class WriterRecordingStore < Parquet::Column::PageStore
  @blobs = {} of UInt64 => Bytes
  @next = 1000_u64

  def initialize(@factory : WriterRecordingFactory)
  end

  def put(value : Bytes) : Parquet::Column::PageKey
    key = @next
    @next += 13
    @blobs[key] = value
    @factory.puts += 1
    @factory.bytes_put += value.size
    Parquet::Column::PageKey.new(key)
  end

  def take(key : Parquet::Column::PageKey) : Bytes
    @factory.takes += 1
    @blobs.delete(key.get) || raise "Unknown key"
  end
end

private def writer_strings(values : Array(String?)) : Arrow::ArrayBase
  builder = Arrow::GenericStringBuilder(Int32).new
  values.each { |value| value ? builder.append_value(value) : builder.append_null }
  builder.finish
end

describe "Arrow writer integrated parity" do
  it "routes dictionary and data pages through a sparse custom store with byte-identical output" do
    array = writer_strings(["alpha", "beta", nil, "alpha", "beta", "alpha", "beta"] of String?)
    batch = writer_batch(array)
    props = WP.builder.set_max_row_group_row_count(3).set_data_page_row_count_limit(1).build
    reference = IO::Memory.new
    default = PA::ArrowWriter.try_new(reference, batch.schema, props)
    default.write(batch)
    default.close
    sink = IO::Memory.new
    factory = WriterRecordingFactory.new
    options = PA::ArrowWriterOptions.new.with_properties(props).with_page_store_factory(factory)
    writer = PA::ArrowWriter.try_new_with_options(sink, batch.schema, options)
    writer.write(batch)
    metadata = writer.close
    sink.to_slice.should eq reference.to_slice
    factory.puts.should be > 0
    factory.puts.should eq factory.takes
    factory.contexts.size.should eq 3
    factory.contexts.map(&.column_index).should eq [0, 0, 0]
    factory.bytes_put.should eq metadata.row_groups.sum(0_i64) { |group| group.column(0).compressed_size }
    metadata.row_groups.all? { |group| !group.column(0).dictionary_page_offset.nil? }.should be_true
    read_writer_bytes(sink.to_slice).sum(&.num_rows).should eq 7
  end

  it "supports column-level encoding and splicing via the public factory" do
    array = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3])
    batch = writer_batch(array)
    sink = IO::Memory.new
    outer = PA::ArrowWriter.try_new(sink, batch.schema)
    file, factory = outer.into_serialized_writer
    columns = factory.create_column_writers(0)
    leaf = PA.compute_leaves(batch.schema.field(0), array)[0]
    columns[0].write(leaf)
    columns[0].get_estimated_total_bytes.should be > 0
    chunk = columns[0].close
    chunk.close_mut.column_index = nil
    group = file.next_row_group
    chunk.append_to_row_group(group)
    group.close
    file.close
    read_writer_bytes(sink.to_slice)[0].column(0).should eq array
  end

  it "preserves metadata, custom root and explicit flush boundaries" do
    batch = writer_batch(Arrow::PrimitiveArray(Int32).from_values([1, 2]))
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new_with_options(sink, batch.schema, PA::ArrowWriterOptions.new.with_schema_root("custom"))
    writer.flush
    writer.write(batch)
    writer.flush
    writer.flushed_row_groups.size.should eq 1
    writer.write(batch)
    writer.append_key_value_metadata(Parquet::File::Metadata::KeyValue.new("extra", "value"))
    meta = writer.finish
    meta.row_groups.map(&.num_rows).should eq [2, 2]
    meta.file_metadata.key_value_metadata.try(&.any? { |entry| entry.key == "extra" && entry.value == "value" }).should be_true
    meta.file_metadata.schema_descr.root_schema.name.should eq "custom"
    expect_raises(Parquet::GeneralError) { writer.finish }
  end

  it "can omit Arrow metadata and finalize into the original sink" do
    batch = writer_batch(Arrow::PrimitiveArray(Int32).from_values([1]))
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new_with_options(sink, batch.schema, PA::ArrowWriterOptions.new.with_skip_arrow_metadata(true))
    writer.inner.same?(sink).should be_true
    writer.write(batch)
    writer.sync
    writer.into_inner.same?(sink).should be_true
    file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(sink.to_slice))
    file.metadata.file_metadata.key_value_metadata.try(&.any? { |entry| entry.key == PA::ARROW_SCHEMA_META_KEY }).should_not be_true
    read_writer_bytes(sink.to_slice)[0].column(0).should eq batch.column(0)
  end

  it "writes an empty schema without row groups" do
    schema = Arrow::Schema.new(Arrow::Fields.from([] of Arrow::Field))
    batch = Arrow::RecordBatch.try_new_with_options(schema, [] of Arrow::ArrayBase, Arrow::RecordBatchOptions.new(row_count: 0))
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, schema)
    writer.write(batch)
    writer.close.row_groups.should be_empty
    read_writer_bytes(sink.to_slice).should be_empty
  end

  [1, 2, 300].each do |limit|
    it "aggregates batches iteratively under row limit #{limit}" do
      array = Arrow::PrimitiveArray(Int32).from_values((0...1000).to_a)
      batch = writer_batch(array)
      sink = IO::Memory.new
      writer = PA::ArrowWriter.try_new(sink, batch.schema, WP.builder.set_max_row_group_row_count(limit).build)
      writer.write(batch.slice(0, 400))
      writer.write(batch.slice(400, 600))
      groups = writer.close.row_groups
      groups.sum(&.num_rows).should eq 1000
      groups.all? { |group| group.num_rows <= limit }.should be_true
      read_writer_bytes(sink.to_slice).sum(&.num_rows).should eq 1000
    end
  end

  it "keeps unlimited row groups together" do
    batch = writer_batch(Arrow::PrimitiveArray(Int32).from_values((0...1000).to_a))
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, batch.schema, WP.builder.set_max_row_group_row_count(nil).set_max_row_group_bytes(nil).build)
    3.times { writer.write(batch) }
    writer.close.row_groups.map(&.num_rows).should eq [3000]
  end

  it "applies byte limits across batches and flushes an already oversized group" do
    array = writer_strings(Array(String?).new(100) { |index| "#{index}".rjust(100, 'x') })
    batch = writer_batch(array)
    sink = IO::Memory.new
    props = WP.builder.set_dictionary_enabled(false).set_max_row_group_row_count(nil).set_max_row_group_bytes(500).build
    writer = PA::ArrowWriter.try_new(sink, batch.schema, props)
    3.times { writer.write(batch) }
    groups = writer.close.row_groups
    groups.size.should be > 1
    groups.sum(&.num_rows).should eq 300
    read_writer_bytes(sink.to_slice).sum(&.num_rows).should eq 300
  end

  {% for type in [Int32, Int64, Int128] %}
    {% for precision in (type == Int32 ? [8] : (type == Int64 ? [8, 16] : [8, 16, 30])) %}
      it "round trips {{type}} decimals of precision {{precision}}" do
        type = DT.{{ (type == Int32 ? "decimal32" : (type == Int64 ? "decimal64" : "decimal128")).id }}({{precision}}, 2)
        array = Arrow::PrimitiveArray({{type}}).from_nullable([{{type}}.new(-123), nil, {{type}}.new(456)] of {{type}}?).with_data_type(type)
        roundtrip_writer(array)
      end
    {% end %}
  {% end %}

  it "round trips Decimal256 stored as fixed-width big-endian bytes" do
    array = Arrow::PrimitiveArray(Arrow::I256).from_nullable([Arrow::I256.new(-123), nil, Arrow::I256.new(456)]).with_data_type(DT.decimal256(60, 3))
    roundtrip_writer(array)
  end

  {% for unit in [Arrow::TimeUnit::Second, Arrow::TimeUnit::Millisecond, Arrow::TimeUnit::Microsecond, Arrow::TimeUnit::Nanosecond] %}
    it "round trips {{unit}} timestamps with and without timezone" do
      [nil, "UTC"].each do |timezone|
        array = Arrow::PrimitiveArray(Int64).from_nullable([-123_i64, nil, 456_i64]).with_data_type(DT.timestamp(Arrow::TimeUnit.from_value({{unit}}), timezone))
        roundtrip_writer(array)
      end
    end
  {% end %}

  it "round trips dates and times without changing the stored units" do
    [DT.date32, DT.time32(Arrow::TimeUnit::Second), DT.time32(Arrow::TimeUnit::Millisecond)].each do |type|
      roundtrip_writer(Arrow::PrimitiveArray(Int32).from_nullable([0, nil, 123]).with_data_type(type))
    end
    [DT.date64, DT.time64(Arrow::TimeUnit::Microsecond), DT.time64(Arrow::TimeUnit::Nanosecond)].each do |type|
      roundtrip_writer(Arrow::PrimitiveArray(Int64).from_nullable([0_i64, nil, 123_i64]).with_data_type(type))
    end
  end

  it "round trips structs with parent masks and nested lists" do
    values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4])
    lists = Arrow::GenericListArray(Int32).from_iter_primitive(Int32, [[1, nil], nil, [] of Int32?, ([2] of Int32?)] of Array(Int32?)?)
    fields = Arrow::Fields.from([Arrow::Field.new("id", DT.int32, true), Arrow::Field.new("items", lists.data_type, true)])
    array = Arrow::StructArray.new(fields, [values, lists] of Arrow::ArrayBase, Arrow::NullBuffer.from_slice([true, false, true, true]))
    roundtrip_writer(array)
    roundtrip_writer(array.slice(1, 3))
  end

  {% for key in [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64] %}
    it "writes {{key}} dictionaries with logical-null values" do
      values = writer_strings(["first", nil, "last"] of String?)
      keys = Arrow::PrimitiveArray({{key}}).from_nullable([{{key}}.new(0), {{key}}.new(1), nil, {{key}}.new(2)] of {{key}}?)
      array = Arrow::DictionaryArray({{key}}).try_new(keys, values)
      batch = writer_batch(array)
      sink = IO::Memory.new
      writer = PA::ArrowWriter.try_new(sink, batch.schema)
      writer.write(batch)
      writer.close
      actual = read_writer_bytes(sink.to_slice)[0].column(0)
      Arrow.cast(actual, DT.utf8).should eq writer_strings(["first", nil, nil, "last"] of String?)
    end
  {% end %}

  {% for run in [Int16, Int32, Int64] %}
    it "expands sliced {{run}} run arrays while writing" do
      array = Arrow::RunArray({{run}}).try_new(Arrow::PrimitiveArray({{run}}).from_values([{{run}}.new(2), {{run}}.new(5)]), Arrow::PrimitiveArray(Int32).from_nullable([1, nil])).slice(1, 3)
      batch = writer_batch(array)
      sink = IO::Memory.new
      writer = PA::ArrowWriter.try_new(sink, batch.schema)
      writer.write(batch)
      writer.close
      read_writer_bytes(sink.to_slice)[0].column(0).should eq Arrow::PrimitiveArray(Int32).from_nullable([1, nil, nil])
    end
  {% end %}

  it "writes exact distinct-value statistics only when requested" do
    array = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 2, 1, 3, 3])
    batch = writer_batch(array)
    [false, true].each do |enabled|
      sink = IO::Memory.new
      props = WP.builder.set_write_row_group_number_distinct_values(enabled).build
      writer = PA::ArrowWriter.try_new(sink, batch.schema, props)
      writer.write(batch)
      writer.write(batch)
      stats = writer.close.row_groups[0].column(0).statistics || raise "Missing statistics"
      stats.distinct_count_opt.should eq(enabled ? 3_u64 : nil)
    end
  end
end

describe "Arrow writer encoding and nested matrix" do
  {% for builder_type in [Arrow::GenericStringBuilder(Int32), Arrow::GenericStringBuilder(Int64), Arrow::StringViewBuilder, Arrow::GenericBinaryBuilder(Int32), Arrow::GenericBinaryBuilder(Int64), Arrow::BinaryViewBuilder] %}
    it "encodes {{builder_type}} with dictionary fallback and byte budgets" do
      builder = {{builder_type}}.new
      {% if builder_type.stringify.includes?("String") %}
        30.times { |index| builder.append_value("value #{index}") }
      {% else %}
        30.times { |index| builder.append_value("value #{index}".to_slice) }
      {% end %}
      builder.append_null
      array = builder.finish
      props = WP.builder.set_dictionary_page_size_limit(8).set_data_page_size_limit(20).set_write_batch_size(1).build
      roundtrip_writer(array, props)
    end
  {% end %}

  it "round trips Float16 and fixed-size binary" do
    array = Arrow::PrimitiveArray(Arrow::Float16).from_nullable([Arrow::Float16.from_f32(-1.5_f32), nil, Arrow::Float16.from_f32(2.25_f32)])
    roundtrip_writer(array)
    builder = Arrow::FixedSizeBinaryBuilder.new(3)
    builder.append_value(Bytes[0, 255, 1])
    builder.append_null
    builder.append_value(Bytes[2, 3, 4])
    roundtrip_writer(builder.finish)
  end

  {% for offset in [Int32, Int64] %}
    it "writes sliced {{offset}} list views with nonmonotonic offsets" do
      values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4])
      field = Arrow::Field.new("item", DT.int32, true)
      array = Arrow::GenericListViewArray({{offset}}).try_new(field,
        Arrow::ScalarBuffer({{offset}}).from([{{offset}}.new(2), {{offset}}.new(0), {{offset}}.new(1)]),
        Arrow::ScalarBuffer({{offset}}).from([{{offset}}.new(2), {{offset}}.new(2), {{offset}}.new(0)]), values)
      roundtrip_writer(array)
      roundtrip_writer(array.slice(1, 2))
    end
  {% end %}

  it "writes fixed-size lists and nested lists with masked parents" do
    values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4, nil, 6])
    field = Arrow::Field.new("item", DT.int32, true)
    array = Arrow::FixedSizeListArray.new(field, 2, values, Arrow::NullBuffer.from_slice([true, false, true]))
    roundtrip_writer(array)
    inner = Arrow::GenericListArray(Int32).from_iter_primitive(Int32, [[1, nil], nil, [] of Int32?, ([2] of Int32?)] of Array(Int32?)?)
    outer = Arrow::GenericListArray(Int32).new(Arrow::Field.new("item", inner.data_type, true), Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 2, 2, 4])), inner)
    roundtrip_writer(outer)
  end

  it "writes maps with null values and null or empty maps" do
    keys = writer_strings(["a", "b", "c"] of String?)
    values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3])
    fields = Arrow::Fields.from([Arrow::Field.new("keys", DT.utf8, false), Arrow::Field.new("values", DT.int32, true)])
    entries = Arrow::StructArray.new(fields, [keys, values] of Arrow::ArrayBase)
    field = Arrow::Field.new("entries", entries.data_type, false)
    array = Arrow::MapArray.new(field, Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 2, 2, 2, 3])), entries, Arrow::NullBuffer.from_slice([true, false, true, true]))
    roundtrip_writer(array)
  end

  it "accepts compatible string representations across writes" do
    first = writer_strings(["a", "b"] of String?)
    builder = Arrow::GenericStringBuilder(Int64).new
    builder.append_value("c")
    builder.append_value("d")
    second = builder.finish
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, writer_batch(first).schema)
    writer.write(writer_batch(first))
    writer.write(writer_batch(second))
    writer.close
    read_writer_bytes(sink.to_slice)[0].column(0).should eq writer_strings(["a", "b", "c", "d"] of String?)
  end

  it "coerces Date64 to whole days with truncation toward zero" do
    array = Arrow::PrimitiveArray(Int64).from_nullable([-86_400_001_i64, nil, 86_400_001_i64]).with_data_type(DT.date64)
    batch = writer_batch(array)
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, batch.schema, WP.builder.set_coerce_types(true).build)
    writer.write(batch)
    writer.close
    read_writer_bytes(sink.to_slice)[0].column(0).should eq Arrow::PrimitiveArray(Int64).from_nullable([-86_400_000_i64, nil, 86_400_000_i64]).with_data_type(DT.date64)
  end

  it "makes deterministic content-defined pages across writes and row groups" do
    array = Arrow::PrimitiveArray(Int32).from_values((0...1000).map { |index| index % 31 })
    props = WP.builder.set_content_defined_chunking(Parquet::File::Properties::CdcOptions.new(64, 128, 0)).set_max_row_group_row_count(300).build
    roundtrip_writer(array, props)
    lists = Arrow::GenericListArray(Int32).from_iter_primitive(Int32, Array(Array(Int32?)?).new(50) { |index| index % 3 == 0 ? nil : ([index, nil, index + 1] of Int32?) })
    roundtrip_writer(lists, props)
  end

  it "handles all-null primitive, string and list columns" do
    roundtrip_writer(Arrow::PrimitiveArray(Int32).from_nullable([nil, nil] of Int32?))
    roundtrip_writer(writer_strings([nil, nil] of String?))
    roundtrip_writer(Arrow::GenericListArray(Int32).from_iter_primitive(Int32, [nil, nil] of Array(Int32?)?))
    roundtrip_writer(Arrow::NullArray.new(3))
  end
end

describe "Arrow writer remaining public contracts" do
  it "writes Decimal256 when Arrow schema metadata is explicitly disabled" do
    array = Arrow::PrimitiveArray(Arrow::I256).from_nullable([Arrow::I256.new(-123), nil, Arrow::I256.new(456)]).with_data_type(DT.decimal256(60, 3))
    batch = writer_batch(array)
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new_with_options(sink, batch.schema, PA::ArrowWriterOptions.new.with_skip_arrow_metadata(true))
    writer.write(batch)
    writer.close
    read_writer_bytes(sink.to_slice)[0].column(0).should eq array
  end

  it "round trips YearMonth and DayTime intervals in the Parquet 12-byte layout" do
    months = Arrow::PrimitiveArray(Int32).from_nullable([-12, nil, 123]).with_data_type(DT.interval(Arrow::IntervalUnit::YearMonth))
    roundtrip_writer(months)
    days = Arrow::PrimitiveArray(Int64).from_nullable([Arrow.interval_day_time(-2, -500), nil, Arrow.interval_day_time(3, 1000)]).with_data_type(DT.interval(Arrow::IntervalUnit::DayTime))
    roundtrip_writer(days)
  end

  it "rejects MonthDayNano intervals rather than dropping nanos" do
    array = Arrow::PrimitiveArray(Int128).from_values([Arrow.interval_month_day_nano(1, 2, 3)]).with_data_type(DT.interval(Arrow::IntervalUnit::MonthDayNano))
    batch = writer_batch(array)
    writer = PA::ArrowWriter.try_new(IO::Memory.new, batch.schema)
    expect_raises(Parquet::NYIError) { writer.write(batch) }
  end

  it "round trips durations for all time units" do
    [Arrow::TimeUnit::Second, Arrow::TimeUnit::Millisecond, Arrow::TimeUnit::Microsecond, Arrow::TimeUnit::Nanosecond].each do |unit|
      roundtrip_writer(Arrow::PrimitiveArray(Int64).from_nullable([-123_i64, nil, 456_i64]).with_data_type(DT.duration(unit)))
    end
  end

  it "accepts an explicit compatible Parquet schema" do
    batch = writer_batch(Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3]))
    schema = PA::ArrowSchemaConverter.new.schema_root("explicit").convert(batch.schema)
    sink = IO::Memory.new
    options = PA::ArrowWriterOptions.new.with_parquet_schema(schema)
    writer = PA::ArrowWriter.try_new_with_options(sink, batch.schema, options)
    writer.write(batch)
    writer.close.file_metadata.schema_descr.root_schema.name.should eq "explicit"
    read_writer_bytes(sink.to_slice)[0].column(0).should eq batch.column(0)
  end

  it "preserves nulls, row groups and statistics with V1/V2 and supported codecs" do
    array = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 3, 5, nil])
    [Parquet::File::Properties::WriterVersion::PARQUET_1_0, Parquet::File::Properties::WriterVersion::PARQUET_2_0].each do |version|
      [Parquet::Compression::UNCOMPRESSED, Parquet::Compression::SNAPPY, Parquet::Compression.gzip].each do |codec|
        props = WP.builder.set_writer_version(version).set_compression(codec).set_data_page_row_count_limit(2).build
        roundtrip_writer(array, props)
      end
    end
  end

  it "enforces both row and byte limits over the same batch" do
    batch = writer_batch(writer_strings(Array(String?).new(100) { |index| "#{index}".rjust(100, 'x') }))
    props = WP.builder.set_dictionary_enabled(false).set_max_row_group_row_count(30).set_max_row_group_bytes(1000).build
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, batch.schema, props)
    writer.write(batch.slice(0, 5))
    writer.write(batch.slice(5, 95))
    groups = writer.close.row_groups
    groups.sum(&.num_rows).should eq 100
    groups.all? { |group| group.num_rows <= 30 }.should be_true
    read_writer_bytes(sink.to_slice).sum(&.num_rows).should eq 100
  end
end

describe "Arrow writer encryption and ancillary APIs" do
  it "round trips encrypted pages and column metadata after splicing" do
    key = Bytes.new(16, 3_u8)
    array = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4])
    batch = writer_batch(array)
    encryption = Parquet::Encryption::FileEncryptionProperties.builder(key).build
    props = WP.builder.set_file_encryption_properties(encryption).set_max_row_group_row_count(2).build
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, batch.schema, props)
    writer.write(batch)
    writer.close
    decryption = Parquet::Encryption::FileDecryptionProperties.builder(key).build
    options = Parquet::File::ReadOptionsBuilder.new.with_file_decryption_properties(decryption).build
    file = Parquet::File::SerializedFileReader.new_with_options(Parquet::File::BytesChunkReader.new(sink.to_slice), options)
    file.metadata.row_groups.all? { |group| !group.column(0).crypto_metadata.nil? }.should be_true
    file.metadata.num_row_groups.should eq 2
    batches = PA::ParquetRecordBatchReaderBuilder.try_new(file).with_batch_size(2).build.to_a
    batches.size.should eq 2
    batches[0].column(0).should eq array.slice(0, 2)
    batches[1].column(0).should eq array.slice(2, 2)
  end

  it "includes padding written through write_all in tracked offsets" do
    array = Arrow::PrimitiveArray(Int32).from_values([1, 2])
    batch = writer_batch(array)
    sink = IO::Memory.new
    writer = PA::ArrowWriter.try_new(sink, batch.schema)
    writer.write_all(Bytes[0, 0, 0])
    writer.bytes_written.should eq 7
    writer.write(batch)
    writer.close
    read_writer_bytes(sink.to_slice)[0].column(0).should eq array
  end
end

describe "Arrow writer statistics and bloom filters" do
  it "retains column bloom filters and respects disabled statistics" do
    array = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 1])
    batch = writer_batch(array)
    sink = IO::Memory.new
    props = WP.builder.set_bloom_filter_enabled(true).set_statistics_enabled(Parquet::File::Properties::EnabledStatistics::None).build
    writer = PA::ArrowWriter.try_new(sink, batch.schema, props)
    writer.write(batch)
    column = writer.close.row_group(0).column(0)
    column.statistics.should be_nil
    column.bloom_filter_offset.should_not be_nil
    length = column.bloom_filter_length || raise "Missing bloom filter length"
    length.should be > 0
    read_writer_bytes(sink.to_slice)[0].column(0).should eq array
  end
end
