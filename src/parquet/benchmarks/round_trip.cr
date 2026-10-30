# parquet/benches/parquet_round_trip.rs, apache/arrow-rs 60.0.0
# ef1fa157977633f0ba21aa921f9ef3d5c669235b. Native deterministic RNG and a
# cold/warm CSV harness replace StdRng/Criterion. No timing equivalence claimed.
module Parquet::Benchmarks::RoundTrip
  DEFAULT_NUM_COLUMNS       =     10
  DEFAULT_NUM_ROWGROUPS     =     10
  DEFAULT_ROWS_PER_PAGE     =  2_000
  DEFAULT_ROWS_PER_ROWGROUP = 10_000
  SEED                      =     31
  ALPHANUMERIC              = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

  # Source payload enum, represented by a kind and optional byte width.
  struct ColumnType
    enum Kind
      String
      StringView
      Binary
      FixedLen
      Int32
      Int64
      Float
      Double
    end

    getter kind : Kind
    getter width : Int32

    def initialize(@kind : Kind, @width : Int32 = 0)
      if !numeric? && @width < (@kind.fixed_len? ? 1 : 2)
        raise ArgumentError.new("invalid benchmark byte width")
      end
    end

    {% for name, kind in {string: "String", string_view: "StringView", binary: "Binary", fixed_len: "FixedLen"} %}
      def self.{{name}}(width : Int32) : self
        new(Kind::{{kind.id}}, width)
      end
    {% end %}
    {% for name, kind in {int32: "Int32", int64: "Int64", float: "Float", double: "Double"} %}
      def self.{{name}} : self
        new(Kind::{{kind.id}})
      end
    {% end %}

    def numeric? : Bool
      @kind.int32? || @kind.int64? || @kind.float? || @kind.double?
    end

    def data_type : ::Arrow::DataType
      case @kind
      in .string?      then ::Arrow::DataType.utf8
      in .string_view? then ::Arrow::DataType.utf8_view
      in .binary?      then ::Arrow::DataType.binary
      in .fixed_len?   then ::Arrow::DataType.fixed_size_binary(@width)
      in .int32?       then ::Arrow::DataType.int32
      in .int64?       then ::Arrow::DataType.int64
      in .float?       then ::Arrow::DataType.float32
      in .double?      then ::Arrow::DataType.float64
      end
    end
  end

  # Copyable source file specification; with_* returns an independent value.
  struct ParquetFileSpec
    getter column_type : ColumnType
    property num_columns : Int32 = DEFAULT_NUM_COLUMNS
    property num_row_groups : Int32 = DEFAULT_NUM_ROWGROUPS
    property rows_per_row_group : Int32 = DEFAULT_ROWS_PER_ROWGROUP
    property rows_per_page : Int32 = DEFAULT_ROWS_PER_PAGE
    property encoding : Encoding = Encoding::PLAIN
    property? use_dict : Bool = true

    def initialize(@column_type : ColumnType)
    end

    {% for field, type in {num_columns: "Int32", num_row_groups: "Int32", rows_per_row_group: "Int32", rows_per_page: "Int32", encoding: "Encoding", use_dict: "Bool"} %}
      def with_{{field.id}}(value : {{type.id}}) : self
        updated = self
        updated.{{field.id}} = value
        updated
      end
    {% end %}

    def rows : Int32
      @num_row_groups * @rows_per_row_group
    end

    def validate! : Nil
      unless @num_columns > 0 && @num_row_groups > 0 && @rows_per_row_group > 0 && @rows_per_page > 0
        raise ArgumentError.new("benchmark dimensions must be positive")
      end
      if @rows_per_row_group > 100 && @rows_per_row_group % 100 != 0
        raise ArgumentError.new("rows per group above 100 must be a multiple of 100")
      end
    end
  end

  record Case, name : String, spec : ParquetFileSpec

  # All source widths, including the two variable-size and fixed-size families.
  def self.types : Array(ColumnType)
    [ColumnType.int32, ColumnType.int64, ColumnType.float, ColumnType.double,
     ColumnType.string(20), ColumnType.string_view(20), ColumnType.string(100), ColumnType.string_view(100),
     ColumnType.binary(20), ColumnType.binary(100), ColumnType.fixed_len(2), ColumnType.fixed_len(16)]
  end

  def self.schema(type : ColumnType, columns : Int32) : ::Arrow::Schema
    fields = Array.new(columns) { |index| ::Arrow::Field.new("col_#{index}", type.data_type, true) }
    ::Arrow::Schema.new(::Arrow::Fields.from(fields))
  end

  def self.create_batch(schema : ::Arrow::Schema, type : ColumnType, seed : Int32,
                        columns : Int32, rows : Int32, null_density : Float64 = 0.0001) : ::Arrow::RecordBatch
    arrays = Array(::Arrow::ArrayBase).new(columns) do |index|
      random = Random.new((seed * columns + index).to_u64)
      create_array(type, rows, null_density, random)
    end
    ::Arrow::RecordBatch.try_new(schema, arrays)
  end

  private def self.create_array(type : ColumnType, rows : Int32, density : Float64, random : Random) : ::Arrow::ArrayBase
    case type.kind
    in .int32?       then numeric_array(Int32, rows, density, random)
    in .int64?       then numeric_array(Int64, rows, density, random)
    in .float?       then numeric_array(Float32, rows, density, random)
    in .double?      then numeric_array(Float64, rows, density, random)
    in .string?      then byte_array(::Arrow::GenericStringBuilder(Int32).new, type, rows, density, random)
    in .string_view? then byte_array(::Arrow::StringViewBuilder.new, type, rows, density, random)
    in .binary?      then byte_array(::Arrow::GenericBinaryBuilder(Int32).new, type, rows, density, random)
    in .fixed_len?   then byte_array(::Arrow::FixedSizeBinaryBuilder.new(type.width), type, rows, density, random)
    end
  end

  private def self.numeric_array(type : T.class, rows : Int32, density : Float64, random : Random) : ::Arrow::ArrayBase forall T
    values = Array(T?).new(rows) do
      if random.rand < density
        nil
      else
        {% if T < Float %}
          T.new(random.rand)
        {% else %}
          random.rand(T)
        {% end %}
      end
    end
    ::Arrow::PrimitiveArray(T).from_nullable(values)
  end

  private def self.byte_array(builder : T, type : ColumnType, rows : Int32, density : Float64, random : Random) : ::Arrow::ArrayBase forall T
    rows.times do
      if random.rand < density
        builder.append_null
        next
      end
      length = type.kind.fixed_len? ? type.width : random.rand(type.width // 2...type.width)
      bytes = Bytes.new(length) do
        if type.kind.string? || type.kind.string_view?
          ALPHANUMERIC.byte_at(random.rand(ALPHANUMERIC.bytesize))
        else
          random.rand(256).to_u8
        end
      end
      {% if T.stringify.includes?("String") %}
        builder.append_value(String.new(bytes))
      {% else %}
        builder.append_value(bytes)
      {% end %}
    end
    builder.finish
  end

  # Complete 46-case source matrix, retaining its order and labels.
  def self.cases : Array(Case)
    result = [] of Case
    types.each do |type|
      name = case_name(type)
      spec = ParquetFileSpec.new(type).with_num_columns(type.numeric? ? 10 : 5)
      result << Case.new("#{name} dict", spec)
      spec = spec.with_use_dict(false)
      result << Case.new("#{name} plain", spec)
      if type.kind.int32? || type.kind.int64?
        result << Case.new("#{name} delta_binary", spec.with_encoding(Encoding::DELTA_BINARY_PACKED))
      end
      if type.numeric? || type.kind.fixed_len?
        result << Case.new("#{name} byte_stream_split", spec.with_encoding(Encoding::BYTE_STREAM_SPLIT))
      end
      unless type.numeric?
        unless type.kind.fixed_len?
          result << Case.new("#{name} delta_length", spec.with_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY))
        end
        result << Case.new("#{name} delta_byte_array", spec.with_encoding(Encoding::DELTA_BYTE_ARRAY))
      end
    end
    result
  end

  private def self.case_name(type : ColumnType) : String
    case type.kind
    when .int32?       then "int32"
    when .int64?       then "int64"
    when .float?       then "f32"
    when .double?      then "f64"
    when .string?      then "String(#{type.width})"
    when .string_view? then "StringView(#{type.width})"
    when .binary?      then "Binary(#{type.width})"
    else                    "Fixed(#{type.width})"
    end
  end

  def self.smoke_spec(spec : ParquetFileSpec) : ParquetFileSpec
    spec.with_num_columns(2).with_num_row_groups(2).with_rows_per_row_group(32).with_rows_per_page(16)
  end

  # Reset per write; the pinned Criterion loop appends successive files.
  # Returned bytes borrow the reusable buffer until the next write.
  def self.file_from_spec(spec : ParquetFileSpec, buffer : IO::Memory = IO::Memory.new) : Bytes
    spec.validate!
    buffer.clear
    shape = schema(spec.column_type, spec.num_columns)
    props = File::Properties::WriterProperties.builder.set_max_row_group_row_count(spec.rows_per_row_group)
      .set_data_page_row_count_limit(spec.rows_per_page).set_encoding(spec.encoding)
      .set_dictionary_enabled(spec.use_dict?).set_compression(Compression::UNCOMPRESSED).build
    writer = Arrow::ArrowWriter.try_new(buffer, shape, props)
    batch = create_batch(shape, spec.column_type, SEED, spec.num_columns, Math.min(spec.rows_per_row_group, 100))
    written = 0
    while written < spec.rows
      writer.write(batch)
      written += batch.num_rows
    end
    metadata = writer.close
    unless metadata.num_row_groups == spec.num_row_groups && metadata.num_rows == spec.rows
      raise general_err!("benchmark file shape mismatch")
    end
    buffer.to_slice
  end

  def self.read_rows(bytes : Bytes) : Int32
    file = File::SerializedFileReader.new(File::BytesChunkReader.new(bytes))
    Arrow::ParquetRecordBatchReaderBuilder.try_new(file).build.sum(0, &.num_rows)
  end

  # Exact values/schema verification outside timing, including repeated seed
  # batches crossing both row-group and reader-batch boundaries.
  def self.validate(bytes : Bytes, spec : ParquetFileSpec, batch_size : Int32 = 1024) : Int32
    expected = create_batch(schema(spec.column_type, spec.num_columns), spec.column_type, SEED,
      spec.num_columns, Math.min(spec.rows_per_row_group, 100))
    file = File::SerializedFileReader.new(File::BytesChunkReader.new(bytes))
    reader = Arrow::ParquetRecordBatchReaderBuilder.try_new(file).with_batch_size(batch_size).build
    total = 0
    reader.each do |batch|
      position = 0
      while position < batch.num_rows
        start = total % expected.num_rows
        count = Math.min(expected.num_rows - start, batch.num_rows - position)
        unless batch.slice(position, count) == expected.slice(start, count)
          raise general_err!("benchmark round-trip values mismatch")
        end
        total += count
        position += count
      end
    end
    raise general_err!("benchmark read row count mismatch") unless total == spec.rows
    total
  end

  record Result, name : String, operation : String, rows : Int32, bytes : Int32,
    cold_seconds : Float64, warm_seconds : Array(Float64) do
    def mean_seconds : Float64
      warm_seconds.sum / warm_seconds.size
    end

    def rows_per_second : Float64
      rows / mean_seconds
    end
  end

  private def self.measure(&operation : ->) : Float64
    start = Time.instant
    operation.call
    start.elapsed.total_seconds
  end

  def self.run(cases : Array(Case), samples : Int32, output : IO = STDOUT) : Array(Result)
    raise ArgumentError.new("samples must be positive") unless samples > 0
    output.puts "case,operation,rows,bytes,cold_seconds,warm_seconds,rows_per_second"
    results = [] of Result
    cases.each do |item|
      buffer = IO::Memory.new(1_000_000)
      bytes = file_from_spec(item.spec, buffer)
      validate(bytes, item.spec)
      write = -> { file_from_spec(item.spec, buffer); nil }
      read = -> { raise general_err!("benchmark read row count mismatch") unless read_rows(bytes) == item.spec.rows }
      { {"write", write}, {"read", read} }.each do |operation, action|
        bytes = buffer.to_slice.dup if operation == "read"
        cold = measure { action.call }
        warm = Array.new(samples) { measure { action.call } }
        result = Result.new(item.name, operation, item.spec.rows, bytes.size, cold, warm)
        results << result
        output.puts "#{item.name},#{operation},#{result.rows},#{result.bytes},#{cold},#{result.mean_seconds},#{result.rows_per_second}"
      end
    end
    results
  end
end
