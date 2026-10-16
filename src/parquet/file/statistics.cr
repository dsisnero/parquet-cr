# Ported from `parquet/src/file/statistics.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Public statistics API (`Statistics`, `ValueStatistics`) and the conversion to
# and from the thrift `PageStatistics` struct.
module Parquet
  # Byte encoding for statistics values (PLAIN, little-endian for fixed-width).
  module StatsBytes
    def self.as_bytes(v : Bool) : Bytes
      Bytes[v ? 0x01_u8 : 0x00_u8]
    end

    def self.as_bytes(v : Int32) : Bytes
      buffer(4, v)
    end

    def self.as_bytes(v : Int64) : Bytes
      buffer(8, v)
    end

    def self.as_bytes(v : Float32) : Bytes
      buffer(4, v)
    end

    def self.as_bytes(v : Float64) : Bytes
      buffer(8, v)
    end

    def self.as_bytes(v : Int96) : Bytes
      v.as_bytes
    end

    def self.as_bytes(v : ByteArray) : Bytes
      v.as_bytes
    end

    def self.as_bytes(v : FixedLenByteArray) : Bytes
      v.as_bytes
    end

    private def self.buffer(size : Int32, value) : Bytes
      io = IO::Memory.new(size)
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      io.to_slice
    end
  end

  # Typed statistics for one column chunk.
  struct ValueStatistics(T)
    getter min : T?
    getter max : T?
    getter distinct_count : UInt64?
    getter null_count : UInt64?
    getter nan_count : UInt64?

    def max_value_exact? : Bool
      @is_max_value_exact
    end

    def min_value_exact? : Bool
      @is_min_value_exact
    end

    def min_max_deprecated? : Bool
      @is_min_max_deprecated
    end

    def min_max_backwards_compatible? : Bool
      @is_min_max_backwards_compatible
    end

    def initialize(
      @min : T?,
      @max : T?,
      @distinct_count : UInt64?,
      @null_count : UInt64?,
      @is_min_max_deprecated : Bool,
      @nan_count : UInt64?,
      @is_max_value_exact : Bool,
      @is_min_value_exact : Bool,
      @is_min_max_backwards_compatible : Bool,
    )
    end

    # Creates new typed statistics.
    def self.new(
      min : T?,
      max : T?,
      distinct_count : UInt64?,
      null_count : UInt64?,
      is_min_max_deprecated : Bool,
    ) : self
      new(
        min, max, distinct_count, null_count, is_min_max_deprecated,
        nil, !max.nil?, !min.nil?, is_min_max_deprecated,
      )
    end

    def with_min_is_exact(is_min_value_exact : Bool) : self
      ValueStatistics(T).new(
        @min, @max, @distinct_count, @null_count, @is_min_max_deprecated,
        @nan_count, @is_max_value_exact, is_min_value_exact, @is_min_max_backwards_compatible,
      )
    end

    def with_max_is_exact(is_max_value_exact : Bool) : self
      ValueStatistics(T).new(
        @min, @max, @distinct_count, @null_count, @is_min_max_deprecated,
        @nan_count, is_max_value_exact, @is_min_value_exact, @is_min_max_backwards_compatible,
      )
    end

    def with_backwards_compatible_min_max(backwards_compatible : Bool) : self
      ValueStatistics(T).new(
        @min, @max, @distinct_count, @null_count, @is_min_max_deprecated,
        @nan_count, @is_max_value_exact, @is_min_value_exact, backwards_compatible,
      )
    end

    def with_nan_count(nan_count : UInt64?) : self
      ValueStatistics(T).new(
        @min, @max, @distinct_count, @null_count, @is_min_max_deprecated,
        nan_count, @is_max_value_exact, @is_min_value_exact, @is_min_max_backwards_compatible,
      )
    end

    def min_opt : T?
      @min
    end

    def max_opt : T?
      @max
    end

    # Whether or not max value is set, and is an exact value.
    def max_is_exact : Bool
      !@max.nil? && @is_max_value_exact
    end

    # Whether or not min value is set, and is an exact value.
    def min_is_exact : Bool
      !@min.nil? && @is_min_value_exact
    end

    def null_count_opt : UInt64?
      @null_count
    end

    def nan_count_opt : UInt64?
      @nan_count
    end

    # Returns min value as bytes, if known.
    def min_bytes_opt : Bytes?
      @min.try { |v| StatsBytes.as_bytes(v) }
    end

    # Returns max value as bytes, if known.
    def max_bytes_opt : Bytes?
      @max.try { |v| StatsBytes.as_bytes(v) }
    end

    def to_s(io : IO) : Nil
      io << "{min: "
      write_display(io, @min)
      io << ", max: "
      write_display(io, @max)
      io << ", distinct_count: "
      write_display(io, @distinct_count)
      io << ", null_count: "
      write_display(io, @null_count)
      io << ", min_max_deprecated: " << @is_min_max_deprecated
      io << ", max_value_exact: " << @is_max_value_exact
      io << ", min_value_exact: " << @is_min_value_exact
      io << '}'
    end

    def inspect(io : IO) : Nil
      io << "{min: " << debug_option(@min)
      io << ", max: " << debug_option(@max)
      io << ", distinct_count: " << debug_option(@distinct_count)
      io << ", null_count: " << debug_option(@null_count)
      io << ", min_max_deprecated: " << @is_min_max_deprecated
      io << ", min_max_backwards_compatible: " << @is_min_max_backwards_compatible
      io << ", max_value_exact: " << @is_max_value_exact
      io << ", min_value_exact: " << @is_min_value_exact
      io << '}'
    end

    private def write_display(io : IO, value) : Nil
      if value.nil?
        io << "N/A"
      else
        io << value
      end
    end

    private def debug_option(value) : String
      value.nil? ? "None" : "Some(#{value})"
    end
  end

  # Strongly typed statistics for a column chunk within a row group.
  #
  # Divergence: upstream is an enum with one variant per physical type; here it
  # is a single class carrying a `ValueStatistics` union, with the same
  # constructors, accessors, `Display`, and Rust-compatible `Debug`.
  class Statistics
    alias Inner = ValueStatistics(Bool) |
                  ValueStatistics(Int32) |
                  ValueStatistics(Int64) |
                  ValueStatistics(Int96) |
                  ValueStatistics(Float32) |
                  ValueStatistics(Float64) |
                  ValueStatistics(ByteArray) |
                  ValueStatistics(FixedLenByteArray)

    getter value : Inner

    def initialize(@value : Inner)
    end

    def self.boolean(min : Bool?, max : Bool?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(Bool).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def self.int32(min : Int32?, max : Int32?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(Int32).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def self.int64(min : Int64?, max : Int64?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(Int64).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def self.int96(min : Int96?, max : Int96?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(Int96).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def self.float(min : Float32?, max : Float32?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(Float32).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def self.double(min : Float64?, max : Float64?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(Float64).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def self.byte_array(min : ByteArray?, max : ByteArray?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(ByteArray).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def self.fixed_len_byte_array(min : FixedLenByteArray?, max : FixedLenByteArray?, distinct_count : UInt64?, null_count : UInt64?, is_deprecated : Bool) : Statistics
      new(ValueStatistics(FixedLenByteArray).new(min, max, distinct_count, null_count, is_deprecated))
    end

    def min_max_deprecated? : Bool
      @value.min_max_deprecated?
    end

    def min_max_backwards_compatible? : Bool
      @value.min_max_backwards_compatible?
    end

    def distinct_count_opt : UInt64?
      @value.distinct_count
    end

    def null_count_opt : UInt64?
      @value.null_count_opt
    end

    def nan_count_opt : UInt64?
      @value.nan_count_opt
    end

    def min_is_exact : Bool
      @value.min_is_exact
    end

    def max_is_exact : Bool
      @value.max_is_exact
    end

    def min_bytes_opt : Bytes?
      @value.min_bytes_opt
    end

    def max_bytes_opt : Bytes?
      @value.max_bytes_opt
    end

    def physical_type : Type
      case @value
      when ValueStatistics(Bool)      then Type::BOOLEAN
      when ValueStatistics(Int32)     then Type::INT32
      when ValueStatistics(Int64)     then Type::INT64
      when ValueStatistics(Int96)     then Type::INT96
      when ValueStatistics(Float32)   then Type::FLOAT
      when ValueStatistics(Float64)   then Type::DOUBLE
      when ValueStatistics(ByteArray) then Type::BYTE_ARRAY
      else                                 Type::FIXED_LEN_BYTE_ARRAY
      end
    end

    def ==(other : Statistics) : Bool
      @value == other.value
    end

    def hash(hasher)
      @value.hash(hasher)
    end

    def to_s(io : IO) : Nil
      @value.to_s(io)
    end

    def inspect(io : IO) : Nil
      io << variant_name << '('
      @value.inspect(io)
      io << ')'
    end

    private def variant_name : String
      case @value
      when ValueStatistics(Bool)      then "Boolean"
      when ValueStatistics(Int32)     then "Int32"
      when ValueStatistics(Int64)     then "Int64"
      when ValueStatistics(Int96)     then "Int96"
      when ValueStatistics(Float32)   then "Float"
      when ValueStatistics(Float64)   then "Double"
      when ValueStatistics(ByteArray) then "ByteArray"
      else                                 "FixedLenByteArray"
      end
    end

    # Converts a thrift `PageStatistics` into `Statistics`.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.from_thrift_page_stats(physical_type : Type, thrift_stats : Format::PageStatistics?) : Statistics?
      stats = thrift_stats
      return nil if stats.nil?

      null_count = nil.as(UInt64?)
      if nc = stats.null_count
        raise general_err!("Statistics null count is negative #{nc}") if nc < 0
        null_count = nc.to_u64
      end

      distinct_count = stats.distinct_count.try(&.to_u64)

      nan_count = nil.as(UInt64?)
      if nn = stats.nan_count
        raise general_err!("Statistics NaN count is negative #{nn}") if nn < 0
        nan_count = nn.to_u64
      end

      old_format = stats.min_value.nil? && stats.max_value.nil?
      min = old_format ? stats.min : stats.min_value
      max = old_format ? stats.max : stats.max_value

      case physical_type
      when Type::BOOLEAN
        check_len(min, max, 1)
      when Type::INT32, Type::FLOAT
        check_len(min, max, 4)
      when Type::INT64, Type::DOUBLE
        check_len(min, max, 8)
      when Type::INT96
        check_len(min, max, 12)
      end

      max_exact = stats.is_max_value_exact || false
      min_exact = stats.is_min_value_exact || false

      case physical_type
      when Type::BOOLEAN
        boolean(
          min.try { |bytes| bytes[0] != 0 },
          max.try { |bytes| bytes[0] != 0 },
          distinct_count, null_count, old_format,
        )
      when Type::INT32
        int32(
          min.try { |bytes| FileStats.le_i32(bytes) },
          max.try { |bytes| FileStats.le_i32(bytes) },
          distinct_count, null_count, old_format,
        )
      when Type::INT64
        int64(
          min.try { |bytes| FileStats.le_i64(bytes) },
          max.try { |bytes| FileStats.le_i64(bytes) },
          distinct_count, null_count, old_format,
        )
      when Type::INT96
        min96 = if data = min
                  raise general_err!("Incorrect Int96 min statistics") unless data.size == 12
                  Int96.try_from_le_slice(data)
                end
        max96 = if data = max
                  raise general_err!("Incorrect Int96 max statistics") unless data.size == 12
                  Int96.try_from_le_slice(data)
                end
        int96(min96, max96, distinct_count, null_count, old_format)
      when Type::FLOAT
        new(
          ValueStatistics(Float32).new(
            min.try { |bytes| FileStats.le_f32(bytes) },
            max.try { |bytes| FileStats.le_f32(bytes) },
            distinct_count, null_count, old_format,
          ).with_nan_count(nan_count)
            .with_max_is_exact(max_exact)
            .with_min_is_exact(min_exact)
        )
      when Type::DOUBLE
        new(
          ValueStatistics(Float64).new(
            min.try { |bytes| FileStats.le_f64(bytes) },
            max.try { |bytes| FileStats.le_f64(bytes) },
            distinct_count, null_count, old_format,
          ).with_nan_count(nan_count)
            .with_max_is_exact(max_exact)
            .with_min_is_exact(min_exact)
        )
      when Type::BYTE_ARRAY
        new(
          ValueStatistics(ByteArray).new(
            min.try { |bytes| ByteArray.new(bytes) },
            max.try { |bytes| ByteArray.new(bytes) },
            distinct_count, null_count, old_format,
          ).with_max_is_exact(max_exact)
            .with_min_is_exact(min_exact)
        )
      else
        new(
          ValueStatistics(FixedLenByteArray).new(
            min.try { |bytes| FixedLenByteArray.new(ByteArray.new(bytes)) },
            max.try { |bytes| FixedLenByteArray.new(ByteArray.new(bytes)) },
            distinct_count, null_count, old_format,
          ).with_nan_count(nan_count)
            .with_max_is_exact(max_exact)
            .with_min_is_exact(min_exact)
        )
      end
    end

    private def self.check_len(min : Bytes?, max : Bytes?, len : Int32) : Nil
      if m = min
        raise general_err!("Insufficient bytes to parse min statistic") if m.size < len
      end
      if m = max
        raise general_err!("Insufficient bytes to parse max statistic") if m.size < len
      end
    end

    # Converts `Statistics` into a thrift `PageStatistics`.
    def self.page_stats_to_thrift(stats : Statistics?) : Format::PageStatistics?
      return nil if stats.nil?

      null_count = stats.null_count_opt.try(&.to_i64)
      distinct_count = stats.distinct_count_opt.try(&.to_i64)
      nan_count = stats.nan_count_opt.try(&.to_i64)

      thrift_stats = Format::PageStatistics.new(
        nil, nil, null_count, distinct_count, nil, nil, nil, nil, nan_count,
      )

      min = stats.min_bytes_opt.try(&.dup)
      max = stats.max_bytes_opt.try(&.dup)

      if stats.min_max_backwards_compatible?
        thrift_stats.min = min
        thrift_stats.max = max
      end

      unless stats.min_max_deprecated?
        thrift_stats.min_value = min
        thrift_stats.max_value = max
      end

      thrift_stats.is_min_value_exact = stats.min_is_exact
      thrift_stats.is_max_value_exact = stats.max_is_exact

      thrift_stats
    end
  end

  # Little-endian decode helpers for statistics.
  module FileStats
    def self.le_i32(data : Bytes) : Int32
      IO::Memory.new(data[0, 4]).read_bytes(Int32, IO::ByteFormat::LittleEndian)
    end

    def self.le_i64(data : Bytes) : Int64
      IO::Memory.new(data[0, 8]).read_bytes(Int64, IO::ByteFormat::LittleEndian)
    end

    def self.le_f32(data : Bytes) : Float32
      IO::Memory.new(data[0, 4]).read_bytes(Float32, IO::ByteFormat::LittleEndian)
    end

    def self.le_f64(data : Bytes) : Float64
      IO::Memory.new(data[0, 8]).read_bytes(Float64, IO::ByteFormat::LittleEndian)
    end
  end
end
