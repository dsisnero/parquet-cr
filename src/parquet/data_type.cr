# Ported from `parquet/src/data_type.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# This file ports the Parquet value types (`Int96`, `ByteArray`,
# `FixedLenByteArray`, `Decimal`) and the `AsBytes` conversion trait. The
# `DataType` trait and its `make_type!` machinery depend on the column
# reader/writer and are ported with the column feature (see `plans/parity.md`).
module Parquet
  # Converts an instance of a data type to a slice of bytes.
  module AsBytes
    abstract def as_bytes : Bytes
  end

  # Rust representation for the deprecated `INT96` physical type.
  struct Int96
    include Comparable(Int96)

    # Julian day of the Unix epoch.
    JULIAN_DAY_OF_EPOCH =     2_440_588_i64
    SECONDS_IN_DAY      =        86_400_i64
    MILLISECONDS        =         1_000_i64
    MICROSECONDS        =     1_000_000_i64
    NANOSECONDS         = 1_000_000_000_i64

    MILLISECONDS_IN_DAY = SECONDS_IN_DAY * MILLISECONDS
    MICROSECONDS_IN_DAY = SECONDS_IN_DAY * MICROSECONDS
    NANOSECONDS_IN_DAY  = SECONDS_IN_DAY * NANOSECONDS

    getter value : StaticArray(UInt32, 3)

    def initialize(@value : StaticArray(UInt32, 3))
    end

    def self.new : Int96
      new(StaticArray[0_u32, 0_u32, 0_u32])
    end

    def self.from(values : Array(UInt32)) : Int96
      raise ArgumentError.new("Invalid INT96 length: #{values.size}") unless values.size == 3

      new(StaticArray[values[0], values[1], values[2]])
    end

    # Returns underlying data.
    def data : StaticArray(UInt32, 3)
      @value
    end

    # Sets data for this INT96 value.
    def set_data(elem0 : UInt32, elem1 : UInt32, elem2 : UInt32) : Nil
      @value = StaticArray[elem0, elem1, elem2]
    end

    # Seconds since epoch. Wraps on overflow.
    def to_seconds : Int64
      day, nanos = data_as_days_and_nanos
      (day.to_i64 - JULIAN_DAY_OF_EPOCH) &* SECONDS_IN_DAY &+ (nanos // 1_000_000_000)
    end

    # Milliseconds since epoch. Wraps on overflow.
    def to_millis : Int64
      day, nanos = data_as_days_and_nanos
      (day.to_i64 - JULIAN_DAY_OF_EPOCH) &* MILLISECONDS_IN_DAY &+ (nanos // 1_000_000)
    end

    # Microseconds since epoch. Wraps on overflow.
    def to_micros : Int64
      day, nanos = data_as_days_and_nanos
      (day.to_i64 - JULIAN_DAY_OF_EPOCH) &* MICROSECONDS_IN_DAY &+ (nanos // 1_000)
    end

    # Nanoseconds since epoch. Wraps on overflow.
    def to_nanos : Int64
      day, nanos = data_as_days_and_nanos
      (day.to_i64 - JULIAN_DAY_OF_EPOCH) &* NANOSECONDS_IN_DAY &+ nanos
    end

    # Order `Int96` for (deprecated) timestamp types: by days, then nanos.
    def <=>(other : Int96) : Int32
      cmp = get_days <=> other.get_days
      cmp == 0 ? get_nanos <=> other.get_nanos : cmp
    end

    def as_bytes : Bytes
      buf = Bytes.new(12)
      @value.each_with_index do |v, i|
        buf[i * 4 + 0] = (v & 0xFF).to_u8!
        buf[i * 4 + 1] = ((v >> 8) & 0xFF).to_u8!
        buf[i * 4 + 2] = ((v >> 16) & 0xFF).to_u8!
        buf[i * 4 + 3] = ((v >> 24) & 0xFF).to_u8!
      end
      buf
    end

    def to_s(io : IO) : Nil
      io << '[' << @value[0] << ", " << @value[1] << ", " << @value[2] << ']'
    end

    protected def get_days : Int32
      @value[2].to_i32!
    end

    protected def get_nanos : Int64
      (@value[1].to_i64 << 32) + @value[0].to_i64
    end

    private def data_as_days_and_nanos : Tuple(Int32, Int64)
      {get_days, get_nanos}
    end
  end

  # Rust representation for the `BYTE_ARRAY` (and `FIXED_LEN_BYTE_ARRAY`)
  # physical types, backed by an optional byte buffer.
  struct ByteArray
    include Comparable(ByteArray)

    @data : Bytes?

    def initialize(@data : Bytes? = nil)
    end

    def self.new(str : String) : ByteArray
      new(str.to_slice)
    end

    def self.new(values : Array(UInt8)) : ByteArray
      new(Bytes.new(values.to_unsafe, values.size))
    end

    # Length of the underlying buffer. Raises if no data is set.
    def len : Int32
      data.size
    end

    # Whether the underlying buffer is empty. Raises if no data is set.
    def empty? : Bool
      len == 0
    end

    # Returns the underlying bytes. Raises if no data is set.
    def data : Bytes
      @data || raise "set_data should have been called"
    end

    # Set data from a byte buffer.
    # ameba:disable Naming/AccessorMethodName
    def set_data(data : Bytes) : Nil
      @data = data
    end

    # Returns a new `ByteArray` view over `[start, start + len)`.
    def slice(start : Int, len : Int) : ByteArray
      self.class.new(data[start, len])
    end

    # Tries to interpret the bytes as UTF-8.
    def as_utf8 : String
      bytes = @data || raise general_err!("Can't convert empty byte array to utf8")
      str = String.new(bytes)
      raise general_err!("invalid utf8") unless str.valid_encoding?

      str
    end

    def as_bytes : Bytes
      data
    end

    # Raw optional buffer (used for null-aware ordering).
    protected def raw_data : Bytes?
      @data
    end

    # Nulls sort first.
    def <=>(other : ByteArray) : Int32
      a = raw_data
      b = other.raw_data
      if a.nil?
        b.nil? ? 0 : -1
      elsif b.nil?
        1
      else
        a <=> b
      end
    end

    def to_s(io : IO) : Nil
      io << '['
      if bytes = @data
        bytes.each_with_index do |b, i|
          io << ", " if i > 0
          io << b
        end
      end
      io << ']'
    end
  end

  # Wrapper type for performance reasons; represents `FIXED_LEN_BYTE_ARRAY` but
  # otherwise behaves like `ByteArray`.
  struct FixedLenByteArray
    include Comparable(FixedLenByteArray)

    getter value : ByteArray

    def initialize(@value : ByteArray)
    end

    def self.new(str : String) : FixedLenByteArray
      new(ByteArray.new(str))
    end

    def self.new(values : Array(UInt8)) : FixedLenByteArray
      new(ByteArray.new(values))
    end

    def len : Int32
      @value.len
    end

    def empty? : Bool
      @value.empty?
    end

    def data : Bytes
      @value.data
    end

    # ameba:disable Naming/AccessorMethodName
    def set_data(data : Bytes) : Nil
      @value.set_data(data)
    end

    def slice(start : Int, len : Int) : ByteArray
      @value.slice(start, len)
    end

    def as_utf8 : String
      @value.as_utf8
    end

    def as_bytes : Bytes
      @value.as_bytes
    end

    def <=>(other : FixedLenByteArray) : Int32
      @value <=> other.value
    end

    def ==(other : FixedLenByteArray) : Bool
      @value == other.value
    end

    def ==(other : ByteArray) : Bool
      @value == other
    end

    def to_s(io : IO) : Nil
      @value.to_s(io)
    end
  end

  # Representation for DECIMAL logical type values.
  #
  # Divergence: upstream models this as an enum with `Int32`/`Int64`/`Bytes`
  # variants. Crystal has no payload enums, so variant identity is encoded in
  # the byte width of `data`; equality and accessors match upstream exactly.
  struct Decimal
    getter data : Bytes
    getter precision : Int32
    getter scale : Int32

    def initialize(@data : Bytes, @precision : Int32, @scale : Int32)
    end

    # Creates a decimal backed by `i32` (4 big-endian bytes).
    def self.from_i32(value : Int32, precision : Int32, scale : Int32) : Decimal
      new(encode_big_endian(value), precision, scale)
    end

    # Creates a decimal backed by `i64` (8 big-endian bytes).
    def self.from_i64(value : Int64, precision : Int32, scale : Int32) : Decimal
      new(encode_big_endian(value), precision, scale)
    end

    # Creates a decimal backed by a `ByteArray`.
    def self.from_bytes(value : ByteArray, precision : Int32, scale : Int32) : Decimal
      new(value.data, precision, scale)
    end

    def self.default : Decimal
      from_i32(0, 0, 0)
    end

    private def self.encode_big_endian(value : Int32) : Bytes
      io = IO::Memory.new(4)
      io.write_bytes(value, IO::ByteFormat::BigEndian)
      io.to_slice
    end

    private def self.encode_big_endian(value : Int64) : Bytes
      io = IO::Memory.new(8)
      io.write_bytes(value, IO::ByteFormat::BigEndian)
      io.to_slice
    end

    def as_bytes : Bytes
      data
    end
  end
end
