require "big"
require "base64"
require "json"
require "../errors"
require "../thrift"
require "../macros"
require "../basic"
require "../data_type"
require "../schema/types"

# Ported from `parquet/src/record/api.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `Row`, `List`, `Map`, `Field`, `RowAccessor`, `ListAccessor`,
# `MapAccessor`, `RowFormatter`, `RowColumnIter`, and the conversion/formatting
# helpers. Divergences: Rust's payload-carrying enums (`Field`) are modelled as
# a sealed class hierarchy (`Field::Int`, `Field::Str`, ...); the `RowAccessor`/
# `ListAccessor`/`MapAccessor`/`RowFormatter` traits are modules; `usize` is
# `Int32`; `serde_json::Value` is `JSON::Any`; the `f16` type is carried as the
# decoded `Float64` value (no native half float); and `ValueStatistics`-style
# `is_*` predicates use `?` naming.
module Parquet
  module Record
    # `serde_json::Value` maps to `JSON::Any`.
    alias Value = JSON::Any

    # Bit pattern of a little-endian IEEE-754 half float, decoded to `Float64`.
    def self.half_to_f64(bytes : Bytes) : Float64
      half_to_f64(bytes[0].to_u16 | (bytes[1].to_u16 << 8))
    end

    # Decodes an IEEE-754 binary16 bit pattern to `Float64`.
    def self.half_to_f64(bits : UInt16) : Float64
      sign = (bits >> 15) & 0x1_u16
      exponent = ((bits >> 10) & 0x1F_u16).to_i32
      fraction = (bits & 0x3FF_u16).to_i32

      value =
        if exponent == 0
          # Subnormal or zero.
          fraction.to_f64 * 2.0 ** -24
        elsif exponent == 0x1F
          # Infinity or NaN.
          fraction == 0 ? Float64::INFINITY : Float64::NAN
        else
          (1.0 + fraction.to_f64 / 1024.0) * 2.0 ** (exponent - 15)
        end

      sign == 1 ? -value : value
    end

    # Splits Crystal's shortest float string into `(negative, digits, point)`
    # where `value = sign * digits * 10^(point - digits.size)`.
    private def self.split_float_str(str : String) : Tuple(Bool, String, Int32)
      negative = str.starts_with?('-')
      str = str[1..] if negative

      if idx = str.index('e') || str.index('E')
        mantissa = str[0, idx]
        exp = str[(idx + 1)..].to_i32
      else
        mantissa = str
        exp = 0
      end

      if dot = mantissa.index('.')
        int_part = mantissa[0, dot]
        frac_part = mantissa[(dot + 1)..].rstrip('0')
      else
        int_part = mantissa
        frac_part = ""
      end

      digits = int_part + frac_part
      digits = "0" if digits.empty?
      point = int_part.size + exp
      {negative, digits, point}
    end

    private def self.full_decimal(digits : String, point : Int32) : String
      if point <= 0
        "0." + "0" * (-point) + digits
      elsif point >= digits.size
        digits + "0" * (point - digits.size)
      else
        digits[0, point] + "." + digits[point..]
      end
    end

    private def self.scientific_decimal(digits : String, point : Int32) : String
      exp = point - 1
      mantissa = digits.size > 1 ? "#{digits[0]}.#{digits[1..]}" : digits
      "#{mantissa}E#{exp}"
    end

    # Formats a `Float32` like Rust's `Display` for `f32`.
    def self.rust_display_f32(value : Float32) : String
      return "NaN" if value.nan?
      return value > 0 ? "inf" : "-inf" if value.infinite?

      negative, digits, point = split_float_str(value.to_s)
      sign = negative ? "-" : ""
      if !(1e-15 <= value.to_f64 <= 1e19)
        sign + scientific_decimal(digits, point)
      elsif value == value.trunc
        sign + full_decimal(digits, point) + ".0"
      else
        sign + full_decimal(digits, point)
      end
    end

    # Formats a `Float64` like Rust's `Display` for `f64`.
    def self.rust_display_f64(value : Float64) : String
      return "NaN" if value.nan?
      return value > 0 ? "inf" : "-inf" if value.infinite?

      negative, digits, point = split_float_str(value.to_s)
      sign = negative ? "-" : ""
      if !(1e-15 <= value <= 1e19)
        sign + scientific_decimal(digits, point)
      elsif value == value.trunc
        sign + full_decimal(digits, point) + ".0"
      else
        sign + full_decimal(digits, point)
      end
    end

    # Formats a decoded half float like Rust's `f16` `Display` (never scientific).
    def self.rust_display_f16(value : Float64) : String
      return "NaN" if value.nan?
      return value > 0 ? "inf" : "-inf" if value.infinite?

      negative, digits, point = split_float_str(value.to_s)
      sign = negative ? "-" : ""
      if value == value.trunc
        sign + full_decimal(digits, point) + ".0"
      else
        sign + full_decimal(digits, point)
      end
    end

    # Converts Parquet date (days since epoch) into a string (`%Y-%m-%d`).
    def self.convert_date_to_string(value : Int32) : String
      seconds = value.to_i64 * 60 * 60 * 24
      Time.unix(seconds).to_s("%Y-%m-%d")
    end

    # Converts Parquet timestamp (millis since epoch) into a string.
    def self.convert_timestamp_millis_to_string(value : Int64) : String
      Time.unix_ms(value).to_s("%Y-%m-%d %H:%M:%S.%L %:z")
    end

    # Converts Parquet timestamp (micros since epoch) into a string.
    def self.convert_timestamp_micros_to_string(value : Int64) : String
      Time.unix_ns(value * 1000).to_s("%Y-%m-%d %H:%M:%S.%6N %:z")
    end

    # Converts Parquet time (millis since midnight) into a string.
    def self.convert_time_millis_to_string(value : Int32) : String
      total_ms = value.to_u64
      hours = total_ms // (60 * 60 * 1000)
      minutes = (total_ms % (60 * 60 * 1000)) // (60 * 1000)
      seconds = (total_ms % (60 * 1000)) // 1000
      millis = total_ms % 1000
      "%02d:%02d:%02d.%03d" % {hours, minutes, seconds, millis}
    end

    # Converts Parquet time (micros since midnight) into a string.
    def self.convert_time_micros_to_string(value : Int64) : String
      total_us = value.to_u64
      hours = total_us // 3_600_000_000_u64
      minutes = (total_us % 3_600_000_000_u64) // 60_000_000_u64
      seconds = (total_us % 60_000_000_u64) // 1_000_000_u64
      micros = total_us % 1_000_000_u64
      "%02d:%02d:%02d.%06d" % {hours, minutes, seconds, micros}
    end

    # Big-endian signed two's-complement bytes -> `BigInt`.
    private def self.bigint_from_signed_bytes_be(bytes : Bytes) : BigInt
      return BigInt.new(0) if bytes.empty?

      num = BigInt.new(0)
      bytes.each { |b| num = (num << 8) | b }
      num -= BigInt.new(1) << (8 * bytes.size) if (bytes[0] & 0x80_u8) != 0
      num
    end

    # Converts a Parquet decimal into a string.
    def self.convert_decimal_to_string(decimal : Decimal) : String
      if decimal.scale < 0 || decimal.precision < decimal.scale
        raise general_err!("Invalid decimal precision/scale")
      end

      num = bigint_from_signed_bytes_be(decimal.data)
      negative = num.sign < 0 ? 1 : 0
      num_str = num.to_s
      point = num_str.size - decimal.scale - negative

      if point <= 0
        while point < 0
          num_str = num_str.insert(negative, '0')
          point += 1
        end
        num_str = num_str.insert(negative, "0.")
      else
        num_str = num_str.insert(point + negative, '.')
      end
      num_str
    end

    # API to represent a single field in a `Row`.
    abstract class Field
      # The variant name (`get_type_name`).
      abstract def type_name : String

      # Determines whether this field is a primitive value.
      def primitive? : ::Bool
        !(is_a?(Field::Group) || is_a?(Field::ListInternal) || is_a?(Field::MapInternal))
      end

      # Converts a Parquet BOOLEAN value.
      def self.convert_bool(_descr : Schema::ColumnDescriptor, value : ::Bool) : Field
        Field::Bool.new(value)
      end

      # Converts a Parquet INT32 value with its converted type.
      def self.convert_int32(descr : Schema::ColumnDescriptor, value : Int32) : Field
        case descr.converted_type
        when ConvertedType::INT_8                       then Field::Byte.new(value.to_i8!)
        when ConvertedType::INT_16                      then Field::Short.new(value.to_i16!)
        when ConvertedType::INT_32, ConvertedType::NONE then Field::Int.new(value)
        when ConvertedType::UINT_8                      then Field::UByte.new(value.to_u8!)
        when ConvertedType::UINT_16                     then Field::UShort.new(value.to_u16!)
        when ConvertedType::UINT_32                     then Field::UInt.new(value.to_u32!)
        when ConvertedType::DATE                        then Field::Date.new(value)
        when ConvertedType::TIME_MILLIS                 then Field::TimeMillis.new(value)
        when ConvertedType::DECIMAL
          Field::Decimal.new(Parquet::Decimal.from_i32(value, descr.type_precision, descr.type_scale))
        else
          raise nyi_err!("Conversion for physical type #{descr.physical_type}, converted type #{descr.converted_type}, value #{value}")
        end
      end

      # Converts a Parquet INT64 value with its converted type.
      def self.convert_int64(descr : Schema::ColumnDescriptor, value : Int64) : Field
        case descr.converted_type
        when ConvertedType::INT_64, ConvertedType::NONE then Field::Long.new(value)
        when ConvertedType::UINT_64                     then Field::ULong.new(value.to_u64!)
        when ConvertedType::TIME_MICROS                 then Field::TimeMicros.new(value)
        when ConvertedType::TIMESTAMP_MILLIS            then Field::TimestampMillis.new(value)
        when ConvertedType::TIMESTAMP_MICROS            then Field::TimestampMicros.new(value)
        when ConvertedType::DECIMAL
          Field::Decimal.new(Parquet::Decimal.from_i64(value, descr.type_precision, descr.type_scale))
        else
          raise nyi_err!("Conversion for physical type #{descr.physical_type}, converted type #{descr.converted_type}, value #{value}")
        end
      end

      # Converts a Parquet INT96 value into a timestamp (millis).
      def self.convert_int96(_descr : Schema::ColumnDescriptor, value : Int96) : Field
        Field::TimestampMillis.new(value.to_millis)
      end

      # Converts a Parquet FLOAT value.
      def self.convert_float(_descr : Schema::ColumnDescriptor, value : Float32) : Field
        Field::Float.new(value)
      end

      # Converts a Parquet DOUBLE value.
      def self.convert_double(_descr : Schema::ColumnDescriptor, value : Float64) : Field
        Field::Double.new(value)
      end

      # Converts a Parquet BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY value.
      def self.convert_byte_array(descr : Schema::ColumnDescriptor, value : ByteArray) : Field
        case descr.physical_type
        when Parquet::Type::BYTE_ARRAY
          case descr.converted_type
          when ConvertedType::UTF8, ConvertedType::ENUM, ConvertedType::JSON
            begin
              Field::Str.new(String.new(value.data))
            rescue ex
              raise general_err!("Error reading BYTE_ARRAY as String. Bytes: #{value.data} Error: #{ex.message}")
            end
          when ConvertedType::BSON, ConvertedType::NONE
            Field::Bytes.new(value)
          when ConvertedType::DECIMAL
            Field::Decimal.new(Parquet::Decimal.from_bytes(value, descr.type_precision, descr.type_scale))
          else
            raise nyi_err!("Conversion for physical type #{descr.physical_type}, converted type #{descr.converted_type}, value #{value.data}")
          end
        when Parquet::Type::FIXED_LEN_BYTE_ARRAY
          case descr.converted_type
          when ConvertedType::DECIMAL
            Field::Decimal.new(Parquet::Decimal.from_bytes(value, descr.type_precision, descr.type_scale))
          when ConvertedType::NONE
            if descr.logical_type_ref.is_a?(LogicalType::Float16)
              if value.len != 2
                raise general_err!(
                  "Error reading FIXED_LEN_BYTE_ARRAY as FLOAT16. Length must be 2, got #{value.len}"
                )
              end
              Field::Float16.new(Record.half_to_f64(value.data))
            else
              Field::Bytes.new(value)
            end
          else
            raise nyi_err!("Conversion for physical type #{descr.physical_type}, converted type #{descr.converted_type}, value #{value.data}")
          end
        else
          raise nyi_err!("Conversion for physical type #{descr.physical_type}, converted type #{descr.converted_type}, value #{value.data}")
        end
      end
    end

    # Accessors for a primitive or complex value at an index (upstream trait).
    module RowAccessor
      macro included
        def is_null(i : Int32) : ::Bool
          field = @fields[i]?
          raise Parquet::IndexOutOfBoundError.new(i.to_u64, @fields.size.to_u64) if field.nil?
          field[1].is_a?(Field::Null)
        end

        {% for entry in [
                          {"get_bool", "Bool", "Bool"},
                          {"get_byte", "Byte", "Int8"},
                          {"get_short", "Short", "Int16"},
                          {"get_int", "Int", "Int32"},
                          {"get_long", "Long", "Int64"},
                          {"get_ubyte", "UByte", "UInt8"},
                          {"get_ushort", "UShort", "UInt16"},
                          {"get_uint", "UInt", "UInt32"},
                          {"get_ulong", "ULong", "UInt64"},
                          {"get_float16", "Float16", "Float64"},
                          {"get_float", "Float", "Float32"},
                          {"get_double", "Double", "Float64"},
                          {"get_timestamp_millis", "TimestampMillis", "Int64"},
                          {"get_timestamp_micros", "TimestampMicros", "Int64"},
                          {"get_decimal", "Decimal", "Decimal"},
                          {"get_string", "Str", "String"},
                          {"get_bytes", "Bytes", "ByteArray"},
                          {"get_group", "Group", "Row"},
                          {"get_list", "ListInternal", "List"},
                          {"get_map", "MapInternal", "Map"},
                        ] %}
          def {{entry[0].id}}(i : Int32) : {{entry[2].id}}
            field = @fields[i]?
            raise Parquet::IndexOutOfBoundError.new(i.to_u64, @fields.size.to_u64) if field.nil?
            value = field[1]
            if value.is_a?(Field::{{entry[1].id}})
              value.value
            else
              raise general_err!("Cannot access #{value.type_name} as {{entry[1].id}}")
            end
          end
        {% end %}
      end
    end

    # Accessors for a value at an index within a `List` (upstream trait).
    module ListAccessor
      macro included
        {% for entry in [
                          {"get_bool", "Bool", "Bool"},
                          {"get_byte", "Byte", "Int8"},
                          {"get_short", "Short", "Int16"},
                          {"get_int", "Int", "Int32"},
                          {"get_long", "Long", "Int64"},
                          {"get_ubyte", "UByte", "UInt8"},
                          {"get_ushort", "UShort", "UInt16"},
                          {"get_uint", "UInt", "UInt32"},
                          {"get_ulong", "ULong", "UInt64"},
                          {"get_float16", "Float16", "Float64"},
                          {"get_float", "Float", "Float32"},
                          {"get_double", "Double", "Float64"},
                          {"get_timestamp_millis", "TimestampMillis", "Int64"},
                          {"get_timestamp_micros", "TimestampMicros", "Int64"},
                          {"get_decimal", "Decimal", "Decimal"},
                          {"get_string", "Str", "String"},
                          {"get_bytes", "Bytes", "ByteArray"},
                          {"get_group", "Group", "Row"},
                          {"get_list", "ListInternal", "List"},
                          {"get_map", "MapInternal", "Map"},
                        ] %}
          def {{entry[0].id}}(i : Int32) : {{entry[2].id}}
            value = @elements[i]
            if value.is_a?(Field::{{entry[1].id}})
              value.value
            else
              raise general_err!("Cannot access #{value.type_name} as {{entry[1].id}}")
            end
          end
        {% end %}
      end
    end

    # Keys/values accessors for a `Map` (upstream trait).
    module MapAccessor
      def get_keys : MapList
        MapList.new(@entries.map { |entry| entry[0] })
      end

      def get_values : MapList
        MapList.new(@entries.map { |entry| entry[1] })
      end
    end

    # Formatting accessor for a `Row` (upstream trait).
    module RowFormatter
      def fmt(i : Int32) : String
        field = @fields[i]?
        field.nil? ? "<IndexOutOfBound>" : field[1].to_s
      end
    end

    # `Row` represents a nested Parquet record.
    class Row
      include RowAccessor
      include RowFormatter

      getter fields : Array(Tuple(String, Field))

      # Divergence: accepts tuples with `Field` subtype members (tuple types are
      # invariant in Crystal) and widens them to `Field`.
      def initialize(fields : Array(Tuple(String, F))) forall F
        @fields = fields.map { |(name, field)| {name, field.as(Field)} }
      end

      # Number of fields in this row.
      def size : Int32
        @fields.size
      end

      # Number of fields in this row (upstream `len`).
      def len : Int32
        @fields.size
      end

      # Moves the columns data out of the row.
      def into_columns : Array(Tuple(String, Field))
        @fields
      end

      # Returns an iterator over `(column name, field)` pairs.
      def get_column_iter : RowColumnIter
        RowColumnIter.new(@fields)
      end

      # Converts the row into a JSON object.
      def to_json_value : Value
        object = Hash(String, Value).new
        @fields.each { |(key, field)| object[key] = field.to_json_value }
        Value.new(object)
      end

      def to_s(io : IO) : Nil
        io << '{'
        @fields.each_with_index do |(key, value), i|
          io << key
          io << ": "
          value.to_s(io)
          io << ", " if i < @fields.size - 1
        end
        io << '}'
      end

      def ==(other : Row) : ::Bool
        @fields == other.fields
      end
    end

    # Iterator over `(column name, field)` pairs in a `Row`.
    class RowColumnIter
      include Iterator(Tuple(String, Field))

      def initialize(@fields : Array(Tuple(String, Field)))
        @curr = 0
      end

      def next : Tuple(String, Field) | Iterator::Stop
        return stop if @curr >= @fields.size

        value = @fields[@curr]
        @curr += 1
        value
      end
    end

    # `List` represents an array of elements.
    class List
      include ListAccessor

      getter elements : Array(Field)

      def initialize(elements : Array(F)) forall F
        @elements = elements.map(&.as(Field))
      end

      # Number of elements in this list (upstream `len`).
      def len : Int32
        @elements.size
      end

      def ==(other : List) : ::Bool
        @elements == other.elements
      end
    end

    # Constructs a `List` from `elements`.
    def self.make_list(elements : Array(F)) : List forall F
      List.new(elements)
    end

    # `Map` represents a list of key->value pairs.
    class Map
      include MapAccessor

      getter entries : Array(Tuple(Field, Field))

      def initialize(entries : Array(Tuple(F1, F2))) forall F1, F2
        @entries = entries.map { |(key, value)| {key.as(Field), value.as(Field)} }
      end

      # Number of entries in this map (upstream `len`).
      def len : Int32
        @entries.size
      end

      def ==(other : Map) : ::Bool
        @entries == other.entries
      end
    end

    # Constructs a `Map` from `entries`.
    def self.make_map(entries : Array(Tuple(F1, F2))) : Map forall F1, F2
      Map.new(entries)
    end

    # A `ListAccessor` over a projected list of fields (`Map` keys or values).
    class MapList
      include ListAccessor

      def initialize(@elements : Array(Field))
      end
    end

    class Field::Null < Field
      def type_name : String
        "Null"
      end

      def to_s(io : IO) : Nil
        io << "null"
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Null)
      end

      def to_json_value : Value
        Value.new(nil)
      end
    end

    class Field::Bool < Field
      getter value : ::Bool

      def initialize(@value : ::Bool)
      end

      def type_name : String
        "Bool"
      end

      def to_s(io : IO) : Nil
        io << @value
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Bool) && @value == other.value
      end

      def to_json_value : Value
        Value.new(@value)
      end
    end

    {% for entry in [
                      {"Byte", "Int8"},
                      {"Short", "Int16"},
                      {"Int", "Int32"},
                      {"Long", "Int64"},
                      {"UByte", "UInt8"},
                      {"UShort", "UInt16"},
                      {"UInt", "UInt32"},
                      {"ULong", "UInt64"},
                    ] %}
      class Field::{{entry[0].id}} < Field
        getter value : {{entry[1].id}}

        def initialize(@value : {{entry[1].id}})
        end

        def type_name : String
          "{{entry[0].id}}"
        end

        def to_s(io : IO) : Nil
          io << @value
        end

        def ==(other : Field) : ::Bool
          other.is_a?(Field::{{entry[0].id}}) && @value == other.value
        end

        def to_json_value : Value
          Value.new(@value.to_i64)
        end
      end
    {% end %}

    class Field::Float16 < Field
      getter value : Float64

      def initialize(@value : Float64)
      end

      def type_name : String
        "Float16"
      end

      def to_s(io : IO) : Nil
        io << Record.rust_display_f16(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Float16) && @value == other.value
      end

      def to_json_value : Value
        @value.finite? ? Value.new(@value) : Value.new(nil)
      end
    end

    class Field::Float < Field
      getter value : Float32

      def initialize(@value : Float32)
      end

      def type_name : String
        "Float"
      end

      def to_s(io : IO) : Nil
        io << Record.rust_display_f32(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Float) && @value == other.value
      end

      def to_json_value : Value
        @value.finite? ? Value.new(@value.to_f64) : Value.new(nil)
      end
    end

    class Field::Double < Field
      getter value : Float64

      def initialize(@value : Float64)
      end

      def type_name : String
        "Double"
      end

      def to_s(io : IO) : Nil
        io << Record.rust_display_f64(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Double) && @value == other.value
      end

      def to_json_value : Value
        @value.finite? ? Value.new(@value) : Value.new(nil)
      end
    end

    class Field::Decimal < Field
      getter value : Parquet::Decimal

      def initialize(@value : Parquet::Decimal)
      end

      def type_name : String
        "Decimal"
      end

      def to_s(io : IO) : Nil
        io << Record.convert_decimal_to_string(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Decimal) && @value == other.value
      end

      def to_json_value : Value
        Value.new(Record.convert_decimal_to_string(@value))
      end
    end

    class Field::Str < Field
      getter value : String

      def initialize(@value : String)
      end

      def type_name : String
        "Str"
      end

      def to_s(io : IO) : Nil
        io << '"' << @value << '"'
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Str) && @value == other.value
      end

      def to_json_value : Value
        Value.new(@value)
      end
    end

    class Field::Bytes < Field
      getter value : ByteArray

      def initialize(@value : ByteArray)
      end

      def type_name : String
        "Bytes"
      end

      def to_s(io : IO) : Nil
        io << '['
        @value.data.each_with_index do |byte, i|
          io << ", " if i > 0
          io << byte
        end
        io << ']'
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Bytes) && @value == other.value
      end

      def to_json_value : Value
        Value.new(Base64.strict_encode(@value.data))
      end
    end

    class Field::Date < Field
      getter value : Int32

      def initialize(@value : Int32)
      end

      def type_name : String
        "Date"
      end

      def to_s(io : IO) : Nil
        io << Record.convert_date_to_string(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Date) && @value == other.value
      end

      def to_json_value : Value
        Value.new(Record.convert_date_to_string(@value))
      end
    end

    class Field::TimeMillis < Field
      getter value : Int32

      def initialize(@value : Int32)
      end

      def type_name : String
        "TimeMillis"
      end

      def to_s(io : IO) : Nil
        io << Record.convert_time_millis_to_string(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::TimeMillis) && @value == other.value
      end

      def to_json_value : Value
        Value.new(Record.convert_time_millis_to_string(@value))
      end
    end

    class Field::TimeMicros < Field
      getter value : Int64

      def initialize(@value : Int64)
      end

      def type_name : String
        "TimeMicros"
      end

      def to_s(io : IO) : Nil
        io << Record.convert_time_micros_to_string(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::TimeMicros) && @value == other.value
      end

      def to_json_value : Value
        Value.new(Record.convert_time_micros_to_string(@value))
      end
    end

    class Field::TimestampMillis < Field
      getter value : Int64

      def initialize(@value : Int64)
      end

      def type_name : String
        "TimestampMillis"
      end

      def to_s(io : IO) : Nil
        io << Record.convert_timestamp_millis_to_string(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::TimestampMillis) && @value == other.value
      end

      def to_json_value : Value
        Value.new(Record.convert_timestamp_millis_to_string(@value))
      end
    end

    class Field::TimestampMicros < Field
      getter value : Int64

      def initialize(@value : Int64)
      end

      def type_name : String
        "TimestampMicros"
      end

      def to_s(io : IO) : Nil
        io << Record.convert_timestamp_micros_to_string(@value)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::TimestampMicros) && @value == other.value
      end

      def to_json_value : Value
        Value.new(Record.convert_timestamp_micros_to_string(@value))
      end
    end

    class Field::Group < Field
      getter value : Row

      def initialize(@value : Row)
      end

      def type_name : String
        "Group"
      end

      def to_s(io : IO) : Nil
        @value.to_s(io)
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::Group) && @value == other.value
      end

      def to_json_value : Value
        @value.to_json_value
      end
    end

    class Field::ListInternal < Field
      getter value : List

      def initialize(@value : List)
      end

      def type_name : String
        "ListInternal"
      end

      def to_s(io : IO) : Nil
        io << '['
        @value.elements.each_with_index do |field, i|
          field.to_s(io)
          io << ", " if i < @value.elements.size - 1
        end
        io << ']'
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::ListInternal) && @value == other.value
      end

      def to_json_value : Value
        Value.new(@value.elements.map(&.to_json_value))
      end
    end

    class Field::MapInternal < Field
      getter value : Map

      def initialize(@value : Map)
      end

      def type_name : String
        "MapInternal"
      end

      def to_s(io : IO) : Nil
        io << '{'
        @value.entries.each_with_index do |(key, value), i|
          key.to_s(io)
          io << " -> "
          value.to_s(io)
          io << ", " if i < @value.entries.size - 1
        end
        io << '}'
      end

      def ==(other : Field) : ::Bool
        other.is_a?(Field::MapInternal) && @value == other.value
      end

      def to_json_value : Value
        object = Hash(String, Value).new
        @value.entries.each do |(key_field, value_field)|
          key_val = key_field.to_json_value
          key_str = key_val.as_s? || key_val.to_json
          object[key_str] = value_field.to_json_value
        end
        Value.new(object)
      end
    end
  end
end
