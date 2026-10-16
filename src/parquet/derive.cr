require "uuid"
require "./data_type"
require "./record/record_reader"
require "./record/record_writer"
require "./schema/types"
require "./file/reader"
require "./file/writer"

# Ported from `parquet_derive/src/{lib.rs,parquet_field.rs}` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `ParquetRecordWriter`/`ParquetRecordReader` derive macros for flat,
# simple structs.
#
# Divergences: Rust `#[derive(..)]` trait impls (for `&[T]` / `Vec<T>`) are
# replaced by `include Parquet::Derive::Record`, which generates nested
# `T::Writer < RecordWriter(T)` (holding `Array(T)`) and `T::Reader <
# RecordReader(T)` (with a `records` getter) plus the per-field schema/write/read
# code. The reader constructs records positionally via the record's
# `initialize`, so a record must declare its fields in the same order as its
# `initialize` parameters (Rust instead uses `Default` + pub field assignment).
# Supported field types are Crystal-native: `Bool`, `Int8/16/32/64`,
# `UInt8/16/32/64`, `Float32/64`, `String`, `Bytes`, `UUID`, `Time` (as
# `TIMESTAMP_MILLIS`), and `Parquet::Derive::Date` (as `DATE`, since Crystal has
# no distinct date type); nilable (`T?`) fields are `OPTIONAL`. Borrowed Rust
# forms (`&str`, `Arc<str>`, `&Option<..>`, `Vec<u8>`) have no Crystal analogue.
# The generated schema group is named `rust_schema` (matching upstream).
module Parquet
  module Derive
    # A date value stored as days since the Unix epoch (`DATE` logical type).
    struct Date
      getter days : Int32

      def initialize(@days : Int32)
      end

      # Creates a date from an integral number of days since the Unix epoch.
      def self.from_days(days : Int32) : Date
        new(days)
      end

      # Creates a date from a UTC `Time` (truncated to whole days).
      def self.from_time(time : Time) : Date
        new((time.to_unix // 86_400).to_i32)
      end

      # Converts back to a UTC `Time` at midnight.
      def to_time : Time
        Time.unix(@days.to_i64 * 86_400)
      end

      def ==(other : Date) : Bool
        @days == other.days
      end
    end

    # Per-field type mapping between a Crystal field type `T` and its Parquet
    # physical column representation (the port of `parquet_field.rs`).
    module Codec(T)
      # Writes a batch of non-nil values to a REQUIRED column.
      def self.write_required(column_writer, values : Array(T)) : Nil
        {% if T == Bool %}
          column_writer.typed(::Bool).write_batch(values, nil, nil)
        {% elsif T == Int8 || T == Int16 || T == UInt8 || T == UInt16 || T == UInt32 %}
          column_writer.typed(::Int32).write_batch(values.map(&.to_i32), nil, nil)
        {% elsif T == Int32 %}
          column_writer.typed(::Int32).write_batch(values, nil, nil)
        {% elsif T == UInt64 %}
          column_writer.typed(::Int64).write_batch(values.map(&.to_i64), nil, nil)
        {% elsif T == Int64 %}
          column_writer.typed(::Int64).write_batch(values, nil, nil)
        {% elsif T == Float32 %}
          column_writer.typed(::Float32).write_batch(values, nil, nil)
        {% elsif T == Float64 %}
          column_writer.typed(::Float64).write_batch(values, nil, nil)
        {% elsif T == String %}
          column_writer.typed(::Parquet::ByteArray).write_batch(values.map { |v| ::Parquet::ByteArray.new(v.to_slice) }, nil, nil)
        {% elsif T == Slice(UInt8) %}
          column_writer.typed(::Parquet::ByteArray).write_batch(values.map { |v| ::Parquet::ByteArray.new(v) }, nil, nil)
        {% elsif T == UUID %}
          column_writer.typed(::Parquet::FixedLenByteArray).write_batch(
            values.map { |v| ::Parquet::FixedLenByteArray.new(::Parquet::ByteArray.new(v.bytes.to_slice)) }, nil, nil)
        {% elsif T == Time %}
          column_writer.typed(::Int64).write_batch(values.map { |v| (v.to_unix_ns // 1_000_000_i64).to_i64 }, nil, nil)
        {% elsif T == ::Parquet::Derive::Date %}
          column_writer.typed(::Int32).write_batch(values.map(&.days), nil, nil)
        {% else %}
          {% raise "parquet derive: unsupported field type #{T}" %}
        {% end %}
      end

      # Writes a batch of nullable values to an OPTIONAL column.
      def self.write_optional(column_writer, values : Array(T?)) : Nil
        {% if T == Bool %}
          physical = [] of Bool
          def_levels = [] of Int16
          values.each { |v| if v
            physical << v; def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Bool).write_batch(physical, def_levels, nil)
        {% elsif T == Int8 || T == Int16 || T == UInt8 || T == UInt16 || T == Int32 || T == UInt32 %}
          physical = [] of Int32
          def_levels = [] of Int16
          values.each { |v| if v
            physical << v.to_i32; def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Int32).write_batch(physical, def_levels, nil)
        {% elsif T == Int64 || T == UInt64 %}
          physical = [] of Int64
          def_levels = [] of Int16
          values.each { |v| if v
            physical << v.to_i64; def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Int64).write_batch(physical, def_levels, nil)
        {% elsif T == Float32 %}
          physical = [] of Float32
          def_levels = [] of Int16
          values.each { |v| if v
            physical << v; def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Float32).write_batch(physical, def_levels, nil)
        {% elsif T == Float64 %}
          physical = [] of Float64
          def_levels = [] of Int16
          values.each { |v| if v
            physical << v; def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Float64).write_batch(physical, def_levels, nil)
        {% elsif T == String %}
          physical = [] of ::Parquet::ByteArray
          def_levels = [] of Int16
          values.each { |v| if v
            physical << ::Parquet::ByteArray.new(v.to_slice); def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Parquet::ByteArray).write_batch(physical, def_levels, nil)
        {% elsif T == Slice(UInt8) %}
          physical = [] of ::Parquet::ByteArray
          def_levels = [] of Int16
          values.each { |v| if v
            physical << ::Parquet::ByteArray.new(v); def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Parquet::ByteArray).write_batch(physical, def_levels, nil)
        {% elsif T == UUID %}
          physical = [] of ::Parquet::FixedLenByteArray
          def_levels = [] of Int16
          values.each { |v| if v
            physical << ::Parquet::FixedLenByteArray.new(::Parquet::ByteArray.new(v.bytes.to_slice)); def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Parquet::FixedLenByteArray).write_batch(physical, def_levels, nil)
        {% elsif T == Time %}
          physical = [] of Int64
          def_levels = [] of Int16
          values.each { |v| if v
            physical << ((v.to_unix_ns // 1_000_000_i64).to_i64); def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Int64).write_batch(physical, def_levels, nil)
        {% elsif T == ::Parquet::Derive::Date %}
          physical = [] of Int32
          def_levels = [] of Int16
          values.each { |v| if v
            physical << v.days; def_levels << 1_i16
          else
            def_levels << 0_i16
          end }
          column_writer.typed(::Int32).write_batch(physical, def_levels, nil)
        {% else %}
          {% raise "parquet derive: unsupported field type #{T}" %}
        {% end %}
      end

      # Reads `num_records` values from a column into an array of `T`. Raises if
      # the column contains nulls (upstream panics on nulls here).
      def self.read_required(col_reader, num_records : Int32) : Array(T)
        {% if T == Bool %}
          typed = ::Parquet::Column.get_typed_column_reader(::Bool, col_reader)
          def_levels = [] of Int16
          raw = [] of Bool
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw
        {% elsif T == Int8 || T == Int16 || T == UInt8 || T == UInt16 || T == Int32 || T == UInt32 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int32, col_reader)
          def_levels = [] of Int16
          raw = [] of Int32
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw.map { |v| narrow_i32(v) }
        {% elsif T == Int64 || T == UInt64 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int64, col_reader)
          def_levels = [] of Int16
          raw = [] of Int64
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw.map { |v| narrow_i64(v) }
        {% elsif T == Float32 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Float32, col_reader)
          def_levels = [] of Int16
          raw = [] of Float32
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw
        {% elsif T == Float64 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Float64, col_reader)
          def_levels = [] of Int16
          raw = [] of Float64
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw
        {% elsif T == String %}
          typed = ::Parquet::Column.get_typed_column_reader(::Parquet::ByteArray, col_reader)
          def_levels = [] of Int16
          raw = [] of ::Parquet::ByteArray
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw.map { |ba| ::String.new(ba.data) }
        {% elsif T == Slice(UInt8) %}
          typed = ::Parquet::Column.get_typed_column_reader(::Parquet::ByteArray, col_reader)
          def_levels = [] of Int16
          raw = [] of ::Parquet::ByteArray
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw.map(&.data)
        {% elsif T == UUID %}
          typed = ::Parquet::Column.get_typed_column_reader(::Parquet::FixedLenByteArray, col_reader)
          def_levels = [] of Int16
          raw = [] of ::Parquet::FixedLenByteArray
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw.map { |fl| ::UUID.new(fl.data) }
        {% elsif T == Time %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int64, col_reader)
          def_levels = [] of Int16
          raw = [] of Int64
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw.map { |v| ::Time.unix_ms(v) }
        {% elsif T == ::Parquet::Derive::Date %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int32, col_reader)
          def_levels = [] of Int16
          raw = [] of Int32
          typed.read_records(num_records, def_levels, nil, raw)
          ensure_no_nulls(def_levels)
          raw.map { |v| ::Parquet::Derive::Date.new(v) }
        {% else %}
          {% raise "parquet derive: unsupported field type #{T}" %}
        {% end %}
      end

      # Reads `num_records` values from a column into an array of `T?`.
      def self.read_optional(col_reader, num_records : Int32) : Array(T?)
        {% if T == Bool %}
          typed = ::Parquet::Column.get_typed_column_reader(::Bool, col_reader)
          def_levels = [] of Int16
          raw = [] of Bool
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == Int8 || T == Int16 || T == UInt8 || T == UInt16 || T == Int32 || T == UInt32 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int32, col_reader)
          def_levels = [] of Int16
          raw = [] of Int32
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == Int64 || T == UInt64 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int64, col_reader)
          def_levels = [] of Int16
          raw = [] of Int64
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == Float32 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Float32, col_reader)
          def_levels = [] of Int16
          raw = [] of Float32
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == Float64 %}
          typed = ::Parquet::Column.get_typed_column_reader(::Float64, col_reader)
          def_levels = [] of Int16
          raw = [] of Float64
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == String %}
          typed = ::Parquet::Column.get_typed_column_reader(::Parquet::ByteArray, col_reader)
          def_levels = [] of Int16
          raw = [] of ::Parquet::ByteArray
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == Slice(UInt8) %}
          typed = ::Parquet::Column.get_typed_column_reader(::Parquet::ByteArray, col_reader)
          def_levels = [] of Int16
          raw = [] of ::Parquet::ByteArray
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == UUID %}
          typed = ::Parquet::Column.get_typed_column_reader(::Parquet::FixedLenByteArray, col_reader)
          def_levels = [] of Int16
          raw = [] of ::Parquet::FixedLenByteArray
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == Time %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int64, col_reader)
          def_levels = [] of Int16
          raw = [] of Int64
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% elsif T == ::Parquet::Derive::Date %}
          typed = ::Parquet::Column.get_typed_column_reader(::Int32, col_reader)
          def_levels = [] of Int16
          raw = [] of Int32
          typed.read_records(num_records, def_levels, nil, raw)
          assemble_optional(def_levels, raw) { |v| from_physical(v) }
        {% else %}
          {% raise "parquet derive: unsupported field type #{T}" %}
        {% end %}
      end

      private def self.ensure_no_nulls(def_levels : Array(Int16)) : Nil
        if def_levels.includes?(0_i16)
          raise ::Parquet::GeneralError.new(
            "Support only valid records, found null records in a required field"
          )
        end
      end

      private def self.assemble_optional(def_levels : Array(Int16), raw : Array(U), & : U -> T) : Array(T?) forall U
        out = Array(T?).new
        if def_levels.empty?
          raw.each { |value| out << yield value }
        else
          raw_index = 0
          def_levels.each do |level|
            if level == 1_i16
              out << yield raw[raw_index]
              raw_index += 1
            else
              out << nil
            end
          end
        end
        out
      end

      # Converts a decoded physical value to `T` (reader direction).
      private def self.from_physical(value) : T
        {% if T == Bool || T == Float32 || T == Float64 %}
          value
        {% elsif T == Int8 %}
          value.to_i8
        {% elsif T == Int16 %}
          value.to_i16
        {% elsif T == Int32 %}
          value
        {% elsif T == UInt8 %}
          value.to_u8
        {% elsif T == UInt16 %}
          value.to_u16
        {% elsif T == UInt32 %}
          value.to_u32
        {% elsif T == Int64 %}
          value
        {% elsif T == UInt64 %}
          value.to_u64
        {% elsif T == String %}
          ::String.new(value.data)
        {% elsif T == Slice(UInt8) %}
          value.data
        {% elsif T == UUID %}
          ::UUID.new(value.data)
        {% elsif T == Time %}
          ::Time.unix_ms(value)
        {% elsif T == ::Parquet::Derive::Date %}
          ::Parquet::Derive::Date.new(value)
        {% else %}
          {% raise "parquet derive: unsupported field type #{T}" %}
        {% end %}
      end

      # Narrows a physical `Int32` to `T`.
      private def self.narrow_i32(value : Int32) : T
        from_physical(value)
      end

      # Narrows a physical `Int64` to `T`.
      private def self.narrow_i64(value : Int64) : T
        from_physical(value)
      end
    end

    # Mixed into a record type to generate its `Writer`/`Reader` classes.
    module Record
      macro included
        def self.__parquet_schema : ::Parquet::Schema::Type
          fields = ::Array(::Parquet::Schema::Type).new
          {% verbatim do %}
            {% for iv in @type.instance_vars %}
              {% field_type = iv.type %}
              {% optional = field_type.union? && field_type.union_types.any? { |candidate| candidate.resolve == Nil } %}
              {% base = (optional ? field_type.union_types.reject { |candidate| candidate.resolve == Nil }.first : field_type).resolve %}
              {% physical = nil %}
              {% logical = nil %}
              {% converted = nil %}
              {% length = nil %}
              {% if base == Bool %}
                {% physical = "::Parquet::Type::BOOLEAN" %}
              {% elsif base == Int8 || base == Int16 || base == Int32 || base == UInt8 || base == UInt16 || base == UInt32 %}
                {% physical = "::Parquet::Type::INT32" %}
                {% if base == Int8 %}{% logical = "::Parquet::LogicalType.integer(8, true)" %}
                {% elsif base == Int16 %}{% logical = "::Parquet::LogicalType.integer(16, true)" %}
                {% elsif base == UInt8 %}{% logical = "::Parquet::LogicalType.integer(8, false)" %}
                {% elsif base == UInt16 %}{% logical = "::Parquet::LogicalType.integer(16, false)" %}
                {% elsif base == UInt32 %}{% logical = "::Parquet::LogicalType.integer(32, false)" %}
                {% end %}
              {% elsif base == Int64 %}
                {% physical = "::Parquet::Type::INT64" %}
                {% logical = "::Parquet::LogicalType.integer(64, true)" %}
              {% elsif base == UInt64 %}
                {% physical = "::Parquet::Type::INT64" %}
                {% logical = "::Parquet::LogicalType.integer(64, false)" %}
              {% elsif base == Float32 %}
                {% physical = "::Parquet::Type::FLOAT" %}
              {% elsif base == Float64 %}
                {% physical = "::Parquet::Type::DOUBLE" %}
              {% elsif base == String %}
                {% physical = "::Parquet::Type::BYTE_ARRAY" %}
                {% logical = "::Parquet::LogicalType::String.new" %}
              {% elsif base == Slice(UInt8) %}
                {% physical = "::Parquet::Type::BYTE_ARRAY" %}
              {% elsif base == UUID %}
                {% physical = "::Parquet::Type::FIXED_LEN_BYTE_ARRAY" %}
                {% logical = "::Parquet::LogicalType::Uuid.new" %}
                {% length = 16 %}
              {% elsif base == Time %}
                {% physical = "::Parquet::Type::INT64" %}
                {% converted = "::Parquet::ConvertedType::TIMESTAMP_MILLIS" %}
              {% elsif base == ::Parquet::Derive::Date %}
                {% physical = "::Parquet::Type::INT32" %}
                {% logical = "::Parquet::LogicalType::Date.new" %}
              {% else %}
                {% raise "parquet derive: unsupported field type #{base}" %}
              {% end %}
              fields << ::Parquet::Schema::Type.primitive_type_builder(
                {{iv.name.stringify}},
                {{physical.id}},
              )
                .with_repetition({{ (optional ? "::Parquet::Repetition::OPTIONAL" : "::Parquet::Repetition::REQUIRED").id }})
                {% if logical %}.with_logical_type({{logical.id}}){% end %}
                {% if converted %}.with_converted_type({{converted.id}}){% end %}
                {% if length %}.with_length({{length}}){% end %}
                .build
            {% end %}
          {% end %}
          ::Parquet::Schema::Type.group_type_builder("rust_schema").with_fields(fields).build
        end

        def self.__parquet_write(records : ::Array({{@type}}), row_group_writer : ::Parquet::File::SerializedRowGroupWriter) : ::Nil
          {% verbatim do %}
            {% for iv in @type.instance_vars %}
              {% field_type = iv.type %}
              {% optional = field_type.union? && field_type.union_types.any? { |candidate| candidate.resolve == Nil } %}
              {% base = (optional ? field_type.union_types.reject { |candidate| candidate.resolve == Nil }.first : field_type).resolve %}
              begin
                column_writer = row_group_writer.next_column.not_nil!
                values = records.map { |record| record.{{iv.name}} }
                {% if optional %}
                  ::Parquet::Derive::Codec({{base}}).write_optional(column_writer, values)
                {% else %}
                  ::Parquet::Derive::Codec({{base}}).write_required(column_writer, values)
                {% end %}
                column_writer.close
              end
            {% end %}
          {% end %}
          nil
        end

        def self.__parquet_read(row_group_reader : ::Parquet::File::RowGroupReader, num_records : ::Int32) : ::Array({{@type}})
          name_to_index = ::Hash(::String, ::Int32).new
          row_group_reader.metadata.schema_descr.columns.each_with_index do |column, index|
            name_to_index[column.name] = index
          end
          out = ::Array({{@type}}).new
          {% verbatim do %}
            {% begin %}
              {% for iv, i in @type.instance_vars %}
                {% field_type = iv.type %}
                {% optional = field_type.union? && field_type.union_types.any? { |candidate| candidate.resolve == Nil } %}
                {% base = (optional ? field_type.union_types.reject { |candidate| candidate.resolve == Nil }.first : field_type).resolve %}
                v_{{i}} = begin
                  column_index = name_to_index[{{iv.name.stringify}}]?
                  if column_index.nil?
                    raise ::Parquet::GeneralError.new("column name '{{iv.name}}' is not found in parquet file!")
                  end
                  column_reader = row_group_reader.get_column_reader(column_index)
                  {% if optional %}
                    ::Parquet::Derive::Codec({{base}}).read_optional(column_reader, num_records)
                  {% else %}
                    ::Parquet::Derive::Codec({{base}}).read_required(column_reader, num_records)
                  {% end %}
                end
              {% end %}

              {% arguments = [] of Nil %}
              {% for _iv, k in @type.instance_vars %}
                {% arguments << ("v_" + k.stringify + "[i]") %}
              {% end %}
              num_records.times do |i|
                out << {{@type}}.new({{arguments.join(", ").id}})
              end
            {% end %}
          {% end %}
          out
        end

        class Writer < ::Parquet::Record::RecordWriter({{@type}})
          def initialize(@records : ::Array({{@type}}))
          end

          def schema : ::Parquet::Schema::Type
            {{@type}}.__parquet_schema
          end

          def write_to_row_group(row_group_writer : ::Parquet::File::SerializedRowGroupWriter) : ::Nil
            {{@type}}.__parquet_write(@records, row_group_writer)
          end
        end

        class Reader < ::Parquet::Record::RecordReader({{@type}})
          getter records : ::Array({{@type}}) = ::Array({{@type}}).new

          def read_from_row_group(row_group_reader : ::Parquet::File::RowGroupReader, num_records : ::Int32) : ::Nil
            @records.concat({{@type}}.__parquet_read(row_group_reader, num_records))
            nil
          end
        end
      end
    end
  end
end
