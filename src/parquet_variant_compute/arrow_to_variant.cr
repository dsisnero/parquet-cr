require "arrow-cr"
require "../parquet_variant"

# Ported from parquet-variant-compute/src/arrow_to_variant.rs (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
module ParquetVariantCompute
  # Reusable row builders for typed Arrow input, including nested sinks.
  module ArrowToVariant
    abstract class RowBuilder
      abstract def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
    end

    class NullRowBuilder < RowBuilder
      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        builder.append_null
      end
    end

    # The macro-generated Rust scalar builders share their null/error policy.
    class ScalarRowBuilder < RowBuilder
      def initialize(@array : ::Arrow::ArrayBase, @options : ::Arrow::CastOptions, &@convert : Int32 -> ParquetVariant::Variant?)
      end

      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        if @array.nulls.try(&.null?(index))
          builder.append_null
        elsif value = @convert.call(index)
          builder.append_value(value)
        elsif @options.safe?
          builder.append_value(ParquetVariant::Variant::Null.new)
        else
          raise ::Arrow::ArrowError.new("Compute error: Failed to convert value at index #{index}: conversion failed")
        end
      end
    end

    # Exhaustive upstream Arrow type dispatch.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.make_row_builder(data_type : ::Arrow::DataType, array : ::Arrow::ArrayBase, options : ::Arrow::CastOptions) : RowBuilder
      kind = data_type.kind
      if kind.duration? || kind.interval?
        raise ParquetVariant::InvalidArgumentError.new("Casting duration/interval types to Variant is not supported. The Variant format does not define duration/interval types.")
      end
      validate_time_unit(data_type)
      return NullRowBuilder.new if kind.null?
      case array
      when ::Arrow::BooleanArray
        booleans = array
        return ScalarRowBuilder.new(array, options) { |index| ParquetVariant::Variant.from(booleans.value(index)) }
      when ::Arrow::PrimitiveArray(::Arrow::Float16)
        halves = array
        return ScalarRowBuilder.new(array, options) { |index| ParquetVariant::Variant.from(halves.value(index).to_f32) }
      when ::Arrow::PrimitiveArray(Int8)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(Int16)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(Int32)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(Int64)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(UInt8)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(UInt16)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(UInt32)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(UInt64)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(Float32)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(Float64)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(Int128)
        return numeric(array, options)
      when ::Arrow::PrimitiveArray(::Arrow::I256)
        wide = array
        return ScalarRowBuilder.new(array, options) do |index|
          if value = wide.value(index).to_i128
            decimal(value, (data_type.scale || 0_i8))
          end
        end
      when ::Arrow::GenericStringArray(Int32), ::Arrow::GenericStringArray(Int64), ::Arrow::StringViewArray,
           ::Arrow::GenericBinaryArray(Int32), ::Arrow::GenericBinaryArray(Int64), ::Arrow::BinaryViewArray, ::Arrow::FixedSizeBinaryArray
        return bytes_or_string(array, options)
      when ::Arrow::GenericListArray(Int32), ::Arrow::GenericListArray(Int64),
           ::Arrow::GenericListViewArray(Int32), ::Arrow::GenericListViewArray(Int64), ::Arrow::FixedSizeListArray
        return ListRowBuilder.new(array, options)
      when ::Arrow::DictionaryArray(Int8), ::Arrow::DictionaryArray(Int16), ::Arrow::DictionaryArray(Int32), ::Arrow::DictionaryArray(Int64),
           ::Arrow::DictionaryArray(UInt8), ::Arrow::DictionaryArray(UInt16), ::Arrow::DictionaryArray(UInt32), ::Arrow::DictionaryArray(UInt64)
        return DictionaryRowBuilder.new(array, options)
      when ::Arrow::RunArray(Int16), ::Arrow::RunArray(Int32), ::Arrow::RunArray(Int64)
        return RunRowBuilder.new(array, options)
      when ::Arrow::UnionArray
        return UnionRowBuilder.new(array, options)
      when ::Arrow::StructArray
        return StructRowBuilder.new(array, options)
      when ::Arrow::MapArray
        return MapRowBuilder.new(array, options)
      end
      raise ParquetVariant::InvalidArgumentError.new("Unsupported Arrow to Variant type: #{data_type}")
    end

    private def self.validate_time_unit(data_type : ::Arrow::DataType) : Nil
      unit = data_type.time_unit
      if data_type.kind.time32? && unit != ::Arrow::TimeUnit::Second && unit != ::Arrow::TimeUnit::Millisecond
        name = unit == ::Arrow::TimeUnit::Microsecond ? "Microsecond" : "Nanosecond"
        raise ParquetVariant::CastError.new("Unsupported Time32 unit: #{name}")
      elsif data_type.kind.time64? && unit != ::Arrow::TimeUnit::Microsecond && unit != ::Arrow::TimeUnit::Nanosecond
        name = unit == ::Arrow::TimeUnit::Second ? "Second" : "Millisecond"
        raise ParquetVariant::CastError.new("Unsupported Time64 unit: #{name}")
      end
    end

    class DictionaryRowBuilder(K) < RowBuilder
      @child : RowBuilder

      def initialize(@array : ::Arrow::DictionaryArray(K), options : ::Arrow::CastOptions)
        values = @array.values
        @child = ArrowToVariant.make_row_builder(values.data_type, values, options)
      end

      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        if key = @array.key(index)
          @child.append_row(builder, key.to_i32)
        else
          builder.append_null
        end
      end
    end

    class UnionRowBuilder < RowBuilder
      @children : Hash(Int8, RowBuilder)

      def initialize(@array : ::Arrow::UnionArray, options : ::Arrow::CastOptions)
        @children = {} of Int8 => RowBuilder
        @array.type_ids.each do |type_id|
          next if @children.has_key?(type_id)
          child = @array.child(type_id)
          @children[type_id] = ArrowToVariant.make_row_builder(child.data_type, child, options)
        end
      end

      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        if child = @children[@array.type_id(index)]?
          child.append_row(builder, @array.value_offset(index))
        else
          builder.append_null
        end
      end
    end

    # The pinned upstream cursor uses raw run ends for row indices, including
    # its behavior on sliced RunArrays. It supports forward and backward jumps.
    class RunRowBuilder(R) < RowBuilder
      @child : RowBuilder
      @ends : Slice(R)
      @run_number = 0
      @run_start = 0_i64

      def initialize(array : ::Arrow::RunArray(R), options : ::Arrow::CastOptions)
        values = array.values
        @child = ArrowToVariant.make_row_builder(values.data_type, values, options)
        @ends = array.run_ends.values
      end

      def select_run_for_index(index : Int32) : Nil
        if index >= @run_start
          run_end = @ends[@run_number]? || raise ParquetVariant::CastError.new("Index #{index} beyond run array")
          return if index < run_end
          if index == run_end
            @run_number += 1
            @run_start = run_end.to_i64
            return
          end
        end
        lower = 0
        upper = @ends.size
        while lower < upper
          middle = lower + (upper - lower) // 2
          @ends[middle] <= index ? (lower = middle + 1) : (upper = middle)
        end
        if lower >= @ends.size
          raise ParquetVariant::CastError.new("Index #{index} beyond run array")
        end
        @run_number = lower
        @run_start = lower == 0 ? 0_i64 : @ends[lower - 1].to_i64
      end

      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        select_run_for_index(index)
        @child.append_row(builder, @run_number)
      end
    end

    private def self.bytes_or_string(array : A, options : ::Arrow::CastOptions) : RowBuilder forall A
      ScalarRowBuilder.new(array, options) { |index| ParquetVariant::Variant.from(array.value(index)) }
    end

    def self.element_range(array : ::Arrow::GenericListArray(O), index : Int32) : Range(Int32, Int32) forall O
      array.value_offsets[index].to_i32...array.value_offsets[index + 1].to_i32
    end

    def self.element_range(array : ::Arrow::GenericListViewArray(O), index : Int32) : Range(Int32, Int32) forall O
      start = array.value_offset(index).to_i32
      start...(start + array.value_size(index).to_i32)
    end

    def self.element_range(array : ::Arrow::FixedSizeListArray, index : Int32) : Range(Int32, Int32)
      start = array.value_offset(index)
      start...(start + array.value_length)
    end

    class ListRowBuilder(L) < RowBuilder
      @child : RowBuilder

      def initialize(@array : L, options : ::Arrow::CastOptions)
        values = @array.values
        @child = ArrowToVariant.make_row_builder(values.data_type, values, options)
      end

      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        if @array.nulls.try(&.null?(index))
          builder.append_null
          return
        end
        list = builder.try_new_list
        ArrowToVariant.element_range(@array, index).each { |row| @child.append_row(list, row) }
        list.finish
      end
    end

    class StructRowBuilder < RowBuilder
      @fields : Array(Tuple(String, RowBuilder))

      def initialize(@array : ::Arrow::StructArray, options : ::Arrow::CastOptions)
        fields = @array.data_type.fields || raise ParquetVariant::InvalidArgumentError.new("Missing struct fields")
        @fields = fields.map_with_index do |field, index|
          column = @array.column(index)
          {field.name, ArrowToVariant.make_row_builder(column.data_type, column, options)}
        end
      end

      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        if @array.nulls.try(&.null?(index))
          builder.append_null
          return
        end
        object = builder.try_new_object
        @fields.each do |name, child|
          child.append_row(ParquetVariant::ObjectFieldBuilder.new(name, object), index)
        end
        object.finish
      end
    end

    class MapRowBuilder < RowBuilder
      @keys : ::Arrow::GenericStringArray(Int32)
      @child : RowBuilder

      def initialize(@array : ::Arrow::MapArray, options : ::Arrow::CastOptions)
        @keys = ::Arrow.cast(@array.keys, ::Arrow::DataType.utf8).as(::Arrow::GenericStringArray(Int32))
        values = @array.values
        @child = ArrowToVariant.make_row_builder(values.data_type, values, options)
      end

      def append_row(builder : ParquetVariant::VariantBuilderExt, index : Int32) : Nil
        if @array.nulls.try(&.null?(index))
          builder.append_null
          return
        end
        object = builder.try_new_object
        (@array.value_offsets[index]...@array.value_offsets[index + 1]).each do |row|
          @child.append_row(ParquetVariant::ObjectFieldBuilder.new(@keys.value(row), object), row)
        end
        object.finish
      end
    end

    private def self.numeric(array : ::Arrow::PrimitiveArray(T), options : ::Arrow::CastOptions) : RowBuilder forall T
      ScalarRowBuilder.new(array, options) { |index| convert_numeric(array.value(index), array.data_type) }
    end

    private def self.decimal(value : T, scale : Int8) : ParquetVariant::Variant? forall T
      {% if T == Int32 %}
        ParquetVariant::Variant.from(ParquetVariant::VariantDecimal4.try_new_with_signed_scale(value, scale))
      {% elsif T == Int64 %}
        ParquetVariant::Variant.from(ParquetVariant::VariantDecimal8.try_new_with_signed_scale(value, scale))
      {% elsif T == Int128 %}
        ParquetVariant::Variant.from(ParquetVariant::VariantDecimal16.try_new_with_signed_scale(value, scale))
      {% else %}
        nil
      {% end %}
    rescue ParquetVariant::InvalidArgumentError
      nil
    end

    private def self.convert_numeric(value : T, data_type : ::Arrow::DataType) : ParquetVariant::Variant? forall T
      case data_type.kind
      when .decimal32?, .decimal64?, .decimal128?
        decimal(value, data_type.scale || 0_i8)
      when .timestamp?
        if time = ::Arrow.as_datetime_with_data_type(data_type, value.to_i64)
          data_type.timezone ? ParquetVariant::Variant.from_datetime_utc(time) : ParquetVariant::Variant.from_naive_datetime(time)
        end
      when .date32?, .date64?
        if time = ::Arrow.as_datetime_with_data_type(data_type, value.to_i64)
          ParquetVariant::Variant.from_date(::Time.utc(time.year, time.month, time.day))
        end
      when .time32?, .time64?
        if time = ::Arrow.as_time_with_data_type(data_type, value.to_i64)
          span = ::Time::Span.new(seconds: time.hour * 3600 + time.minute * 60 + time.second, nanoseconds: time.nanosecond)
          ParquetVariant::Variant.from_time(span)
        end
      else
        {% unless T == Int128 %}
          ParquetVariant::Variant.from(value)
        {% end %}
      end
    rescue ::ArgumentError | ::OverflowError
      nil
    end
  end
end
