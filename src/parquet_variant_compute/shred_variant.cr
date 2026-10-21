require "./variant_array"
require "./variant_value_array_builder"
require "./variant_to_arrow"

# Ported from parquet-variant-compute/src/shred_variant.rs (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
# Rust's builder enums become a class hierarchy; counts use Int32, errors
# raise, and schema builders mutate and return self. List paths are unsupported
# upstream too. Composite typed values must be read through unshred_variant.
module ParquetVariantCompute
  # Shreds a binary variant into typed columns with lossless binary fallbacks.
  def self.shred_variant(array : VariantArray, as_type : ::Arrow::DataType) : VariantArray
    shred_variant_with_options(array, as_type, ::Arrow::CastOptions.default)
  end

  def self.shred_variant_with_options(
    array : VariantArray,
    as_type : ::Arrow::DataType,
    cast_options : ::Arrow::CastOptions,
  ) : VariantArray
    if array.typed_value_column
      raise ParquetVariant::InvalidArgumentError.new("Input is already shredded")
    end
    builder = ShredVariant.make_row_builder(as_type, cast_options, array.len, ShredVariant::NullValue::TopLevelVariant)
    array.len.times do |index|
      if array.is_null(index)
        builder.append_null
      else
        builder.append_value(array.value(index))
      end
    end
    value, typed_value, nulls = builder.finish
    VariantArray.from_parts(array.metadata_column, value, typed_value, nulls)
  end

  # Builders for the shredded (value, typed_value) pair.
  module ShredVariant
    # Encoding of a missing row depends on its position in the variant tree.
    enum NullValue
      TopLevelVariant
      ObjectField
      ArrayElement

      def append_to(nulls : ::Arrow::NullBufferBuilder, values : VariantValueArrayBuilder) : Nil
        top_level_variant? ? nulls.append_null : nulls.append_non_null
        array_element? ? values.append_value(ParquetVariant::Variant::Null.new) : values.append_null
      end
    end

    abstract class RowBuilder
      abstract def append_null : Nil
      abstract def append_value(value : ParquetVariant::Variant) : Bool
      abstract def finish : Tuple(::Arrow::BinaryViewArray, ::Arrow::ArrayBase, ::Arrow::NullBuffer?)
    end

    # Wraps a shredded element pair for the shared list builders.
    class ElementBuilder < VariantToArrow::RowBuilder
      def initialize(@builder : RowBuilder)
      end

      def append_null : Nil
        @builder.append_null
      end

      def append_value(value : ParquetVariant::Variant) : Bool
        @builder.append_value(value)
      end

      def finish : ::Arrow::ArrayBase
        value, typed_value, nulls = @builder.finish
        ShreddedVariantFieldArray.from_parts(value, typed_value, nulls).inner
      end
    end

    def self.make_row_builder(
      data_type : ::Arrow::DataType,
      cast_options : ::Arrow::CastOptions,
      capacity : Int32,
      null_value : NullValue,
      shred : Bool = true,
    ) : RowBuilder
      kind = data_type.kind
      if kind.struct?
        fields = data_type.fields || raise ParquetVariant::InvalidArgumentError.new("Missing struct fields")
        return ObjectRowBuilder.new(fields, cast_options, capacity, null_value, shred)
      end
      if kind.list? || kind.large_list? || kind.list_view? || kind.large_list_view? || kind.fixed_size_list?
        typed = VariantToArrow.make_typed_row_builder(data_type, cast_options, capacity, true)
        return ValueRowBuilder.new(typed, capacity, null_value, true)
      end
      unless primitive_type?(data_type)
        suffix = kind.fixed_size_binary? ? ". Only FixedSizeBinary(16) for UUID is supported." : ""
        raise ParquetVariant::InvalidArgumentError.new("#{data_type} is not a valid variant shredding type#{suffix}")
      end
      typed = VariantToArrow.make_primitive_row_builder(data_type, cast_options, capacity, shred)
      ValueRowBuilder.new(typed, capacity, null_value, false)
    end

    private def self.primitive_type?(data_type : ::Arrow::DataType) : Bool
      case data_type.kind
      when .boolean?, .int8?, .int16?, .int32?, .int64?, .float32?, .float64?,
           .decimal32?, .decimal64?, .decimal128?, .date32?, .binary?,
           .binary_view?, .large_binary?, .utf8?, .utf8_view?, .large_utf8?
        true
      when .time64?
        data_type.time_unit == ::Arrow::TimeUnit::Microsecond
      when .timestamp?
        data_type.time_unit == ::Arrow::TimeUnit::Microsecond || data_type.time_unit == ::Arrow::TimeUnit::Nanosecond
      when .fixed_size_binary?
        data_type.byte_width == 16
      else
        false
      end
    end

    # Primitive and array rows share the outer null/fallback buffers.
    class ValueRowBuilder < RowBuilder
      def initialize(
        @typed : VariantToArrow::RowBuilder,
        capacity : Int32,
        @null_value : NullValue,
        @list : Bool,
      )
        @values = VariantValueArrayBuilder.new(capacity)
        @nulls = ::Arrow::NullBufferBuilder.new(capacity)
      end

      def append_null : Nil
        @null_value.append_to(@nulls, @values)
        @typed.append_null
      end

      def append_value(value : ParquetVariant::Variant) : Bool
        @nulls.append_non_null
        if @list && !value.is_a?(ParquetVariant::Variant::List)
          @values.append_value(value)
          @typed.append_null
          return false
        end
        if @typed.append_value(value)
          @values.append_null
        else
          @values.append_value(value)
        end
        true
      end

      def finish : Tuple(::Arrow::BinaryViewArray, ::Arrow::ArrayBase, ::Arrow::NullBuffer?)
        {@values.build, @typed.finish, @nulls.finish}
      end
    end

    # Routes requested object fields to shredded children and keeps the rest
    # in a binary object using the original metadata dictionary.
    class ObjectRowBuilder < RowBuilder
      @children : Hash(String, RowBuilder)

      def initialize(
        fields : ::Arrow::Fields,
        cast_options : ::Arrow::CastOptions,
        capacity : Int32,
        @null_value : NullValue,
        shred : Bool,
      )
        @children = {} of String => RowBuilder
        fields.each do |field|
          @children[field.name] = ShredVariant.make_row_builder(field.data_type, cast_options, capacity, NullValue::ObjectField, shred)
        end
        @values = VariantValueArrayBuilder.new(capacity)
        @nulls = ::Arrow::NullBufferBuilder.new(capacity)
        @typed_nulls = ::Arrow::NullBufferBuilder.new(capacity)
      end

      def append_null : Nil
        @null_value.append_to(@nulls, @values)
        append_typed_null
      end

      def append_value(value : ParquetVariant::Variant) : Bool
        @nulls.append_non_null
        object = value.as_object
        unless object
          @values.append_value(value)
          append_typed_null
          return false
        end
        seen = Set(String).new
        # Create a residual object only when an unrequested field occurs.
        residual = nil.as(ParquetVariant::ObjectBuilder?)
        object.iter.each do |(name, field_value)|
          if child = @children[name]?
            child.append_value(field_value)
            seen << name
          else
            residual ||= @values.builder_ext(value.metadata).try_new_object
            residual.insert_bytes(name, field_value)
          end
        end
        @children.each { |name, child| child.append_null unless seen.includes?(name) }
        if residual
          residual.finish
        else
          @values.append_null
        end
        @typed_nulls.append_non_null
        true
      end

      private def append_typed_null : Nil
        @typed_nulls.append_null
        @children.each_value(&.append_null)
      end

      def finish : Tuple(::Arrow::BinaryViewArray, ::Arrow::ArrayBase, ::Arrow::NullBuffer?)
        builder = StructArrayBuilder.new
        @children.each do |name, child|
          value, typed_value, nulls = child.finish
          field = ShreddedVariantFieldArray.from_parts(value, typed_value, nulls)
          builder.with_field(name, field.inner, false)
        end
        if nulls = @typed_nulls.finish
          builder.with_nulls(nulls)
        end
        {@values.build, builder.build, @nulls.finish}
      end
    end
  end

  # Target type and nullability for a shredding schema path.
  struct ShreddingField
    getter data_type : ::Arrow::DataType
    getter? nullable : Bool

    def initialize(@data_type : ::Arrow::DataType, @nullable : Bool = true)
    end

    def self.from(field : ShreddingField) : ShreddingField
      field
    end

    def self.from(field : ::Arrow::Field) : ShreddingField
      new(field.data_type, field.nullable?)
    end

    def self.from(data_type : ::Arrow::DataType) : ShreddingField
      new(data_type)
    end

    def self.from(field : Tuple(::Arrow::DataType, Bool)) : ShreddingField
      new(field[0], field[1])
    end
  end

  # Incrementally builds a shredding target from field paths, sorted by name.
  # Later paths replace conflicting leaves, just as upstream does.
  class ShreddedSchemaBuilder
    def initialize
      @root = VariantSchemaNode.new
    end

    def with_path(path : String, field) : self
      with_path(ParquetVariant::VariantPath.parse(path), field)
    rescue error : ParquetVariant::ParseError
      raise ParquetVariant::InvalidArgumentError.new(error.message || "Invalid path")
    end

    def with_path(path : ParquetVariant::VariantPath, field) : self
      @root.insert_path(path.path, ShreddingField.from(field))
      self
    end

    def build : ::Arrow::DataType
      @root.to_shredding_type
    end
  end

  # A schema leaf or a sorted map of nested struct fields.
  class VariantSchemaNode
    def initialize
      @leaf = ShreddingField.new(::Arrow::DataType.null).as(ShreddingField?)
      @children = {} of String => VariantSchemaNode
    end

    def insert_path(segments : Array(ParquetVariant::VariantPathElement), field : ShreddingField) : Nil
      if segments.empty?
        @leaf = field
        @children.clear
        return
      end
      head = segments.first
      unless head.field?
        raise ParquetVariant::InvalidArgumentError.new("List paths are not supported yet")
      end
      @leaf = nil
      name = head.name || raise ParquetVariant::InvalidArgumentError.new("Missing field name")
      child = @children[name] ||= VariantSchemaNode.new
      child.insert_path(segments[1..], field)
    end

    def to_shredding_type : ::Arrow::DataType
      if leaf = @leaf
        return leaf.data_type
      end
      fields = @children.keys.sort!.map { |name| @children[name].to_shredding_field(name) }
      ::Arrow::DataType.struct(::Arrow::Fields.new(fields))
    end

    def to_shredding_field(name : String) : ::Arrow::Field
      ::Arrow::Field.new(name, to_shredding_type, @leaf.try(&.nullable?) || @leaf.nil?)
    end
  end
end
