require "./variant_array"
require "./variant_to_arrow"
require "./unshred_variant"

# Ported from parquet-variant-compute/src/variant_get.rs (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
# Rust's Success/Missing/NotShredded enum becomes ShreddingState | PathStep;
# ArrayRef is Arrow::ArrayBase, and Variant output is its storage StructArray.
# Counts and path indices use Int32; fallible operations raise. Typed output
# shares the existing row builders and their documented target limitations.
module ParquetVariantCompute
  # Path, requested output field, and safe/strict cast behavior for variant_get.
  struct GetOptions
    property path : ParquetVariant::VariantPath
    property as_type : ::Arrow::Field?
    property cast_options : ::Arrow::CastOptions

    def initialize(
      @path : ParquetVariant::VariantPath = ParquetVariant::VariantPath.parse(""),
      @as_type : ::Arrow::Field? = nil,
      @cast_options : ::Arrow::CastOptions = ::Arrow::CastOptions.default,
    )
    end

    def self.new_with_path(path : ParquetVariant::VariantPath) : GetOptions
      new(path: path)
    end

    def with_as_type(@as_type : ::Arrow::Field?) : self
      self
    end

    def with_cast_options(@cast_options : ::Arrow::CastOptions) : self
      self
    end
  end

  # Extracts a path as an Arrow array or a variant storage StructArray.
  def self.variant_get(input : ::Arrow::ArrayBase, options : GetOptions = GetOptions.new) : ::Arrow::ArrayBase
    array = VariantArray.try_new(input)
    VariantGet.shredded_get_path(array, options.path.path, options.as_type, options.cast_options)
  end

  def self.variant_get(input : VariantArray, options : GetOptions = GetOptions.new) : ::Arrow::ArrayBase
    variant_get(input.inner, options)
  end

  # Shredded traversal and row-wise fallback for variant_get.
  module VariantGet
    # A missing path is known absent only when the binary fallback is all-null.
    enum PathStep
      Missing
      NotShredded
    end

    def self.follow_path_element(
      state : ShreddingState,
      element : ParquetVariant::VariantPathElement,
    ) : ShreddingState | PathStep
      typed = state.typed_value
      if typed && element.field?
        if next_state = follow_field(typed, element.name || "")
          return next_state
        end
      end
      if typed && element.index?
        case typed
        when ::Arrow::GenericListArray(Int32), ::Arrow::GenericListArray(Int64),
             ::Arrow::GenericListViewArray(Int32), ::Arrow::GenericListViewArray(Int64)
          if next_state = take_list_index(typed, element.index || 0)
            return next_state
          end
        end
      end
      state.value.null_count == state.value.len ? PathStep::Missing : PathStep::NotShredded
    end

    private def self.follow_field(typed : ::Arrow::ArrayBase, name : String) : ShreddingState?
      object = typed.as?(::Arrow::StructArray)
      return nil unless object
      field = object.column_by_name(name)
      return nil unless field
      unless nested = field.as?(::Arrow::StructArray)
        raise ParquetVariant::InvalidArgumentError.new(
          "Shredded object field '#{name}' must be a Struct containing 'value' and/or 'typed_value', got #{field.data_type}"
        )
      end
      ShreddingState.from_struct(nested)
    end

    private def self.list_range(list : ::Arrow::GenericListArray(O), row : Int32) : Tuple(Int64, Int64) forall O
      {list.value_offsets[row].to_i64, list.value_length(row).to_i64}
    end

    private def self.list_range(list : ::Arrow::GenericListViewArray(O), row : Int32) : Tuple(Int64, Int64) forall O
      {list.value_offset(row).to_i64, list.value_size(row).to_i64}
    end

    private def self.take_list_index(list : L, index : Int32) : ShreddingState? forall L
      elements = list.values.as?(::Arrow::StructArray)
      return nil unless elements
      state = ShreddingState.from_struct(elements)
      return nil if state.typed_value.nil? && state.value.null_count == state.value.len

      indices = ::Arrow::PrimitiveBuilder(UInt64).with_capacity(list.len)
      list.len.times do |row|
        start, length = list_range(list, row)
        if 0 <= index && index < length
          indices.append_value((start + index).to_u64)
        else
          indices.append_null
        end
      end
      index_array = indices.finish
      value = ::Arrow.take(state.value, index_array)
      typed = state.typed_value.try { |array| ::Arrow.take(array, index_array) }
      ShreddingState.new(value, typed)
    end

    def self.shredded_get_path(
      input : VariantArray,
      path : Array(ParquetVariant::VariantPathElement),
      as_field : ::Arrow::Field?,
      cast_options : ::Arrow::CastOptions,
    ) : ::Arrow::ArrayBase
      state = input.shredding_state
      nulls = input.nulls
      path.each_with_index do |element, index|
        case step = follow_path_element(state, element)
        when ShreddingState
          nulls = ::Arrow::NullBuffer.union(nulls, state.typed_value.try(&.nulls))
          state = step
        when PathStep
          if step.missing?
            return missing_output(input, as_field)
          end
          target = VariantArray.from_parts(input.metadata_column, state.value, nil, nulls)
          return basic_variant(target, ParquetVariant::VariantPath.new(path[index..]), as_field, cast_options)
        end
      end
      target = VariantArray.from_parts(input.metadata_column, state.value, state.typed_value, nulls)
      return target.inner unless as_field
      if perfect = try_perfect_shredding(target, as_field)
        return perfect
      end
      if !variant_field?(as_field) && as_field.data_type.kind.struct? && target.typed_value_column
        fields = as_field.data_type.fields || raise ParquetVariant::InvalidArgumentError.new("Missing struct fields")
        children = fields.map do |field|
          shredded_get_path(target, [ParquetVariant::VariantPathElement.field(field.name)], field, cast_options)
        end
        return ::Arrow::StructArray.try_new(fields, children, target.nulls)
      end
      basic_variant(target, ParquetVariant::VariantPath.parse(""), as_field, cast_options)
    end

    private def self.variant_field?(field : ::Arrow::Field?) : Bool
      field.try(&.has_valid_extension_type?(VariantType)) || false
    rescue Parquet::ArrowError
      # Custom VariantType validation uses the port's Parquet error hierarchy.
      false
    end

    private def self.missing_output(input : VariantArray, as_field : ::Arrow::Field?) : ::Arrow::ArrayBase
      if variant_field?(as_field)
        return VariantArray.from_parts_unshredded(
          input.metadata_column, VariantArray.all_null_value_column(input.len), ::Arrow::NullBuffer.new_null(input.len)
        ).inner
      end
      ::Arrow.new_null_array(as_field.try(&.data_type) || ::Arrow::DataType.null, input.len)
    end

    private def self.try_perfect_shredding(target : VariantArray, field : ::Arrow::Field) : ::Arrow::ArrayBase?
      return nil if field.data_type.kind.struct?
      typed = target.typed_value_column
      return nil unless typed && typed.data_type == field.data_type
      return nil unless target.value_column.null_count == target.len
      return typed if target.nulls.nil? || typed.data_type.kind.null?

      nulls = ::Arrow::NullBuffer.union(target.nulls, typed.nulls)
      ::Arrow.make_array(typed.to_data.into_builder.nulls(nulls).build)
    rescue ::Arrow::ArrowError
      nil
    end

    def self.basic_variant(
      target : VariantArray,
      path : ParquetVariant::VariantPath,
      as_field : ::Arrow::Field?,
      cast_options : ::Arrow::CastOptions,
    ) : ::Arrow::ArrayBase
      requested_variant = variant_field?(as_field)
      if requested_variant
        if as_field.try(&.data_type.fields).try(&.any? { |field| field.name == "typed_value" })
          raise nyi_err!("variant_get with shredded `Variant` output is not yet supported")
        end
        target = ParquetVariantCompute.unshred_variant(target)
        return target.inner if path.empty?
      end
      data_type = requested_variant ? nil : as_field.try(&.data_type)
      builder = VariantToArrow.make_row_builder(target.metadata_column, path, data_type, cast_options, target.len)
      target.len.times do |index|
        if target.is_null(index)
          builder.append_null
          next
        end
        value = begin
          target.try_value(index)
        rescue error : Parquet::ParquetError
          raise error unless cast_options.safe?
          builder.append_null
          next
        end
        builder.append_value(value)
      end
      builder.finish
    end
  end
end
