require "./state"
require "./object_builder"

# Ported from `parquet-variant/src/builder/list.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ListState` and `ListBuilder` (variant list construction). Divergences:
# `Drop`-based rollback is the explicit `abandon`; `Extend` is `extend`; Rust
# `Into<Variant>` generics use `VariantValue`; `VariantBuilderExt` lives in
# `builder/ext.cr`.
module ParquetVariant
  # Internal rollback state for list building.
  class ListState < BuilderSpecificState
    def initialize(@offsets : Array(Int32), @saved_offsets_size : Int32)
    end

    def rollback : Nil
      while @offsets.size > @saved_offsets_size
        @offsets.pop
      end
      nil
    end
  end

  # A builder for creating `Variant::List` values.
  class ListBuilder
    include VariantBuilderExt

    getter offsets : Array(Int32)

    def initialize(@parent_state : ParentState, @validate_unique_fields : Bool)
      @offsets = [] of Int32
    end

    def with_validate_unique_fields(validate_unique_fields : Bool) : ListBuilder
      @validate_unique_fields = validate_unique_fields
      self
    end

    # Returns a nested object builder appended to this list.
    def new_object : ObjectBuilder
      ObjectBuilder.new(parent_state, @validate_unique_fields)
    end

    # Returns a nested list builder appended to this list.
    def new_list : ListBuilder
      ListBuilder.new(parent_state, @validate_unique_fields)
    end

    # Variant arrays cannot encode NULL values, only `Variant::Null` (upstream
    # `VariantBuilderExt::append_null`).
    def append_null : Nil
      append_value(Variant.from_nil)
    end

    def try_new_list : ListBuilder
      new_list
    end

    def try_new_object : ObjectBuilder
      new_object
    end

    # Appends `value` to the list.
    def append_value(value : VariantValue) : Nil
      ValueBuilder.append_variant(parent_state, ValueConversion.to_variant(value))
      nil
    end

    # Fallible version of `append_value` (errors are raised).
    def try_append_value(value : VariantValue) : Nil
      ValueBuilder.try_append_variant(parent_state, ValueConversion.to_variant(value))
      nil
    end

    # Appends `value` by copying raw object/list bytes when possible.
    def append_value_bytes(value : VariantValue) : Nil
      ValueBuilder.append_variant_bytes(parent_state, ValueConversion.to_variant(value))
      nil
    end

    # Builder-style `append_value` that returns `self`.
    def with_value(value : VariantValue) : ListBuilder
      append_value(value)
      self
    end

    # Builder-style `try_append_value` that returns `self`.
    def try_with_value(value : VariantValue) : ListBuilder
      try_append_value(value)
      self
    end

    # Appends each value in `values`.
    def extend(values : Enumerable(VariantValue)) : Nil
      values.each { |value| append_value(value) }
      nil
    end

    # Finalizes this list and appends it to its parent.
    def finish : Nil
      value_builder = @parent_state.value_builder
      starting_offset = @parent_state.saved_value_builder_offset
      data_size = value_builder.offset - starting_offset

      num_elements = @offsets.size
      is_large = num_elements > 255
      offset_size = MetadataBuilder.int_size(data_size)
      num_elements_size = is_large ? 4 : 1
      large_bit = is_large ? 1 : 0

      header = ((large_bit << 4) |
                ((offset_size.value - 1) << 2) |
                VariantBasicType::Array.value).to_u8

      bytes = [] of UInt8
      bytes << header
      ParquetVariant.append_packed(bytes, num_elements, num_elements_size)
      @offsets.each { |offset| ParquetVariant.append_packed(bytes, offset, offset_size.value) }
      ParquetVariant.append_packed(bytes, data_size, offset_size.value)

      value_builder.splice(starting_offset, bytes)
      @parent_state.finish
      nil
    end

    # Rolls back this (unfinished) list builder's parent state.
    def abandon : Nil
      @parent_state.rollback
      nil
    end

    private def parent_state : ParentState
      ParentState.list(
        @parent_state.value_builder,
        @parent_state.metadata_builder,
        @offsets,
        @parent_state.saved_value_builder_offset,
      )
    end
  end

  class ParentState
    # Creates a parent state for a list element, eagerly registering its offset
    # (rolled back on `rollback`).
    def self.list(
      value_builder : ValueBuilder,
      metadata_builder : VariantMetadataBuilder,
      offsets : Array(Int32),
      saved_parent_value_builder_offset : Int32,
    ) : ParentState
      saved_value_builder_offset = value_builder.offset
      saved_offsets_size = offsets.size
      offsets << (saved_value_builder_offset - saved_parent_value_builder_offset)

      state = ListState.new(offsets, saved_offsets_size)
      new(
        value_builder, metadata_builder, state,
        saved_value_builder_offset, metadata_builder.num_field_names
      )
    end
  end
end
