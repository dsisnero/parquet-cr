require "./state"
require "./metadata"

# Ported from `parquet-variant/src/builder/object.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ObjectState` and `ObjectBuilder` (variant object construction).
# Divergences: the insertion-ordered `IndexMap<u32, usize>` is an
# `Array(Tuple(UInt32, Int32))`; `Drop`-based rollback is the explicit
# `abandon`; `Extend` is `extend`; Rust `Into<Variant>` generics use
# `VariantValue`; the `VariantBuilderExt`/`ObjectFieldBuilder` wrapper (used by
# `parquet-variant-compute`) is not ported.
module ParquetVariant
  # Internal rollback state for object building.
  class ObjectState < BuilderSpecificState
    def initialize(@fields : Array(Tuple(UInt32, Int32)), @saved_fields_size : Int32)
    end

    def rollback : Nil
      while @fields.size > @saved_fields_size
        @fields.pop
      end
      nil
    end
  end

  # A builder for creating `Variant::Object` values.
  class ObjectBuilder
    getter fields : Array(Tuple(UInt32, Int32))

    def initialize(@parent_state : ParentState, @validate_unique_fields : Bool)
      @fields = [] of Tuple(UInt32, Int32)
    end

    # Adds a field with `key` and `value` to the object.
    def insert(key : String, value : VariantValue) : Nil
      state = parent_state(key)
      ValueBuilder.append_variant(state, ValueConversion.to_variant(value))
      nil
    end

    # Fallible version of `insert` (errors are raised, as in the rest of the port).
    def try_insert(key : String, value : VariantValue) : Nil
      state = parent_state(key)
      ValueBuilder.try_append_variant(state, ValueConversion.to_variant(value))
      nil
    end

    # Adds a field by copying raw object/list bytes when possible.
    def insert_bytes(key : String, value : VariantValue) : Nil
      state = parent_state(key)
      ValueBuilder.append_variant_bytes(state, ValueConversion.to_variant(value))
      nil
    end

    # Builder-style `insert` that returns `self`.
    def with_field(key : String, value : VariantValue) : ObjectBuilder
      insert(key, value)
      self
    end

    # Builder-style `try_insert` that returns `self`.
    def try_with_field(key : String, value : VariantValue) : ObjectBuilder
      try_insert(key, value)
      self
    end

    def with_validate_unique_fields(validate_unique_fields : Bool) : ObjectBuilder
      @validate_unique_fields = validate_unique_fields
      self
    end

    # Returns a nested object builder for a new field named `key`.
    def new_object(key : String) : ObjectBuilder
      ObjectBuilder.new(parent_state(key), @validate_unique_fields)
    end

    # Returns a nested list builder for a new field named `key`.
    def new_list(key : String) : ListBuilder
      ListBuilder.new(parent_state(key), @validate_unique_fields)
    end

    # Inserts `{key, value}` pairs.
    def extend(pairs : Enumerable(Tuple(String, VariantValue))) : Nil
      pairs.each { |(key, value)| insert(key, value) }
      nil
    end

    # Finalizes this object and appends it to its parent.
    def finish : Nil
      metadata_builder = @parent_state.metadata_builder
      @fields.sort_by! { |(field_id, _)| metadata_builder.field_name(field_id.to_i32) }

      max_id = @fields.empty? ? 0_u32 : @fields.max_of { |(field_id, _)| field_id }
      id_size = MetadataBuilder.int_size(max_id.to_i32)

      value_builder = @parent_state.value_builder
      starting_offset = @parent_state.saved_value_builder_offset
      current_offset = value_builder.offset
      data_size = current_offset - starting_offset
      offset_size = MetadataBuilder.int_size(data_size)

      num_fields = @fields.size
      is_large = num_fields > 255

      bytes = [] of UInt8
      large_bit = is_large ? 1 : 0
      header = ((large_bit << 6) |
                ((id_size.value - 1) << 4) |
                ((offset_size.value - 1) << 2) |
                VariantBasicType::Object.value).to_u8
      bytes << header

      if is_large
        ParquetVariant.append_packed(bytes, num_fields, 4)
      else
        ParquetVariant.append_packed(bytes, num_fields, 1)
      end

      @fields.each { |(field_id, _)| ParquetVariant.append_packed(bytes, field_id.to_i64, id_size.value) }
      @fields.each { |(_, offset)| ParquetVariant.append_packed(bytes, offset.to_i64, offset_size.value) }
      ParquetVariant.append_packed(bytes, data_size.to_i64, offset_size.value)

      value_builder.splice(starting_offset, bytes)
      @parent_state.finish
      nil
    end

    # Rolls back this (unfinished) object builder's parent state.
    def abandon : Nil
      @parent_state.rollback
      nil
    end

    private def parent_state(field_name : String) : ParentState
      ParentState.try_object(
        @parent_state.value_builder,
        @parent_state.metadata_builder,
        @fields,
        @parent_state.saved_value_builder_offset,
        field_name,
        @validate_unique_fields,
      )
    end
  end

  class ParentState
    # Creates a parent state for an object field, eagerly registering the
    # field's name and offset (both rolled back on `rollback`).
    def self.try_object(
      value_builder : ValueBuilder,
      metadata_builder : VariantMetadataBuilder,
      fields : Array(Tuple(UInt32, Int32)),
      saved_parent_value_builder_offset : Int32,
      field_name : String,
      validate_unique_fields : Bool,
    ) : ParentState
      saved_value_builder_offset = value_builder.offset
      saved_fields_size = fields.size
      saved_metadata_builder_dict_size = metadata_builder.num_field_names
      field_id = metadata_builder.try_upsert_field_name(field_name)
      field_start = saved_value_builder_offset - saved_parent_value_builder_offset

      existing = fields.index { |(id, _)| id == field_id }
      if existing
        if validate_unique_fields
          raise InvalidArgumentError.new("Duplicate field name: #{field_name}")
        end
        fields[existing] = {field_id, field_start}
      else
        fields << {field_id, field_start}
      end

      state = ObjectState.new(fields, saved_fields_size)
      new(
        value_builder, metadata_builder, state,
        saved_value_builder_offset, saved_metadata_builder_dict_size
      )
    end
  end
end
