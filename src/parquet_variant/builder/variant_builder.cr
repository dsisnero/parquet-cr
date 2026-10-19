require "./state"
require "./object_builder"
require "./list_builder"

# Ported from `parquet-variant/src/builder.rs` (`VariantBuilder`) and
# `parquet-variant/src/builder/metadata.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the top-level `VariantBuilder`. Divergences: a builder may hold at most
# one top-level value (enforced by raising); Rust `Into<Variant>` generics use
# `VariantValue`; `try_*` methods raise instead of returning a `Result`; the
# builder is not borrow-checked, so use `abandon` on an unfinished child to roll
# it back.
module ParquetVariant
  # Message raised when a second top-level value is appended.
  ONE_TOP_LEVEL_VALUE_MSG =
    "VariantBuilder already contains a top-level variant value; only one is allowed"

  # Message raised when finishing a builder with no top-level value.
  EMPTY_BUILDER_MSG =
    "VariantBuilder is empty; append a top-level value before calling finish()"

  # Top-level builder for a single, self-contained `Variant` value.
  class VariantBuilder
    include VariantBuilderExt

    getter value_builder : ValueBuilder
    getter metadata_builder : WritableMetadataBuilder

    def initialize
      @value_builder = ValueBuilder.new
      @metadata_builder = WritableMetadataBuilder.new
      @validate_unique_fields = false
    end

    # Pre-populates the metadata dictionary with `metadata`'s field names.
    def with_metadata(metadata : VariantMetadata) : VariantBuilder
      @metadata_builder.extend(metadata.iter)
      self
    end

    # Enables validation of unique field keys in nested objects.
    def with_validate_unique_fields(validate_unique_fields : Bool) : VariantBuilder
      @validate_unique_fields = validate_unique_fields
      self
    end

    # Pre-populates the metadata dictionary with `field_names`, in order.
    def with_field_names(field_names : Enumerable(String)) : VariantBuilder
      @metadata_builder.extend(field_names)
      self
    end

    # Adds a single field name to the metadata dictionary.
    def add_field_name(field_name : String) : Nil
      @metadata_builder.upsert_field_name(field_name)
      nil
    end

    # A variant value cannot encode NULL, only `Variant::Null` (upstream
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

    # Creates a `ListBuilder` for a new top-level `Variant::List`.
    def new_list : ListBuilder
      check_no_top_level_value
      state = ParentState.variant(@value_builder, @metadata_builder)
      ListBuilder.new(state, @validate_unique_fields)
    end

    # Creates an `ObjectBuilder` for a new top-level `Variant::Object`.
    def new_object : ObjectBuilder
      check_no_top_level_value
      state = ParentState.variant(@value_builder, @metadata_builder)
      ObjectBuilder.new(state, @validate_unique_fields)
    end

    # Appends a top-level value.
    def append_value(value : VariantValue) : Nil
      ensure_no_top_level_value
      state = ParentState.variant(@value_builder, @metadata_builder)
      ValueBuilder.append_variant(state, ValueConversion.to_variant(value))
      nil
    end

    # Fallible version of `append_value` (errors are raised).
    def try_append_value(value : VariantValue) : Nil
      check_no_top_level_value
      state = ParentState.variant(@value_builder, @metadata_builder)
      ValueBuilder.try_append_variant(state, ValueConversion.to_variant(value))
      nil
    end

    # Appends a top-level value by copying raw object/list bytes when possible.
    def append_value_bytes(value : VariantValue) : Nil
      check_no_top_level_value
      state = ParentState.variant(@value_builder, @metadata_builder)
      ValueBuilder.append_variant_bytes(state, ValueConversion.to_variant(value))
      nil
    end

    # Builder-style `append_value` that returns `self`.
    def with_value(value : VariantValue) : VariantBuilder
      append_value(value)
      self
    end

    # Builder-style `try_append_value` that returns `self`.
    def try_with_value(value : VariantValue) : VariantBuilder
      try_append_value(value)
      self
    end

    # Finishes the builder, returning `{metadata_bytes, value_bytes}`.
    def finish : Tuple(Bytes, Bytes)
      try_finish
    end

    # Finishes the builder, raising if no top-level value was appended.
    def try_finish : Tuple(Bytes, Bytes)
      unless has_top_level_value
        raise InvalidArgumentError.new(EMPTY_BUILDER_MSG)
      end
      @metadata_builder.finish
      {@metadata_builder.into_inner, @value_builder.into_inner}
    end

    private def has_top_level_value : Bool
      @value_builder.offset != 0
    end

    private def ensure_no_top_level_value : Nil
      raise InvalidArgumentError.new(ONE_TOP_LEVEL_VALUE_MSG) if has_top_level_value
    end

    private def check_no_top_level_value : Nil
      raise InvalidArgumentError.new(ONE_TOP_LEVEL_VALUE_MSG) if has_top_level_value
    end
  end
end
