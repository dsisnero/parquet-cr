# Ported from `parquet-variant/src/builder.rs` (`VariantBuilderExt`) and
# `parquet-variant/src/builder/object.rs` (`ObjectFieldBuilder`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `VariantBuilderExt` gives `VariantBuilder`, `ListBuilder` and
# `ObjectFieldBuilder` a uniform interface for appending values and creating
# nested builders, which is what `parquet-variant-json`'s `append_json` and
# `parquet-variant-compute` build on.
#
# Divergences: the Rust trait with an associated `State` type is a module; the
# fallible `try_new_list`/`try_new_object` raise instead of returning `Result`
# (as everywhere in the port), and `new_list`/`new_object` delegate to them (the
# builders that also define those methods keep their own).
module ParquetVariant
  # Extends the variant builders with a uniform value/nested-builder API
  # (upstream `VariantBuilderExt`).
  module VariantBuilderExt
    # Appends a NULL value. The semantics depend on the implementer: builders
    # append `Variant::Null`, while an object field is interpreted as missing.
    abstract def append_null : Nil

    # Appends a variant value.
    abstract def append_value(value : VariantValue) : Nil

    # Creates a nested list builder (raises when it cannot be created).
    abstract def try_new_list : ListBuilder

    # Creates a nested object builder (raises when it cannot be created).
    abstract def try_new_object : ObjectBuilder

    # Creates a nested list builder.
    def new_list : ListBuilder
      try_new_list
    end

    # Creates a nested object builder.
    def new_object : ObjectBuilder
      try_new_object
    end
  end

  # A `VariantBuilderExt` that inserts a field into a variant object (upstream
  # `ObjectFieldBuilder`).
  class ObjectFieldBuilder
    include VariantBuilderExt

    getter key : String
    getter builder : ObjectBuilder

    def initialize(@key : String, @builder : ObjectBuilder)
    end

    # A NULL object field is interpreted as missing, so nothing gets inserted.
    def append_null : Nil
      nil
    end

    def append_value(value : VariantValue) : Nil
      @builder.insert(@key, value)
    end

    def try_new_list : ListBuilder
      @builder.new_list(@key)
    end

    def try_new_object : ObjectBuilder
      @builder.new_object(@key)
    end
  end
end
