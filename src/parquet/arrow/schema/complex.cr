require "arrow-cr"
require "../../basic"
require "../../errors"
require "../../schema/types"
require "../mod"
require "./primitive"

# Ported from `parquet/src/arrow/schema/complex.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ParquetField`, `ParquetFieldType`, `VisitorContext`, `Visitor`
# (`visit_primitive`/`visit_struct`/`visit_list`/`visit_map`/`dispatch`),
# `convert_field`, `convert_schema`, and `convert_type`.
#
# Divergences: the payload-carrying `ParquetFieldType` enum is a sealed class
# hierarchy (`Primitive`/`Group`); `usize` column indices are `Int32`; the
# `Visitor`/`VisitorContext` structs are small mutable classes (upstream uses
# `&mut self`); arrow extension-type mapping (`try_add_extension_type`) is an
# identity helper because arrow-cr has no extension types and upstream only
# enables that mapping behind non-default features; virtual columns
# (`ParquetFieldType::Virtual`, `convert_virtual_field`,
# `parquet_to_arrow_field_levels_with_virtual`) are deferred until arrow-cr grows
# canonical extension types.
module Parquet
  module Arrow
    # Returns a type's repetition, defaulting to REQUIRED when unset (the root).
    def self.get_repetition(type : Schema::Type) : Repetition
      info = type.basic_info
      info.has_repetition? ? info.repetition : Repetition::REQUIRED
    end

    # The way a `ParquetField` maps to parquet (upstream `ParquetFieldType`).
    abstract class ParquetFieldType
      def self.primitive(col_idx : Int32, primitive_type : Schema::Type) : ParquetFieldType
        Primitive.new(col_idx, primitive_type)
      end

      def self.group(children : Array(ParquetField)) : ParquetFieldType
        Group.new(children)
      end

      # A leaf column.
      class Primitive < ParquetFieldType
        # The index of the column in parquet.
        getter col_idx : Int32
        # The type of the column in parquet.
        getter primitive_type : Schema::Type

        def initialize(@col_idx : Int32, @primitive_type : Schema::Type)
        end
      end

      # A group (struct/list/map).
      class Group < ParquetFieldType
        getter children : Array(ParquetField)

        def initialize(@children : Array(ParquetField))
        end
      end
    end

    # A parquet schema element expressed in arrow terms
    # (upstream `ParquetField`).
    class ParquetField
      # The level which represents an insertion into the current list.
      property rep_level : Int16
      # The level at which this field is fully defined.
      property def_level : Int16
      # Whether the field is nullable.
      # ameba:disable Naming/QueryBoolMethods
      property nullable : Bool
      # The arrow type of the column data.
      property arrow_type : ::Arrow::DataType
      # The type of this field.
      getter field_type : ParquetFieldType

      def initialize(
        @rep_level : Int16,
        @def_level : Int16,
        @nullable : Bool,
        @arrow_type : ::Arrow::DataType,
        @field_type : ParquetFieldType,
      )
      end

      # Converts `self` into an arrow list whose element is the current type
      # (used for deprecated repeated columns).
      def into_list(name : String) : ParquetField
        ParquetField.new(
          rep_level,
          def_level,
          false,
          ::Arrow::DataType.list(::Arrow::Field.new(name, arrow_type, false)),
          ParquetFieldType.group([self])
        )
      end

      # Converts `self` into an arrow list, using `list_data_type` (from the
      # embedded arrow schema) to pick the list flavour and element hint.
      def into_list_with_arrow_list_hint(
        parquet_field_type : Schema::Type,
        list_data_type : ::Arrow::DataType?,
      ) : ParquetField
        arrow_field_hint =
          if list = list_data_type
            case list.kind
            when .list?, .large_list?, .fixed_size_list?
              list.field
            else
              raise general_err!(
                "Internal error: should be validated earlier that list_data_type is only a type of list"
              )
            end
          end

        # Only add the field id to the list and not to the element.
        arrow_field = Arrow.convert_field(parquet_field_type, self, arrow_field_hint, false)
        arrow_field = ::Arrow::Field.new(
          arrow_field.name, arrow_field.data_type, false, arrow_field.metadata
        )

        list_type =
          if list = list_data_type
            case list.kind
            when .large_list?
              ::Arrow::DataType.large_list(arrow_field)
            when .fixed_size_list?
              ::Arrow::DataType.fixed_size_list(arrow_field, list.size || 0)
            else
              ::Arrow::DataType.list(arrow_field)
            end
          else
            ::Arrow::DataType.list(arrow_field)
          end

        ParquetField.new(rep_level, def_level, false, list_type, ParquetFieldType.group([self]))
      end

      # Returns the children of a group field.
      def children : Array(ParquetField)?
        kind = field_type
        kind.is_a?(ParquetFieldType::Group) ? kind.children : nil
      end
    end

    # Encodes the context of the parent of the field currently under
    # consideration (upstream `VisitorContext`).
    class VisitorContext
      getter rep_level : Int16
      getter def_level : Int16
      # An optional `DataType` sourced from the embedded arrow schema.
      getter data_type : ::Arrow::DataType?
      # Whether a repeated child should be treated as a list (with the arrow
      # type as the element hint) — supports the deprecated parquet list
      # representation.
      getter? treat_repeated_as_list_arrow_hint : Bool

      def initialize(
        @rep_level : Int16,
        @def_level : Int16,
        @data_type : ::Arrow::DataType?,
        @treat_repeated_as_list_arrow_hint : Bool,
      )
      end

      # Computes the definition/repetition level and nullability of a child.
      def levels(repetition : Repetition) : Tuple(Int16, Int16, Bool)
        case repetition
        when Repetition::OPTIONAL then {(@def_level + 1).to_i16, @rep_level, true}
        when Repetition::REQUIRED then {@def_level, @rep_level, false}
        else                           {(@def_level + 1).to_i16, (@rep_level + 1).to_i16, false}
        end
      end
    end

    # Walks the parquet schema depth-first, mapping it to arrow data structures
    # (upstream `Visitor`).
    class Visitor
      # The column index of the next leaf column.
      @next_col_idx : Int32
      # Mask of columns to include.
      @mask : ProjectionMask

      def initialize(@mask : ProjectionMask)
        @next_col_idx = 0
      end

      def visit_primitive(primitive_type : Schema::Type, context : VisitorContext) : ParquetField?
        col_idx = @next_col_idx
        @next_col_idx += 1

        return nil unless @mask.leaf_included?(col_idx)

        repetition = Arrow.get_repetition(primitive_type)
        def_level, rep_level, nullable = context.levels(repetition)

        primitive_arrow_data_type =
          if repetition.repeated? && context.treat_repeated_as_list_arrow_hint?
            hint = context.data_type
            if hint
              case hint.kind
              when .list?, .large_list?, .fixed_size_list?
                hint.field.try(&.data_type)
              else
                raise arrow_err!(
                  "incompatible arrow schema, expected list got #{hint} for repeated primitive field"
                )
              end
            end
          else
            context.data_type
          end

        arrow_type = Arrow.convert_primitive(primitive_type, primitive_arrow_data_type)

        primitive_field = ParquetField.new(
          rep_level,
          def_level,
          nullable,
          arrow_type,
          ParquetFieldType.primitive(col_idx, primitive_type)
        )

        if repetition.repeated?
          if context.treat_repeated_as_list_arrow_hint?
            primitive_field.into_list_with_arrow_list_hint(primitive_type, context.data_type)
          else
            primitive_field.into_list(primitive_type.name)
          end
        else
          primitive_field
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def visit_struct(struct_type : Schema::Type, context : VisitorContext) : ParquetField?
        repetition = Arrow.get_repetition(struct_type)
        def_level, rep_level, nullable = context.levels(repetition)

        parquet_fields = struct_type.get_fields

        # Extract any arrow fields from the hints.
        arrow_struct =
          if repetition.repeated? && context.treat_repeated_as_list_arrow_hint?
            if hint = context.data_type
              case hint.kind
              when .list?, .large_list?, .fixed_size_list?
                hint.field.try(&.data_type)
              else
                raise arrow_err!(
                  "incompatible arrow schema, expected list got #{hint} for repeated struct field"
                )
              end
            end
          else
            context.data_type
          end

        arrow_fields =
          if arrow_struct
            unless arrow_struct.kind.struct?
              raise arrow_err!("incompatible arrow schema, expected struct got #{arrow_struct}")
            end
            fields = arrow_struct.fields || ::Arrow::Fields.empty
            if fields.size != parquet_fields.size
              raise arrow_err!(
                "incompatible arrow schema, expected #{parquet_fields.size} struct fields got #{fields.size}"
              )
            end
            fields
          end

        child_fields = [] of ::Arrow::Field
        children = [] of ParquetField

        # Perform a DFS of children.
        parquet_fields.each_with_index do |parquet_field, idx|
          data_type =
            if fields = arrow_fields
              field = fields[idx]
              if field.name != parquet_field.name
                raise arrow_err!(
                  "incompatible arrow schema, expected field named #{parquet_field.name} got #{field.name}"
                )
              end
              field.data_type
            end

          arrow_field_hint = arrow_fields.try(&.[idx])
          child_context = VisitorContext.new(
            rep_level,
            def_level,
            data_type,
            # Each child is independently responsible for its own
            # repeated-to-list conversion.
            true
          )

          if child = dispatch(parquet_field, child_context)
            child_fields << Arrow.convert_field(parquet_field, child, arrow_field_hint, true)
            children << child
          end
        end

        return nil if children.empty?

        struct_field = ParquetField.new(
          rep_level,
          def_level,
          nullable,
          ::Arrow::DataType.struct(::Arrow::Fields.from(child_fields)),
          ParquetFieldType.group(children)
        )

        if repetition.repeated?
          if context.treat_repeated_as_list_arrow_hint?
            struct_field.into_list_with_arrow_list_hint(struct_type, context.data_type)
          else
            struct_field.into_list(struct_type.name)
          end
        else
          struct_field
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def visit_map(map_type : Schema::Type, context : VisitorContext) : ParquetField?
        rep_level = (context.rep_level + 1).to_i16
        def_level, nullable = case Arrow.get_repetition(map_type)
                              when Repetition::REQUIRED then {context.def_level + 1, false}
                              when Repetition::OPTIONAL then {context.def_level + 2, true}
                              else
                                raise arrow_err!("Map cannot be repeated")
                              end

        map_fields = map_type.get_fields
        if map_fields.size != 1
          raise arrow_err!("Map field must have exactly one key_value child, found #{map_fields.size}")
        end

        map_key_value = map_fields[0]
        unless map_key_value.basic_info.repetition == Repetition::REPEATED
          raise arrow_err!("Child of map field must be repeated")
        end

        key_value_fields = map_key_value.get_fields

        # According to the specification the values are optional (#1642). In this
        # case, return the keys as a list.
        return visit_list(map_type, context) if key_value_fields.size == 1

        if key_value_fields.size != 2
          raise arrow_err!("Child of map field must have two children, found #{key_value_fields.size}")
        end

        map_key = key_value_fields[0]
        map_value = key_value_fields[1]

        if map_key.basic_info.repetition == Repetition::REPEATED
          raise arrow_err!("Map keys cannot be repeated")
        end

        if map_value.basic_info.repetition == Repetition::REPEATED
          raise arrow_err!("Map values cannot be repeated")
        end

        # Extract the arrow fields.
        arrow_map = nil.as(::Arrow::Field?)
        arrow_key = nil.as(::Arrow::Field?)
        arrow_value = nil.as(::Arrow::Field?)
        sorted = false

        if hint = context.data_type
          unless hint.kind.map?
            raise arrow_err!("incompatible arrow schema, expected map got #{hint}")
          end

          entry = hint.field
          unless entry && entry.data_type.kind.struct?
            raise arrow_err!("Map data type should contain struct got #{entry.try(&.data_type)}")
          end

          entry_fields = entry.data_type.fields || ::Arrow::Fields.empty
          if entry_fields.size != 2
            raise arrow_err!(
              "Map data type should contain struct with two children, got #{entry_fields.size}"
            )
          end

          arrow_map = entry
          arrow_key = entry_fields[0]
          arrow_value = entry_fields[1]
          sorted = hint.is_sorted || false
        end

        maybe_key = dispatch(map_key, VisitorContext.new(
          rep_level,
          def_level,
          arrow_key.try(&.data_type),
          # Keys are not repeated.
          false
        ))

        maybe_value = dispatch(map_value, VisitorContext.new(
          rep_level,
          def_level,
          arrow_value.try(&.data_type),
          # Values can be repeated.
          true
        ))

        # Both columns need to be projected.
        return nil unless (key = maybe_key) && (value = maybe_value)

        key_field = Arrow.convert_field(map_key, key, arrow_key, true)
        # The key is always non-nullable (#5630).
        key_field = ::Arrow::Field.new(key_field.name, key_field.data_type, false, key_field.metadata)
        value_field = Arrow.convert_field(map_value, value, arrow_value, true)

        field_metadata = arrow_map.try(&.metadata) || ::Arrow::Metadata.new
        map_field = ::Arrow::Field.new_struct(
          map_key_value.name,
          ::Arrow::Fields.from([key_field, value_field]),
          # The inner map field is always non-nullable (#1697).
          false
        )
        map_field = map_field.with_metadata(field_metadata)

        ParquetField.new(
          rep_level,
          def_level,
          nullable,
          ::Arrow::DataType.map(map_field, sorted),
          ParquetFieldType.group([key, value])
        )
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def visit_list(list_type : Schema::Type, context : VisitorContext) : ParquetField?
        if list_type.primitive?
          raise arrow_err!("cannot be processed as primitive: #{list_type.name}")
        end

        fields = list_type.get_fields
        if fields.size != 1
          raise arrow_err!("list type must have a single child, found #{fields.size}")
        end

        repeated_field = fields[0]
        unless Arrow.get_repetition(repeated_field).repeated?
          raise arrow_err!("List child must be repeated")
        end

        # If the list is nullable.
        def_level, nullable = case Arrow.get_repetition(list_type)
                              when Repetition::REQUIRED then {context.def_level, false}
                              when Repetition::OPTIONAL then {context.def_level + 1, true}
                              else
                                raise arrow_err!("List type cannot be repeated")
                              end

        arrow_field =
          if hint = context.data_type
            case hint.kind
            when .list?, .large_list?, .fixed_size_list?, .list_view?, .large_list_view?
              hint.field
            else
              raise arrow_err!("incompatible arrow schema, expected list got #{hint}")
            end
          end

        if repeated_field.primitive?
          # If the repeated field is not a group, then its type is the element
          # type and elements are required.
          child_context = VisitorContext.new(
            context.rep_level,
            def_level.to_i16,
            arrow_field.try(&.data_type),
            false
          )

          if field = visit_primitive(repeated_field, child_context)
            # `visit_primitive` infers a non-nullable list; update if necessary.
            field.nullable = nullable
            return field
          end
          return nil
        end

        # Test whether the repeated field is a struct or a one-tuple.
        items = repeated_field.get_fields
        if items.size != 1 ||
           (!repeated_field.is_list? &&
           !repeated_field.has_single_repeated_child? &&
           (repeated_field.name == "array" || repeated_field.name == "#{list_type.name}_tuple"))
          # If the repeated field is a group with multiple fields, then its type
          # is the element type and elements are required. The same applies to a
          # group with one field named `array`/`<list>_tuple` (the element type),
          # as long as it is not annotated and its child is not `repeated`.
          child_context = VisitorContext.new(
            context.rep_level,
            def_level.to_i16,
            arrow_field.try(&.data_type),
            false
          )

          if field = visit_struct(repeated_field, child_context)
            field.nullable = nullable
            return field
          end
          return nil
        end

        # Regular list handling logic.
        item_type = items[0]
        rep_level = (context.rep_level + 1).to_i16
        def_level = (def_level + 1).to_i16

        new_context = VisitorContext.new(
          rep_level,
          def_level,
          arrow_field.try(&.data_type),
          true
        )

        return nil unless item = dispatch(item_type, new_context)

        item_field = Arrow.convert_field(item_type, item, arrow_field, true)

        # Use the arrow type as the hint for the list flavour.
        arrow_type =
          if hint = context.data_type
            case hint.kind
            when .large_list?      then ::Arrow::DataType.large_list(item_field)
            when .fixed_size_list? then ::Arrow::DataType.fixed_size_list(item_field, hint.size || 0)
            when .list_view?       then ::Arrow::DataType.list_view(item_field)
            when .large_list_view? then ::Arrow::DataType.large_list_view(item_field)
            else                        ::Arrow::DataType.list(item_field)
            end
          else
            ::Arrow::DataType.list(item_field)
          end

        ParquetField.new(
          rep_level,
          def_level,
          nullable,
          arrow_type,
          ParquetFieldType.group([item])
        )
      end

      # Dispatches on the parquet converted type.
      def dispatch(cur_type : Schema::Type, context : VisitorContext) : ParquetField?
        return visit_primitive(cur_type, context) if cur_type.primitive?

        case cur_type.basic_info.converted_type
        when ConvertedType::LIST
          visit_list(cur_type, context)
        when ConvertedType::MAP, ConvertedType::MAP_KEY_VALUE
          visit_map(cur_type, context)
        else
          visit_struct(cur_type, context)
        end
      end
    end

    # Computes the arrow `Field` for a child column.
    #
    # The resulting field has the type dictated by `field`, a name dictated by
    # `parquet_type`, and any metadata from `arrow_hint`.
    def self.convert_field(
      parquet_type : Schema::Type,
      field : ParquetField,
      arrow_hint : ::Arrow::Field?,
      add_field_id : Bool,
    ) : ::Arrow::Field
      name = parquet_type.name
      data_type = field.arrow_type
      nullable = field.nullable

      if hint = arrow_hint
        # Preserve dictionary metadata when the inferred type is a dictionary.
        ret =
          if data_type.kind.dictionary? && (id = hint.dict_id) && (ordered = hint.dict_is_ordered)
            ::Arrow::Field.new_dict(name, data_type, nullable, id, ordered)
          else
            ::Arrow::Field.new(name, data_type, nullable)
          end
        return ret.with_metadata(hint.metadata)
      end

      ret = ::Arrow::Field.new(name, data_type, nullable)
      basic_info = parquet_type.basic_info
      if add_field_id && basic_info.has_id?
        ret = ::Arrow::Field.new(
          name,
          data_type,
          nullable,
          ::Arrow::Metadata.from({PARQUET_FIELD_ID_META_KEY => basic_info.id.to_s})
        )
      end
      # Extension types are not mapped (see `logical_type_for_*`).
      ret
    end

    # Computes the `ParquetField` for the given schema, projecting through
    # `mask` and using `embedded_arrow_schema` as the type hint.
    #
    # Note: out-of-order column projection is not supported.
    def self.convert_schema(
      schema : Schema::SchemaDescriptor,
      mask : ProjectionMask,
      embedded_arrow_schema : ::Arrow::Fields?,
    ) : ParquetField?
      visitor = Visitor.new(mask)
      context = VisitorContext.new(
        0_i16,
        0_i16,
        embedded_arrow_schema.try { |fields| ::Arrow::DataType.struct(fields) },
        true
      )
      visitor.dispatch(schema.schema, context)
    end

    # Computes the `ParquetField` for the given parquet type.
    def self.convert_type(parquet_type : Schema::Type) : ParquetField
      visitor = Visitor.new(ProjectionMask.all)
      context = VisitorContext.new(0_i16, 0_i16, nil, true)
      field = visitor.dispatch(parquet_type, context)
      field || raise general_err!("Could not convert parquet type to an arrow field")
    end
  end
end
