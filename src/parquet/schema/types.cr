# Ported from `parquet/src/schema/types.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the schema type model (`Type`, `BasicTypeInfo`, the primitive/group
# builders, and `check_contains`). `ColumnDescriptor`, `SchemaDescriptor`, the
# def/rep-level tree builder, and the thrift conversion are ported with the file
# metadata feature (see `plans/parity.md`).
module Parquet
  module Schema
    # Alias for the physical type (`basic::Type`).
    alias PhysicalType = Parquet::Type

    # `Arc<Type>` maps to `Type` directly: Crystal classes are references.
    alias TypePtr = Type

    # Representation of a Parquet schema type.
    #
    # Divergence: upstream is an enum (`Type::PrimitiveType` / `Type::GroupType`);
    # modelled here as a sealed class hierarchy. Predicate methods use Crystal
    # `?` naming (`primitive?`, `group?`, `is_schema` -> `schema?`, etc.).
    abstract class Type
      abstract def basic_info : BasicTypeInfo
      abstract def primitive? : Bool
      abstract def group? : Bool

      # Creates a primitive type builder.
      def self.primitive_type_builder(name : String, physical_type : PhysicalType) : PrimitiveTypeBuilder
        PrimitiveTypeBuilder.new(name, physical_type)
      end

      # Creates a group type builder.
      def self.group_type_builder(name : String) : GroupTypeBuilder
        GroupTypeBuilder.new(name)
      end

      # ameba:disable Naming/AccessorMethodName
      def get_basic_info : BasicTypeInfo
        basic_info
      end

      # Returns this type's field name.
      def name : String
        basic_info.name
      end

      # Returns `true` if this type is the top-level schema type (message type).
      def schema? : Bool
        group? && !basic_info.has_repetition?
      end

      # Returns `true` if this type is repeated or optional. A type without
      # repetition defined is treated as required.
      def optional? : Bool
        bi = basic_info
        bi.has_repetition? && bi.repetition != Repetition::REQUIRED
      end

      # Gets the physical type of this primitive type (raises otherwise).
      # ameba:disable Naming/AccessorMethodName
      def get_physical_type : PhysicalType
        raise general_err!("Cannot call get_physical_type() on a non-primitive type")
      end

      # ameba:disable Naming/AccessorMethodName
      def get_precision : Int32
        raise general_err!("Cannot call get_precision() on non-primitive type")
      end

      # ameba:disable Naming/AccessorMethodName
      def get_scale : Int32
        raise general_err!("Cannot call get_scale() on non-primitive type")
      end

      # ameba:disable Naming/AccessorMethodName
      def get_fields : Array(Type)
        raise general_err!("Cannot call get_fields() on a non-group type")
      end

      # Checks if `sub_type` schema is part of the current schema.
      # ameba:disable Metrics/CyclomaticComplexity
      def check_contains(sub_type : Type) : Bool
        basic_match = basic_info.name == sub_type.basic_info.name &&
                      (schema? && sub_type.schema? ||
                       !schema? && !sub_type.schema? &&
                       basic_info.repetition == sub_type.basic_info.repetition)

        if primitive? && basic_match && sub_type.primitive?
          get_physical_type == sub_type.get_physical_type
        elsif group? && basic_match && sub_type.group?
          field_map = {} of String => Type
          get_fields.each { |field| field_map[field.name] = field }
          sub_type.get_fields.all? do |field|
            match = field_map[field.name]?
            !match.nil? && match.check_contains(field)
          end
        else
          false
        end
      end
    end

    # A primitive leaf field.
    class PrimitiveType < Type
      getter basic_info : BasicTypeInfo
      getter physical_type : PhysicalType
      getter type_length : Int32
      getter scale : Int32
      getter precision : Int32

      def initialize(
        @basic_info : BasicTypeInfo,
        @physical_type : PhysicalType,
        @type_length : Int32,
        @scale : Int32,
        @precision : Int32,
      )
      end

      def primitive? : Bool
        true
      end

      def group? : Bool
        false
      end

      # ameba:disable Naming/AccessorMethodName
      def get_physical_type : PhysicalType
        @physical_type
      end

      # ameba:disable Naming/AccessorMethodName
      def get_precision : Int32
        @precision
      end

      # ameba:disable Naming/AccessorMethodName
      def get_scale : Int32
        @scale
      end

      def ==(other : PrimitiveType) : Bool
        basic_info == other.basic_info &&
          physical_type == other.physical_type &&
          type_length == other.type_length &&
          scale == other.scale &&
          precision == other.precision
      end
    end

    # A group of fields (similar to a struct).
    class GroupType < Type
      getter basic_info : BasicTypeInfo
      getter fields : Array(Type)

      def initialize(@basic_info : BasicTypeInfo, @fields : Array(Type))
      end

      def primitive? : Bool
        false
      end

      def group? : Bool
        true
      end

      # ameba:disable Naming/AccessorMethodName
      def get_fields : Array(Type)
        @fields
      end

      def ==(other : GroupType) : Bool
        basic_info == other.basic_info && fields == other.fields
      end
    end

    # Basic type info: name, repetition, logical/converted type, id, sort order.
    struct BasicTypeInfo
      getter name : String
      getter converted_type : ConvertedType
      getter logical_type : LogicalType?
      getter sort_order : SortOrder

      def initialize(
        @name : String,
        @repetition : Repetition?,
        @converted_type : ConvertedType,
        @logical_type : LogicalType?,
        @id : Int32?,
        @sort_order : SortOrder,
      )
      end

      def has_repetition? : Bool
        !@repetition.nil?
      end

      def repetition : Repetition
        @repetition || raise general_err!("repetition is not set")
      end

      def logical_type_ref : LogicalType?
        @logical_type
      end

      def has_id? : Bool
        !@id.nil?
      end

      def id : Int32
        @id || raise general_err!("id is not set")
      end
    end

    # A builder for primitive types. All attributes are optional except the name
    # and physical type. Repetition defaults to `OPTIONAL`.
    class PrimitiveTypeBuilder
      @repetition : Repetition
      @converted_type : ConvertedType
      @logical_type : LogicalType?
      @length : Int32
      @precision : Int32
      @scale : Int32
      @id : Int32?

      def initialize(@name : String, @physical_type : PhysicalType)
        @repetition = Repetition::OPTIONAL
        @converted_type = ConvertedType::NONE
        @logical_type = nil
        @length = -1
        @precision = -1
        @scale = -1
        @id = nil
      end

      def with_repetition(repetition : Repetition) : self
        @repetition = repetition
        self
      end

      def with_converted_type(converted_type : ConvertedType) : self
        @converted_type = converted_type
        self
      end

      def with_logical_type(logical_type : LogicalType?) : self
        @logical_type = logical_type
        self
      end

      def with_length(length : Int32) : self
        @length = length
        self
      end

      def with_precision(precision : Int32) : self
        @precision = precision
        self
      end

      def with_scale(scale : Int32) : self
        @scale = scale
        self
      end

      def with_id(id : Int32?) : self
        @id = id
        self
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def build : Type
        sort_order = ColumnOrder.column_order_for_type(@logical_type, @converted_type, @physical_type).sort_order
        converted_type = @converted_type

        if @physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY && @length < 0
          raise general_err!("Invalid FIXED_LEN_BYTE_ARRAY length: #{@length} for field '#{@name}'")
        end

        if logical_type = @logical_type
          if converted_type != ConvertedType::NONE
            if ConvertedType.from(logical_type) != converted_type
              raise general_err!(
                "Logical type #{logical_type.rust_debug} is incompatible with converted type #{converted_type} for field '#{@name}'"
              )
            end
          else
            # Populate the converted type for backwards compatibility.
            converted_type = ConvertedType.from(logical_type)
          end
          check_logical_physical(logical_type, @physical_type)
        end

        case converted_type
        when ConvertedType::NONE
          # nothing to check
        when ConvertedType::UTF8, ConvertedType::BSON, ConvertedType::JSON
          unless @physical_type == PhysicalType::BYTE_ARRAY
            raise general_err!(
              "#{converted_type} cannot annotate field '#{@name}' because it is not a BYTE_ARRAY field"
            )
          end
        when ConvertedType::DECIMAL
          check_decimal_precision_scale
        when ConvertedType::DATE,
             ConvertedType::TIME_MILLIS,
             ConvertedType::UINT_8,
             ConvertedType::UINT_16,
             ConvertedType::UINT_32,
             ConvertedType::INT_8,
             ConvertedType::INT_16,
             ConvertedType::INT_32
          unless @physical_type == PhysicalType::INT32
            raise general_err!(
              "#{converted_type} cannot annotate field '#{@name}' because it is not a INT32 field"
            )
          end
        when ConvertedType::TIME_MICROS,
             ConvertedType::TIMESTAMP_MILLIS,
             ConvertedType::TIMESTAMP_MICROS,
             ConvertedType::UINT_64,
             ConvertedType::INT_64
          unless @physical_type == PhysicalType::INT64
            raise general_err!(
              "#{converted_type} cannot annotate field '#{@name}' because it is not a INT64 field"
            )
          end
        when ConvertedType::INTERVAL
          unless @physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY && @length == 12
            raise general_err!(
              "INTERVAL cannot annotate field '#{@name}' because it is not a FIXED_LEN_BYTE_ARRAY(12) field"
            )
          end
        when ConvertedType::ENUM
          unless @physical_type == PhysicalType::BYTE_ARRAY
            raise general_err!(
              "ENUM cannot annotate field '#{@name}' because it is not a BYTE_ARRAY field"
            )
          end
        else
          raise general_err!("#{converted_type} cannot be applied to primitive field '#{@name}'")
        end

        basic_info = BasicTypeInfo.new(
          @name,
          @repetition,
          converted_type,
          @logical_type,
          @id,
          sort_order,
        )
        PrimitiveType.new(basic_info, @physical_type, @length, @scale, @precision)
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def check_logical_physical(logical_type : LogicalType, physical : PhysicalType) : Nil
        generic = -> { raise general_err!("Cannot annotate #{logical_type.rust_debug} from #{physical} for field '#{@name}'") }

        case logical_type
        when LogicalType::Map, LogicalType::List, LogicalType::File
          raise general_err!("#{logical_type.rust_debug} cannot be applied to a primitive type for field '#{@name}'")
        when LogicalType::Enum
          generic.call unless physical == PhysicalType::BYTE_ARRAY
        when LogicalType::Decimal
          decimal = logical_type.value
          if decimal.scale != @scale
            raise general_err!(
              "DECIMAL logical type scale #{decimal.scale} must match self.scale #{@scale} for field '#{@name}'"
            )
          end
          if decimal.precision != @precision
            raise general_err!(
              "DECIMAL logical type precision #{decimal.precision} must match self.precision #{@precision} for field '#{@name}'"
            )
          end
          check_decimal_precision_scale
        when LogicalType::Date
          generic.call unless physical == PhysicalType::INT32
        when LogicalType::Time
          time = logical_type.value
          if physical == PhysicalType::INT32
            generic.call unless time.unit == TimeUnit::MILLIS
          elsif physical == PhysicalType::INT64
            if time.unit == TimeUnit::MILLIS
              raise general_err!("Cannot use millisecond unit on INT64 type for field '#{@name}'")
            end
          else
            generic.call
          end
        when LogicalType::Timestamp
          generic.call unless physical == PhysicalType::INT64
        when LogicalType::Integer
          int = logical_type.value
          ok = (physical == PhysicalType::INT32 && int.bit_width <= 32) ||
               (physical == PhysicalType::INT64 && int.bit_width == 64)
          generic.call unless ok
        when LogicalType::Unknown, LogicalType::UnknownField
          # Null type / unknown logical type: use physical type.
        when LogicalType::String, LogicalType::Json, LogicalType::Bson
          generic.call unless physical == PhysicalType::BYTE_ARRAY
        when LogicalType::Geometry, LogicalType::Geography
          generic.call unless physical == PhysicalType::BYTE_ARRAY
        when LogicalType::Uuid
          if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY && @length == 16
            # ok
          elsif physical == PhysicalType::FIXED_LEN_BYTE_ARRAY
            raise general_err!(
              "UUID cannot annotate field '#{@name}' because it is not a FIXED_LEN_BYTE_ARRAY(16) field"
            )
          else
            generic.call
          end
        when LogicalType::Float16
          if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY && @length == 2
            # ok
          elsif physical == PhysicalType::FIXED_LEN_BYTE_ARRAY
            raise general_err!(
              "FLOAT16 cannot annotate field '#{@name}' because it is not a FIXED_LEN_BYTE_ARRAY(2) field"
            )
          else
            generic.call
          end
        else
          generic.call
        end
      end

      private def check_decimal_precision_scale : Nil
        case @physical_type
        when PhysicalType::INT32,
             PhysicalType::INT64,
             PhysicalType::BYTE_ARRAY,
             PhysicalType::FIXED_LEN_BYTE_ARRAY
          # supported
        else
          raise general_err!("DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY")
        end

        raise general_err!("Invalid DECIMAL precision: #{@precision}") if @precision < 1
        raise general_err!("Invalid DECIMAL scale: #{@scale}") if @scale < 0
        if @scale > @precision
          raise general_err!("Invalid DECIMAL: scale (#{@scale}) cannot be greater than precision (#{@precision})")
        end

        case @physical_type
        when PhysicalType::INT32
          if @precision > 9
            raise general_err!("Cannot represent INT32 as DECIMAL with precision #{@precision}")
          end
        when PhysicalType::INT64
          if @precision > 18
            raise general_err!("Cannot represent INT64 as DECIMAL with precision #{@precision}")
          end
        when PhysicalType::FIXED_LEN_BYTE_ARRAY
          length = @length &* 8
          max_precision = Math.log10((2.0 ** (length - 1)) - 1.0).floor.to_i32
          if @precision > max_precision
            raise general_err!(
              "Cannot represent FIXED_LEN_BYTE_ARRAY as DECIMAL with length #{@length} and " \
              "precision #{@precision}. The max precision can only be #{max_precision}"
            )
          end
        end
      end
    end

    # A builder for group types. All attributes are optional except the name; a
    # missing repetition means the root (message) type.
    class GroupTypeBuilder
      @repetition : Repetition?
      @converted_type : ConvertedType
      @logical_type : LogicalType?
      @fields : Array(Type)
      @id : Int32?

      def initialize(@name : String)
        @repetition = nil
        @converted_type = ConvertedType::NONE
        @logical_type = nil
        @fields = [] of Type
        @id = nil
      end

      def with_repetition(repetition : Repetition) : self
        @repetition = repetition
        self
      end

      def with_converted_type(converted_type : ConvertedType) : self
        @converted_type = converted_type
        self
      end

      def with_logical_type(logical_type : LogicalType?) : self
        @logical_type = logical_type
        self
      end

      def with_fields(fields : Array(Type)) : self
        @fields = fields
        self
      end

      def with_id(id : Int32?) : self
        @id = id
        self
      end

      def build : Type
        if @logical_type.is_a?(LogicalType::File)
          validate_file_type_fields(@fields)
        end

        converted_type = @converted_type
        if !@logical_type.nil? && converted_type == ConvertedType::NONE
          converted_type = ConvertedType.from(@logical_type)
        end

        basic_info = BasicTypeInfo.new(
          @name,
          @repetition,
          converted_type,
          @logical_type,
          @id,
          SortOrder::UNDEFINED,
        )
        GroupType.new(basic_info, @fields)
      end

      # Validates the fields of a `FILE`-annotated group against the spec.
      private def validate_file_type_fields(fields : Array(Type)) : Nil
        name = @name
        valid : Array(Tuple(String, PhysicalType, LogicalType?)) = [
          {"uri", PhysicalType::BYTE_ARRAY, LogicalType::String.new.as(LogicalType)},
          {"offset", PhysicalType::INT64, nil},
          {"size", PhysicalType::INT64, nil},
          {"content_type", PhysicalType::BYTE_ARRAY, LogicalType::String.new.as(LogicalType)},
          {"checksum", PhysicalType::BYTE_ARRAY, LogicalType::String.new.as(LogicalType)},
          {"inline", PhysicalType::BYTE_ARRAY, nil},
        ]

        fields.each do |field|
          field_name = field.name
          entry = valid.find { |(n, _, _)| n == field_name }
          unless entry
            raise general_err!(
              "FILE type group '#{name}' contains unrecognized field '#{field_name}'. " \
              "Valid fields are: uri, offset, size, content_type, checksum, inline"
            )
          end
          expected_physical = entry[1]
          expected_logical = entry[2]

          bi = field.basic_info
          is_optional = bi.has_repetition? && bi.repetition == Repetition::OPTIONAL
          unless is_optional
            raise general_err!("FILE type field '#{field_name}' must be OPTIONAL in group '#{name}'")
          end

          if field.group?
            raise general_err!("FILE type field '#{field_name}' in group '#{name}' must be a primitive type")
          end
          if field.get_physical_type != expected_physical
            raise general_err!(
              "FILE type field '#{field_name}' in group '#{name}' must have physical type #{expected_physical}"
            )
          end
          if bi.logical_type_ref != expected_logical
            raise general_err!(
              "FILE type field '#{field_name}' in group '#{name}' must have logical type #{Basic.debug_option(expected_logical)}"
            )
          end
        end
      end
    end

    # Represents the location of a column in a Parquet schema.
    struct ColumnPath
      getter parts : Array(String)

      def initialize(@parts : Array(String))
      end

      def self.new(single_path : String) : ColumnPath
        new([single_path])
      end

      def self.from(parts : Array(String)) : ColumnPath
        new(parts)
      end

      def self.from(single_path : String) : ColumnPath
        new(single_path)
      end

      # String representation, e.g. `"a.b.c"`.
      def string : String
        @parts.join('.')
      end

      # Appends more components to the end of the path.
      def append(tail : Array(String)) : Nil
        @parts.concat(tail)
      end

      def to_s(io : IO) : Nil
        io << string.inspect
      end
    end

    # Physical type for leaf-level primitive columns, plus the maximum
    # definition/repetition levels required to reassemble nested data.
    class ColumnDescriptor
      getter path : ColumnPath
      getter max_def_level : Int16
      getter max_rep_level : Int16
      getter repeated_ancestor_def_level : Int16

      def initialize(
        @primitive_type : Type,
        @max_def_level : Int16,
        @max_rep_level : Int16,
        @path : ColumnPath,
        @repeated_ancestor_def_level : Int16 = 0_i16,
      )
      end

      def self.new_with_repeated_ancestor(
        primitive_type : Type,
        max_def_level : Int16,
        max_rep_level : Int16,
        path : ColumnPath,
        repeated_ancestor_def_level : Int16,
      ) : ColumnDescriptor
        new(primitive_type, max_def_level, max_rep_level, path, repeated_ancestor_def_level)
      end

      # Returns self type for this leaf column.
      def self_type : Type
        @primitive_type
      end

      def self_type_ptr : Type
        @primitive_type
      end

      # ameba:disable Naming/AccessorMethodName
      def get_basic_info : BasicTypeInfo
        @primitive_type.basic_info
      end

      def name : String
        @primitive_type.name
      end

      def converted_type : ConvertedType
        @primitive_type.basic_info.converted_type
      end

      def logical_type_ref : LogicalType?
        @primitive_type.basic_info.logical_type_ref
      end

      def physical_type : PhysicalType
        @primitive_type.get_physical_type
      end

      def type_length : Int32
        @primitive_type.as(PrimitiveType).type_length
      end

      def type_precision : Int32
        @primitive_type.get_precision
      end

      def type_scale : Int32
        @primitive_type.get_scale
      end

      def sort_order : SortOrder
        @primitive_type.basic_info.sort_order
      end

      def ==(other : ColumnDescriptor) : Bool
        self_type == other.self_type &&
          max_def_level == other.max_def_level &&
          max_rep_level == other.max_rep_level &&
          repeated_ancestor_def_level == other.repeated_ancestor_def_level &&
          path == other.path
      end
    end

    # Schema of a Parquet file: the top-level `Type` plus a `ColumnDescriptor`
    # for each primitive (leaf) column, in DFS order.
    class SchemaDescriptor
      getter schema : Type
      getter leaves : Array(ColumnDescriptor)
      getter leaf_to_base : Array(Int32)

      def initialize(@schema : Type)
        unless @schema.group?
          raise general_err!("SchemaDescriptor should take a GroupType")
        end

        @leaves = [] of ColumnDescriptor
        @leaf_to_base = [] of Int32
        @schema.get_fields.each_with_index do |field, root_idx|
          path = [] of String
          build_tree(field, root_idx.to_i32, 0_i16, 0_i16, 0_i16, path)
        end
      end

      # Upstream derives `PartialEq` for `SchemaDescriptor`.
      def ==(other : SchemaDescriptor) : Bool
        schema == other.schema && leaves == other.leaves && leaf_to_base == other.leaf_to_base
      end

      # Returns the `ColumnDescriptor` for a field position.
      def column(i : Int32) : ColumnDescriptor
        unless 0 <= i < @leaves.size
          raise general_err!("Index out of bound: #{i} not in [0, #{@leaves.size})")
        end

        @leaves[i]
      end

      def columns : Array(ColumnDescriptor)
        @leaves
      end

      def num_columns : Int32
        @leaves.size
      end

      def get_column_root(i : Int32) : Type
        @schema.get_fields[get_column_root_idx(i)]
      end

      def get_column_root_ptr(i : Int32) : Type
        get_column_root(i)
      end

      def get_column_root_idx(leaf : Int32) : Int32
        unless 0 <= leaf < @leaves.size
          raise general_err!("Index out of bound: #{leaf} not in [0, #{@leaves.size})")
        end

        @leaf_to_base[leaf]
      end

      def root_schema : Type
        @schema
      end

      def root_schema_ptr : Type
        @schema
      end

      def name : String
        @schema.name
      end

      private def build_tree(
        tp : Type,
        root_idx : Int32,
        max_rep_level : Int16,
        max_def_level : Int16,
        repeated_ancestor_def_level : Int16,
        path_so_far : Array(String),
      ) : Nil
        bi = tp.basic_info
        unless bi.has_repetition?
          raise general_err!("Type must have repetition set")
        end

        path_so_far << tp.name
        case bi.repetition
        when Repetition::OPTIONAL
          max_def_level += 1
        when Repetition::REPEATED
          max_def_level += 1
          max_rep_level += 1
          repeated_ancestor_def_level = max_def_level
        else
          # REQUIRED: no level increase
        end

        if tp.primitive?
          desc = ColumnDescriptor.new_with_repeated_ancestor(
            tp,
            max_def_level,
            max_rep_level,
            ColumnPath.new(path_so_far.dup),
            repeated_ancestor_def_level,
          )
          @leaves << desc
          @leaf_to_base << root_idx
        else
          tp.get_fields.each do |field|
            build_tree(field, root_idx, max_rep_level, max_def_level, repeated_ancestor_def_level, path_so_far)
            path_so_far.pop
          end
        end
      end
    end
  end
end
