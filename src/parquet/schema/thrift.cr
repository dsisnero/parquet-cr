require "./types"

# Thrift <-> schema conversion (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `SchemaElement` mirrors the thrift struct of the same name (upstream lives in
# `file::metadata::thrift`); the conversions live in `schema/types.rs` and
# `file/metadata/thrift/mod.rs`. They are grouped here to avoid a require cycle
# between the schema and metadata modules.
module Parquet
  module Schema
    # Mirrors thrift struct `SchemaElement`.
    #
    # Divergence: upstream carries a `'a` lifetime and borrowed `string<'a>`;
    # here `name` is an owned `String`.
    define_thrift_struct(SchemaElement) do
      optional 1, type, PhysicalType
      optional 2, type_length, Int32
      optional 3, repetition_type, FieldRepetitionType
      required 4, name, String
      optional 5, num_children, Int32
      optional 6, converted_type, ConvertedType
      optional 7, scale, Int32
      optional 8, precision, Int32
      optional 9, field_id, Int32
      optional 10, logical_type, LogicalType
    end

    # Walks the tree and counts nodes.
    def self.num_nodes(tp : Type) : Int32
      raise general_err!("Root schema must be Group type") unless tp.group?

      n_nodes = 1
      tp.get_fields.each { |field| n_nodes += count_nodes(field) }
      n_nodes
    end

    private def self.count_nodes(tp : Type) : Int32
      n_nodes = 1
      if tp.group?
        tp.get_fields.each { |field| n_nodes += count_nodes(field) }
      end
      n_nodes
    end

    # Writes a schema tree as a list body of `SchemaElement`s.
    def self.write_schema(schema : Type, writer : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      raise general_err!("Root schema must be Group type") unless schema.group?

      write_schema_helper(schema, writer)
    end

    private def self.write_schema_helper(node : Type, writer : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      if prim = node.as?(PrimitiveType)
        basic_info = prim.basic_info
        element = SchemaElement.new(
          prim.physical_type,
          prim.type_length >= 0 ? prim.type_length : nil,
          basic_info.repetition,
          basic_info.name,
          nil,
          basic_info.converted_type == ConvertedType::NONE ? nil : basic_info.converted_type,
          prim.scale >= 0 ? prim.scale : nil,
          prim.precision >= 0 ? prim.precision : nil,
          basic_info.has_id? ? basic_info.id : nil,
          basic_info.logical_type_ref,
        )
        element.write_thrift(writer)
      else
        group = node.as(GroupType)
        basic_info = group.basic_info
        element = SchemaElement.new(
          nil,
          nil,
          basic_info.has_repetition? ? basic_info.repetition : nil,
          basic_info.name,
          group.fields.size,
          basic_info.converted_type == ConvertedType::NONE ? nil : basic_info.converted_type,
          nil,
          nil,
          basic_info.has_id? ? basic_info.id : nil,
          basic_info.logical_type_ref,
        )
        element.write_thrift(writer)
        group.fields.each { |field| write_schema_helper(field, writer) }
      end
    end

    # Parses a thrift `SchemaElement` array into a schema `Type`.
    def self.parquet_schema_from_array(elements : Array(SchemaElement)) : Type
      index = 0
      num_elements = elements.size
      schema_nodes = [] of Type

      while index < num_elements
        index, node = schema_from_array_helper(elements, num_elements, index)
        schema_nodes << node
      end

      unless schema_nodes.size == 1
        raise general_err!("Expected exactly one root node, but found #{schema_nodes.size}")
      end
      unless schema_nodes[0].group?
        raise general_err!("Expected root node to be a group type")
      end

      schema_nodes[0]
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def self.schema_from_array_helper(
      elements : Array(SchemaElement),
      num_elements : Int32,
      index : Int32,
    ) : Tuple(Int32, Type)
      is_root_node = index == 0

      if index >= num_elements
        raise general_err!("Index out of bound, index = #{index}, len = #{num_elements}")
      end
      element = elements[index]

      # Empty schema.
      if is_root_node && (element.num_children.nil? || element.num_children == 0)
        return {index + 1, Type.group_type_builder(element.name).build}
      end

      converted_type = element.converted_type || ConvertedType::NONE
      logical_type = element.logical_type

      check_logical_type(logical_type)

      field_id = element.field_id

      num_children = element.num_children
      case num_children
      when nil, 0
        # Primitive type (or a type-less leaf, treated as a group).
        repetition = element.repetition_type
        if repetition.nil?
          raise general_err!("Repetition level must be defined for a primitive type")
        end

        if physical_type = element.type
          length = element.type_length || -1
          scale = element.scale || -1
          precision = element.precision || -1
          builder = Type.primitive_type_builder(element.name, physical_type)
            .with_repetition(repetition)
            .with_converted_type(converted_type)
            .with_logical_type(logical_type)
            .with_length(length)
            .with_precision(precision)
            .with_scale(scale)
            .with_id(field_id)
          {index + 1, builder.build}
        else
          builder = Type.group_type_builder(element.name)
            .with_converted_type(converted_type)
            .with_logical_type(logical_type)
            .with_id(field_id)
          # Root of the schema does not have a repetition_type.
          builder = builder.with_repetition(repetition) unless is_root_node
          {index + 1, builder.build}
        end
      else
        n = num_children
        repetition = element.repetition_type

        fields = [] of Type
        next_index = index + 1
        n.times do
          next_index, child = schema_from_array_helper(elements, num_elements, next_index)
          fields << child
        end

        builder = Type.group_type_builder(element.name)
          .with_converted_type(converted_type)
          .with_logical_type(logical_type)
          .with_fields(fields)
          .with_id(field_id)

        unless is_root_node
          rep = repetition
          if rep.nil?
            raise general_err!("Repetition level must be defined for non-root types")
          end
          builder = builder.with_repetition(rep)
        end

        {next_index, builder.build}
      end
    end

    private def self.check_logical_type(logical_type : LogicalType?) : Nil
      if lt = logical_type
        if lt.is_a?(LogicalType::Integer)
          bit_width = lt.value.bit_width
          unless {8_i8, 16_i8, 32_i8, 64_i8}.includes?(bit_width)
            raise general_err!("Bit width must be 8, 16, 32, or 64 for Integer logical type")
          end
        end
      end
    end

    # Test/convenience helpers mirroring `file::metadata::thrift` helpers.

    # Serializes a schema into a thrift list-of-`SchemaElement` byte buffer.
    def self.schema_to_buf(schema : Type) : Bytes
      nodes = num_nodes(schema)
      io = IO::Memory.new
      writer = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
      writer.write_list_begin(::Parquet::Thrift::ElementType::Struct, nodes)
      write_schema(schema, writer)
      io.to_slice
    end

    # Reads a list of `SchemaElement`s from a byte buffer.
    def self.buf_to_schema_list(buf : Bytes) : Array(SchemaElement)
      prot = ::Parquet::Thrift::ThriftSliceInputProtocol.new(buf)
      ::Parquet::Thrift.read_thrift_vec(prot, SchemaElement)
    end

    # Full schema -> thrift -> schema round-trip.
    def self.roundtrip_schema(schema : Type) : Type
      buf = schema_to_buf(schema)
      parquet_schema_from_array(buf_to_schema_list(buf))
    end
  end
end
