# Ported from `parquet/src/parquet_macros.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: upstream `macro_rules!` parse raw Thrift IDL text and generate the
# whole type plus its serialization impls. Crystal macros cannot consume IDL as
# tokens, so these macros instead *annotate* a Crystal `enum` declared with
# native syntax, adding `read_thrift`, `write_thrift`, and
# `thrift_element_type`. Behavior (wire format and error messages) matches
# upstream.

# Add Thrift compact protocol support to an enum mirroring a Thrift `enum`
# (an i32-backed enumeration).
macro define_thrift_enum(klass)
  enum {{klass}}
    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : self
      raw = prot.read_i32
      from_value?(raw) || raise general_err!("Unexpected {{klass.id}} #{raw}")
    end

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::I32
    end

    def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      prot.write_i32(value)
    end
  end
end

# Add Thrift compact protocol support to an enum mirroring a Thrift `union`
# whose variants are all empty structs. Enum members equal the union field ids.
#
# On the wire each variant is a struct field header followed by an empty struct
# (`0` stop byte), then the union's own `0` stop byte.
macro define_thrift_union_all_empty(klass)
  enum {{klass}}
    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : self
      field_ident = prot.read_field_begin(0_i16)
      if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        raise general_err!("Received empty union from remote {{klass.id}}")
      end

      variant = from_value?(field_ident.id.to_i32)
      raise general_err!("Unexpected {{klass.id}} #{field_ident.id}") unless variant

      prot.skip_empty_struct
      end_ident = prot.read_field_begin(field_ident.id)
      unless end_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        raise general_err!("Received multiple fields for union from remote {{klass.id}}")
      end

      variant
    end

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::Struct
    end

    def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      prot.write_empty_struct(value.to_i16, 0_i16)
      prot.write_struct_end
    end
  end
end
