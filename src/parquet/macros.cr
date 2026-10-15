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

# Generate a struct mirroring a Thrift `struct` from a declarative block, plus
# its compact-protocol support.
#
# Usage:
# ```
# define_thrift_struct IntegerType do
#   required 1, bit_width, Int8
#   optional 2, is_signed, Bool
# end
# ```
#
# `required` fields are non-nil; `optional` fields become `T?`. Wire behavior
# (field ids, deltas, `bool` encoded in the field header, required-field
# validation, unknown-field skipping) matches upstream `thrift_struct!`.
macro define_thrift_struct(klass, &block)
  {% fields = block.body.is_a?(Expressions) ? block.body.expressions : [block.body] %}
  struct {{klass}}
    {% for ex in fields %}
      {% if ex.is_a?(Call) && (ex.name == "required" || ex.name == "optional") %}
        {% if ex.name == "required" %}
          property {{ex.args[1].id}} : {{ex.args[2]}}
        {% else %}
          property {{ex.args[1].id}} : {{ex.args[2]}}?
        {% end %}
      {% end %}
    {% end %}

    def initialize(
      {% for ex in fields %}
        {% if ex.is_a?(Call) && ex.name == "required" %}
          @{{ex.args[1].id}} : {{ex.args[2]}},
        {% elsif ex.is_a?(Call) && ex.name == "optional" %}
          @{{ex.args[1].id}} : {{ex.args[2]}}? = nil,
        {% end %}
      {% end %}
    )
    end

    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : self
      {% for ex in fields %}
        {% if ex.is_a?(Call) && (ex.name == "required" || ex.name == "optional") %}
          {{ex.args[1].id}} : {{ex.args[2]}}? = nil
        {% end %}
      {% end %}
      last_field_id = 0_i16
      loop do
        field_ident = prot.read_field_begin(last_field_id)
        break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

        case field_ident.id
        {% for ex in fields %}
          {% if ex.is_a?(Call) && (ex.name == "required" || ex.name == "optional") %}
        when {{ex.args[0].id}}_i16
            {% if ex.args[2].id == "Bool" %}
          {{ex.args[1].id}} = field_ident.bool_val
            {% else %}
          {{ex.args[1].id}} = ::Parquet::Thrift.read_value(prot, {{ex.args[2]}})
            {% end %}
          {% end %}
        {% end %}
        else
          prot.skip(field_ident.field_type)
        end
        last_field_id = field_ident.id
      end
      {% for ex in fields %}
        {% if ex.is_a?(Call) && ex.name == "required" %}
      if {{ex.args[1].id}}.nil?
        raise general_err!("Required field {{ex.args[1].id}} is missing")
      end
        {% end %}
      {% end %}
      new(
        {% for ex in fields %}
          {% if ex.is_a?(Call) && ex.name == "required" %}
        {{ex.args[1].id}}.as({{ex.args[2]}}),
          {% elsif ex.is_a?(Call) && ex.name == "optional" %}
        {{ex.args[1].id}},
          {% end %}
        {% end %}
      )
    end

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::Struct
    end

    def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      last_field_id = 0_i16
      {% for ex in fields %}
        {% if ex.is_a?(Call) && ex.name == "required" %}
      last_field_id = ::Parquet::Thrift.write_thrift_field(prot, self.{{ex.args[1].id}}, {{ex.args[0].id}}_i16, last_field_id)
        {% elsif ex.is_a?(Call) && ex.name == "optional" %}
      if __present = self.{{ex.args[1].id}}
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, __present, {{ex.args[0].id}}_i16, last_field_id)
      end
        {% end %}
      {% end %}
      prot.write_struct_end
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
