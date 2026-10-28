# Ported from `parquet/src/parquet_macros.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: upstream `macro_rules!` parse raw Thrift IDL text and generate the
# whole type plus its serialization impls. Crystal uses native enums or
# declarative struct/union blocks instead of raw IDL tokens. Generated methods
# expose `read_thrift`, `write_thrift`, and `thrift_element_type`; mixed unions
# use a Kind enum plus optional payloads. Wire/error behavior follows the pin.

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
  {% fields = block.body.is_a?(Expressions) ? block.body.expressions : (block.body.is_a?(Nop) ? [] of Nil : [block.body]) %}
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
          @{{ex.args[1].id}} : {{ex.args[2]}}?,
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
      unless self.{{ex.args[1].id}}.nil?
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, self.{{ex.args[1].id}}.as({{ex.args[2]}}), {{ex.args[0].id}}_i16, last_field_id)
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

# Generate a native Kind + optional-payload struct for a mixed Thrift union.
# `unit id, KindName, constructor` represents an empty struct; `variant id,
# KindName, constructor, Type` represents a typed payload. Strict unions reject
# unknown ids; the forward-compatible form skips them and retains only the id.
macro define_thrift_union(klass, with_unknown = false, &block)
  {% variants = block.body.is_a?(Expressions) ? block.body.expressions : (block.body.is_a?(Nop) ? [] of Nil : [block.body]) %}
  {% if variants.empty? %}
    {% raise "Thrift unions require at least one variant" %}
  {% end %}
  {% for ex, index in variants %}
    {% unless ex.is_a?(Call) && ((ex.name == "unit" && ex.args.size == 3) || (ex.name == "variant" && ex.args.size == 4)) %}
      {% raise "Expected unit(id, Kind, name) or variant(id, Kind, name, Type)" %}
    {% end %}
    {% unless ex.args[0].is_a?(NumberLiteral) %}
      {% raise "Thrift union field ids must be integer literals" %}
    {% end %}
    {% if ex.args[0] < -32768 || ex.args[0] > 32767 %}
      {% raise "Thrift union field id must fit Int16" %}
    {% end %}
    {% for other, other_index in variants %}
      {% if other_index < index && other.args[0] == ex.args[0] %}
        {% raise "Duplicate Thrift union field id" %}
      {% end %}
    {% end %}
  {% end %}

  struct {{klass}}
    enum Kind
      {% for ex in variants %}
        {{ex.args[1].id}}
      {% end %}
      {% if with_unknown %}
        Unknown
      {% end %}
    end

    getter kind : Kind
    {% for ex in variants %}
      {% if ex.name == "variant" %}
        getter {{ex.args[2].id}} : {{ex.args[3]}}?
      {% end %}
    {% end %}
    {% if with_unknown %}
      getter unknown_field_id : Int16?
    {% end %}

    private def initialize(
      @kind : Kind,
      {% for ex in variants %}
        {% if ex.name == "variant" %}
          @{{ex.args[2].id}} : {{ex.args[3]}}? = nil,
        {% end %}
      {% end %}
      {% if with_unknown %}
        @unknown_field_id : Int16? = nil,
      {% end %}
    )
    end

    {% for ex in variants %}
      {% if ex.name == "unit" %}
        {{ex.args[2].id.stringify.upcase.id}} = new(Kind::{{ex.args[1].id}})

        def self.{{ex.args[2].id}} : self
          new(Kind::{{ex.args[1].id}})
        end
      {% else %}
        def self.{{ex.args[2].id}}(value : {{ex.args[3]}}) : self
          new(Kind::{{ex.args[1].id}}, {{ex.args[2].id}}: value)
        end

        def {{ex.args[2].id}}! : {{ex.args[3]}}
          value = @{{ex.args[2].id}}
          if value.nil?
            raise general_err!("{{klass.id}} is not {{ex.args[2].id.stringify.upcase.id}}")
          end
          value
        end
      {% end %}
    {% end %}
    {% if with_unknown %}
      def self.unknown(field_id : Int16) : self
        new(Kind::Unknown, unknown_field_id: field_id)
      end
    {% end %}

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::Struct
    end

    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : self
      field_ident = prot.read_field_begin(0_i16)
      if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        raise general_err!("Received empty union from remote {{klass.id}}")
      end
      value = case field_ident.id
        {% for ex in variants %}
        when {{ex.args[0].id}}_i16
          {% if ex.name == "unit" %}
            prot.skip_empty_struct
            {{ex.args[2].id}}
          {% else %}
            {{ex.args[2].id}}(::Parquet::Thrift.read_value(prot, {{ex.args[3]}}))
          {% end %}
        {% end %}
        else
          {% if with_unknown %}
            prot.skip(field_ident.field_type)
            unknown(field_ident.id)
          {% else %}
            raise general_err!("Unexpected {{klass.id}} #{field_ident.id}")
          {% end %}
        end
      end_ident = prot.read_field_begin(field_ident.id)
      unless end_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        # Pinned Rust concat! leaves a literal {} in this expression-form error.
        raise general_err!("Received multiple fields for union from remote {}{{klass.id}}")
      end
      value
    end

    def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      case @kind
      {% for ex in variants %}
      when Kind::{{ex.args[1].id}}
        {% if ex.name == "unit" %}
          prot.write_empty_struct({{ex.args[0].id}}_i16, 0_i16)
        {% else %}
          ::Parquet::Thrift.write_thrift_field(prot, self.{{ex.args[2].id}}!, {{ex.args[0].id}}_i16, 0_i16)
        {% end %}
      {% end %}
      {% if with_unknown %}
      when Kind::Unknown
        raise general_err!("Trying to write unknown variant")
      {% end %}
      else
        raise general_err!("Invalid {{klass.id}} variant")
      end
      prot.write_struct_end
    end
  end
end

macro define_thrift_union_with_unknown(klass, &block)
  define_thrift_union({{klass}}, true) do
    {{block.body}}
  end
end
