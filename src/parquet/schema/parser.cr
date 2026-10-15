# Ported from `parquet/src/schema/parser.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Parses a textual Parquet message type (e.g. `message schema { ... }`) into a
# `Type`. `parse_message_type` is a `Parquet::Schema` method (upstream free
# function `schema::parser::parse_message_type`).
module Parquet
  module Schema
    # Parses a message type string into a schema `Type`. Raises `ParquetError`
    # on parse or validation failure.
    def self.parse_message_type(message_type : String) : Type
      Parser.new(Tokenizer.from_str(message_type)).parse_message_type
    end

    # Asserts a token equals `expected`.
    def self.assert_token(token : String?, expected : String) : Nil
      if token.nil?
        raise general_err!("Expected '#{expected}', but no token found (None)")
      elsif token != expected
        raise general_err!("Expected '#{expected}', found token '#{token}'")
      end
    end

    # Parses an optional `= <id>` into an `Int32`, backtracking when absent.
    def self.parse_optional_id(tokenizer : Tokenizer) : Int32?
      if tokenizer.next == "="
        tokenizer.next.try(&.to_i?)
      else
        tokenizer.backtrack
        nil
      end
    end

    def self.parse_i32(value : String?, not_found_msg : String, parse_fail_msg : String) : Int32
      v = value || raise general_err!(not_found_msg)
      v.to_i32? || raise general_err!(parse_fail_msg)
    end

    def self.parse_bool(value : String?, not_found_msg : String, parse_fail_msg : String) : Bool
      v = value || raise general_err!(not_found_msg)
      case v.downcase
      when "true"  then true
      when "false" then false
      else              raise general_err!(parse_fail_msg)
      end
    end

    def self.parse_timeunit(value : String?, not_found_msg : String, parse_fail_msg : String) : TimeUnit
      v = value || raise general_err!(not_found_msg)
      case v.upcase
      when "MILLIS" then TimeUnit::MILLIS
      when "MICROS" then TimeUnit::MICROS
      when "NANOS"  then TimeUnit::NANOS
      else               raise general_err!(parse_fail_msg)
      end
    end

    # Tokenizer splitting a message type into tokens, preserving delimiters and
    # allowing one-token backtracking.
    class Tokenizer
      def initialize(@tokens : Array(String))
        @index = 0
      end

      def self.from_str(string : String) : Tokenizer
        tokens = string.split.flat_map { |token| split_token(token) }
        new(tokens)
      end

      def self.schema_delim?(char : Char) : Bool
        char == ';' || char == '{' || char == '}' || char == '(' || char == ')' || char == '=' || char == ','
      end

      # Splits a string into tokens, e.g. `"(UTF8);"` -> `["(", "UTF8", ")", ";"]`.
      def self.split_token(string : String) : Array(String)
        buffer = [] of String
        tail = string
        loop do
          idx = nil
          tail.each_char.with_index do |char, index|
            if schema_delim?(char)
              idx = index
              break
            end
          end
          break if idx.nil?

          head = tail[0, idx]
          buffer << head unless head.empty?
          buffer << tail[idx, 1]
          tail = tail[(idx + 1)..]
        end
        buffer << tail unless tail.empty?
        buffer
      end

      def next : String?
        if @index < @tokens.size
          @index += 1
          @tokens[@index - 1]
        end
      end

      def backtrack : Nil
        @index -= 1
      end
    end

    # Internal schema parser.
    class Parser
      def initialize(@tokenizer : Tokenizer)
      end

      def parse_message_type : Type
        case @tokenizer.next
        when "message"
          name = @tokenizer.next || raise general_err!("Expected name, found None")
          Type.group_type_builder(name).with_fields(parse_child_types).build
        else
          raise general_err!("Message type does not start with 'message'")
        end
      end

      # Parses child types for a group type.
      private def parse_child_types : Array(Type)
        Schema.assert_token(@tokenizer.next, "{")
        vec = [] of Type
        while value = @tokenizer.next
          break if value == "}"

          @tokenizer.backtrack
          vec << add_type
        end
        vec
      end

      private def add_type : Type
        rep_token = @tokenizer.next || raise general_err!("Expected repetition, found None")
        repetition = Repetition.from_str(rep_token.upcase)

        token = @tokenizer.next
        if token && token.upcase == "GROUP"
          add_group_type(repetition)
        elsif token
          physical_type = PhysicalType.from_str(token.upcase)
          add_primitive_type(repetition, physical_type)
        else
          raise general_err!("Invalid type, could not extract next token")
        end
      end

      private def add_group_type(repetition : Repetition) : Type
        name = @tokenizer.next || raise general_err!("Expected name, found None")

        logical_type : LogicalType?
        converted_type : ConvertedType
        if @tokenizer.next == "("
          type_token = @tokenizer.next || raise general_err!("Expected converted type, found None")
          logical_type, converted_type = parse_type_annotation(type_token.upcase)
          Schema.assert_token(@tokenizer.next, ")")
        else
          @tokenizer.backtrack
          logical_type = nil
          converted_type = ConvertedType::NONE
        end

        id = Schema.parse_optional_id(@tokenizer)
        fields = parse_child_types

        builder = Type.group_type_builder(name)
          .with_logical_type(logical_type)
          .with_converted_type(converted_type)
          .with_fields(fields)
          .with_id(id)
        builder = builder.with_repetition(repetition)
        builder.build
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def add_primitive_type(repetition : Repetition, physical_type : PhysicalType) : Type
        length = -1_i32
        if physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY
          Schema.assert_token(@tokenizer.next, "(")
          length = Schema.parse_i32(
            @tokenizer.next,
            "Expected length for FIXED_LEN_BYTE_ARRAY, found None",
            "Failed to parse length for FIXED_LEN_BYTE_ARRAY",
          )
          Schema.assert_token(@tokenizer.next, ")")
        end

        name = @tokenizer.next || raise general_err!("Expected name, found None")

        logical_type : LogicalType? = nil
        converted_type = ConvertedType::NONE
        precision = -1_i32
        scale = -1_i32

        if @tokenizer.next == "("
          type_token = @tokenizer.next || raise general_err!("Expected logical or converted type, found None")
          upper = type_token.upcase
          logical_type, converted_type = parse_type_annotation(upper)

          if lt = logical_type
            case lt
            when LogicalType::Decimal
              if @tokenizer.next == "("
                precision = Schema.parse_i32(
                  @tokenizer.next,
                  "Expected precision, found None",
                  "Failed to parse precision for DECIMAL type",
                )
                if @tokenizer.next == ","
                  scale = Schema.parse_i32(
                    @tokenizer.next,
                    "Expected scale, found None",
                    "Failed to parse scale for DECIMAL type",
                  )
                  Schema.assert_token(@tokenizer.next, ")")
                else
                  scale = 0
                end
                new_logical = LogicalType.decimal(scale, precision)
                logical_type = new_logical
                converted_type = ConvertedType.from(new_logical)
              end
              # else: token consumed, no backtrack (mirrors upstream match body)
            when LogicalType::Time
              if @tokenizer.next == "("
                unit = Schema.parse_timeunit(
                  @tokenizer.next,
                  "Invalid timeunit found",
                  "Failed to parse timeunit for TIME type",
                )
                if @tokenizer.next == ","
                  adjusted = Schema.parse_bool(
                    @tokenizer.next,
                    "Invalid boolean found",
                    "Failed to parse timezone info for TIME type",
                  )
                  Schema.assert_token(@tokenizer.next, ")")
                  new_logical = LogicalType.time(adjusted, unit)
                  logical_type = new_logical
                  converted_type = ConvertedType.from(new_logical)
                else
                  @tokenizer.backtrack
                end
              end
            when LogicalType::Timestamp
              if @tokenizer.next == "("
                unit = Schema.parse_timeunit(
                  @tokenizer.next,
                  "Invalid timeunit found",
                  "Failed to parse timeunit for TIMESTAMP type",
                )
                if @tokenizer.next == ","
                  adjusted = Schema.parse_bool(
                    @tokenizer.next,
                    "Invalid boolean found",
                    "Failed to parse timezone info for TIMESTAMP type",
                  )
                  Schema.assert_token(@tokenizer.next, ")")
                  new_logical = LogicalType.timestamp(adjusted, unit)
                  logical_type = new_logical
                  converted_type = ConvertedType.from(new_logical)
                else
                  @tokenizer.backtrack
                end
              end
            when LogicalType::Integer
              # Upstream guard consumes the next token; no backtrack when absent.
              if @tokenizer.next == "("
                bit_width = Schema.parse_i32(
                  @tokenizer.next,
                  "Invalid bit_width found",
                  "Failed to parse bit_width for INTEGER type",
                ).to_i8!

                case physical_type
                when PhysicalType::INT32
                  unless {8_i8, 16_i8, 32_i8}.includes?(bit_width)
                    raise general_err!("Incorrect bit width #{bit_width} for INT32")
                  end
                when PhysicalType::INT64
                  unless bit_width == 64_i8
                    raise general_err!("Incorrect bit width #{bit_width} for INT64")
                  end
                else
                  raise general_err!("Logical type Integer cannot be used with physical type #{physical_type}")
                end

                if @tokenizer.next == ","
                  is_signed = Schema.parse_bool(
                    @tokenizer.next,
                    "Invalid boolean found",
                    "Failed to parse is_signed for INTEGER type",
                  )
                  Schema.assert_token(@tokenizer.next, ")")
                  new_logical = LogicalType.integer(bit_width, is_signed)
                  logical_type = new_logical
                  converted_type = ConvertedType.from(new_logical)
                else
                  @tokenizer.backtrack
                end
              end
            else
              # no additional parameters
            end
          elsif converted_type == ConvertedType::DECIMAL
            if @tokenizer.next == "("
              precision = Schema.parse_i32(
                @tokenizer.next,
                "Expected precision, found None",
                "Failed to parse precision for DECIMAL type",
              )
              scale = if @tokenizer.next == ","
                        Schema.parse_i32(
                          @tokenizer.next,
                          "Expected scale, found None",
                          "Failed to parse scale for DECIMAL type",
                        )
                      else
                        @tokenizer.backtrack
                        0
                      end
              Schema.assert_token(@tokenizer.next, ")")
            else
              @tokenizer.backtrack
            end
          end

          Schema.assert_token(@tokenizer.next, ")")
        else
          @tokenizer.backtrack
        end

        id = Schema.parse_optional_id(@tokenizer)
        Schema.assert_token(@tokenizer.next, ";")

        Type.primitive_type_builder(name, physical_type)
          .with_repetition(repetition)
          .with_logical_type(logical_type)
          .with_converted_type(converted_type)
          .with_length(length)
          .with_precision(precision)
          .with_scale(scale)
          .with_id(id)
          .build
      end

      # Tries a logical type first, falling back to a converted type.
      private def parse_type_annotation(upper : String) : Tuple(LogicalType?, ConvertedType)
        logical = begin
          LogicalType.from_str(upper)
        rescue Parquet::ParquetError
          nil
        end
        if logical
          {logical, ConvertedType.from(logical)}
        else
          {nil, ConvertedType.from_str(upper)}
        end
      end
    end
  end
end
