require "../parquet/errors"

# Ported from `parquet-variant/src/path.rs` and the `parse_path` helpers in
# `parquet-variant/src/utils.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `VariantPath`, `VariantPathElement`, and the dot/bracket path parser.
# Divergences: `arrow_schema::ArrowError::ParseError` is
# `ParquetVariant::ParseError` (Display `"Parser error: {msg}"`); Rust's `Cow`
# field name is an owned `String`; `usize` indices are `Int32`; `join` mutates
# and returns `self` (upstream consumes); parsing scans bytes (ASCII delimiters
# never appear inside a UTF-8 multi-byte sequence).
module ParquetVariant
  # Upstream `arrow_schema::ArrowError::ParseError`.
  class ParseError < ::Parquet::ParquetError
    def initialize(message : String)
      super("Parser error: #{message}")
    end
  end

  # Element of a `VariantPath`: a field name or a list index.
  struct VariantPathElement
    enum Kind
      Field
      Index
    end

    getter kind : Kind
    getter name : String?
    getter index : Int32?

    private def initialize(@kind : Kind, @name : String?, @index : Int32?)
    end

    # Creates a field element with `name`.
    def self.field(name : String) : VariantPathElement
      new(Kind::Field, name, nil)
    end

    # Creates an index element with `index`.
    def self.index(index : Int32) : VariantPathElement
      new(Kind::Index, nil, index)
    end

    def field? : Bool
      @kind.field?
    end

    def index? : Bool
      @kind.index?
    end

    def ==(other : VariantPathElement) : Bool
      @kind == other.kind && @name == other.name && @index == other.index
    end

    def to_s(io : IO) : Nil
      case @kind
      when Kind::Field
        io << "Field(" << @name << ')'
      else
        io << "Index(" << @index << ')'
      end
    end
  end

  # A qualified path to a potential subfield or index of a variant value.
  class VariantPath
    getter path : Array(VariantPathElement)

    def initialize(@path : Array(VariantPathElement))
    end

    # Creates a path from `elements`.
    def self.from_iter(elements : Array(VariantPathElement)) : VariantPath
      new(elements)
    end

    # Parses a dot-notation (with bracket indexes) path string.
    def self.parse(s : String) : VariantPath
      new(Utils.parse_path(s))
    end

    # Creates a path from a single list `index`.
    def self.from(index : Int32) : VariantPath
      new([VariantPathElement.index(index)] of VariantPathElement)
    end

    # Appends `element`, returning `self`.
    def join(element : VariantPathElement) : VariantPath
      @path << element
      self
    end

    # Appends a field named `name`, returning `self`.
    def join(name : String) : VariantPath
      join(VariantPathElement.field(name))
    end

    # Appends a list `index`, returning `self`.
    def join(index : Int32) : VariantPath
      join(VariantPathElement.index(index))
    end

    # Appends `element`.
    def push(element : VariantPathElement) : Nil
      @path << element
      nil
    end

    # Returns whether this path has no elements.
    def empty? : Bool
      @path.empty?
    end

    def [](index : Int32) : VariantPathElement
      @path[index]
    end

    def ==(other : VariantPath) : Bool
      @path == other.path
    end
  end

  # Path parsing helpers (upstream `utils::parse_path`).
  module Utils
    # Parses a dot-notation path with bracket field/index accessors.
    def self.parse_path(s : String) : Array(VariantPathElement)
      bytes = s.to_slice
      unless bytes.empty?
        raise ParseError.new("Unexpected leading '.'") if bytes[0] == '.'.ord.to_u8
      end

      elements = [] of VariantPathElement
      i = 0
      while i < bytes.size
        b = bytes[i]
        if b == '.'.ord.to_u8
          i += 1
          finish = scan_field(s, i)
          if finish == i
            if i >= bytes.size
              raise ParseError.new("Unexpected trailing '.'")
            else
              raise ParseError.new("Unexpected '#{bytes[i].chr}' at byte #{i}")
            end
          end
          elements << VariantPathElement.field(byte_slice(s, i, finish))
          i = finish
        elsif b == '['.ord.to_u8
          element, finish = parse_in_bracket(s, i)
          elements << element
          i = finish
        elsif b == ']'.ord.to_u8
          raise ParseError.new("Unexpected ']' at byte #{i}")
        else
          finish = scan_field(s, i)
          elements << VariantPathElement.field(byte_slice(s, i, finish))
          i = finish
        end
      end
      elements
    end

    private def self.byte_slice(s : String, start : Int32, finish : Int32) : String
      String.new(s.to_slice[start, finish - start])
    end

    # Returns the byte offset of the next field terminator from `start`.
    private def self.scan_field(s : String, start : Int32) : Int32
      best = s.bytesize
      {'.', '[', ']'}.each do |char|
        if index = s.byte_index(char, start)
          best = index if index < best
        end
      end
      best
    end

    private def self.parse_in_bracket(s : String, i : Int32) : Tuple(VariantPathElement, Int32)
      start = i + 1
      io = IO::Memory.new
      index = start
      end_pos = nil
      while index < s.bytesize
        byte = s.byte_at(index)
        if byte == '\\'.ord.to_u8
          if index + 1 < s.bytesize
            len = utf8_char_len(s.byte_at(index + 1))
            k = 0
            while k < len && index + 1 + k < s.bytesize
              io.write_byte(s.byte_at(index + 1 + k))
              k += 1
            end
            index += len
          end
        elsif byte == ']'.ord.to_u8
          end_pos = index
          break
        else
          io.write_byte(byte)
        end
        index += 1
      end

      if end_pos.nil?
        raise ParseError.new("Unclosed '[' at byte #{i}")
      end

      unescaped = String.new(io.to_slice)
      element =
        if inner = quoted_inner(unescaped)
          VariantPathElement.field(inner)
        else
          value = unescaped.to_i32?
          if value.nil?
            raise ParseError.new(
              "Invalid token in bracket request: `#{unescaped}`. Expected a quoted string or a number(e.g., `['field']` or `[123]`)"
            )
          end
          VariantPathElement.index(value)
        end
      {element, end_pos + 1}
    end

    # Returns the inner text for a singly- or doubly-quoted string.
    private def self.quoted_inner(value : String) : String?
      return nil if value.bytesize < 2

      first = value.byte_at(0)
      last = value.byte_at(value.bytesize - 1)
      single = first == '\''.ord.to_u8 && last == '\''.ord.to_u8
      double = first == '"'.ord.to_u8 && last == '"'.ord.to_u8
      return nil unless single || double

      String.new(value.to_slice[1, value.bytesize - 2])
    end

    private def self.utf8_char_len(lead : UInt8) : Int32
      if lead < 0x80
        1
      elsif lead < 0xE0
        2
      elsif lead < 0xF0
        3
      else
        4
      end
    end
  end
end
