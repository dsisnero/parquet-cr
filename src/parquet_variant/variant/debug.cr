require "../variant"

# Ported from the `Debug` impls in `parquet-variant/src/variant.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `Variant` (compact `{:?}` and alternate `{:#?}`), `HexString`, `InvalidVariant`,
# and the derived decimals. Divergences: exposed as `Variant#debug_string(pretty)`
# / `Variant#debug_to(io, base, pretty)` instead of `std::fmt::Debug`; Rust's
# `{}`-style pretty indentation (4 spaces) is reproduced.
module ParquetVariant
  abstract class Variant
    # Renders this variant using Rust `Debug` formatting (`pretty` selects `{:#?}`).
    def debug_string(pretty : Bool = false) : ::String
      io = IO::Memory.new
      debug_to(io, 0, pretty)
      io.to_s
    end

    # Writes this variant's Rust `Debug` representation to `io`, using `base` as
    # the indentation of the line on which the value starts (pretty mode only).
    def debug_to(io : IO, base : ::Int32, pretty : Bool) : Nil
      v = self
      case v
      when Null         then io << "Null"
      when BooleanTrue  then io << "BooleanTrue"
      when BooleanFalse then io << "BooleanFalse"
      when Int8         then debug_tuple(io, "Int8", base, pretty) { |io_inner| io_inner << v.value }
      when Int16        then debug_tuple(io, "Int16", base, pretty) { |io_inner| io_inner << v.value }
      when Int32        then debug_tuple(io, "Int32", base, pretty) { |io_inner| io_inner << v.value }
      when Int64        then debug_tuple(io, "Int64", base, pretty) { |io_inner| io_inner << v.value }
      when Float        then debug_tuple(io, "Float", base, pretty) { |io_inner| io_inner << v.value }
      when Double       then debug_tuple(io, "Double", base, pretty) { |io_inner| io_inner << v.value }
      when Date
        debug_tuple(io, "Date", base, pretty) { |io_inner| io_inner << format_date(v.value) }
      when TimestampMicros
        debug_tuple(io, "TimestampMicros", base, pretty) { |io_inner| io_inner << format_datetime(v.value, utc: true) }
      when TimestampNtzMicros
        debug_tuple(io, "TimestampNtzMicros", base, pretty) { |io_inner| io_inner << format_datetime(v.value, utc: false) }
      when TimestampNanos
        debug_tuple(io, "TimestampNanos", base, pretty) { |io_inner| io_inner << format_datetime(v.value, utc: true) }
      when TimestampNtzNanos
        debug_tuple(io, "TimestampNtzNanos", base, pretty) { |io_inner| io_inner << format_datetime(v.value, utc: false) }
      when Time
        debug_tuple(io, "Time", base, pretty) { |io_inner| io_inner << format_time_span(v.value) }
      when Decimal4
        debug_tuple(io, "Decimal4", base, pretty) do |io_inner|
          debug_decimal(io_inner, "VariantDecimal4", v.value.integer.to_s, v.value.scale, base + 4, pretty)
        end
      when Decimal8
        debug_tuple(io, "Decimal8", base, pretty) do |io_inner|
          debug_decimal(io_inner, "VariantDecimal8", v.value.integer.to_s, v.value.scale, base + 4, pretty)
        end
      when Decimal16
        debug_tuple(io, "Decimal16", base, pretty) do |io_inner|
          debug_decimal(io_inner, "VariantDecimal16", v.value.integer.to_s, v.value.scale, base + 4, pretty)
        end
      when Binary
        # Upstream `write!(f, "Binary({:?})", HexString(bytes))` ignores the
        # alternate flag, so it stays on one line in `{:#?}`.
        io << "Binary(" << hex_bytes(v.value) << ')'
      when String
        debug_tuple(io, "String", base, pretty) { |io_inner| io_inner << string_debug(v.value) }
      when ShortString
        debug_tuple(io, "ShortString", base, pretty) do |io_inner|
          debug_tuple(io_inner, "ShortString", pretty ? base + 4 : 0, pretty) do |io_nested|
            io_nested << string_debug(v.value.value)
          end
        end
      when Uuid
        debug_tuple(io, "Uuid", base, pretty) { |io_inner| io_inner << v.value }
      when Object
        debug_map(io, v.value.iter, base, pretty)
      when List
        debug_list(io, v.value.iter, base, pretty)
      end
    end

    private def debug_tuple(io : IO, name : ::String, base : ::Int32, pretty : Bool, & : IO ->) : Nil
      io << name << '('
      if pretty
        io << '\n'
        pad(io, base + 4)
        yield io
        io << ",\n"
        pad(io, base)
      else
        yield io
      end
      io << ')'
    end

    private def debug_decimal(
      io : IO,
      name : ::String,
      integer : ::String,
      scale : UInt8,
      base : ::Int32,
      pretty : Bool,
    ) : Nil
      io << name << " {"
      if pretty
        io << '\n'
        pad(io, base + 4)
        io << "integer: " << integer << ",\n"
        pad(io, base + 4)
        io << "scale: " << scale << ",\n"
        pad(io, base)
        io << '}'
      else
        io << " integer: " << integer << ", scale: " << scale << " }"
      end
    end

    private def debug_map(io : IO, pairs : Array(Tuple(::String, Variant)), base : ::Int32, pretty : Bool) : Nil
      io << '{'
      if pretty
        pairs.each do |(name, value)|
          io << '\n'
          pad(io, base + 4)
          io << string_debug(name) << ": "
          value.debug_to(io, base + 4, pretty)
          io << ','
        end
        io << '\n'
        pad(io, base)
        io << '}'
      else
        pairs.each_with_index do |(name, value), i|
          io << ", " if i > 0
          io << string_debug(name) << ": "
          value.debug_to(io, 0, pretty)
        end
        io << '}'
      end
    end

    private def debug_list(io : IO, values : Array(Variant), base : ::Int32, pretty : Bool) : Nil
      io << '['
      if pretty
        values.each do |value|
          io << '\n'
          pad(io, base + 4)
          value.debug_to(io, base + 4, pretty)
          io << ','
        end
        io << '\n'
        pad(io, base)
        io << ']'
      else
        values.each_with_index do |value, i|
          io << ", " if i > 0
          value.debug_to(io, 0, pretty)
        end
        io << ']'
      end
    end

    private def pad(io : IO, n : ::Int32) : Nil
      io << " " * n
    end

    private def hex_bytes(bytes : Bytes) : ::String
      ::String.build do |io|
        bytes.each_with_index do |byte, i|
          io << ' ' if i > 0
          io << byte.to_s(16).rjust(2, '0')
        end
      end
    end

    private def string_debug(value : ::String) : ::String
      ::String.build do |io|
        io << '"'
        value.each_char do |char|
          case char
          when '"'  then io << "\\\""
          when '\\' then io << "\\\\"
          when '\n' then io << "\\n"
          when '\r' then io << "\\r"
          when '\t' then io << "\\t"
          else           io << char
          end
        end
        io << '"'
      end
    end

    private def format_date(value : ::Time) : ::String
      "#{value.year.to_s.rjust(4, '0')}-#{value.month.to_s.rjust(2, '0')}-#{value.day.to_s.rjust(2, '0')}"
    end

    private def format_datetime(value : ::Time, utc : Bool) : ::String
      base = "#{format_date(value)}T#{value.hour.to_s.rjust(2, '0')}:#{value.minute.to_s.rjust(2, '0')}:#{value.second.to_s.rjust(2, '0')}"
      base += fraction(value.nanosecond)
      base += "Z" if utc
      base
    end

    private def format_time_span(value : ::Time::Span) : ::String
      base = "#{value.hours.to_s.rjust(2, '0')}:#{value.minutes.to_s.rjust(2, '0')}:#{value.seconds.to_s.rjust(2, '0')}"
      base + fraction(value.nanoseconds)
    end

    private def fraction(nanoseconds : ::Int32) : ::String
      return "" if nanoseconds == 0

      if nanoseconds % 1_000_000 == 0
        ".#{(nanoseconds // 1_000_000).to_s.rjust(3, '0')}"
      elsif nanoseconds % 1_000 == 0
        ".#{(nanoseconds // 1_000).to_s.rjust(6, '0')}"
      else
        ".#{nanoseconds.to_s.rjust(9, '0')}"
      end
    end
  end
end
