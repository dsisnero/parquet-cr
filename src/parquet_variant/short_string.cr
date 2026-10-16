require "./errors"

# Ported from `parquet-variant/src/variant.rs` (`ShortString`, `MAX_SHORT_STRING_BYTES`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the variant short-string wrapper. Divergences: `&str` is an owned
# `String`; errors are raised; `Deref`/`AsRef`/`From` are `#value`/`#as_str`/`#to_s`.
module ParquetVariant
  # The maximum length, in bytes, of a variant short string.
  MAX_SHORT_STRING_BYTES = 63

  # A variant short string (at most 63 bytes).
  struct ShortString
    getter value : String

    def initialize(@value : String)
    end

    # Attempts to interpret `value` as a short string, raising if too long.
    def self.try_new(value : String) : ShortString
      if value.bytesize > MAX_SHORT_STRING_BYTES
        raise InvalidArgumentError.new("value is larger than #{MAX_SHORT_STRING_BYTES} bytes")
      end

      new(value)
    end

    # Returns the underlying short string.
    def as_str : String
      @value
    end

    def to_s(io : IO) : Nil
      io << @value
    end

    def ==(other : ShortString) : Bool
      @value == other.value
    end
  end
end
