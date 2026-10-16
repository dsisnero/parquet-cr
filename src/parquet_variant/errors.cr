require "../parquet/errors"

# Ported from `arrow-schema`'s `ArrowError` variants used by `parquet-variant`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: Crystal raises exceptions instead of returning
# `Result<_, ArrowError>`; the `InvalidArgumentError`/`ParseError` variants are
# subclasses with the upstream Display prefixes.
module ParquetVariant
  # `ArrowError::InvalidArgumentError`.
  class InvalidArgumentError < ::Parquet::ParquetError
    def initialize(message : String)
      super("Invalid argument error: #{message}")
    end
  end

  # `ArrowError::ParseError`.
  class ParseError < ::Parquet::ParquetError
    def initialize(message : String)
      super("Parser error: #{message}")
    end
  end
end
