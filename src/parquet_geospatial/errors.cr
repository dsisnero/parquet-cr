require "../parquet/errors"

# `arrow-schema`'s `ArrowError` variants used by `parquet-geospatial`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: Crystal raises exceptions instead of returning
# `Result<_, ArrowError>`; each variant is a subclass with the upstream Display
# prefix.
module ParquetGeospatial
  # `ArrowError::JsonError`.
  class JsonError < ::Parquet::ParquetError
    def initialize(message : String)
      super("Json error: #{message}")
    end
  end

  # `ArrowError::InvalidArgumentError`.
  class InvalidArgumentError < ::Parquet::ParquetError
    def initialize(message : String)
      super("Invalid argument error: #{message}")
    end
  end
end
