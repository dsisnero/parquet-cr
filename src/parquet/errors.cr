module Parquet
  # Base class for all Parquet errors.
  #
  # Mirrors upstream `parquet::errors::ParquetError`. Upstream is a single
  # `#[non_exhaustive]` enum; Crystal cannot model that as an exception, so each
  # variant is a subclass. `Display` strings are preserved exactly so that
  # callers and tests observing messages behave identically.
  #
  # Divergence: upstream `Result<T, E = ParquetError>` (a Rust `Result`) is
  # mapped to Crystal exceptions — fallible functions raise `ParquetError`
  # subclasses. See `plans/parity.md`.
  class ParquetError < Exception
  end

  # `ParquetError::General`: general error, code violating the normal workflow.
  class GeneralError < ParquetError
    def initialize(message : String)
      super("Parquet error: #{message}")
    end
  end

  # `ParquetError::NYI`: not yet implemented.
  class NYIError < ParquetError
    def initialize(message : String)
      super("NYI: #{message}")
    end
  end

  # `ParquetError::EOF`: end of file / insufficient bytes to decode.
  class EOFError < ParquetError
    def initialize(message : String)
      super("EOF: #{message}")
    end
  end

  # `ParquetError::ArrowError`: reading into or writing from Arrow.
  #
  # Upstream gates this behind the `arrow` feature. Kept ungated so the variant
  # set matches when Arrow interop is ported.
  class ArrowError < ParquetError
    def initialize(message : String)
      super("Arrow: #{message}")
    end
  end

  # `ParquetError::IndexOutOfBound(usize, usize)`.
  class IndexOutOfBoundError < ParquetError
    getter index : UInt64
    getter bound : UInt64

    def initialize(@index : UInt64, @bound : UInt64)
      super("Index #{index} out of bound: #{bound}")
    end
  end

  # `ParquetError::External(Box<dyn Error + Send + Sync>)`.
  class ExternalError < ParquetError
    getter error : Exception

    def initialize(@error : Exception)
      super("External: #{error.message}")
    end
  end

  # `ParquetError::NeedMoreData(usize)`: total bytes required, not additional.
  class NeedMoreDataError < ParquetError
    getter needed : UInt64

    def initialize(@needed : UInt64)
      super("NeedMoreData: #{needed}")
    end
  end

  # `ParquetError::NeedMoreDataRange(Range<u64>)`: half-open byte range needed.
  class NeedMoreDataRangeError < ParquetError
    getter range : Range(UInt64, UInt64)

    def initialize(@range : Range(UInt64, UInt64))
      super("NeedMoreDataRange: #{range.begin}..#{range.end}")
    end
  end

  # `From<TryFromIntError>`: integer conversion overflow.
  def self.overflow_error(error : Exception) : GeneralError
    GeneralError.new("Integer overflow: #{error.message}")
  end

  # `From<io::Error>`: any IO failure becomes an external error.
  def self.from_io_error(error : IO::Error) : ExternalError
    ExternalError.new(error)
  end
end

# Convenient error constructors, mirroring upstream `general_err!` etc. These
# are top-level macros because upstream defines them crate-globally, and they
# are used unqualified throughout the port.
#
# Upstream macros take either a literal or a format string plus args. Crystal
# string interpolation already performs the formatting, so each macro accepts a
# single interpolated string expression.
macro general_err!(message)
  Parquet::GeneralError.new({{message}})
end

macro nyi_err!(message)
  Parquet::NYIError.new({{message}})
end

macro eof_err!(message)
  Parquet::EOFError.new({{message}})
end

macro arrow_err!(message)
  Parquet::ArrowError.new({{message}})
end
