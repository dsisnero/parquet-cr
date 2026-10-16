require "./errors"

# Ported from `parquet/src/compression.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the compression *level* value types (`GzipLevel`, `BrotliLevel`,
# `ZstdLevel`) used by `Compression`. Divergence: Rust's `CompressionLevel<T>`
# trait (with `MINIMUM_LEVEL`/`MAXIMUM_LEVEL` associated consts) is a
# `CompressionUtil.validate_level` helper; gzip/brotli levels are carried as
# `Int32` (upstream `u32`) to keep a single representation. The codecs
# themselves (`compression.rs` `Codec` implementations) are a separate feature
# (see `plans/parity.md` Phase 1.8).
module Parquet
  # Options controlling codec behaviour (`parquet/src/compression.rs`).
  struct CodecOptions
    getter backward_compatible_lz4 : Bool

    def initialize(@backward_compatible_lz4 : Bool)
    end

    def self.default : CodecOptions
      CodecOptionsBuilder.default.build
    end

    def_equals @backward_compatible_lz4
    def_hash @backward_compatible_lz4
  end

  # Builder for `CodecOptions`.
  class CodecOptionsBuilder
    def self.default : CodecOptionsBuilder
      new
    end

    def initialize
      @backward_compatible_lz4 = true
    end

    def set_backward_compatible_lz4(value : Bool) : CodecOptionsBuilder
      @backward_compatible_lz4 = value
      self
    end

    def build : CodecOptions
      CodecOptions.new(@backward_compatible_lz4)
    end
  end

  # Shared compression-level range validation.
  module CompressionUtil
    def self.validate_level(level : Int32, minimum : Int32, maximum : Int32) : Nil
      unless minimum <= level <= maximum
        raise general_err!("valid compression range #{minimum}..=#{maximum} exceeded.")
      end
    end
  end

  # Represents a valid gzip compression level. Defaults to 6.
  struct GzipLevel
    MINIMUM_LEVEL = 0
    MAXIMUM_LEVEL = 9

    getter compression_level : Int32

    def initialize(@compression_level : Int32)
    end

    def self.default : GzipLevel
      new(6)
    end

    def self.try_new(level : Int32) : GzipLevel
      CompressionUtil.validate_level(level, MINIMUM_LEVEL, MAXIMUM_LEVEL)
      new(level)
    end

    def to_s(io : IO) : Nil
      io << "GzipLevel(" << @compression_level << ')'
    end
  end

  # Represents a valid brotli compression level. Defaults to 1.
  struct BrotliLevel
    MINIMUM_LEVEL =  0
    MAXIMUM_LEVEL = 11

    getter compression_level : Int32

    def initialize(@compression_level : Int32)
    end

    def self.default : BrotliLevel
      new(1)
    end

    def self.try_new(level : Int32) : BrotliLevel
      CompressionUtil.validate_level(level, MINIMUM_LEVEL, MAXIMUM_LEVEL)
      new(level)
    end

    def to_s(io : IO) : Nil
      io << "BrotliLevel(" << @compression_level << ')'
    end
  end

  # Represents a valid zstd compression level. Defaults to 1.
  struct ZstdLevel
    MINIMUM_LEVEL = -131_072
    MAXIMUM_LEVEL =       22

    getter compression_level : Int32

    def initialize(@compression_level : Int32)
    end

    def self.default : ZstdLevel
      new(1)
    end

    def self.try_new(level : Int32) : ZstdLevel
      CompressionUtil.validate_level(level, MINIMUM_LEVEL, MAXIMUM_LEVEL)
      new(level)
    end

    def to_s(io : IO) : Nil
      io << "ZstdLevel(" << @compression_level << ')'
    end
  end
end
