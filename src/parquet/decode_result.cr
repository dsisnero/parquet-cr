# Ported from `parquet/src/lib.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: Rust's `enum DecodeResult<T>` is a generic class carrying a `Kind`
# plus the payload (enums cannot be generic in Crystal).
module Parquet
  # The result of a push-based decoder.
  class DecodeResult(T)
    enum Kind
      NeedsData
      Data
      Finished
    end

    getter kind : Kind
    getter ranges : Array(Range(UInt64, UInt64))
    @data : T?

    def initialize(
      @kind : Kind,
      @ranges : Array(Range(UInt64, UInt64)) = [] of Range(UInt64, UInt64),
      @data : T? = nil,
    )
    end

    # The decoder needs the given byte ranges to proceed.
    def self.needs_data(ranges : Array(Range(UInt64, UInt64))) : DecodeResult(T)
      new(Kind::NeedsData, ranges)
    end

    # The decoder produced an output item.
    def self.data(value : T) : DecodeResult(T)
      new(Kind::Data, data: value)
    end

    # The decoder finished processing.
    def self.finished : DecodeResult(T)
      new(Kind::Finished)
    end

    def needs_data? : Bool
      @kind == Kind::NeedsData
    end

    def data? : Bool
      @kind == Kind::Data
    end

    def finished? : Bool
      @kind == Kind::Finished
    end

    def data : T
      @data || raise general_err!("DecodeResult is not Data")
    end
  end
end
