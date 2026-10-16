require "./errors"

# Ported from `parquet-variant/src/utils.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Byte-slicing and search helpers used by the variant encoding.
# Divergences: `&[u8]` is `Bytes`; errors are raised; `slice_from_slice` takes an
# explicit range (no `SliceIndex` generic); `array_from_slice` returns a `Bytes`
# slice rather than a fixed-size array.
module ParquetVariant
  module Utils
    # Helper for reporting integer overflow errors consistently.
    def self.overflow_error(msg : String) : InvalidArgumentError
      InvalidArgumentError.new("Integer overflow computing #{msg}")
    end

    # Returns the sub-slice `bytes[range]`, or raises if out of bounds.
    def self.slice_from_slice(bytes : Bytes, range : Range(Int32, Int32)) : Bytes
      first = range.begin
      last = range.end
      if first < 0 || last > bytes.size || first > last
        raise InvalidArgumentError.new(
          "Tried to extract byte(s) #{range} from #{bytes.size}-byte buffer"
        )
      end
      bytes[first, last - first]
    end

    # Returns the sub-slice at `base_offset + range`, or raises if out of bounds.
    def self.slice_from_slice_at_offset(
      bytes : Bytes,
      base_offset : Int32,
      range : Range(Int32, Int32),
    ) : Bytes
      start_byte = base_offset.to_i64 + range.begin
      end_byte = base_offset.to_i64 + range.end
      if start_byte > Int32::MAX || end_byte > Int32::MAX
        raise overflow_error("slice offset")
      end
      slice_from_slice(bytes, start_byte.to_i32...end_byte.to_i32)
    end

    # Returns `n` bytes starting at `offset`.
    def self.array_from_slice(bytes : Bytes, offset : Int32, n : Int32) : Bytes
      slice_from_slice_at_offset(bytes, offset, 0...n)
    end

    # Returns the first byte, raising if `slice` is empty.
    def self.first_byte_from_slice(slice : Bytes) : UInt8
      slice.first? || raise InvalidArgumentError.new("Received empty bytes")
    end

    # Returns the sub-slice as a UTF-8 `String`, raising on invalid UTF-8.
    #
    # `String.new(Bytes)` does not validate UTF-8 in Crystal, so validate here.
    def self.string_from_slice(slice : Bytes, offset : Int32, range : Range(Int32, Int32)) : String
      buffer = slice_from_slice_at_offset(slice, offset, range)
      string = String.new(buffer)
      raise InvalidArgumentError.new("invalid UTF-8 string") unless string.valid_encoding?

      string
    end

    # Fallible binary search over a range. `cmp` returns `nil` to abort the
    # search; otherwise returns `{found, index}` (`found` is false and `index` is
    # the insertion point when absent).
    def self.try_binary_search_range_by(
      range : Range(Int32, Int32),
      cmp : Int32 -> Int32?,
    ) : Tuple(Bool, Int32)?
      start = range.begin
      finish = range.end
      while start < finish
        mid = start + (finish - start) // 2
        ordering = cmp.call(mid)
        return nil if ordering.nil?

        if ordering == 0
          return {true, mid}
        elsif ordering > 0
          finish = mid
        else
          start = mid + 1
        end
      end
      {false, start}
    end
  end
end
