require "./encoder"

# Ported from `parquet/src/column/writer/mod.rs` truncation helpers
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the statistics-truncation helpers: `truncate_utf8`,
# `truncate_and_increment_utf8`, `increment_utf8`, and `increment`.
# Divergence: these are `Parquet::Column::Writer` module methods; `String`
# char-boundary checks use the raw UTF-8 continuation-byte test.
module Parquet
  module Column
    module Writer
      # Truncate a UTF-8 string to the longest prefix that is still valid UTF-8,
      # shorter than `length` bytes and non-empty.
      def self.truncate_utf8(data : String, length : Int32) : Bytes?
        bytes = data.to_slice
        x = Math.min(length, bytes.size)
        while x >= 1
          return bytes[0, x].dup if x == bytes.size || (bytes[x] & 0xC0_u8) != 0x80_u8

          x -= 1
        end
        nil
      end

      # Truncate a UTF-8 string and increment its final character.
      def self.truncate_and_increment_utf8(data : String, length : Int32) : Bytes?
        bytes = data.to_slice
        lower_bound = Math.max(length - 3, 0)
        split = nil.as(Int32?)

        x = Math.min(length, bytes.size)
        while x >= lower_bound
          if x == bytes.size || (bytes[x] & 0xC0_u8) != 0x80_u8
            split = x
            break
          end
          x -= 1
        end
        boundary = split
        return nil if boundary.nil?

        prefix = String.new(bytes[0, boundary])
        increment_utf8(prefix)
      end

      # Increment the final character of a UTF-8 string without changing its
      # byte width. Returns `nil` if no character can be incremented.
      def self.increment_utf8(data : String) : Bytes?
        bytes = data.to_slice
        chars = [] of Tuple(Char, Int32)
        byte_index = 0
        data.each_char do |char|
          chars << {char, byte_index}
          byte_index += char.bytesize
        end

        chars.reverse_each do |(char, start)|
          length = char.bytesize
          next_char =
            begin
              (char.ord + 1).chr
            rescue ArgumentError
              next
            end
          next unless next_char.bytesize == length

          result = [] of UInt8
          result.concat(bytes[0, start])
          result.concat(next_char.to_s.to_slice)
          return Slice.new(result.to_unsafe, result.size).dup
        end
        nil
      end

      # Increment the bytes from right to left. Returns `nil` if all bytes are
      # `UInt8::MAX`.
      def self.increment(data : Array(UInt8)) : Array(UInt8)?
        result = data.dup
        i = result.size - 1
        while i >= 0
          result[i] = result[i] &+ 1_u8
          return result unless result[i] == 0_u8

          i -= 1
        end
        nil
      end

      # Whether min/max value truncation is allowed for this column.
      def self.can_truncate_value(descr : Schema::ColumnDescriptor) : Bool
        case descr.physical_type
        when Parquet::Type::FIXED_LEN_BYTE_ARRAY
          logical_type = descr.logical_type_ref
          !(logical_type.is_a?(LogicalType::Decimal) || logical_type.is_a?(LogicalType::Float16))
        when Parquet::Type::BYTE_ARRAY
          true
        else
          false
        end
      end

      # Whether this column's logical type is a UTF-8 string.
      # ameba:disable Naming/PredicateName
      def self.is_utf8(descr : Schema::ColumnDescriptor) : Bool
        descr.logical_type_ref.is_a?(LogicalType::String) ||
          descr.converted_type == ConvertedType::UTF8
      end

      # Truncates a binary min statistic to at most `truncation_length` bytes.
      #
      # Returns `(data, did_truncate)`.
      def self.truncate_min_value(descr : Schema::ColumnDescriptor, truncation_length : Int32?, data : Bytes) : Tuple(Bytes, Bool)
        length = truncation_length
        return {data, false} if length.nil? || data.size <= length

        if is_utf8(descr)
          begin
            string = String.new(data)
            raise "invalid utf8" unless string.valid_encoding?

            truncated = truncate_utf8(string, length)
            truncated.nil? ? {data, false} : {truncated, true}
          rescue
            return {data[0, length], true}
          end
        else
          {data[0, length], true}
        end
      end

      # Truncates a binary max statistic to at most `truncation_length` bytes and
      # increments the final byte(s) to yield a valid upper bound.
      #
      # Returns `(data, did_truncate)`.
      def self.truncate_max_value(descr : Schema::ColumnDescriptor, truncation_length : Int32?, data : Bytes) : Tuple(Bytes, Bool)
        length = truncation_length
        return {data, false} if length.nil? || data.size <= length

        result =
          if is_utf8(descr)
            begin
              string = String.new(data)
              raise "invalid utf8" unless string.valid_encoding?

              truncate_and_increment_utf8(string, length)
            rescue
              increment(data[0, length].to_a)
            end
          else
            increment(data[0, length].to_a)
          end

        return {data, false} if result.nil?

        bytes = result.is_a?(Bytes) ? result : Slice.new(result.to_unsafe, result.size).dup
        {bytes, true}
      end
    end
  end
end
