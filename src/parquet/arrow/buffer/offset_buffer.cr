require "arrow-cr"
require "../../errors"
require "../record_reader/buffer"
require "./bit_util"

# Ported from `parquet/src/arrow/buffer/offset_buffer.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the byte-array `ValuesBuffer` (`OffsetBuffer<I>`), including its
# `ValuesBuffer` null padding, so byte array readers can use the arrow record
# reader.
#
# Divergences: offsets are a plain `Array(I)` plus a values `Array(UInt8)`
# (upstream `Vec<I>`/`Vec<u8>`); `I` is the Crystal integer type
# (`Int32`/`Int64`) instead of an `OffsetSizeTrait`; `into_array` builds the
# array through arrow-cr's `GenericByteArray`/`GenericBinaryArray` (upstream goes
# through `ArrayDataBuilder`); `extend_from_dictionary` is generic over the key
# and offset integer types; `usize` counts are `Int32`.
module Parquet
  module Arrow
    module Buffer
      # A buffer of variable-length byte arrays (`OffsetBuffer<I>`).
      class OffsetBuffer(I) < RecordReader::ValuesBuffer
        getter offsets : Array(I)
        getter values : Array(UInt8)

        # Creates a buffer with capacity for at least `capacity` values; the
        # values buffer is not pre-allocated (its size is unpredictable).
        def self.with_capacity(capacity : Int32) : OffsetBuffer(I)
          # Crystal arrays expose no capacity API, so the hint is ignored
          # (upstream pre-allocates `capacity + 1` offsets).
          _ = capacity
          new
        end

        def initialize
          @offsets = [] of I
          @offsets << I.new(0)
          @values = [] of UInt8
        end

        # The number of byte arrays in this buffer.
        def len : Int32
          @offsets.size - 1
        end

        def empty? : Bool
          len == 0
        end

        # Appends `data`, optionally verifying that it starts on a UTF-8
        # codepoint boundary.
        #
        # Note: this does not verify that all of `data` is valid UTF-8; call
        # `check_valid_utf8` once all data has been written.
        def try_push(data : Bytes, validate_utf8 : Bool) : Nil
          if validate_utf8 && !data.empty?
            byte = data[0]
            # A valid code point does not start with 0b10xxxxxx.
            if (byte & 0xC0) == 0x80
              raise general_err!("encountered non UTF-8 data")
            end
          end

          @values.concat(data)
          @offsets << I.new(@values.size)
          nil
        end

        # Extends this buffer with dictionary keys: for each key, appends
        # `dict_values[dict_offsets[key]...dict_offsets[key + 1]]`.
        #
        # `K`/`V` are the key and offset integer types (upstream
        # `ArrowNativeType`; a negative key is out of bounds, as upstream's
        # `as_usize` would make it).
        def extend_from_dictionary(keys : Array(K), dict_offsets : Array(V), dict_values : Bytes) : Nil forall K, V
          keys.each do |key|
            index = key.to_i64
            if index < 0 || index + 1 >= dict_offsets.size
              raise general_err!(
                "dictionary key beyond bounds of dictionary: 0..#{Math.max(dict_offsets.size - 1, 0)}"
              )
            end

            start_offset = dict_offsets[index].to_i32
            end_offset = dict_offsets[index + 1].to_i32
            @values.concat(dict_values[start_offset...end_offset])
            @offsets << I.new(@values.size)
          end
          nil
        end

        # Validates that `values[start_offset...]` is valid UTF-8.
        #
        # Must be combined with checking that the offsets start on a character
        # boundary (see `try_push`).
        def check_valid_utf8(start_offset : Int32) : Nil
          slice = Bytes.new(@values.size - start_offset) { |i| @values[start_offset + i] }
          unless String.new(slice).valid_encoding?
            raise general_err!("encountered non UTF-8 data")
          end
          nil
        end

        # Converts this buffer into an arrow binary/string array.
        def into_array(null_buffer : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
          offsets = ::Arrow::OffsetBuffer(I).new(
            ::Arrow::ScalarBuffer(I).from(@offsets.dup), false
          )
          data = ::Arrow::Buffer.from_slice_ref(Bytes.new(@values.size) { |i| @values[i] })

          case data_type.kind
          when .utf8?, .large_utf8?
            ::Arrow::GenericStringArray(I).new(offsets, data, null_buffer)
          when .binary?, .large_binary?
            ::Arrow::GenericBinaryArray(I).new(offsets, data, null_buffer)
          else
            raise general_err!("invalid data type for byte array reader - #{data_type}")
          end
        end

        def reserve_exact(additional : Int32) : Nil
          _ = additional
        end

        def pad_nulls(
          read_offset : Int32,
          values_read : Int32,
          levels_read : Int32,
          valid_mask : Bytes,
        ) : Nil
          if @offsets.size != read_offset + values_read + 1
            raise general_err!(
              "found inconsistent offsets while padding nulls: expected #{read_offset + values_read + 1} offsets, got #{@offsets.size}"
            )
          end

          target = read_offset + levels_read + 1
          if @offsets.size < target
            (target - @offsets.size).times { @offsets << I.new(0) }
          elsif @offsets.size > target
            @offsets.truncate(target..)
          end

          last_pos = read_offset + levels_read + 1
          last_start_offset = I.new(@values.size)

          value_pos = read_offset + values_read - 1
          Parquet::Arrow::Buffer.iter_set_bits_rev(valid_mask).each do |level_pos|
            break if value_pos < read_offset
            if level_pos < value_pos || level_pos >= last_pos
              raise general_err!("found corrupt level data while padding nulls")
            end

            end_offset = @offsets[value_pos + 1]
            start_offset = @offsets[value_pos]

            # Fill in any nulls
            (level_pos + 1...last_pos).each { |idx| @offsets[idx] = end_offset }

            return nil if level_pos == value_pos

            @offsets[level_pos] = start_offset
            last_pos = level_pos
            last_start_offset = start_offset
            value_pos -= 1
          end

          # Pad leading nulls up to `last_pos`.
          (read_offset + 1...last_pos).each { |idx| @offsets[idx] = last_start_offset }
          nil
        end
      end
    end
  end
end
