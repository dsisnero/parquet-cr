require "../../errors"
require "../buffer/bit_util"

# Ported from `parquet/src/arrow/record_reader/buffer.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `ValuesBuffer` trait (null padding) and upstream's `ValuesBuffer for
# Vec<T>` impl.
#
# Divergences: the trait is an abstract generic class and upstream's `Vec<T>`
# impl becomes `ArrayValuesBuffer(T)` (reopening Crystal's `Array` for a mixin
# would collide with stdlib constructors); the value used to pad is passed to the
# constructor instead of relying on Rust's `Default` bound; upstream's
# `debug_assert!(level_pos >= value_pos)` is dropped; `usize` counts are `Int32`.
module Parquet
  module Arrow
    module RecordReader
      # A buffer of decoded values that supports padding with nulls.
      abstract class ValuesBuffer(T)
        # Reserves capacity for `additional` more logical values.
        abstract def reserve_exact(additional : Int32) : Nil

        # If a column contains nulls, more level data may be read than value
        # data, as null values are not encoded. This moves the decoded values
        # into the positions that correspond to the non-null level positions.
        #
        # - `read_offset` - the offset in this buffer to start null padding from
        # - `values_read` - the number of values read
        # - `levels_read` - the number of levels read
        # - `valid_mask` - a packed mask of the valid levels
        abstract def pad_nulls(
          read_offset : Int32,
          values_read : Int32,
          levels_read : Int32,
          valid_mask : Bytes,
        ) : Nil
      end

      # The `Vec<T>` implementation of `ValuesBuffer` (upstream
      # `impl<T: Copy + Default> ValuesBuffer for Vec<T>`).
      class ArrayValuesBuffer(T) < ValuesBuffer(T)
        getter values : Array(T)
        # The value inserted for null slots.
        getter default : T

        # Wraps an existing array.
        def initialize(@values : Array(T), @default : T)
        end

        # Creates an empty buffer that pads nulls with `default`.
        def initialize(default : T)
          @values = [] of T
          @default = default
        end

        # Creates an empty buffer with the given capacity (upstream
        # `ValuesBuffer::with_capacity`).
        def self.with_capacity(capacity : Int32, default : T) : ArrayValuesBuffer(T)
          new(Array(T).new(capacity), default)
        end

        # Crystal arrays expose no capacity API, so this is a no-op (upstream
        # reserves exact capacity to avoid reallocations).
        def reserve_exact(additional : Int32) : Nil
          _ = additional
        end

        def pad_nulls(
          read_offset : Int32,
          values_read : Int32,
          levels_read : Int32,
          valid_mask : Bytes,
        ) : Nil
          target = read_offset + levels_read
          if @values.size < target
            (target - @values.size).times { @values << @default }
          elsif @values.size > target
            @values.truncate(target..)
          end

          value_pos = read_offset + values_read - 1
          remaining = values_read
          Parquet::Arrow::Buffer.iter_set_bits_rev(valid_mask).each do |level_pos|
            break if remaining == 0
            break if level_pos <= value_pos

            @values[level_pos] = @values[value_pos]
            value_pos -= 1
            remaining -= 1
          end
          nil
        end

        # Appends a value (upstream pushes into the `Vec` directly).
        def <<(value : T) : Nil
          @values << value
        end

        # Appends values (upstream extends the `Vec` directly).
        def concat(values : Enumerable(T)) : Nil
          @values.concat(values)
        end

        def size : Int32
          @values.size
        end

        def [](index : Int32) : T
          @values[index]
        end

        def to_a : Array(T)
          @values
        end

        def clear : Nil
          @values.clear
        end
      end
    end
  end
end
