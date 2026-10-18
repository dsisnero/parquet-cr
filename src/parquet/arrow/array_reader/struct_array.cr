require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../record_reader/definition_levels"

# Ported from `parquet/src/arrow/array_reader/struct_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `StructArrayReader`: reads one child reader per struct field, keeps them
# in sync, and derives the struct's null bitmap from the first child's
# definition/repetition levels.
#
# Divergences: `usize` counts are `Int32`; `StructArray::new_unchecked_with_length`
# maps to arrow-cr's positional `StructArray#initialize(len, data_type, columns,
# nulls)` (no validation), and `StructArray::from(Vec::new())` maps to
# `StructArray.new_empty_fields(0)` for a reader without children.
module Parquet
  module Arrow
    # An `ArrayReader` that builds a `StructArray` from child readers
    # (upstream `StructArrayReader`).
    class StructArrayReader < ArrayReader
      # `def_level`/`rep_level` are the struct's own levels; `nullable` controls
      # whether a struct null bitmap is derived from the children's levels.
      def initialize(
        @data_type : ::Arrow::DataType,
        @children : Array(ArrayReader),
        @struct_def_level : Int16,
        @struct_rep_level : Int16,
        @nullable : Bool,
        @padding_threshold : Int16?,
      )
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        read = nil.as(Int32?)
        @children.each do |child|
          child_read = child.read_records(batch_size)
          if expected = read
            unless expected == child_read
              raise general_err!(
                "StructArrayReader out of sync in read_records, expected #{expected} read, got #{child_read}"
              )
            end
          else
            read = child_read
          end
        end
        read || 0
      end

      def consume_batch : ::Arrow::ArrayBase
        return ::Arrow::StructArray.new_empty_fields(0) if @children.empty?

        children_arrays = @children.map(&.consume_batch)
        unless @data_type.kind.struct?
          raise general_err!(
            "Internal: StructArrayReader must have struct data type, got #{@data_type}"
          )
        end

        item_count = children_arrays.first?.try(&.len) || 0

        unless children_arrays.each_cons(2).all? { |(left, right)| left.len == right.len }
          raise general_err!("Not all children array length are the same!")
        end

        # Build the struct null bitmap when the struct is nullable, skipping
        # parent-level padding (`def < threshold`) and inner-list continuations
        # (`rep > struct_rep_level`).
        nulls =
          if @nullable
            def_levels = @children.first.def_levels || raise general_err!("child def levels are None")
            rep_levels = @children.first.rep_levels
            rep_filter = rep_levels.try { |levels| {levels, @struct_rep_level} }

            bitmap = ::Arrow::BooleanBufferBuilder.new(item_count)
            bits = RecordReader.build_filtered_validity_bitmap(
              def_levels, rep_filter, @padding_threshold, @struct_def_level, bitmap
            )
            unless bits == item_count
              raise general_err!(
                "Failed to decode level data for struct array: expected #{item_count} validity bits but got #{bits}"
              )
            end
            ::Arrow::NullBuffer.new(bitmap.finish)
          else
            nil
          end

        ::Arrow::StructArray.new(item_count, @data_type, children_arrays, nulls)
      end

      def skip_records(num_records : Int32) : Int32
        skipped = nil.as(Int32?)
        @children.each do |child|
          child_skipped = child.skip_records(num_records)
          if expected = skipped
            unless expected == child_skipped
              raise general_err!(
                "StructArrayReader out of sync, expected #{expected} skipped, got #{child_skipped}"
              )
            end
          else
            skipped = child_skipped
          end
        end
        skipped || 0
      end

      # Children's definition levels describe the same parent structure, so the
      # first child's levels are returned (upstream does the same).
      def def_levels : Array(Int16)?
        @children.first?.try(&.def_levels)
      end

      def rep_levels : Array(Int16)?
        @children.first?.try(&.rep_levels)
      end

      def max_def_level : Int16
        @struct_def_level
      end
    end
  end
end
