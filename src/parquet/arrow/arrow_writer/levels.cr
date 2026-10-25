require "arrow-cr"
require "../../errors"
require "../../column/chunker"

# parquet/src/arrow/arrow_writer/levels.rs, arrow-rs 60.0.0 (ef1fa157...).
module Parquet::Arrow
  # Leaf array and its logical Parquet records, in depth-first field order.
  class ArrayLevels
    getter array : ::Arrow::ArrayBase
    getter def_levels : Array(Int16)?
    getter rep_levels : Array(Int16)?
    getter non_null_indices : Array(Int32)
    getter max_def_level : Int16
    getter max_rep_level : Int16

    def initialize(@array, @max_def_level, @max_rep_level, @def_levels = nil, @rep_levels = nil, @non_null_indices = [] of Int32)
      @def_levels = [] of Int16 if @max_def_level > 0 && @def_levels.nil?
      @rep_levels = [] of Int16 if @max_rep_level > 0 && @rep_levels.nil?
    end

    def emit(definition : Int16, repetition : Int16, index : Int32? = nil) : Nil
      @def_levels.try(&.push(definition))
      @rep_levels.try(&.push(repetition))
      @non_null_indices << index if index
    end

    def slice_for_chunk(chunk : Column::Chunker::CdcChunk) : ArrayLevels
      indices = @non_null_indices[chunk.value_offset, chunk.num_values]
      start = indices.first? || 0
      finish = indices.last?.try { |index| index + 1 } || 0
      ArrayLevels.new(@array.slice(start, finish - start), @max_def_level, @max_rep_level,
        @def_levels.try(&.[chunk.level_offset, chunk.num_levels]), @rep_levels.try(&.[chunk.level_offset, chunk.num_levels]), indices.map { |index| index - start })
    end
  end

  # Recursive level traversal. Null ancestors emit one record per descendant
  # leaf; repeated children reset repetition only for their first record.
  class LevelInfoBuilder
    getter leaves : Array(ArrayLevels)
    @children = [] of LevelInfoBuilder
    @leaf : ArrayLevels?
    @logical_nulls : ::Arrow::NullBuffer?
    @ranges : Array(Range(Int32, Int32))?
    @definition : Int16
    @repetition : Int16

    def initialize(field : ::Arrow::Field, @array : ::Arrow::ArrayBase, parent_def : Int16 = 0_i16, parent_rep : Int16 = 0_i16)
      unless self.class.types_compatible(field.data_type, @array.data_type)
        raise ArrowError.new("Incompatible type. Field '#{field.name}' has type #{field.data_type}, array has type #{@array.data_type}")
      end
      @definition = parent_def + (field.nullable? ? 1_i16 : 0_i16)
      @repetition = parent_rep
      case array = @array
      when ::Arrow::RunArray(Int16), ::Arrow::RunArray(Int32), ::Arrow::RunArray(Int64)
        indices = ::Arrow::PrimitiveArray(Int32).from_values(Array.new(array.len) { |index| array.get_physical_index(index) })
        flat = ::Arrow.take(array.values, indices)
        @children << LevelInfoBuilder.new(::Arrow::Field.new(field.name, flat.data_type, field.nullable?), flat, parent_def, parent_rep)
      when ::Arrow::StructArray
        array.fields.each_with_index do |child, index|
          @children << LevelInfoBuilder.new(child, array.column(index), @definition, parent_rep)
        end
      when ::Arrow::GenericListArray(Int32), ::Arrow::GenericListArray(Int64),
           ::Arrow::GenericListViewArray(Int32), ::Arrow::GenericListViewArray(Int64), ::Arrow::FixedSizeListArray
        @definition += 1_i16
        @repetition += 1_i16
        @ranges = Array.new(array.len) { |index| self.class.element_range(array, index) }
        child = array.data_type.field || raise ArrowError.new("Missing list field")
        @children << LevelInfoBuilder.new(child, array.values, @definition, @repetition)
      when ::Arrow::MapArray
        @definition += 1_i16
        @repetition += 1_i16
        @ranges = Array.new(array.len) { |index| array.value_offsets[index]...array.value_offsets[index + 1] }
        @children << LevelInfoBuilder.new(array.entries_field, array.entries, @definition, @repetition)
      when ::Arrow::UnionArray
        raise NYIError.new("Datatype #{array.data_type} is not yet supported")
      else
        @leaf = ArrayLevels.new(array, @definition, @repetition)
        @logical_nulls = array.logical_nulls
      end
      @leaves = @leaf.try { |leaf| [leaf] } || @children.flat_map(&.leaves)
      @null_def = parent_def
    end

    @null_def : Int16

    def write(index : Int32, repetition : Int16 = 0_i16) : Nil
      if leaf = @leaf
        null = leaf.max_def_level > 0 && (@logical_nulls.try(&.null?(index)) || false)
        leaf.emit(leaf.max_def_level - (null ? 1_i16 : 0_i16), repetition, null ? nil : index)
      elsif @array.nulls.try(&.null?(index))
        missing(@null_def, repetition)
      elsif ranges = @ranges
        range = ranges[index]
        if range.begin == range.end
          missing(@definition - 1_i16, repetition)
        else
          range.each_with_index do |child_index, ordinal|
            @children.each { |child| child.write(child_index, ordinal == 0 ? repetition : @repetition) }
          end
        end
      else
        @children.each(&.write(index, repetition))
      end
    end

    def missing(definition : Int16, repetition : Int16) : Nil
      @leaves.each(&.emit(definition, repetition))
    end

    def self.types_compatible(expected : ::Arrow::DataType, actual : ::Arrow::DataType) : Bool
      return true if expected.equals_datatype(actual)
      expected = expected.value || expected if expected.kind.dictionary?
      actual = actual.value || actual if actual.kind.dictionary?
      return true if expected == actual
      strings = [::Arrow::DataType.utf8, ::Arrow::DataType.large_utf8, ::Arrow::DataType.utf8_view]
      binaries = [::Arrow::DataType.binary, ::Arrow::DataType.large_binary, ::Arrow::DataType.binary_view]
      (strings.includes?(expected) && strings.includes?(actual)) || (binaries.includes?(expected) && binaries.includes?(actual))
    end

    def self.element_range(array : ::Arrow::GenericListArray(O), index : Int32) : Range(Int32, Int32) forall O
      array.value_offsets[index].to_i32...array.value_offsets[index + 1].to_i32
    end

    def self.element_range(array : ::Arrow::GenericListViewArray(O), index : Int32) : Range(Int32, Int32) forall O
      start = array.value_offset(index).to_i32
      start...(start + array.value_size(index).to_i32)
    end

    def self.element_range(array : ::Arrow::FixedSizeListArray, index : Int32) : Range(Int32, Int32)
      start = array.value_offset(index)
      start...(start + array.value_length)
    end
  end

  def self.calculate_array_levels(array : ::Arrow::ArrayBase, field : ::Arrow::Field) : Array(ArrayLevels)
    builder = LevelInfoBuilder.new(field, array)
    array.len.times { |index| builder.write(index) }
    builder.leaves
  end

  alias ArrowLeafColumn = ArrayLevels

  def self.compute_leaves(field : ::Arrow::Field, array : ::Arrow::ArrayBase) : Array(ArrowLeafColumn)
    calculate_array_levels(array, field)
  end
end
