require "../basic"
require "../errors"
require "../schema/types"
require "./schema"

# Ported from `parquet/src/arrow/mod.rs` (`ProjectionMask`, `parquet_column`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: `ProjectionMask` exposes its `mask` and a public
# `leaves_with_mask` constructor (upstream keeps the field private and its tests
# live in the same crate); `leaf_included` → `leaf_included?`; `union`/`intersect`
# take the other mask by position and mutate `self` like upstream.
module Parquet
  module Arrow
    # A mask of Parquet leaf columns to include in a projection.
    class ProjectionMask
      # `nil` means "all leaf columns"; otherwise the value at a leaf index says
      # whether it is included.
      getter mask : Array(Bool)?

      # Creates a mask from an explicit per-leaf mask (`nil` = all).
      def initialize(@mask : Array(Bool)? = nil)
      end

      # Upstream `#[derive(PartialEq, Eq)]`: masks are equal when their per-leaf
      # masks are equal (`nil` = all).
      def ==(other : ProjectionMask) : Bool
        @mask == other.mask
      end

      # Creates a mask which selects all columns.
      def self.all : ProjectionMask
        new(nil)
      end

      # Creates a mask which selects no columns.
      def self.none(num_leaf_columns : Int32) : ProjectionMask
        new(Array(Bool).new(num_leaf_columns, false))
      end

      # Creates a mask from an explicit per-leaf mask (upstream's struct literal,
      # used by the ported tests).
      def self.leaves_with_mask(mask : Array(Bool)) : ProjectionMask
        new(mask)
      end

      # Creates a mask which selects only the specified leaf columns.
      #
      # Repeated or out-of-order indices do not change the result.
      def self.leaves(schema : Schema::SchemaDescriptor, indices : Enumerable(Int32)) : ProjectionMask
        mask = Array(Bool).new(schema.num_columns.to_i32, false)
        indices.each { |leaf_idx| mask[leaf_idx] = true }
        new(mask)
      end

      # Creates a mask which selects only the specified root columns.
      def self.roots(schema : Schema::SchemaDescriptor, indices : Enumerable(Int32)) : ProjectionMask
        root_mask = Array(Bool).new(schema.schema.get_fields.size, false)
        indices.each { |root_idx| root_mask[root_idx] = true }

        mask = Array(Bool).new(schema.num_columns.to_i32) do |leaf_idx|
          root_mask[schema.get_column_root_idx(leaf_idx.to_i32)]
        end
        new(mask)
      end

      # Creates a mask which selects the leaf columns below the given names.
      #
      # A name matches a column path when its dot-separated components are a
      # prefix of the path's components ("a.b.c" never matches "a.b").
      def self.columns(schema : Schema::SchemaDescriptor, names : Enumerable(String)) : ProjectionMask
        mask = Array(Bool).new(schema.num_columns.to_i32, false)
        names.each do |name|
          name_path = name.split('.')
          schema.columns.each_with_index do |column, idx|
            path = column.path.parts
            # Searching for "a.b.c" cannot match "a.b"
            next if name_path.size > path.size
            if name_path.each_with_index.all? { |part, i| part == path[i] }
              mask[idx] = true
            end
          end
        end
        new(mask)
      end

      # Returns whether the leaf column `leaf_idx` is included by the mask.
      def leaf_included?(leaf_idx : Int32) : Bool
        mask = @mask
        return true unless mask
        mask[leaf_idx]
      end

      # Unions this mask with `other` (an all-columns mask wins).
      def union(other : ProjectionMask) : Nil
        other_mask = other.mask
        if (self_mask = @mask).nil? || other_mask.nil?
          @mask = nil
        else
          @mask = self_mask.map_with_index { |value, i| value || other_mask[i] }
        end
        nil
      end

      # Intersects this mask with `other`.
      def intersect(other : ProjectionMask) : Nil
        other_mask = other.mask
        self_mask = @mask
        if self_mask.nil?
          @mask = other_mask.try(&.dup)
        elsif other_mask
          @mask = self_mask.map_with_index { |value, i| value && other_mask[i] }
        end
        nil
      end

      # Returns a new mask that excludes any leaf columns that are part of a
      # nested type, such as a struct, list or map (upstream
      # `without_nested_types`).
      #
      # Even a one-leaf group is nested: caching it would drop its parent
      # definition levels. Returns nil when no non-nested column is selected.
      #
      # Divergence: upstream is `pub(crate)` (used by `push_decoder` and the
      # selection machinery); it is public here so the ported tests can reach
      # it without an in-crate test module.
      def without_nested_types(schema : Schema::SchemaDescriptor) : ProjectionMask?
        num_leaves = schema.num_columns
        num_roots = schema.schema.get_fields.size

        # Count how many leaves each root column has.
        root_leaf_counts = Array(Int32).new(num_roots, 0)
        (0...num_leaves).each do |leaf_idx|
          root_idx = schema.get_column_root_idx(leaf_idx)
          root_leaf_counts[root_idx] += 1
        end

        # Keep only top-level primitive columns.
        included_leaves = [] of Int32
        (0...num_leaves).each do |leaf_idx|
          next unless leaf_included?(leaf_idx)

          root = schema.get_column_root(leaf_idx)
          root_idx = schema.get_column_root_idx(leaf_idx)
          if root_leaf_counts[root_idx] == 1 && root.primitive?
            included_leaves << leaf_idx
          end
        end

        return nil if included_leaves.empty?

        ProjectionMask.leaves(schema, included_leaves)
      end
    end

    # Looks up the parquet column by name, returning the parquet leaf index and
    # the corresponding arrow field.
    #
    # Nested arrow fields are not supported: a list/map/struct can correspond to
    # several levels in the parquet schema (see the Parquet logical type rules).
    def self.parquet_column(
      parquet_schema : Schema::SchemaDescriptor,
      arrow_schema : ::Arrow::Schema,
      name : String,
    ) : Tuple(Int32, ::Arrow::Field)?
      found = arrow_schema.fields.find(name)
      return nil unless found

      root_idx, field = found
      return nil if field.data_type.nested?

      parquet_idx = (0...parquet_schema.columns.size).find do |leaf_idx|
        parquet_schema.get_column_root_idx(leaf_idx.to_i32) == root_idx
      end
      return nil unless parquet_idx

      {parquet_idx.to_i32, field}
    end
  end
end
