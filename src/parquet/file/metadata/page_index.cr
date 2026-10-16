# Ported from `parquet/src/file/metadata/page_index.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `PageIndexProvider`, `RowGroupPageIndex`, `PageIndex`, and
# `PageIndexBuilder`. Divergences: the trait's `as_any` downcast is not modelled
# (no trait objects in Crystal) and predicate names are `?`-suffixed. The
# `HeapSize` impl (`memory.rs`) is deferred.
module Parquet
  module File
    module Metadata
      # Trait for accessing Parquet `PageIndex` data for efficient page-level
      # skipping. Modelled as an abstract class; `PageIndex` is the default
      # implementation.
      abstract class PageIndexProvider
        # Whether offset index structures are available via this provider.
        abstract def offset_indexes? : Bool

        # Whether column index structures are available via this provider.
        abstract def column_indexes? : Bool

        # Whether both the offset and column index structures are present.
        def complete? : Bool
          column_indexes? && offset_indexes?
        end

        # Returns the column index for a row group and column, if present.
        abstract def column_index(row_group_idx : Int32, column_idx : Int32) : ColumnIndexMetaData?

        # Returns the offset index for a row group and column, if present.
        abstract def offset_index(row_group_idx : Int32, column_idx : Int32) : OffsetIndexMetaData?

        # Returns the expected number of data pages for a column chunk.
        def num_data_pages(row_group_idx : Int32, column_idx : Int32) : Int32?
          if oi = offset_index(row_group_idx, column_idx)
            oi.page_locations.size
          else
            column_index(row_group_idx, column_idx).try(&.num_pages)
          end
        end

        # Returns the physical locations of all data pages in a column chunk.
        def page_locations(row_group_idx : Int32, column_idx : Int32) : Array(PageLocation)?
          offset_index(row_group_idx, column_idx).try(&.page_locations)
        end
      end

      # Convenient access to page index data for a specific row group.
      struct RowGroupPageIndex
        getter row_group_idx : Int32
        getter page_index : PageIndexProvider?

        def initialize(@row_group_idx : Int32, @page_index : PageIndexProvider?)
        end

        # Returns the column index for a column in this row group.
        def column_index(column_idx : Int32) : ColumnIndexMetaData?
          page_index.try(&.column_index(@row_group_idx, column_idx))
        end

        # Returns the offset index for a column in this row group.
        def offset_index(column_idx : Int32) : OffsetIndexMetaData?
          page_index.try(&.offset_index(@row_group_idx, column_idx))
        end

        # Returns the page locations for a column in this row group.
        def page_locations(column_idx : Int32) : Array(PageLocation)?
          offset_index(column_idx).try(&.page_locations)
        end

        # Returns the expected number of data pages for a column in this row group.
        def num_data_pages(column_idx : Int32) : Int32?
          page_index.try(&.num_data_pages(@row_group_idx, column_idx))
        end
      end

      # Dense representation of the Parquet `PageIndex`.
      class PageIndex < PageIndexProvider
        getter column_indexes : Array(Array(ColumnIndexMetaData?))?
        getter offset_indexes : Array(Array(OffsetIndexMetaData?))?

        def initialize(
          @column_indexes : Array(Array(ColumnIndexMetaData?))?,
          @offset_indexes : Array(Array(OffsetIndexMetaData?))?,
        )
        end

        # Converts this `PageIndex` into a `PageIndexBuilder`.
        def into_builder : PageIndexBuilder
          PageIndexBuilder.new_from(self)
        end

        # Returns the raw column indexes structure.
        def column_indexes_raw : Array(Array(ColumnIndexMetaData?))?
          @column_indexes
        end

        # Returns the raw offset indexes structure.
        def offset_indexes_raw : Array(Array(OffsetIndexMetaData?))?
          @offset_indexes
        end

        def offset_indexes? : Bool
          !@offset_indexes.nil?
        end

        def column_indexes? : Bool
          !@column_indexes.nil?
        end

        def column_index(row_group_idx : Int32, column_idx : Int32) : ColumnIndexMetaData?
          rg = @column_indexes.try(&.[row_group_idx]?)
          rg.try(&.[column_idx]?)
        end

        def offset_index(row_group_idx : Int32, column_idx : Int32) : OffsetIndexMetaData?
          rg = @offset_indexes.try(&.[row_group_idx]?)
          rg.try(&.[column_idx]?)
        end

        def_equals @column_indexes, @offset_indexes
      end

      # Builder for constructing `PageIndex` structures.
      #
      # Empty index structures are converted to `nil` in `build`, so
      # `column_indexes?`/`offset_indexes?` correctly report `false` for
      # unpopulated indexes.
      class PageIndexBuilder
        @column_indexes : Array(Array(ColumnIndexMetaData?))?
        @offset_indexes : Array(Array(OffsetIndexMetaData?))?

        # Creates a builder with space for the given row groups and columns.
        def initialize(num_row_groups : Int32, num_columns : Int32)
          @column_indexes = empty_index(num_row_groups, num_columns, ColumnIndexMetaData)
          @offset_indexes = empty_index(num_row_groups, num_columns, OffsetIndexMetaData)
        end

        # Creates a builder from an existing `PageIndex`.
        def self.new_from(page_index : PageIndex) : PageIndexBuilder
          builder = new(0, 0)
          builder.load(page_index.column_indexes, page_index.offset_indexes)
          builder
        end

        # Allocates space for column indexes.
        def allocate_column_indexes(num_row_groups : Int32, num_columns : Int32) : Nil
          @column_indexes = empty_index(num_row_groups, num_columns, ColumnIndexMetaData)
        end

        # Allocates space for offset indexes.
        def allocate_offset_indexes(num_row_groups : Int32, num_columns : Int32) : Nil
          @offset_indexes = empty_index(num_row_groups, num_columns, OffsetIndexMetaData)
        end

        # Sets the column index for a row group and column.
        def put_column_index(column_index : ColumnIndexMetaData, row_group_idx : Int32, column_idx : Int32) : Nil
          return unless indexes = @column_indexes

          row_group = indexes[row_group_idx]?
          row_group[column_idx] = column_index if row_group && column_idx < row_group.size
        end

        # Sets the offset index for a row group and column.
        def put_offset_index(offset_index : OffsetIndexMetaData, row_group_idx : Int32, column_idx : Int32) : Nil
          return unless indexes = @offset_indexes

          row_group = indexes[row_group_idx]?
          row_group[column_idx] = offset_index if row_group && column_idx < row_group.size
        end

        # Consumes the builder and returns a `PageIndex`.
        def build : PageIndex
          column_indexes = index_empty?(@column_indexes) ? nil : @column_indexes
          offset_indexes = index_empty?(@offset_indexes) ? nil : @offset_indexes
          PageIndex.new(column_indexes, offset_indexes)
        end

        protected def load(
          @column_indexes : Array(Array(ColumnIndexMetaData?))?,
          @offset_indexes : Array(Array(OffsetIndexMetaData?))?,
        ) : Nil
        end

        # Creates an empty index structure with every entry set to `nil`.
        private def empty_index(num_row_groups : Int32, num_columns : Int32, _type : T.class) : Array(Array(T?)) forall T
          Array(Array(T?)).new(num_row_groups) { Array(T?).new(num_columns, nil) }
        end

        # Whether an index structure is entirely empty (all entries are `nil`).
        private def index_empty?(index : Array(Array(T?))?) : Bool forall T
          index.nil? || index.all?(&.all?(Nil))
        end
      end
    end
  end
end
