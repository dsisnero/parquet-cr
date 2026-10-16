# Ported from `parquet/src/file/metadata/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `LevelHistogram`, `ColumnIndexBuilder`, and `OffsetIndexBuilder`.
# `ParquetMetaData`, `FileMetaData`, `RowGroupMetaData`, `ColumnChunkMetaData`,
# and their thrift conversion live in sibling files (see `plans/parity.md`).
module Parquet
  module File
    module Metadata
      alias ColumnIndexMetaData = ::Parquet::File::PageIndex::ColumnIndexMetaData
      alias PrimitiveColumnIndex = ::Parquet::File::PageIndex::PrimitiveColumnIndex
      alias ByteArrayColumnIndex = ::Parquet::File::PageIndex::ByteArrayColumnIndex
      alias OffsetIndexMetaData = ::Parquet::File::PageIndex::OffsetIndexMetaData
      alias PageLocation = ::Parquet::File::PageIndex::PageLocation

      # A histogram of repetition or definition levels.
      #
      # Each histogram is a vector of length `max_level + 1`; the value at index
      # `i` is the number of values at level `i`.
      struct LevelHistogram
        getter inner : Array(Int64)

        def initialize(@inner : Array(Int64))
        end

        # Creates a new histogram of length `max_level + 1`.
        #
        # Returns `nil` when `max_level <= 0` (histograms are unnecessary).
        def self.try_new(max_level : Int16) : LevelHistogram?
          if max_level > 0
            new(Array(Int64).new(max_level.to_i32 + 1, 0_i64))
          end
        end

        def self.from(inner : Array(Int64)) : LevelHistogram
          new(inner)
        end

        # Returns a reference to the histogram's values.
        def values : Array(Int64)
          @inner
        end

        # Returns the inner vector, consuming self.
        def into_inner : Array(Int64)
          @inner
        end

        # Returns the histogram value at the given index, or `nil` if out of bounds.
        def get(index : Int) : Int64?
          @inner[index]?
        end

        # Adds the values from the other histogram to this histogram.
        def add(other : LevelHistogram) : Nil
          raise ArgumentError.new("histogram length mismatch: #{len} != #{other.len}") unless len == other.len

          other.inner.each_with_index { |value, i| @inner[i] += value }
        end

        def len : Int32
          @inner.size
        end

        def empty? : Bool
          @inner.empty?
        end

        # Sets the values of all histogram levels to 0.
        def reset : Nil
          @inner.fill(0_i64)
        end

        # Increments the count for a level value by `count`.
        def increment_by(level : Int16, count : Int64) : Nil
          @inner[level.to_i32] += count
        end

        # Updates histogram values using provided repetition levels.
        def update_from_levels(levels : Array(Int16)) : Nil
          levels.each { |level| increment_by(level, 1_i64) }
        end
      end

      # Builder for a Parquet `ColumnIndex`, part of the Parquet `PageIndex`.
      class ColumnIndexBuilder
        getter column_type : Parquet::Type

        def initialize(@column_type : Parquet::Type)
          @null_pages = [] of Bool
          @min_values = [] of Bytes
          @max_values = [] of Bytes
          @null_counts = [] of Int64
          @nan_counts = [] of Int64?
          @boundary_order = BoundaryOrder::UNORDERED
          @repetition_level_histograms = nil
          @definition_level_histograms = nil
          @valid = true
        end

        # Appends statistics for the next page.
        #
        # For floating-point columns (`FLOAT`, `DOUBLE`, or `FLOAT16`),
        # `nan_count` must always be non-nil. For other columns it must be nil.
        def append(
          null_page : Bool,
          min_value : Bytes,
          max_value : Bytes,
          null_count : Int64,
          nan_count : Int64?,
        ) : Nil
          @null_pages << null_page
          @min_values << min_value
          @max_values << max_value
          @null_counts << null_count
          @nan_counts << nan_count
        end

        # Appends the given page-level histograms to the index histograms.
        # Does nothing if the builder is not valid.
        def append_histograms(
          repetition_level_histogram : LevelHistogram?,
          definition_level_histogram : LevelHistogram?,
        ) : Nil
          return unless @valid

          if rep = repetition_level_histogram
            hist = (@repetition_level_histograms ||= [] of Int64)
            hist.concat(rep.values)
          end
          if dfn = definition_level_histogram
            hist = (@definition_level_histograms ||= [] of Int64)
            hist.concat(dfn.values)
          end
        end

        # Sets the boundary order of the column index.
        # ameba:disable Naming/AccessorMethodName
        def set_boundary_order(boundary_order : BoundaryOrder) : Nil
          @boundary_order = boundary_order
        end

        # Marks this column index as invalid.
        def to_invalid : Nil
          @valid = false
        end

        # Whether the information in the builder is valid.
        def valid? : Bool
          @valid
        end

        # Builds the column index. Callers should check `valid?` first.
        def build : ColumnIndexMetaData
          case @column_type
          when Parquet::Type::BOOLEAN
            ColumnIndexMetaData.new(@column_type, build_page_index(Bool, false))
          when Parquet::Type::INT32
            ColumnIndexMetaData.new(@column_type, build_page_index(Int32, false))
          when Parquet::Type::INT64
            ColumnIndexMetaData.new(@column_type, build_page_index(Int64, false))
          when Parquet::Type::INT96
            ColumnIndexMetaData.new(@column_type, build_page_index(Int96, false))
          when Parquet::Type::FLOAT
            ColumnIndexMetaData.new(@column_type, build_page_index(Float32, true))
          when Parquet::Type::DOUBLE
            ColumnIndexMetaData.new(@column_type, build_page_index(Float64, true))
          when Parquet::Type::BYTE_ARRAY
            ColumnIndexMetaData.new(@column_type, build_byte_array_index(false))
          when Parquet::Type::FIXED_LEN_BYTE_ARRAY
            ColumnIndexMetaData.new(@column_type, build_byte_array_index(true))
          else
            raise general_err!("Unexpected column type #{@column_type}")
          end
        end

        # Parquet spec requires `nan_counts` to be present for all pages or
        # absent entirely. Callers must provide consistent values.
        private def build_nan_counts(nan_counts : Array(Int64?)) : Array(Int64)?
          has_some = nan_counts.any? { |x| !x.nil? }
          has_none = nan_counts.any?(Nil)

          if has_some && !has_none
            nan_counts.map { |x| x || 0_i64 }
          elsif !has_some && has_none
            nil
          else
            nan_counts.map { |x| x || 0_i64 }
          end
        end

        private def build_page_index(type : T.class, may_have_nan : Bool) : PrimitiveColumnIndex(T) forall T
          nan_counts = may_have_nan && !@nan_counts.empty? ? build_nan_counts(@nan_counts) : nil

          PrimitiveColumnIndex(T).try_new(
            @null_pages,
            @boundary_order,
            @null_counts,
            nan_counts,
            @repetition_level_histograms,
            @definition_level_histograms,
            @min_values,
            @max_values,
          )
        end

        private def build_byte_array_index(may_have_nan : Bool) : ByteArrayColumnIndex
          nan_counts = may_have_nan && !@nan_counts.empty? ? build_nan_counts(@nan_counts) : nil

          ByteArrayColumnIndex.try_new(
            @null_pages,
            @boundary_order,
            @null_counts,
            nan_counts,
            @repetition_level_histograms,
            @definition_level_histograms,
            @min_values,
            @max_values,
          )
        end
      end

      # Builder for a Parquet `OffsetIndex`, part of the Parquet `PageIndex`.
      class OffsetIndexBuilder
        def initialize
          @offset_array = [] of Int64
          @compressed_page_size_array = [] of Int32
          @first_row_index_array = [] of Int64
          @unencoded_byte_array_data_bytes_array = nil
          @current_first_row_index = 0_i64
        end

        # Appends the row count of the next page.
        def append_row_count(row_count : Int64) : Nil
          @first_row_index_array << @current_first_row_index
          @current_first_row_index += row_count
        end

        # Appends the offset and size of the next page.
        def append_offset_and_size(offset : Int64, compressed_page_size : Int32) : Nil
          @offset_array << offset
          @compressed_page_size_array << compressed_page_size
        end

        # Appends the unencoded byte array data bytes of the next page.
        def append_unencoded_byte_array_data_bytes(unencoded_byte_array_data_bytes : Int64?) : Nil
          if val = unencoded_byte_array_data_bytes
            arr = (@unencoded_byte_array_data_bytes_array ||= [] of Int64)
            arr << val
          end
        end

        # Builds the thrift metadata of the offset index.
        def build : OffsetIndexMetaData
          locations = Array(PageLocation).new(@offset_array.size) do |i|
            PageLocation.new(@offset_array[i], @compressed_page_size_array[i], @first_row_index_array[i])
          end
          OffsetIndexMetaData.new(locations, @unencoded_byte_array_data_bytes_array)
        end
      end
    end
  end
end
