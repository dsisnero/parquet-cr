require "./offset_index"

# Ported from `parquet/src/file/page_index/column_index.rs`,
# `parquet/src/file/page_index/index_reader.rs`, and
# `parquet/src/util/bit_util.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ColumnIndex`, `PrimitiveColumnIndex<T>`, and `ThriftColumnIndex`, plus
# the little-endian `try_from_le_slice` decoding. `ByteArrayColumnIndex`,
# `ColumnIndexMetaData`, `decode_column_index`, and the page-index builders
# follow (see `plans/parity.md`).
module Parquet
  # Little-endian plain-value decoding for column-index min/max.
  module FromBytes
    def self.array_from_slice(bytes : Bytes, n : Int32) : Bytes
      if bytes.size < n
        raise general_err!("error converting value, expected #{n} bytes got #{bytes.size}")
      end
      bytes[0, n]
    end

    def self.try_from_le_slice(type : Int32.class, bytes : Bytes) : Int32
      IO::Memory.new(array_from_slice(bytes, 4)).read_bytes(Int32, IO::ByteFormat::LittleEndian)
    end

    def self.try_from_le_slice(type : Int64.class, bytes : Bytes) : Int64
      IO::Memory.new(array_from_slice(bytes, 8)).read_bytes(Int64, IO::ByteFormat::LittleEndian)
    end

    def self.try_from_le_slice(type : Float32.class, bytes : Bytes) : Float32
      IO::Memory.new(array_from_slice(bytes, 4)).read_bytes(Float32, IO::ByteFormat::LittleEndian)
    end

    def self.try_from_le_slice(type : Float64.class, bytes : Bytes) : Float64
      IO::Memory.new(array_from_slice(bytes, 8)).read_bytes(Float64, IO::ByteFormat::LittleEndian)
    end

    def self.try_from_le_slice(type : Bool.class, bytes : Bytes) : Bool
      array_from_slice(bytes, 1)[0] != 0
    end

    def self.try_from_le_slice(type : Int96.class, bytes : Bytes) : Int96
      Int96.try_from_le_slice(bytes)
    end

    def self.try_from_le_slice(type : ByteArray.class, bytes : Bytes) : ByteArray
      ByteArray.new(bytes)
    end

    def self.try_from_le_slice(type : FixedLenByteArray.class, bytes : Bytes) : FixedLenByteArray
      FixedLenByteArray.new(ByteArray.new(bytes))
    end

    # Zero value used as a placeholder for null pages.
    def self.default(type : Int32.class) : Int32
      0_i32
    end

    def self.default(type : Int64.class) : Int64
      0_i64
    end

    def self.default(type : Float32.class) : Float32
      0_f32
    end

    def self.default(type : Float64.class) : Float64
      0_f64
    end

    def self.default(type : Bool.class) : Bool
      false
    end

    def self.default(type : Int96.class) : Int96
      Int96.new
    end

    def self.default(type : ByteArray.class) : ByteArray
      ByteArray.new
    end

    def self.default(type : FixedLenByteArray.class) : FixedLenByteArray
      FixedLenByteArray.new(ByteArray.new)
    end
  end

  module File
    module PageIndex
      # Common bits of the column index.
      struct ColumnIndex
        getter null_pages : Array(Bool)
        getter boundary_order : BoundaryOrder
        getter null_counts : Array(Int64)?
        getter nan_counts : Array(Int64)?
        getter repetition_level_histograms : Array(Int64)?
        getter definition_level_histograms : Array(Int64)?

        def initialize(
          @null_pages : Array(Bool),
          @boundary_order : BoundaryOrder,
          @null_counts : Array(Int64)?,
          @nan_counts : Array(Int64)?,
          @repetition_level_histograms : Array(Int64)?,
          @definition_level_histograms : Array(Int64)?,
        )
        end

        # Returns the number of pages.
        def num_pages : Int32
          @null_pages.size
        end

        # Returns the number of null values in the page indexed by `idx`.
        def null_count(idx : Int32) : Int64?
          @null_counts.try(&.[idx])
        end

        # Returns the number of NaN values in the page indexed by `idx`.
        def nan_count(idx : Int32) : Int64?
          @nan_counts.try(&.[idx])
        end

        def repetition_level_histogram(idx : Int32) : Array(Int64)?
          hists = @repetition_level_histograms
          return nil if hists.nil?

          num_lvls = hists.size // num_pages
          start = num_lvls * idx
          hists[start, num_lvls]
        end

        def definition_level_histogram(idx : Int32) : Array(Int64)?
          hists = @definition_level_histograms
          return nil if hists.nil?

          num_lvls = hists.size // num_pages
          start = num_lvls * idx
          hists[start, num_lvls]
        end

        # Returns whether the page indexed by `idx` consists of all null values.
        def null_page?(idx : Int32) : Bool
          @null_pages[idx]
        end
      end

      # Column index for primitive types.
      #
      # Divergence: fields are public accessors; the `ColumnIndex` methods are
      # delegated (upstream uses `Deref`).
      struct PrimitiveColumnIndex(T)
        getter column_index : ColumnIndex
        getter min_values : Array(T)
        getter max_values : Array(T)

        def initialize(@column_index : ColumnIndex, @min_values : Array(T), @max_values : Array(T))
        end

        def self.try_new(
          null_pages : Array(Bool),
          boundary_order : BoundaryOrder,
          null_counts : Array(Int64)?,
          nan_counts : Array(Int64)?,
          repetition_level_histograms : Array(Int64)?,
          definition_level_histograms : Array(Int64)?,
          min_bytes : Array(Bytes),
          max_bytes : Array(Bytes),
        ) : PrimitiveColumnIndex(T)
          len = null_pages.size

          if min_bytes.size != len || max_bytes.size != len
            raise general_err!(
              "ColumnIndex min/max length mismatch: expected #{len}, got min=#{min_bytes.size} max=#{max_bytes.size}"
            )
          end
          if nc = null_counts
            unless nc.size == len
              raise general_err!("ColumnIndex null_counts length mismatch: expected #{len}, got #{nc.size}")
            end
          end
          if rep = repetition_level_histograms
            raise general_err!("Invalid repetition_level_histograms length") if len != 0 && rep.size % len != 0
          end
          if dfn = definition_level_histograms
            raise general_err!("Invalid definition_level_histograms length") if len != 0 && dfn.size % len != 0
          end

          min_values = [] of T
          max_values = [] of T
          null_pages.each_with_index do |is_null, i|
            if is_null
              min_values << FromBytes.default(T)
              max_values << FromBytes.default(T)
            else
              min_values << FromBytes.try_from_le_slice(T, min_bytes[i])
              max_values << FromBytes.try_from_le_slice(T, max_bytes[i])
            end
          end

          new(
            ColumnIndex.new(null_pages, boundary_order, null_counts, nan_counts, repetition_level_histograms, definition_level_histograms),
            min_values,
            max_values,
          )
        end

        def self.try_from_thrift(index : ThriftColumnIndex) : PrimitiveColumnIndex(T)
          try_new(
            index.null_pages,
            index.boundary_order,
            index.null_counts,
            index.nan_counts,
            index.repetition_level_histograms,
            index.definition_level_histograms,
            index.min_values,
            index.max_values,
          )
        end

        def num_pages : Int32
          @column_index.num_pages
        end

        def null_count(idx : Int32) : Int64?
          @column_index.null_count(idx)
        end

        def nan_count(idx : Int32) : Int64?
          @column_index.nan_count(idx)
        end

        def repetition_level_histogram(idx : Int32) : Array(Int64)?
          @column_index.repetition_level_histogram(idx)
        end

        def definition_level_histogram(idx : Int32) : Array(Int64)?
          @column_index.definition_level_histogram(idx)
        end

        def null_page?(idx : Int32) : Bool
          @column_index.null_page?(idx)
        end

        # Returns the min value for the page, or `nil` when all values are null.
        def min_value(idx : Int32) : T?
          null_page?(idx) ? nil : @min_values[idx]
        end

        # Returns the max value for the page, or `nil` when all values are null.
        def max_value(idx : Int32) : T?
          null_page?(idx) ? nil : @max_values[idx]
        end

        def min_values_iter : Iterator(T?)
          (0...@min_values.size).each.map { |i| min_value(i) }
        end

        def max_values_iter : Iterator(T?)
          (0...@max_values.size).each.map { |i| max_value(i) }
        end
      end

      alias BoolList = Array(Bool)
      alias BytesList = Array(Bytes)

      # Private struct used for decoding `ColumnIndex`, then discarded.
      define_thrift_struct(ThriftColumnIndex) do
        required 1, null_pages, BoolList
        required 2, min_values, BytesList
        required 3, max_values, BytesList
        required 4, boundary_order, BoundaryOrder
        optional 5, null_counts, Int64List
        optional 6, repetition_level_histograms, Int64List
        optional 7, definition_level_histograms, Int64List
        optional 8, nan_counts, Int64List
      end
    end
  end
end
