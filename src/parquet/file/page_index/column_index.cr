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

    # Little-endian byte encoding, the inverse of `try_from_le_slice`. Used by
    # the column-index write path (upstream `ParquetValueType::as_bytes`).
    def self.to_le_slice(value : Int32) : Bytes
      buf = Bytes.new(4)
      IO::Memory.new(buf).write_bytes(value, IO::ByteFormat::LittleEndian)
      buf
    end

    def self.to_le_slice(value : Int64) : Bytes
      buf = Bytes.new(8)
      IO::Memory.new(buf).write_bytes(value, IO::ByteFormat::LittleEndian)
      buf
    end

    def self.to_le_slice(value : Float32) : Bytes
      buf = Bytes.new(4)
      IO::Memory.new(buf).write_bytes(value, IO::ByteFormat::LittleEndian)
      buf
    end

    def self.to_le_slice(value : Float64) : Bytes
      buf = Bytes.new(8)
      IO::Memory.new(buf).write_bytes(value, IO::ByteFormat::LittleEndian)
      buf
    end

    def self.to_le_slice(value : Bool) : Bytes
      Bytes[value ? 1_u8 : 0_u8]
    end

    def self.to_le_slice(value : Int96) : Bytes
      value.as_bytes
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

        def boundary_order : BoundaryOrder
          @column_index.boundary_order
        end

        def null_counts : Array(Int64)?
          @column_index.null_counts
        end

        def nan_counts : Array(Int64)?
          @column_index.nan_counts
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

        # Writes the struct, encoding min/max manually as `list<binary>`.
        def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @column_index.null_pages, 1_i16, 0_i16)

          len = @column_index.null_pages.size
          prot.write_field_begin(::Parquet::Thrift::FieldType::List, 2_i16, last_field_id)
          prot.write_list_begin(::Parquet::Thrift::ElementType::Binary, len)
          len.times do |i|
            value = min_value(i)
            ::Parquet::Thrift.write_thrift_value(prot, value ? FromBytes.to_le_slice(value) : Bytes.new(0))
          end

          prot.write_field_begin(::Parquet::Thrift::FieldType::List, 3_i16, 2_i16)
          prot.write_list_begin(::Parquet::Thrift::ElementType::Binary, len)
          len.times do |i|
            value = max_value(i)
            ::Parquet::Thrift.write_thrift_value(prot, value ? FromBytes.to_le_slice(value) : Bytes.new(0))
          end

          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @column_index.boundary_order, 4_i16, 3_i16)
          if nc = @column_index.null_counts
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, nc, 5_i16, last_field_id)
          end
          if rep = @column_index.repetition_level_histograms
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, rep, 6_i16, last_field_id)
          end
          if dfn = @column_index.definition_level_histograms
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, dfn, 7_i16, last_field_id)
          end
          if nan = @column_index.nan_counts
            ::Parquet::Thrift.write_thrift_field(prot, nan, 8_i16, last_field_id)
          end
          prot.write_struct_end
        end
      end

      # Column index for byte arrays (fixed length and variable).
      #
      # Divergence: fields are public accessors and the `ColumnIndex` methods are
      # delegated (upstream uses `Deref`); offsets are `Int32` rather than
      # `usize`. `min_value`/`max_value` return byte slices into the packed
      # `min_bytes`/`max_bytes` buffers.
      struct ByteArrayColumnIndex
        getter column_index : ColumnIndex
        getter min_bytes : Bytes
        getter min_offsets : Array(Int32)
        getter max_bytes : Bytes
        getter max_offsets : Array(Int32)

        def initialize(
          @column_index : ColumnIndex,
          @min_bytes : Bytes,
          @min_offsets : Array(Int32),
          @max_bytes : Bytes,
          @max_offsets : Array(Int32),
        )
        end

        def self.try_new(
          null_pages : Array(Bool),
          boundary_order : BoundaryOrder,
          null_counts : Array(Int64)?,
          nan_counts : Array(Int64)?,
          repetition_level_histograms : Array(Int64)?,
          definition_level_histograms : Array(Int64)?,
          min_values : Array(Bytes),
          max_values : Array(Bytes),
        ) : ByteArrayColumnIndex
          len = null_pages.size

          if min_values.size != len || max_values.size != len
            raise general_err!(
              "ColumnIndex min/max length mismatch: expected #{len}, got min=#{min_values.size} max=#{max_values.size}"
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

          min_len = min_values.sum(&.size)
          max_len = max_values.sum(&.size)
          min_bytes = Bytes.new(min_len)
          max_bytes = Bytes.new(max_len)
          min_offsets = Array(Int32).new(len + 1, 0)
          max_offsets = Array(Int32).new(len + 1, 0)

          min_pos = 0
          max_pos = 0
          null_pages.each_with_index do |is_null, i|
            if is_null
              min_offsets[i] = min_pos
              max_offsets[i] = max_pos
            else
              min = min_values[i]
              min_bytes[min_pos, min.size].copy_from(min)
              min_offsets[i] = min_pos
              min_pos += min.size

              max = max_values[i]
              max_bytes[max_pos, max.size].copy_from(max)
              max_offsets[i] = max_pos
              max_pos += max.size
            end
          end
          min_offsets[len] = min_pos
          max_offsets[len] = max_pos

          new(
            ColumnIndex.new(null_pages, boundary_order, null_counts, nan_counts, repetition_level_histograms, definition_level_histograms),
            min_bytes,
            min_offsets,
            max_bytes,
            max_offsets,
          )
        end

        def self.try_from_thrift(index : ThriftColumnIndex) : ByteArrayColumnIndex
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

        def boundary_order : BoundaryOrder
          @column_index.boundary_order
        end

        def null_counts : Array(Int64)?
          @column_index.null_counts
        end

        def nan_counts : Array(Int64)?
          @column_index.nan_counts
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
        def min_value(idx : Int32) : Bytes?
          return nil if null_page?(idx)

          @min_bytes[@min_offsets[idx]...@min_offsets[idx + 1]]
        end

        # Returns the max value for the page, or `nil` when all values are null.
        def max_value(idx : Int32) : Bytes?
          return nil if null_page?(idx)

          @max_bytes[@max_offsets[idx]...@max_offsets[idx + 1]]
        end

        def min_values_iter : Iterator(Bytes?)
          (0...num_pages).each.map { |i| min_value(i) }
        end

        def max_values_iter : Iterator(Bytes?)
          (0...num_pages).each.map { |i| max_value(i) }
        end

        # Writes the struct, encoding min/max manually as `list<binary>`.
        def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @column_index.null_pages, 1_i16, 0_i16)

          len = @column_index.null_pages.size
          prot.write_field_begin(::Parquet::Thrift::FieldType::List, 2_i16, last_field_id)
          prot.write_list_begin(::Parquet::Thrift::ElementType::Binary, len)
          len.times do |i|
            ::Parquet::Thrift.write_thrift_value(prot, min_value(i) || Bytes.new(0))
          end

          prot.write_field_begin(::Parquet::Thrift::FieldType::List, 3_i16, 2_i16)
          prot.write_list_begin(::Parquet::Thrift::ElementType::Binary, len)
          len.times do |i|
            ::Parquet::Thrift.write_thrift_value(prot, max_value(i) || Bytes.new(0))
          end

          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @column_index.boundary_order, 4_i16, 3_i16)
          if nc = @column_index.null_counts
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, nc, 5_i16, last_field_id)
          end
          if rep = @column_index.repetition_level_histograms
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, rep, 6_i16, last_field_id)
          end
          if dfn = @column_index.definition_level_histograms
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, dfn, 7_i16, last_field_id)
          end
          if nan = @column_index.nan_counts
            ::Parquet::Thrift.write_thrift_field(prot, nan, 8_i16, last_field_id)
          end
          prot.write_struct_end
        end
      end

      # Parsed `ColumnIndex` information for a Parquet file.
      #
      # Divergence: upstream is an enum with eight variants
      # (`BOOLEAN`/`INT32`/`INT64`/`INT96`/`FLOAT`/`DOUBLE`/`BYTE_ARRAY`/
      # `FIXED_LEN_BYTE_ARRAY`). Modelled as a struct carrying the physical
      # `Type` plus the inner index; recover the variant via `kind` and
      # `inner.as(...)`. `is_null_page` maps to `null_page?`; `get_boundary_order`
      # returns a non-optional `BoundaryOrder`; `num_pages`/`num` indices are
      # `Int32`.
      struct ColumnIndexMetaData
        alias Index = PrimitiveColumnIndex(Bool) |
                      PrimitiveColumnIndex(Int32) |
                      PrimitiveColumnIndex(Int64) |
                      PrimitiveColumnIndex(Int96) |
                      PrimitiveColumnIndex(Float32) |
                      PrimitiveColumnIndex(Float64) |
                      ByteArrayColumnIndex

        getter kind : Type
        getter inner : Index

        def initialize(@kind : Type, @inner : Index)
        end

        # Returns whether the min/max elements inside the index are ordered.
        def sorted? : Bool
          boundary_order != BoundaryOrder::UNORDERED
        end

        # Returns the `BoundaryOrder` of this page index.
        #
        # Divergence: upstream `get_boundary_order` returns
        # `Option<BoundaryOrder>`; here it is the non-optional `boundary_order`.
        def boundary_order : BoundaryOrder
          @inner.boundary_order
        end

        def null_counts : Array(Int64)?
          @inner.null_counts
        end

        def nan_counts : Array(Int64)?
          @inner.nan_counts
        end

        # Returns the number of pages.
        def num_pages : Int32
          @inner.num_pages
        end

        # Returns the number of null values in the page indexed by `idx`.
        def null_count(idx : Int32) : Int64?
          @inner.null_count(idx)
        end

        # Returns the number of NaN values in the page indexed by `idx`.
        def nan_count(idx : Int32) : Int64?
          @inner.nan_count(idx)
        end

        def repetition_level_histogram(idx : Int32) : Array(Int64)?
          @inner.repetition_level_histogram(idx)
        end

        def definition_level_histogram(idx : Int32) : Array(Int64)?
          @inner.definition_level_histogram(idx)
        end

        # Returns whether the page indexed by `idx` consists of all null values.
        def null_page?(idx : Int32) : Bool
          @inner.null_page?(idx)
        end

        def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
          @inner.write_thrift(prot)
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
