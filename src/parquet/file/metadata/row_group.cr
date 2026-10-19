require "./column_chunk"

# Ported from `parquet/src/file/metadata/mod.rs` and
# `parquet/src/file/metadata/thrift/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `RowGroupMetaData` (+ builder and thrift conversion).
# `ParquetMetaData`/`FileMetaData` follow (see `plans/parity.md`).
module Parquet
  module File
    module Metadata
      # Metadata for a row group.
      #
      # Divergence: fields are public accessors (upstream private).
      class RowGroupMetaData
        property columns : Array(ColumnChunkMetaData)
        property num_rows : Int64
        property sorting_columns : Array(SortingColumn)?
        property total_byte_size : Int64
        getter schema_descr : Parquet::Schema::SchemaDescriptor
        property file_offset : Int64?
        property ordinal : Int32?

        def initialize(@schema_descr : Parquet::Schema::SchemaDescriptor)
          @columns = [] of ColumnChunkMetaData
          @num_rows = 0_i64
          @sorting_columns = nil
          @total_byte_size = 0_i64
          @file_offset = nil
          @ordinal = nil
        end

        # Returns a builder for row group metadata.
        def self.builder(schema_descr : Parquet::Schema::SchemaDescriptor) : RowGroupMetaDataBuilder
          RowGroupMetaDataBuilder.new(schema_descr)
        end

        # Number of columns in this row group.
        def num_columns : Int32
          @columns.size
        end

        # Returns column chunk metadata for the `i`-th column.
        def column(i : Int32) : ColumnChunkMetaData
          @columns[i]
        end

        # The column chunks of this row group (upstream `columns`).
        def columns : Array(ColumnChunkMetaData)
          @columns
        end

        def columns_mut : Array(ColumnChunkMetaData)
          @columns
        end

        # Total size of all compressed column data in this row group.
        def compressed_size : Int64
          @columns.sum(&.total_compressed_size)
        end

        def schema_descr_ptr : Parquet::Schema::SchemaDescriptor
          @schema_descr
        end

        def into_builder : RowGroupMetaDataBuilder
          RowGroupMetaDataBuilder.new(self)
        end

        def ==(other : RowGroupMetaData) : Bool
          columns == other.columns &&
            num_rows == other.num_rows &&
            sorting_columns == other.sorting_columns &&
            total_byte_size == other.total_byte_size &&
            file_offset == other.file_offset &&
            ordinal == other.ordinal &&
            schema_descr == other.schema_descr
        end

        def self.thrift_element_type : ::Parquet::Thrift::ElementType
          ::Parquet::Thrift::ElementType::Struct
        end

        # Serializes this as a thrift `RowGroup`.
        def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @columns, 1_i16, 0_i16)
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @total_byte_size, 2_i16, last_field_id)
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @num_rows, 3_i16, last_field_id)

          if sorting = @sorting_columns
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, sorting, 4_i16, last_field_id)
          end
          if off = @file_offset
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, off, 5_i16, last_field_id)
          end
          # always written (optional per spec)
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, compressed_size, 6_i16, last_field_id)

          if prot.write_row_group_ordinal?
            if ordinal = @ordinal
              if Int16::MIN <= ordinal <= Int16::MAX
                ::Parquet::Thrift.write_thrift_field(prot, ordinal.to_i16, 7_i16, last_field_id)
              end
            end
          end

          prot.write_struct_end
        end
      end

      # Builder for `RowGroupMetaData`.
      class RowGroupMetaDataBuilder
        def initialize(@meta : RowGroupMetaData)
        end

        def self.new(schema_descr : Parquet::Schema::SchemaDescriptor) : RowGroupMetaDataBuilder
          new(RowGroupMetaData.new(schema_descr))
        end

        # ameba:disable Naming/AccessorMethodName
        def set_num_rows(value : Int64) : self
          @meta.num_rows = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_sorting_columns(value : Array(SortingColumn)?) : self
          @meta.sorting_columns = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_total_byte_size(value : Int64) : self
          @meta.total_byte_size = value
          self
        end

        # Takes ownership of the column metadata, clearing the list.
        def take_columns : Array(ColumnChunkMetaData)
          columns = @meta.columns
          @meta.columns = [] of ColumnChunkMetaData
          columns
        end

        # ameba:disable Naming/AccessorMethodName
        def set_column_metadata(value : Array(ColumnChunkMetaData)) : self
          @meta.columns = value
          self
        end

        def add_column_metadata(value : ColumnChunkMetaData) : self
          @meta.columns << value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_ordinal(value : Int32) : self
          @meta.ordinal = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_file_offset(value : Int64) : self
          @meta.file_offset = value
          self
        end

        def build : RowGroupMetaData
          schema_columns = @meta.schema_descr.num_columns
          if schema_columns != @meta.columns.size
            raise general_err!("Column length mismatch: #{schema_columns} != #{@meta.columns.size}")
          end

          @meta
        end

        def build_unchecked : RowGroupMetaData
          @meta
        end
      end

      # Reads a thrift `RowGroup`.
      # ameba:disable Metrics/CyclomaticComplexity
      def self.read_row_group(
        prot : ::Parquet::Thrift::ThriftCompactInputProtocol,
        schema_descr : Parquet::Schema::SchemaDescriptor,
      ) : RowGroupMetaData
        row_group = RowGroupMetaDataBuilder.new(schema_descr).build_unchecked

        rg_columns = 1_u8 << 1
        rg_tot_byte_size = 1_u8 << 2
        rg_num_rows = 1_u8 << 3
        rg_all_required = rg_columns | rg_tot_byte_size | rg_num_rows
        mask = 0_u8

        last_field_id = 0_i16
        loop do
          field_ident = prot.read_field_begin(last_field_id)
          break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

          case field_ident.id
          when 1
            list_ident = prot.read_list_begin
            ::Parquet::Thrift.validate_list_type(::Parquet::Thrift::ElementType::Struct, list_ident)
            if schema_descr.num_columns != list_ident.size
              raise general_err!(
                "Column count mismatch. Schema has #{schema_descr.num_columns} columns while Row Group has #{list_ident.size}"
              )
            end
            list_ident.size.times do |i|
              row_group.columns << read_column_chunk(prot, schema_descr.column(i), i)
            end
            mask |= rg_columns
          when 2
            row_group.total_byte_size = prot.read_i64
            mask |= rg_tot_byte_size
          when 3
            row_group.num_rows = prot.read_i64
            mask |= rg_num_rows
          when 4
            row_group.sorting_columns = ::Parquet::Thrift.read_thrift_vec(prot, SortingColumn)
          when 5
            row_group.file_offset = prot.read_i64
          when 7
            row_group.ordinal = prot.read_i16.to_i32
          else
            prot.skip(field_ident.field_type)
          end
          last_field_id = field_ident.id
        end

        unless mask == rg_all_required
          raise general_err!("Required field columns is missing") if (mask & rg_columns) == 0
          raise general_err!("Required field total_byte_size is missing") if (mask & rg_tot_byte_size) == 0
          raise general_err!("Required field num_rows is missing") if (mask & rg_num_rows) == 0
        end

        row_group
      end
    end
  end
end
