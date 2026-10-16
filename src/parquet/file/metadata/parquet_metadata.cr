require "./row_group"
require "../../schema/thrift"

# Ported from `parquet/src/file/metadata/mod.rs`,
# `parquet/src/file/metadata/thrift/mod.rs`,
# `parquet/src/file/metadata/writer.rs`, and
# `parquet/src/file/metadata/reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `FileMetaData`, `ParquetMetaData`, and the footer metadata write/read
# (`ParquetMetaDataWriter`/`ParquetMetaDataReader.decode_metadata`). Page
# indexes, encryption, and options are not yet ported (see `plans/parity.md`).
module Parquet
  module File
    module Metadata
      # File-level metadata for a Parquet file.
      class FileMetaData
        getter version : Int32
        getter num_rows : Int64
        getter created_by : String?
        getter key_value_metadata : Array(KeyValue)?
        getter schema_descr : Parquet::Schema::SchemaDescriptor
        getter column_orders : Array(ColumnOrder)?

        def initialize(
          @version : Int32,
          @num_rows : Int64,
          @created_by : String?,
          @key_value_metadata : Array(KeyValue)?,
          @schema_descr : Parquet::Schema::SchemaDescriptor,
          @column_orders : Array(ColumnOrder)?,
        )
        end

        # Returns the schema `Type` for this file.
        def schema : Parquet::Schema::Type
          @schema_descr.root_schema
        end

        def schema_descr_ptr : Parquet::Schema::SchemaDescriptor
          @schema_descr
        end

        # Returns the column order for the `i`-th column (UNDEFINED if absent).
        def column_order(i : Int32) : ColumnOrder
          @column_orders.try(&.fetch(i, ColumnOrder::UNDEFINED)) || ColumnOrder::UNDEFINED
        end

        def ==(other : FileMetaData) : Bool
          version == other.version &&
            num_rows == other.num_rows &&
            created_by == other.created_by &&
            key_value_metadata == other.key_value_metadata &&
            schema_descr == other.schema_descr &&
            column_orders == other.column_orders
        end
      end

      # Metadata for a Parquet file.
      class ParquetMetaData
        getter file_metadata : FileMetaData
        getter row_groups : Array(RowGroupMetaData)
        getter page_index : PageIndexProvider?

        def initialize(
          @file_metadata : FileMetaData,
          @row_groups : Array(RowGroupMetaData),
          @page_index : PageIndexProvider? = nil,
        )
        end

        def num_rows : Int64
          @file_metadata.num_rows
        end

        def row_group(i : Int32) : RowGroupMetaData
          @row_groups[i]
        end

        def num_row_groups : Int32
          @row_groups.size
        end

        def row_group_num_rows(i : Int32) : Int64
          @row_groups[i].num_rows
        end

        # Converts this metadata into a `ParquetMetaDataBuilder`.
        def into_builder : ParquetMetaDataBuilder
          ParquetMetaDataBuilder.new_from_metadata(self)
        end

        # Returns a `RowGroupPageIndex` view for a specific row group.
        def page_index_for_row_group(row_group_idx : Int32) : RowGroupPageIndex
          RowGroupPageIndex.new(row_group_idx, @page_index)
        end

        # Overrides the page index.
        # ameba:disable Naming/AccessorMethodName
        def set_page_index(index : PageIndexProvider?) : Nil
          @page_index = index
        end

        # Compares file metadata, row groups, and the page index.
        #
        # Divergence: the page index compares by value for `PageIndex` and by
        # reference for other `PageIndexProvider`s (upstream falls back to
        # `Arc::ptr_eq`).
        def ==(other : ParquetMetaData) : Bool
          file_metadata == other.file_metadata &&
            row_groups == other.row_groups &&
            page_index == other.page_index
        end
      end

      # Builder for creating / manipulating `ParquetMetaData`.
      #
      # Divergence: upstream is a tuple struct wrapping `ParquetMetaData`; this
      # holds the fields directly. `new_from_metadata` mirrors the Rust
      # `From<ParquetMetaData>` impl.
      class ParquetMetaDataBuilder
        getter file_metadata : FileMetaData
        getter row_groups : Array(RowGroupMetaData)
        getter page_index : PageIndexProvider?

        def initialize(@file_metadata : FileMetaData)
          @row_groups = [] of RowGroupMetaData
          @page_index = nil
        end

        # Creates a builder from existing metadata.
        def self.new_from_metadata(metadata : ParquetMetaData) : ParquetMetaDataBuilder
          new(metadata.file_metadata).set_row_groups(metadata.row_groups).set_page_index(metadata.page_index)
        end

        # Adds a row group to the metadata.
        def add_row_group(row_group : RowGroupMetaData) : self
          @row_groups << row_group
          self
        end

        # Sets all the row groups to the specified list.
        # ameba:disable Naming/AccessorMethodName
        def set_row_groups(row_groups : Array(RowGroupMetaData)) : self
          @row_groups = row_groups
          self
        end

        # Takes ownership of the row groups, clearing the list.
        def take_row_groups : Array(RowGroupMetaData)
          row_groups = @row_groups
          @row_groups = [] of RowGroupMetaData
          row_groups
        end

        # Sets the `PageIndexProvider`.
        # ameba:disable Naming/AccessorMethodName
        def set_page_index(page_index : PageIndexProvider?) : self
          @page_index = page_index
          self
        end

        # Takes the current `PageIndexProvider`, replacing it with `nil`.
        def take_page_index : PageIndexProvider?
          page_index = @page_index
          @page_index = nil
          page_index
        end

        # Creates a new `ParquetMetaData` from the builder.
        def build : ParquetMetaData
          ParquetMetaData.new(@file_metadata, @row_groups, @page_index)
        end
      end

      # Ensures row group ordinals are usable after decode.
      def self.ensure_row_group_ordinals(row_groups : Array(RowGroupMetaData)) : Nil
        # All set (honor as written) or mixed (leave as-is): nothing to backfill.
        return if row_groups.any?(&.ordinal)

        row_groups.each_with_index do |row_group, index|
          if index > Int32::MAX
            raise general_err!("Row group ordinal #{index} exceeds i32 max value")
          end
          row_group.ordinal = index
        end
      end

      # Serializes a `FileMetaData` (the thrift `FileMetaData` struct).
      def self.write_file_metadata(
        file_metadata : FileMetaData,
        row_groups : Array(RowGroupMetaData),
        writer : ::Parquet::Thrift::ThriftCompactOutputProtocol,
        write_path_in_schema : Bool,
      ) : Nil
        writer.set_write_path_in_schema(write_path_in_schema)
        writer.set_write_row_group_ordinal(row_groups.size <= Int16::MAX)

        last_field_id = ::Parquet::Thrift.write_thrift_field(writer, file_metadata.version, 1_i16, 0_i16)

        writer.write_field_begin(::Parquet::Thrift::FieldType::List, 2_i16, last_field_id)
        root = file_metadata.schema_descr.root_schema
        writer.write_list_begin(::Parquet::Thrift::ElementType::Struct, Parquet::Schema.num_nodes(root))
        Parquet::Schema.write_schema(root, writer)
        last_field_id = 2_i16

        last_field_id = ::Parquet::Thrift.write_thrift_field(writer, file_metadata.num_rows, 3_i16, last_field_id)
        last_field_id = ::Parquet::Thrift.write_thrift_field(writer, row_groups, 4_i16, last_field_id)

        if kv = file_metadata.key_value_metadata
          last_field_id = ::Parquet::Thrift.write_thrift_field(writer, kv, 5_i16, last_field_id)
        end
        if created_by = file_metadata.created_by
          last_field_id = ::Parquet::Thrift.write_thrift_field(writer, created_by, 6_i16, last_field_id)
        end
        if column_orders = file_metadata.column_orders
          ::Parquet::Thrift.write_thrift_field(writer, column_orders, 7_i16, last_field_id)
        end

        writer.write_struct_end
      end

      # Decodes `ParquetMetaData` from the thrift-encoded footer (without the
      # trailing length + magic).
      # ameba:disable Metrics/CyclomaticComplexity
      def self.parquet_metadata_from_bytes(bytes : Bytes) : ParquetMetaData
        prot = ::Parquet::Thrift::ThriftSliceInputProtocol.new(bytes)

        version = nil.as(Int32?)
        num_rows = nil.as(Int64?)
        row_groups = nil.as(Array(RowGroupMetaData)?)
        key_value_metadata = nil.as(Array(KeyValue)?)
        created_by = nil.as(String?)
        column_orders = nil.as(Array(ColumnOrder)?)
        schema_descr = nil.as(Parquet::Schema::SchemaDescriptor?)

        last_field_id = 0_i16
        loop do
          field_ident = prot.read_field_begin(last_field_id)
          break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

          case field_ident.id
          when 1
            version = prot.read_i32
          when 2
            elements = ::Parquet::Thrift.read_thrift_vec(prot, Parquet::Schema::SchemaElement)
            schema_descr = Parquet::Schema::SchemaDescriptor.new(Parquet::Schema.parquet_schema_from_array(elements))
          when 3
            num_rows = prot.read_i64
          when 4
            descr = schema_descr
            raise general_err!("Required field schema is missing") if descr.nil?

            list_ident = prot.read_list_begin
            ::Parquet::Thrift.validate_list_type(::Parquet::Thrift::ElementType::Struct, list_ident)
            rgs = [] of RowGroupMetaData
            list_ident.size.times do
              rgs << read_row_group(prot, descr)
            end
            ensure_row_group_ordinals(rgs)
            row_groups = rgs
          when 5
            key_value_metadata = ::Parquet::Thrift.read_thrift_vec(prot, KeyValue)
          when 6
            created_by = prot.read_string
          when 7
            column_orders = ::Parquet::Thrift.read_thrift_vec(prot, ColumnOrder)
          else
            prot.skip(field_ident.field_type)
          end
          last_field_id = field_ident.id
        end

        raise general_err!("Required field version is missing") if version.nil?
        raise general_err!("Required field num_rows is missing") if num_rows.nil?
        raise general_err!("Required field row_groups is missing") if row_groups.nil?

        descr = schema_descr
        raise general_err!("Required field schema is missing") if descr.nil?

        if orders = column_orders
          raise general_err!("Column order length mismatch") unless orders.size == descr.num_columns
          orders.each_with_index do |order, i|
            next unless order.kind == ColumnOrder::Kind::TypeDefinedOrder

            column = descr.column(i)
            sort_order = ColumnOrder.get_sort_order_for_type(
              column.logical_type_ref, column.converted_type, column.physical_type, true
            )
            orders[i] = ColumnOrder.type_defined_order(sort_order)
          end
        end

        fmd = FileMetaData.new(version, num_rows, created_by, key_value_metadata, descr, column_orders)
        ParquetMetaData.new(fmd, row_groups)
      end

      # Writes `ParquetMetaData` into an IO sink as a Parquet footer.
      class ParquetMetaDataWriter
        def initialize(@buf : IO, @metadata : ParquetMetaData)
          @write_path_in_schema = true
        end

        def with_write_path_in_schema(val : Bool) : self
          @write_path_in_schema = val
          self
        end

        def finish : Nil
          meta_io = IO::Memory.new
          writer = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(meta_io)
          Metadata.write_file_metadata(
            @metadata.file_metadata, @metadata.row_groups, writer, @write_path_in_schema
          )
          bytes = meta_io.to_slice
          @buf.write(bytes)
          @buf.write_bytes(bytes.size.to_u32, IO::ByteFormat::LittleEndian)
          @buf.write("PAR1".to_slice)
        end
      end

      # Reads `ParquetMetaData` from a thrift-encoded footer.
      class ParquetMetaDataReader
        # Decodes `ParquetMetaData` from the thrift-encoded footer bytes
        # (without the trailing length + magic).
        def self.decode_metadata(bytes : Bytes) : ParquetMetaData
          Metadata.parquet_metadata_from_bytes(bytes)
        end
      end
    end
  end
end
