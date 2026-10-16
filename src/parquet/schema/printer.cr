require "../basic"
require "../file/metadata"
require "../file/metadata/column_chunk"
require "../file/metadata/row_group"
require "../file/metadata/parquet_metadata"
require "./types"

# Ported from `parquet/src/schema/printer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Prints a Parquet schema and file metadata.
# Divergences: the `print_*` free functions are `Parquet::Schema` module
# methods; `print_row_group_metadata`/`print_column_chunk_metadata` are public;
# `fmt::Write`/`io::Write` are `IO`; logical-type payloads are accessed through
# the wrapper's `value` (e.g. `LogicalType::Integer#value.bit_width`);
# `has_id` → `has_id?`.
module Parquet
  module Schema
    # Prints Parquet metadata information.
    def self.print_parquet_metadata(io : IO, metadata : File::Metadata::ParquetMetaData) : Nil
      print_file_metadata(io, metadata.file_metadata)
      io << '\n' << '\n'
      io << "num of row groups: " << metadata.num_row_groups << '\n'
      io << "row groups:\n"
      io << '\n'
      metadata.row_groups.each_with_index do |row_group, i|
        io << "row group " << i << ":\n"
        print_dashes(io, 80)
        print_row_group_metadata(io, row_group)
      end
    end

    # Prints file metadata information.
    def self.print_file_metadata(io : IO, file_metadata : File::Metadata::FileMetaData) : Nil
      io << "version: " << file_metadata.version << '\n'
      io << "num of rows: " << file_metadata.num_rows << '\n'
      if created_by = file_metadata.created_by
        io << "created by: " << created_by << '\n'
      end
      if metadata = file_metadata.key_value_metadata
        io << "metadata:\n"
        metadata.each do |entry|
          io << "  " << entry.key << ": " << (entry.value || "") << '\n'
        end
      end
      print_schema(io, file_metadata.schema)
    end

    # Prints a Parquet `Type`.
    def self.print_schema(io : IO, tp : Type) : Nil
      builder = String::Builder.new
      Printer.new(builder).print(tp)
      io << builder.to_s << '\n'
    end

    # Prints a row group's metadata.
    def self.print_row_group_metadata(io : IO, rg_metadata : File::Metadata::RowGroupMetaData) : Nil
      io << "total byte size: " << rg_metadata.total_byte_size << '\n'
      io << "num of rows: " << rg_metadata.num_rows << '\n'
      io << '\n'
      io << "num of columns: " << rg_metadata.num_columns << '\n'
      io << "columns: \n"
      rg_metadata.columns.each_with_index do |column, i|
        io << '\n'
        io << "column " << i << ":\n"
        print_dashes(io, 80)
        print_column_chunk_metadata(io, column)
      end
    end

    def self.print_column_chunk_metadata(io : IO, cc_metadata : File::Metadata::ColumnChunkMetaData) : Nil
      io << "column type: " << cc_metadata.column_type << '\n'
      io << "column path: " << cc_metadata.column_path << '\n'
      io << "encodings: " << cc_metadata.encodings_list.map(&.to_s).join(" ") << '\n'
      io << "file path: " << (cc_metadata.file_path || "N/A") << '\n'
      io << "file offset: " << cc_metadata.file_offset << '\n'
      io << "num of values: " << cc_metadata.num_values << '\n'
      io << "compression: " << cc_metadata.compression << '\n'
      io << "total compressed size (in bytes): " << cc_metadata.compressed_size << '\n'
      io << "total uncompressed size (in bytes): " << cc_metadata.uncompressed_size << '\n'
      io << "data page offset: " << cc_metadata.data_page_offset << '\n'
      io << "index page offset: " << (cc_metadata.index_page_offset.try(&.to_s) || "N/A") << '\n'
      io << "dictionary page offset: " << (cc_metadata.dictionary_page_offset.try(&.to_s) || "N/A") << '\n'
      io << "statistics: " << (cc_metadata.statistics.try(&.to_s) || "N/A") << '\n'
      io << "bloom filter offset: " << (cc_metadata.bloom_filter_offset.try(&.to_s) || "N/A") << '\n'
      io << "bloom filter length: " << (cc_metadata.bloom_filter_length.try(&.to_s) || "N/A") << '\n'
      io << "offset index offset: " << (cc_metadata.offset_index_offset.try(&.to_s) || "N/A") << '\n'
      io << "offset index length: " << (cc_metadata.offset_index_length.try(&.to_s) || "N/A") << '\n'
      io << "column index offset: " << (cc_metadata.column_index_offset.try(&.to_s) || "N/A") << '\n'
      io << "column index length: " << (cc_metadata.column_index_length.try(&.to_s) || "N/A") << '\n'
      io << '\n'
    end

    def self.print_dashes(io : IO, num : Int32) : Nil
      num.times { io << '-' }
      io << '\n'
    end

    INDENT_WIDTH = 2

    # Returns the `TimeUnit` name.
    def self.print_timeunit(unit : TimeUnit) : String
      case unit
      when TimeUnit::MILLIS then "MILLIS"
      when TimeUnit::MICROS then "MICROS"
      else                       "NANOS"
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def self.print_logical_and_converted(
      logical_type : LogicalType?,
      converted_type : ConvertedType,
      precision : Int32,
      scale : Int32,
    ) : String
      if logical_type
        case logical_type
        when LogicalType::Integer
          type = logical_type.value
          "INTEGER(#{type.bit_width},#{type.is_signed})"
        when LogicalType::Decimal
          type = logical_type.value
          "DECIMAL(#{type.precision},#{type.scale})"
        when LogicalType::Timestamp
          type = logical_type.value
          "TIMESTAMP(#{print_timeunit(type.unit)},#{type.is_adjusted_to_u_t_c})"
        when LogicalType::Time
          type = logical_type.value
          "TIME(#{print_timeunit(type.unit)},#{type.is_adjusted_to_u_t_c})"
        when LogicalType::Date    then "DATE"
        when LogicalType::Bson    then "BSON"
        when LogicalType::Json    then "JSON"
        when LogicalType::String  then "STRING"
        when LogicalType::Uuid    then "UUID"
        when LogicalType::Enum    then "ENUM"
        when LogicalType::List    then "LIST"
        when LogicalType::Map     then "MAP"
        when LogicalType::Float16 then "FLOAT16"
        when LogicalType::Variant
          version = logical_type.value.specification_version
          "VARIANT(#{version.nil? ? "None" : "Some(#{version})"})"
        when LogicalType::Geometry
          crs = logical_type.value.crs
          crs ? "GEOMETRY(#{crs})" : "GEOMETRY"
        when LogicalType::Geography
          geography = logical_type.value
          algorithm = geography.algorithm_or_default
          if crs = geography.crs
            "GEOGRAPHY(#{algorithm}, #{crs})"
          else
            "GEOGRAPHY(#{algorithm})"
          end
        when LogicalType::File
          "FILE"
        when LogicalType::Unknown
          "UNKNOWN"
        when LogicalType::UnknownField
          "_Unknown(#{logical_type.field_id})"
        else
          ""
        end
      else
        # Also print the converted type if no logical type is available.
        case converted_type
        when ConvertedType::NONE
          ""
        when ConvertedType::DECIMAL
          precision_scale =
            if precision > 0 && scale > 0
              "(#{precision},#{scale})"
            elsif precision > 0 && scale == 0
              "(#{precision})"
            else
              ""
            end
          "DECIMAL#{precision_scale}"
        else
          converted_type.to_s
        end
      end
    end

    # Prints a Parquet message type.
    class Printer
      def initialize(@output : IO)
        @indent = 0
      end

      def print_indent : Nil
        @indent.times { @output << ' ' }
      end

      def print(tp : Type) : Nil
        print_indent
        case tp
        when PrimitiveType
          basic_info = tp.basic_info
          physical_type = tp.physical_type
          phys_type_str =
            if physical_type == Parquet::Type::FIXED_LEN_BYTE_ARRAY
              "#{physical_type} (#{tp.type_length})"
            else
              physical_type.to_s
            end
          @output << basic_info.repetition << ' ' << phys_type_str << ' ' << basic_info.name
          @output << " [" << basic_info.id << ']' if basic_info.has_id?
          logical_type_str = Schema.print_logical_and_converted(
            basic_info.logical_type_ref, basic_info.converted_type, tp.precision, tp.scale
          )
          if logical_type_str.empty?
            @output << ';'
          else
            @output << " (" << logical_type_str << ");"
          end
        when GroupType
          basic_info = tp.basic_info
          if basic_info.has_repetition?
            @output << basic_info.repetition << " group " << basic_info.name << ' '
            @output << '[' << basic_info.id << "] " if basic_info.has_id?
            logical_str = Schema.print_logical_and_converted(
              basic_info.logical_type_ref, basic_info.converted_type, 0, 0
            )
            @output << '(' << logical_str << ") " unless logical_str.empty?
          else
            @output << "message " << basic_info.name << ' '
            @output << '[' << basic_info.id << "] " if basic_info.has_id?
          end
          @output << "{\n"

          @indent += INDENT_WIDTH
          tp.fields.each do |child|
            print(child)
            @output << '\n'
          end
          @indent -= INDENT_WIDTH
          print_indent
          @output << '}'
        end
      end
    end
  end
end
