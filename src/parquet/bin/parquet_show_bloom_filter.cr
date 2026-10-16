require "../errors"
require "../basic"
require "../bloom_filter"
require "../file/chunk_reader"
require "../file/reader"
require "../file/serialized_reader"
require "../file/properties"
require "./cli"

# Ported from `parquet/src/bin/parquet-show-bloom-filter.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Reads bloom filter data from a Parquet file and checks membership.
# Divergences: `clap` parsing is in `bin/parquet-show-bloom-filter.cr`; `run`
# writes to an `IO` and an invalid column path raises `GeneralError` (upstream
# prints and exits); parse-error messages embed Crystal's exception message.
module Parquet
  module Bin
    # `parquet-show-bloom-filter` tool logic.
    module ParquetShowBloomFilter
      # Checks `values` against the bloom filter for `column` in `file_name`.
      def self.run(io : IO, file_name : String, column : String, values : Array(String)) : Nil
        props = File::Properties::ReaderProperties.builder.set_read_bloom_filter(true).build
        options = File::ReadOptionsBuilder.new.with_reader_properties(props).build
        reader = File::SerializedFileReader.new_with_options(File::FileChunkReader.new(file_name), options)
        metadata = reader.metadata
        column_path = Cli.parse_column_path(column)

        metadata.row_groups.each_with_index do |row_group, rg_idx|
          io << "Row group #" << rg_idx << '\n'
          io << "=" * 80 << '\n'

          column_index = row_group.columns.index { |col| col.column_path.parts == column_path }
          if column_index.nil?
            candidates = row_group.columns.map(&.column_path.string).join(", ")
            io << "No column named " << column << " found, candidate columns are: " << candidates << '\n'
            next
          end

          column_meta = row_group.columns[column_index]
          sbbf = reader.get_row_group(rg_idx).get_column_bloom_filter(column_index)
          if sbbf.nil?
            io << "No bloom filter found for column " << column << '\n'
            next
          end

          values.each do |value|
            case result = check_filter(sbbf, value, column_meta)
            when Bool
              io << "Value " << value << " is " << (result ? "present" : "absent") << " in bloom filter\n"
            when String
              io << result << '\n'
            end
          end
        end
      end

      # Checks a textual `value` against `sbbf` for `column`'s physical type.
      def self.check_filter(
        sbbf : BloomFilter::Sbbf,
        value : String,
        column : File::Metadata::ColumnChunkMetaData,
      ) : Bool | String
        case column.column_type
        when Parquet::Type::INT32
          begin
            sbbf.check(value.to_i32)
          rescue ex
            "Unable to parse value '#{value}' to i32: #{ex.message}"
          end
        when Parquet::Type::INT64
          begin
            sbbf.check(value.to_i64)
          rescue ex
            "Unable to parse value '#{value}' to i64: #{ex.message}"
          end
        when Parquet::Type::BYTE_ARRAY
          sbbf.check(value)
        else
          "Unsupported column type for checking bloom filter: #{column.column_type}"
        end
      end
    end
  end
end
