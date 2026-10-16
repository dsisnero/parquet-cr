require "./column_index"

# Ported from `parquet/src/file/page_index/index_reader.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `decode_column_index`. `decode_offset_index` (and `acc_range`) are
# ported with the file reader/`PageIndex` machinery (see `plans/parity.md`).
module Parquet
  module File
    module PageIndex
      # Decode a Thrift `ColumnIndex` from the provided bytes.
      #
      # The passed-in bytes contain a serialized Thrift `ColumnIndex` struct as
      # read from a Parquet file. `column_type` selects the typed index.
      def self.decode_column_index(data : Bytes, column_type : Parquet::Type) : ColumnIndexMetaData
        prot = ::Parquet::Thrift::ThriftSliceInputProtocol.new(data)
        index = ThriftColumnIndex.read_thrift(prot)

        case column_type
        when Parquet::Type::BOOLEAN
          ColumnIndexMetaData.new(column_type, PrimitiveColumnIndex(Bool).try_from_thrift(index))
        when Parquet::Type::INT32
          ColumnIndexMetaData.new(column_type, PrimitiveColumnIndex(Int32).try_from_thrift(index))
        when Parquet::Type::INT64
          ColumnIndexMetaData.new(column_type, PrimitiveColumnIndex(Int64).try_from_thrift(index))
        when Parquet::Type::INT96
          ColumnIndexMetaData.new(column_type, PrimitiveColumnIndex(Int96).try_from_thrift(index))
        when Parquet::Type::FLOAT
          ColumnIndexMetaData.new(column_type, PrimitiveColumnIndex(Float32).try_from_thrift(index))
        when Parquet::Type::DOUBLE
          ColumnIndexMetaData.new(column_type, PrimitiveColumnIndex(Float64).try_from_thrift(index))
        when Parquet::Type::BYTE_ARRAY
          ColumnIndexMetaData.new(column_type, ByteArrayColumnIndex.try_from_thrift(index))
        when Parquet::Type::FIXED_LEN_BYTE_ARRAY
          ColumnIndexMetaData.new(column_type, ByteArrayColumnIndex.try_from_thrift(index))
        else
          raise general_err!("Unexpected column type #{column_type}")
        end
      end
    end
  end
end
