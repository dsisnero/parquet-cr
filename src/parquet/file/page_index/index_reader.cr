require "./column_index"

# Ported from `parquet/src/file/page_index/index_reader.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `decode_column_index`, `decode_offset_index`, and `acc_range`.
# `decode_offset_index` omits the hand-coded `try_from_fast` path (the macro
# `read_thrift` decodes the same wire form) — the same divergence already
# documented for `OffsetIndexMetaData`.
module Parquet
  module File
    module PageIndex
      # Computes the covering range of two optional ranges.
      #
      # For example `acc_range(7...9, 1...3) == 1...9`.
      def self.acc_range(a : Range(UInt64, UInt64)?, b : Range(UInt64, UInt64)?) : Range(UInt64, UInt64)?
        if a && b
          Math.min(a.begin, b.begin)...Math.max(a.end, b.end)
        else
          a || b
        end
      end

      # Decodes a Thrift `OffsetIndex` from the provided bytes.
      def self.decode_offset_index(data : Bytes) : OffsetIndexMetaData
        prot = ::Parquet::Thrift::ThriftSliceInputProtocol.new(data)
        OffsetIndexMetaData.read_thrift(prot)
      end

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
