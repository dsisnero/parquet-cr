require "./parquet/errors"
require "./parquet/thrift"
require "./parquet/macros"
require "./parquet/basic"
require "./parquet/data_type"
require "./parquet/format"
require "./parquet/schema/types"
require "./parquet/schema/parser"
require "./parquet/schema/thrift"
require "./parquet/file/statistics"
require "./parquet/file/metadata"
require "./parquet/file/metadata/column_chunk"
require "./parquet/file/metadata/row_group"
require "./parquet/file/metadata/parquet_metadata"

module Parquet
  VERSION = "0.1.0"
end
