# Shared short aliases for Parquet types used across specs.
alias PLT = Parquet::LogicalType
alias PCT = Parquet::ConvertedType
alias PRep = Parquet::Repetition
alias PTU = Parquet::TimeUnit
alias PPhys = Parquet::Type
alias PEdgeAlgo = Parquet::EdgeInterpolationAlgorithm
alias PColumnOrder = Parquet::ColumnOrder
alias PSortOrder = Parquet::SortOrder
alias PConvertedType = Parquet::ConvertedType
alias PLogicalType = Parquet::LogicalType
alias PPhysicalType = Parquet::Type
alias PTimeUnit = Parquet::TimeUnit

alias ST = Parquet::Schema::Type
alias SPrimitiveType = Parquet::Schema::PrimitiveType
alias SColumnPath = Parquet::Schema::ColumnPath
alias SColumnDescriptor = Parquet::Schema::ColumnDescriptor
alias SSchemaDescriptor = Parquet::Schema::SchemaDescriptor
alias STokenizer = Parquet::Schema::Tokenizer

alias PMeta = Parquet::File::Metadata
alias PPI = Parquet::File::PageIndex

alias BitReader = Parquet::Util::BitReader
alias BitWriter = Parquet::Util::BitWriter
alias BitUtil = Parquet::Util::BitUtil

alias RleEncoder = Parquet::Encodings::RleEncoder
alias RleDecoder = Parquet::Encodings::RleDecoder
alias LevelEncoder = Parquet::Encodings::LevelEncoder
alias PlainEncoder = Parquet::Encodings::PlainEncoder
alias PlainDecoder = Parquet::Encodings::PlainDecoder
