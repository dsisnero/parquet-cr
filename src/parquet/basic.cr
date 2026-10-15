# Ported from `parquet/src/basic.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# This file currently ports the physical/logical *enum* definitions that mirror
# the Thrift IDL. `LogicalType`, `Compression`, `SortOrder`, `ColumnOrder`,
# `EncodingMask`, and the geospatial enums are ported with their features; see
# `plans/parity.md`.
module Parquet
  # Mirrors thrift enum `Type`.
  #
  # Types supported by Parquet. These physical types are intended to be used in
  # combination with the encodings to control the on-disk storage format.
  enum Type : Int32
    BOOLEAN              = 0
    INT32                = 1
    INT64                = 2
    INT96                = 3 # deprecated, only used by legacy implementations
    FLOAT                = 4
    DOUBLE               = 5
    BYTE_ARRAY           = 6
    FIXED_LEN_BYTE_ARRAY = 7

    def self.from_str(str : String) : Type
      return BYTE_ARRAY if str == "BINARY"

      parse?(str) || raise general_err!("Invalid parquet type #{str}")
    end

    def self.from_str?(str : String) : Type?
      from_str(str)
    rescue Parquet::ParquetError
      nil
    end
  end
  define_thrift_enum(Type)

  # Mirrors thrift enum `ConvertedType`.
  #
  # Common types used by frameworks when using Parquet. This is only metadata
  # and not needed to read or write the data. Renamed from `LogicalType` in
  # version 4.0.0.
  enum ConvertedType : Int32
    NONE             = -1
    UTF8             =  0
    MAP              =  1
    MAP_KEY_VALUE    =  2
    LIST             =  3
    ENUM             =  4
    DECIMAL          =  5
    DATE             =  6
    TIME_MILLIS      =  7
    TIME_MICROS      =  8
    TIMESTAMP_MILLIS =  9
    TIMESTAMP_MICROS = 10
    UINT_8           = 11
    UINT_16          = 12
    UINT_32          = 13
    UINT_64          = 14
    INT_8            = 15
    INT_16           = 16
    INT_32           = 17
    INT_64           = 18
    JSON             = 19
    BSON             = 20
    INTERVAL         = 21

    def self.from_str(str : String) : ConvertedType
      parse?(str) || raise general_err!("Invalid parquet converted type #{str}")
    end

    def self.from_str?(str : String) : ConvertedType?
      from_str(str)
    rescue Parquet::ParquetError
      nil
    end
  end
  define_thrift_enum(ConvertedType)

  # Mirrors thrift union `TimeUnit`. All variants are empty structs.
  enum TimeUnit : Int32
    MILLIS = 1
    MICROS = 2
    NANOS  = 3
  end
  define_thrift_union_all_empty(TimeUnit)

  # Mirrors thrift enum `FieldRepetitionType`.
  #
  # Representation of field types in schema.
  enum FieldRepetitionType : Int32
    # This field is required (can not be null) and each row has exactly 1 value.
    REQUIRED = 0
    # The field is optional (can be null) and each row has 0 or 1 values.
    OPTIONAL = 1
    # The field is repeated and can contain 0 or more values.
    REPEATED = 2

    def self.from_str(str : String) : FieldRepetitionType
      parse?(str) || raise general_err!("Invalid parquet repetition #{str}")
    end

    def self.from_str?(str : String) : FieldRepetitionType?
      from_str(str)
    rescue Parquet::ParquetError
      nil
    end
  end
  define_thrift_enum(FieldRepetitionType)

  # Type alias for thrift `FieldRepetitionType`.
  alias Repetition = FieldRepetitionType

  # Mirrors thrift enum `Encoding`.
  #
  # Encodings supported by Parquet. Not all encodings are valid for all types.
  enum Encoding : Int32
    PLAIN                   =  0
    PLAIN_DICTIONARY        =  2
    RLE                     =  3
    BIT_PACKED              =  4
    DELTA_BINARY_PACKED     =  5
    DELTA_LENGTH_BYTE_ARRAY =  6
    DELTA_BYTE_ARRAY        =  7
    RLE_DICTIONARY          =  8
    BYTE_STREAM_SPLIT       =  9
    ALP                     = 10

    # Upstream accepts the exact uppercase name or its all-lowercase form.
    def self.from_str(str : String) : Encoding
      parse?(str) || parse?(str.upcase) || raise general_err!("unknown encoding: #{str}")
    end

    def self.from_str?(str : String) : Encoding?
      from_str(str)
    rescue Parquet::ParquetError
      nil
    end
  end
  define_thrift_enum(Encoding)

  # Mirrors thrift enum `CompressionCodec`.
  #
  # Supported compression algorithms.
  enum CompressionCodec : Int32
    UNCOMPRESSED = 0
    SNAPPY       = 1
    GZIP         = 2
    LZO          = 3
    BROTLI       = 4
    LZ4          = 5
    ZSTD         = 6
    LZ4_RAW      = 7
  end
  define_thrift_enum(CompressionCodec)

  # Mirrors thrift enum `PageType`.
  #
  # Available data pages for the Parquet file format.
  enum PageType : Int32
    DATA_PAGE       = 0
    INDEX_PAGE      = 1
    DICTIONARY_PAGE = 2
    DATA_PAGE_V2    = 3
  end
  define_thrift_enum(PageType)

  # Mirrors thrift enum `BoundaryOrder`.
  #
  # Whether lists of min/max elements inside ColumnIndex are ordered and, if so,
  # in which direction.
  enum BoundaryOrder : Int32
    UNORDERED  = 0
    ASCENDING  = 1
    DESCENDING = 2
  end
  define_thrift_enum(BoundaryOrder)
end
