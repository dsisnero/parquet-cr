# Ported from `parquet/src/basic.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# This file currently ports the physical/logical *enum* definitions that mirror
# the Thrift IDL. `LogicalType`, `Compression`, `SortOrder`, `ColumnOrder`,
# `EncodingMask`, and the geospatial enums are ported with their features; see
# `plans/parity.md`.
require "./compression"

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

  # Mirrors thrift enum `EdgeInterpolationAlgorithm`.
  #
  # Hand-coded (not via `define_thrift_enum`) to carry an `_Unknown` value for
  # forward compatibility. Modelled as a value wrapping the i32 code.
  struct EdgeInterpolationAlgorithm
    getter value : Int32

    def initialize(@value : Int32)
    end

    SPHERICAL = new(0)
    VINCENTY  = new(1)
    THOMAS    = new(2)
    ANDOYER   = new(3)
    KARNEY    = new(4)

    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : self
      new(prot.read_i32)
    end

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::I32
    end

    def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      prot.write_i32(@value)
    end

    def self.from_str(str : String) : EdgeInterpolationAlgorithm
      case str.upcase
      when "SPHERICAL" then SPHERICAL
      when "VINCENTY"  then VINCENTY
      when "THOMAS"    then THOMAS
      when "ANDOYER"   then ANDOYER
      when "KARNEY"    then KARNEY
      else
        raise general_err!("Unknown edge interpolation algorithm: #{str.upcase}")
      end
    end

    def self.from_str?(str : String) : EdgeInterpolationAlgorithm?
      from_str(str)
    rescue Parquet::ParquetError
      nil
    end

    def to_s(io : IO) : Nil
      case @value
      when 0 then io << "SPHERICAL"
      when 1 then io << "VINCENTY"
      when 2 then io << "THOMAS"
      when 3 then io << "ANDOYER"
      when 4 then io << "KARNEY"
      else        io << "_Unknown(" << @value << ')'
      end
    end
  end

  define_thrift_struct(DecimalType) do
    required 1, scale, Int32
    required 2, precision, Int32
  end

  define_thrift_struct(TimestampType) do
    required 1, is_adjusted_to_u_t_c, Bool
    required 2, unit, TimeUnit
  end

  # Identical to `TimestampType`.
  alias TimeType = TimestampType

  define_thrift_struct(IntType) do
    required 1, bit_width, Int8
    required 2, is_signed, Bool
  end

  define_thrift_struct(VariantType) do
    optional 1, specification_version, Int8
  end

  define_thrift_struct(GeometryType) do
    optional 1, crs, String
  end

  define_thrift_struct(GeographyType) do
    optional 1, crs, String
    optional 2, algorithm, EdgeInterpolationAlgorithm
  end

  struct GeographyType
    # Accessor returning the algorithm, defaulting to `SPHERICAL` when unset.
    # (Upstream `GeographyType::algorithm`; renamed to avoid clashing with the
    # field getter.)
    def algorithm_or_default : EdgeInterpolationAlgorithm
      @algorithm || EdgeInterpolationAlgorithm::SPHERICAL
    end
  end

  # Rust-`Debug`-compatible formatting. Upstream `#[derive(Debug)]` output is
  # reproduced so that error messages embedding `{:?}` match upstream exactly.
  struct DecimalType
    def rust_debug : String
      "DecimalType { scale: #{scale}, precision: #{precision} }"
    end
  end

  struct TimestampType
    def rust_debug : String
      "TimestampType { is_adjusted_to_u_t_c: #{is_adjusted_to_u_t_c}, unit: #{unit} }"
    end
  end

  struct IntType
    def rust_debug : String
      "IntType { bit_width: #{bit_width}, is_signed: #{is_signed} }"
    end
  end

  struct VariantType
    def rust_debug : String
      "VariantType { specification_version: #{Basic.debug_option(specification_version)} }"
    end
  end

  struct GeometryType
    def rust_debug : String
      "GeometryType { crs: #{Basic.debug_option(crs)} }"
    end
  end

  struct GeographyType
    def rust_debug : String
      "GeographyType { crs: #{Basic.debug_option(crs)}, algorithm: #{Basic.debug_option(algorithm)} }"
    end
  end

  struct EdgeInterpolationAlgorithm
    def rust_debug : String
      case @value
      when 0 then "SPHERICAL"
      when 1 then "VINCENTY"
      when 2 then "THOMAS"
      when 3 then "ANDOYER"
      when 4 then "KARNEY"
      else        "_Unknown(#{@value})"
      end
    end
  end

  # Helpers mirroring Rust `Option<T>` / `String` `Debug` formatting.
  module Basic
    def self.debug_option(value : Nil) : String
      "None"
    end

    def self.debug_option(value : T) : String forall T
      "Some(#{debug_value(value)})"
    end

    def self.debug_value(value : String) : String
      value.inspect
    end

    def self.debug_value(value : T) : String forall T
      if value.responds_to?(:rust_debug)
        value.rust_debug
      else
        value.to_s
      end
    end
  end

  # Mirrors thrift union `LogicalType` with an unknown-variant fallback.
  #
  # Logical types used by version 2.4.0+ of the Parquet format. Modelled as a
  # sealed class hierarchy (Crystal has no enums with payloads).
  abstract class LogicalType
    def ==(other : LogicalType) : Bool
      other.class == self.class
    end

    def hash(hasher)
      self.class.hash(hasher)
    end

    # Unit variants.
    class Map < LogicalType; end

    class List < LogicalType; end

    class Enum < LogicalType; end

    class Date < LogicalType; end

    class Json < LogicalType; end

    class Bson < LogicalType; end

    class Uuid < LogicalType; end

    class Float16 < LogicalType; end

    class Unknown < LogicalType; end

    class File < LogicalType; end

    class String < LogicalType; end

    # Payload variants.
    class Decimal < LogicalType
      getter value : DecimalType

      def initialize(@value : DecimalType)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(Decimal) && other.value == value
      end

      def hash(hasher)
        value.hash(hasher)
      end
    end

    class Time < LogicalType
      getter value : TimestampType

      def initialize(@value : TimestampType)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(Time) && other.value == value
      end

      def hash(hasher)
        value.hash(hasher)
      end
    end

    class Timestamp < LogicalType
      getter value : TimestampType

      def initialize(@value : TimestampType)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(Timestamp) && other.value == value
      end

      def hash(hasher)
        value.hash(hasher)
      end
    end

    class Integer < LogicalType
      getter value : IntType

      def initialize(@value : IntType)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(Integer) && other.value == value
      end

      def hash(hasher)
        value.hash(hasher)
      end
    end

    class Variant < LogicalType
      getter value : VariantType

      def initialize(@value : VariantType)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(Variant) && other.value == value
      end

      def hash(hasher)
        value.hash(hasher)
      end
    end

    class Geometry < LogicalType
      getter value : GeometryType

      def initialize(@value : GeometryType)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(Geometry) && other.value == value
      end

      def hash(hasher)
        value.hash(hasher)
      end
    end

    class Geography < LogicalType
      getter value : GeographyType

      def initialize(@value : GeographyType)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(Geography) && other.value == value
      end

      def hash(hasher)
        value.hash(hasher)
      end
    end

    # Unknown variant, retaining the field id encountered on the wire.
    class UnknownField < LogicalType
      getter field_id : Int16

      def initialize(@field_id : Int16)
      end

      def ==(other : LogicalType) : Bool
        other.is_a?(UnknownField) && other.field_id == field_id
      end

      def hash(hasher)
        field_id.hash(hasher)
      end
    end

    # Rust-`Debug`-compatible formatting (see `#[derive(Debug)]` upstream).
    # ameba:disable Metrics/CyclomaticComplexity
    def rust_debug : ::String
      v = self
      case v
      when String       then "String"
      when Map          then "Map"
      when List         then "List"
      when Enum         then "Enum"
      when Decimal      then "Decimal(#{v.value.rust_debug})"
      when Date         then "Date"
      when Time         then "Time(#{v.value.rust_debug})"
      when Timestamp    then "Timestamp(#{v.value.rust_debug})"
      when Integer      then "Integer(#{v.value.rust_debug})"
      when Unknown      then "Unknown"
      when Json         then "Json"
      when Bson         then "Bson"
      when Uuid         then "Uuid"
      when Float16      then "Float16"
      when Variant      then "Variant(#{v.value.rust_debug})"
      when Geometry     then "Geometry(#{v.value.rust_debug})"
      when Geography    then "Geography(#{v.value.rust_debug})"
      when File         then "File"
      when UnknownField then "_Unknown { field_id: #{v.field_id} }"
      else                   "Unknown"
      end
    end

    def self.integer(bit_width : Int, is_signed : Bool) : LogicalType
      Integer.new(IntType.new(bit_width.to_i8, is_signed))
    end

    def self.decimal(scale : Int32, precision : Int32) : LogicalType
      Decimal.new(DecimalType.new(scale, precision))
    end

    def self.time(is_adjusted_to_u_t_c : Bool, unit : TimeUnit) : LogicalType
      Time.new(TimestampType.new(is_adjusted_to_u_t_c, unit))
    end

    def self.timestamp(is_adjusted_to_u_t_c : Bool, unit : TimeUnit) : LogicalType
      Timestamp.new(TimestampType.new(is_adjusted_to_u_t_c, unit))
    end

    def self.variant(specification_version : Int?) : LogicalType
      Variant.new(VariantType.new(specification_version.try(&.to_i8)))
    end

    def self.geometry(crs : ::String?) : LogicalType
      Geometry.new(GeometryType.new(crs))
    end

    def self.geography(crs : ::String?, algorithm : EdgeInterpolationAlgorithm?) : LogicalType
      Geography.new(GeographyType.new(crs, algorithm))
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def self.from_str(str : ::String) : LogicalType
      case str
      when "INTEGER"   then integer(8, false)
      when "MAP"       then Map.new
      when "LIST"      then List.new
      when "ENUM"      then Enum.new
      when "DECIMAL"   then decimal(-1, -1)
      when "DATE"      then Date.new
      when "TIME"      then time(false, TimeUnit::MILLIS)
      when "TIMESTAMP" then timestamp(false, TimeUnit::MILLIS)
      when "STRING"    then String.new
      when "JSON"      then Json.new
      when "BSON"      then Bson.new
      when "UUID"      then Uuid.new
      when "UNKNOWN"   then Unknown.new
      when "INTERVAL"  then raise general_err!("Interval parquet logical type not yet supported")
      when "FLOAT16"   then Float16.new
      when "VARIANT"   then variant(nil)
      when "FILE"      then File.new
      when "GEOMETRY"  then geometry(nil)
      when "GEOGRAPHY" then geography(nil, EdgeInterpolationAlgorithm::SPHERICAL)
      else
        raise general_err!("Invalid parquet logical type #{str}")
      end
    end

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::Struct
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : LogicalType
      field_ident = prot.read_field_begin(0_i16)
      if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        raise general_err!("Received empty union from remote LogicalType")
      end

      ret =
        case field_ident.id
        when 1
          prot.skip_empty_struct
          String.new
        when 2
          prot.skip_empty_struct
          Map.new
        when 3
          prot.skip_empty_struct
          List.new
        when 4
          prot.skip_empty_struct
          Enum.new
        when 5 then Decimal.new(DecimalType.read_thrift(prot))
        when 6
          prot.skip_empty_struct
          Date.new
        when  7 then Time.new(TimestampType.read_thrift(prot))
        when  8 then Timestamp.new(TimestampType.read_thrift(prot))
        when 10 then Integer.new(IntType.read_thrift(prot))
        when 11
          prot.skip_empty_struct
          Unknown.new
        when 12
          prot.skip_empty_struct
          Json.new
        when 13
          prot.skip_empty_struct
          Bson.new
        when 14
          prot.skip_empty_struct
          Uuid.new
        when 15
          prot.skip_empty_struct
          Float16.new
        when 16 then Variant.new(VariantType.read_thrift(prot))
        when 17 then Geometry.new(GeometryType.read_thrift(prot))
        when 18 then Geography.new(GeographyType.read_thrift(prot))
        when 19
          prot.skip_empty_struct
          File.new
        else
          prot.skip(field_ident.field_type)
          UnknownField.new(field_ident.id)
        end

      end_ident = prot.read_field_begin(field_ident.id)
      unless end_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        raise general_err!("Received multiple fields for union from remote LogicalType")
      end

      ret
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      variant = self
      case variant
      when String    then prot.write_empty_struct(1_i16, 0_i16)
      when Map       then prot.write_empty_struct(2_i16, 0_i16)
      when List      then prot.write_empty_struct(3_i16, 0_i16)
      when Enum      then prot.write_empty_struct(4_i16, 0_i16)
      when Decimal   then ::Parquet::Thrift.write_thrift_field(prot, variant.value, 5_i16, 0_i16)
      when Date      then prot.write_empty_struct(6_i16, 0_i16)
      when Time      then ::Parquet::Thrift.write_thrift_field(prot, variant.value, 7_i16, 0_i16)
      when Timestamp then ::Parquet::Thrift.write_thrift_field(prot, variant.value, 8_i16, 0_i16)
      when Integer   then ::Parquet::Thrift.write_thrift_field(prot, variant.value, 10_i16, 0_i16)
      when Unknown   then prot.write_empty_struct(11_i16, 0_i16)
      when Json      then prot.write_empty_struct(12_i16, 0_i16)
      when Bson      then prot.write_empty_struct(13_i16, 0_i16)
      when Uuid      then prot.write_empty_struct(14_i16, 0_i16)
      when Float16   then prot.write_empty_struct(15_i16, 0_i16)
      when Variant   then ::Parquet::Thrift.write_thrift_field(prot, variant.value, 16_i16, 0_i16)
      when Geometry  then ::Parquet::Thrift.write_thrift_field(prot, variant.value, 17_i16, 0_i16)
      when Geography then ::Parquet::Thrift.write_thrift_field(prot, variant.value, 18_i16, 0_i16)
      when File      then prot.write_empty_struct(19_i16, 0_i16)
      when UnknownField
        raise general_err!("Trying to write unknown variant")
      end
      prot.write_struct_end
    end
  end

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

  # A bitmask representing the `Encoding`s employed by a column chunk.
  struct EncodingMask
    getter value : Int32

    # Highest valued discriminant in the `Encoding` enum.
    MAX_ENCODING = Encoding::ALP.value

    # Unused bit positions (including the never-used GROUP_VAR_INT value of 1).
    ALLOWED_MASK = (~((1_u32 << (MAX_ENCODING.to_u32 + 1_u32)) - 1_u32)) | (1_u32 << 1)

    def initialize(@value : Int32)
    end

    # Creates a new `EncodingMask`, erroring if a disallowed bit is set.
    def self.try_new(val : Int32) : EncodingMask
      raw = val.unsafe_as(UInt32)
      if (raw & ALLOWED_MASK) != 0
        raise general_err!("Attempt to create invalid mask: 0x#{"%x" % raw}")
      end

      new(val)
    end

    # Returns an integer representation of this mask.
    def as_i32 : Int32
      @value
    end

    # Creates a mask from a collection of `Encoding`s.
    def self.new_from_encodings(encodings : Enumerable(Encoding)) : EncodingMask
      mask = 0_i32
      encodings.each { |e| mask |= 1_i32 << e.value }
      new(mask)
    end

    # Marks the given `Encoding` as present.
    def insert(val : Encoding) : Nil
      @value |= 1_i32 << val.value
    end

    # Tests whether the given `Encoding` is present.
    def set?(val : Encoding) : Bool
      (@value & (1_i32 << val.value)) != 0
    end

    # Tests whether this mask has only the bit for the given `Encoding` set.
    def only?(val : Encoding) : Bool
      @value == (1_i32 << val.value)
    end

    # Tests whether all `Encoding`s in a set are present.
    def all_set(encodings : Enumerable(Encoding)) : Bool
      encodings.all? { |e| set?(e) }
    end

    # Returns the `Encoding`s present in this mask, ascending by value.
    def encodings : Array(Encoding)
      result = [] of Encoding
      (0..MAX_ENCODING).each do |i|
        if (@value & (1_i32 << i)) != 0
          encoding = Encoding.from_value?(i)
          raise general_err!("Impossible encoding #{i}") if encoding.nil?
          result << encoding
        end
      end
      result
    end

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::I32
    end

    # Reads a thrift `list<Encoding>` into a bitmask.
    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : EncodingMask
      mask = 0_i32
      list_ident = prot.read_list_begin
      ::Parquet::Thrift.validate_list_type(::Parquet::Thrift::ElementType::I32, list_ident)
      list_ident.size.times do
        val = Encoding.read_thrift(prot)
        mask |= 1_i32 << val.value
      end
      new(mask)
    end
  end

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

  # Supported block compression algorithms.
  #
  # Divergence: Rust's `Compression` enum with payload variants
  # (`GZIP(GzipLevel)`/`BROTLI(BrotliLevel)`/`ZSTD(ZstdLevel)`) is a Crystal
  # struct carrying a `Kind` plus an optional level (`Int32?`); the unit
  # variants are constants and the payload variants are `Compression.gzip`/
  # `.brotli`/`.zstd` (validating the level). `Display` reproduces the Rust
  # `#[derive(Debug)]` form, e.g. `GZIP(GzipLevel(6))`.
  struct Compression
    # Compression algorithm discriminant.
    enum Kind
      UNCOMPRESSED
      SNAPPY
      GZIP
      LZO
      BROTLI
      LZ4
      ZSTD
      LZ4_RAW
    end

    getter kind : Kind
    getter level : Int32?

    def initialize(@kind : Kind, @level : Int32? = nil)
    end

    UNCOMPRESSED = new(Kind::UNCOMPRESSED)
    SNAPPY       = new(Kind::SNAPPY)
    LZO          = new(Kind::LZO)
    LZ4          = new(Kind::LZ4)
    LZ4_RAW      = new(Kind::LZ4_RAW)

    def self.default : Compression
      UNCOMPRESSED
    end

    # GZIP compression at `level` (defaults to 6).
    def self.gzip(level : Int32 = GzipLevel.default.compression_level) : Compression
      GzipLevel.try_new(level)
      new(Kind::GZIP, level)
    end

    # BROTLI compression at `level` (defaults to 1).
    def self.brotli(level : Int32 = BrotliLevel.default.compression_level) : Compression
      BrotliLevel.try_new(level)
      new(Kind::BROTLI, level)
    end

    # ZSTD compression at `level` (defaults to 1).
    def self.zstd(level : Int32 = ZstdLevel.default.compression_level) : Compression
      ZstdLevel.try_new(level)
      new(Kind::ZSTD, level)
    end

    def self.from_codec(codec : CompressionCodec) : Compression
      case codec
      when CompressionCodec::UNCOMPRESSED then UNCOMPRESSED
      when CompressionCodec::SNAPPY       then SNAPPY
      when CompressionCodec::GZIP         then gzip
      when CompressionCodec::LZO          then LZO
      when CompressionCodec::BROTLI       then brotli
      when CompressionCodec::LZ4          then LZ4
      when CompressionCodec::ZSTD         then zstd
      when CompressionCodec::LZ4_RAW      then LZ4_RAW
      else
        raise general_err!("unknown compression codec: #{codec}")
      end
    end

    def codec : CompressionCodec
      case @kind
      when Kind::UNCOMPRESSED then CompressionCodec::UNCOMPRESSED
      when Kind::SNAPPY       then CompressionCodec::SNAPPY
      when Kind::GZIP         then CompressionCodec::GZIP
      when Kind::LZO          then CompressionCodec::LZO
      when Kind::BROTLI       then CompressionCodec::BROTLI
      when Kind::LZ4          then CompressionCodec::LZ4
      when Kind::ZSTD         then CompressionCodec::ZSTD
      else                         CompressionCodec::LZ4_RAW
      end
    end

    # Parses `str`, e.g. `"SNAPPY"`, `"gzip(9)"`, `"zstd(-3)"`.
    def self.from_str(str : String) : Compression
      codec, level = split_compression_string(str)
      case codec
      when "UNCOMPRESSED", "uncompressed"
        check_level_is_none(level)
        UNCOMPRESSED
      when "SNAPPY", "snappy"
        check_level_is_none(level)
        SNAPPY
      when "GZIP", "gzip"
        gzip(require_level(codec, level))
      when "LZO", "lzo"
        check_level_is_none(level)
        LZO
      when "BROTLI", "brotli"
        brotli(require_level(codec, level))
      when "LZ4", "lz4"
        check_level_is_none(level)
        LZ4
      when "ZSTD", "zstd"
        zstd(require_level(codec, level))
      when "LZ4_RAW", "lz4_raw"
        check_level_is_none(level)
        LZ4_RAW
      else
        raise general_err!("unsupported compression #{codec}")
      end
    end

    def self.from_str?(str : String) : Compression?
      from_str(str)
    rescue Parquet::ParquetError
      nil
    end

    def to_s(io : IO) : Nil
      case @kind
      when Kind::GZIP   then io << "GZIP(GzipLevel(" << @level << "))"
      when Kind::BROTLI then io << "BROTLI(BrotliLevel(" << @level << "))"
      when Kind::ZSTD   then io << "ZSTD(ZstdLevel(" << @level << "))"
      else                   io << @kind
      end
    end

    def_equals @kind, @level
    def_hash @kind, @level

    private def self.split_compression_string(str : String) : Tuple(String, Int32?)
      if idx = str.index('(')
        codec = str[0, idx]
        level_str = str[idx + 1, str.size - idx - 1]
        parsed = level_str.size > 0 ? level_str[0, level_str.size - 1] : ""
        level = parsed.to_i32? || raise general_err!("invalid compression level: #{level_str}")
        {codec, level}
      else
        {str, nil}
      end
    end

    private def self.check_level_is_none(level : Int32?) : Nil
      raise general_err!("compression level is not supported") unless level.nil?
    end

    private def self.require_level(codec : String, level : Int32?) : Int32
      level || raise general_err!("#{codec} requires a compression level")
    end
  end

  # `Compression` -> `CompressionCodec` conversion (upstream `From<Compression>
  # for CompressionCodec`).
  enum CompressionCodec
    def self.from(compression : Compression) : CompressionCodec
      compression.codec
    end
  end

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

  # Sort order used to aggregate min/max statistics for a column.
  enum SortOrder
    SIGNED
    UNSIGNED
    UNDEFINED
    TOTAL_ORDER
    INT96_TIMESTAMP

    def signed? : Bool
      self == SIGNED
    end
  end

  # Column order that specifies what method was used to aggregate min/max values
  # for statistics.
  #
  # Divergence: upstream is an enum with a `TYPE_DEFINED_ORDER(SortOrder)`
  # payload and unit variants. Modelled as a struct carrying a `Kind` plus the
  # inner `SortOrder`; unit variants are exposed as constants.
  struct ColumnOrder
    enum Kind
      TypeDefinedOrder
      Ieee754TotalOrder
      Int96TimestampOrder
      Undefined
      Unknown
    end

    getter kind : Kind
    getter order : SortOrder

    def initialize(@kind : Kind, @order : SortOrder = SortOrder::UNDEFINED)
    end

    def self.type_defined_order(order : SortOrder) : ColumnOrder
      new(Kind::TypeDefinedOrder, order)
    end

    IEEE_754_TOTAL_ORDER  = new(Kind::Ieee754TotalOrder)
    INT96_TIMESTAMP_ORDER = new(Kind::Int96TimestampOrder)
    UNDEFINED             = new(Kind::Undefined)
    UNKNOWN               = new(Kind::Unknown)

    # Returns the `ColumnOrder` for a physical/logical type.
    def self.column_order_for_type(
      logical_type : LogicalType?,
      converted_type : ConvertedType,
      physical_type : Type,
    ) : ColumnOrder
      if logical_type.is_a?(LogicalType::Float16) ||
         physical_type == Type::FLOAT || physical_type == Type::DOUBLE
        IEEE_754_TOTAL_ORDER
      elsif physical_type == Type::INT96
        INT96_TIMESTAMP_ORDER
      else
        type_defined_order(
          get_sort_order_for_type(logical_type, converted_type, physical_type, true)
        )
      end
    end

    # Returns the sort order for a physical/logical type.
    def self.get_sort_order_for_type(
      logical_type : LogicalType?,
      converted_type : ConvertedType,
      physical_type : Type,
      is_type_defined : Bool,
    ) : SortOrder
      if variant = logical_type
        case variant
        when LogicalType::String, LogicalType::Enum, LogicalType::Json, LogicalType::Bson
          SortOrder::UNSIGNED
        when LogicalType::Integer
          variant.value.is_signed ? SortOrder::SIGNED : SortOrder::UNSIGNED
        when LogicalType::Map, LogicalType::List
          SortOrder::UNDEFINED
        when LogicalType::Decimal, LogicalType::Date, LogicalType::Time, LogicalType::Timestamp
          SortOrder::SIGNED
        when LogicalType::Unknown
          SortOrder::UNDEFINED
        when LogicalType::Uuid
          SortOrder::UNSIGNED
        when LogicalType::Float16
          is_type_defined ? SortOrder::SIGNED : SortOrder::TOTAL_ORDER
        else
          SortOrder::UNDEFINED
        end
      else
        get_converted_sort_order(converted_type, physical_type, is_type_defined)
      end
    end

    def self.get_converted_sort_order(
      converted_type : ConvertedType,
      physical_type : Type,
      is_type_defined : Bool,
    ) : SortOrder
      case converted_type
      when ConvertedType::UTF8, ConvertedType::JSON, ConvertedType::BSON, ConvertedType::ENUM
        SortOrder::UNSIGNED
      when ConvertedType::INT_8, ConvertedType::INT_16, ConvertedType::INT_32, ConvertedType::INT_64
        SortOrder::SIGNED
      when ConvertedType::UINT_8, ConvertedType::UINT_16, ConvertedType::UINT_32, ConvertedType::UINT_64
        SortOrder::UNSIGNED
      when ConvertedType::DECIMAL,
           ConvertedType::DATE,
           ConvertedType::TIME_MILLIS,
           ConvertedType::TIME_MICROS,
           ConvertedType::TIMESTAMP_MILLIS,
           ConvertedType::TIMESTAMP_MICROS
        SortOrder::SIGNED
      when ConvertedType::INTERVAL,
           ConvertedType::LIST,
           ConvertedType::MAP,
           ConvertedType::MAP_KEY_VALUE
        SortOrder::UNDEFINED
      else
        get_default_sort_order(physical_type, is_type_defined)
      end
    end

    # Returns the default sort order based on physical type.
    def self.get_default_sort_order(physical_type : Type, is_type_defined : Bool) : SortOrder
      case physical_type
      when Type::BOOLEAN
        SortOrder::UNSIGNED
      when Type::INT32, Type::INT64
        SortOrder::SIGNED
      when Type::INT96
        is_type_defined ? SortOrder::UNDEFINED : SortOrder::INT96_TIMESTAMP
      when Type::FLOAT, Type::DOUBLE
        is_type_defined ? SortOrder::SIGNED : SortOrder::TOTAL_ORDER
      else
        SortOrder::UNSIGNED
      end
    end

    # Returns the sort order associated with this column order.
    def sort_order : SortOrder
      case @kind
      when Kind::TypeDefinedOrder    then @order
      when Kind::Ieee754TotalOrder   then SortOrder::TOTAL_ORDER
      when Kind::Int96TimestampOrder then SortOrder::INT96_TIMESTAMP
      when Kind::Undefined           then SortOrder::SIGNED
      else                                SortOrder::UNDEFINED
      end
    end

    def self.thrift_element_type : ::Parquet::Thrift::ElementType
      ::Parquet::Thrift::ElementType::Struct
    end

    def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : ColumnOrder
      field_ident = prot.read_field_begin(0_i16)
      if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        raise general_err!("Received empty union from remote ColumnOrder")
      end

      ret =
        case field_ident.id
        when 1
          # NOTE: the sort order is corrected after parsing.
          prot.skip_empty_struct
          type_defined_order(SortOrder::SIGNED)
        when 2
          prot.skip_empty_struct
          IEEE_754_TOTAL_ORDER
        when 3
          prot.skip_empty_struct
          INT96_TIMESTAMP_ORDER
        else
          prot.skip(field_ident.field_type)
          UNKNOWN
        end

      end_ident = prot.read_field_begin(field_ident.id)
      unless end_ident.field_type == ::Parquet::Thrift::FieldType::Stop
        raise general_err!("Received multiple fields for union from remote ColumnOrder")
      end

      ret
    end

    def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
      case @kind
      when Kind::TypeDefinedOrder
        prot.write_field_begin(::Parquet::Thrift::FieldType::Struct, 1_i16, 0_i16)
        prot.write_struct_end
      when Kind::Ieee754TotalOrder
        prot.write_field_begin(::Parquet::Thrift::FieldType::Struct, 2_i16, 0_i16)
        prot.write_struct_end
      when Kind::Int96TimestampOrder
        prot.write_field_begin(::Parquet::Thrift::FieldType::Struct, 3_i16, 0_i16)
        prot.write_struct_end
      else
        raise general_err!("Attempt to write undefined ColumnOrder")
      end
      prot.write_struct_end
    end

    def to_s(io : IO) : Nil
      case @kind
      when Kind::TypeDefinedOrder    then io << "TYPE_DEFINED_ORDER(" << @order << ')'
      when Kind::Ieee754TotalOrder   then io << "IEEE_754_TOTAL_ORDER"
      when Kind::Int96TimestampOrder then io << "INT96_TIMESTAMP_ORDER"
      when Kind::Undefined           then io << "UNDEFINED"
      else                                io << "UNKNOWN"
      end
    end
  end

  # `From<Option<LogicalType>> for ConvertedType` (see `basic.rs`).
  #
  # The reverse conversion is intentionally not provided upstream to avoid type
  # loss (decimal scale/precision, nanosecond time/timestamp).
  enum ConvertedType
    # ameba:disable Metrics/CyclomaticComplexity
    def self.from(value : LogicalType?) : ConvertedType
      case value
      when Nil                  then NONE
      when LogicalType::String  then UTF8
      when LogicalType::Map     then MAP
      when LogicalType::List    then LIST
      when LogicalType::Enum    then ENUM
      when LogicalType::Decimal then DECIMAL
      when LogicalType::Date    then DATE
      when LogicalType::Time
        case value.value.unit
        when TimeUnit::MILLIS then TIME_MILLIS
        when TimeUnit::MICROS then TIME_MICROS
        else                       NONE
        end
      when LogicalType::Timestamp
        case value.value.unit
        when TimeUnit::MILLIS then TIMESTAMP_MILLIS
        when TimeUnit::MICROS then TIMESTAMP_MICROS
        else                       NONE
        end
      when LogicalType::Integer
        int_type = value.value
        case {int_type.bit_width, int_type.is_signed}
        when {8_i8, true}   then INT_8
        when {16_i8, true}  then INT_16
        when {32_i8, true}  then INT_32
        when {64_i8, true}  then INT_64
        when {8_i8, false}  then UINT_8
        when {16_i8, false} then UINT_16
        when {32_i8, false} then UINT_32
        when {64_i8, false} then UINT_64
        else
          raise general_err!(
            "Integer type bit_width=#{int_type.bit_width}, signed=#{int_type.is_signed} is not supported"
          )
        end
      when LogicalType::Json then JSON
      when LogicalType::Bson then BSON
      else                        NONE
      end
    end
  end
end
