require "uuid"
require "./errors"
require "./utils"
require "./short_string"
require "./decoder"
require "./path"
require "./variant/metadata"
require "./variant/decimal"
require "./variant/object"
require "./variant/list"

# Ported from `parquet-variant/src/variant.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `MAX_NESTING_DEPTH` and the `Variant` value type (an enum with payloads
# upstream) as a sealed class hierarchy. Divergences: Rust's `From` impls are
# named `Variant.from_*`/`Variant.from` factory methods because Crystal has no
# distinct date/time-of-day types (`NaiveDate`/`DateTime<Utc>`/`NaiveDateTime`
# all map to `Time`, `NaiveTime` to `Time::Span`); `Option<()>` maps to `Bool`;
# `Result`/`Option` map to raises/`nil`; `&[u8]` is `Bytes`; the `half::f16`
# `From` impl is unavailable. Nested subtype type-names shadow Crystal built-ins
# inside this class body, so built-ins are written with a leading `::`.
module ParquetVariant
  # The maximum number of nested objects and arrays a `Variant` may contain.
  MAX_NESTING_DEPTH = 128

  # Represents a [Parquet Variant](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
  # value.
  #
  # The lifetime parameters of the upstream type are elided: `Bytes`/`String`
  # values are owned in Crystal.
  abstract class Variant
    # Attempts to interpret a metadata and value buffer pair as a new `Variant`,
    # fully validating the result.
    def self.try_new(metadata : Bytes, value : Bytes) : Variant
      metadata = VariantMetadata.try_new(metadata)
      try_new_with_metadata(metadata, value)
    end

    # Interprets a metadata and value buffer pair as a new `Variant`, performing
    # only basic (constant-cost) validation.
    def self.new(metadata : Bytes, value : Bytes) : Variant
      metadata = VariantMetadata.try_new_with_shallow_validation(metadata)
      try_new_with_metadata_and_shallow_validation(metadata, value)
    end

    # Creates a new variant with existing `metadata`, fully validating it.
    def self.try_new_with_metadata(metadata : VariantMetadata, value : Bytes) : Variant
      try_new_with_metadata_at_depth(metadata, value, 0)
    end

    # Same as `try_new_with_metadata`, tracking the recursion depth.
    def self.try_new_with_metadata_at_depth(
      metadata : VariantMetadata,
      value : Bytes,
      depth : ::Int32,
    ) : Variant
      try_new_with_metadata_and_shallow_validation(metadata, value)
        .with_full_validation_at_depth(depth)
    end

    # Creates a new variant with existing `metadata`, performing only basic
    # (constant-cost) validation.
    def self.new_with_metadata(metadata : VariantMetadata, value : Bytes) : Variant
      try_new_with_metadata_and_shallow_validation(metadata, value)
    end

    # The actual constructor, which only performs shallow validation.
    def self.try_new_with_metadata_and_shallow_validation(
      metadata : VariantMetadata,
      value : Bytes,
    ) : Variant
      value_metadata = Utils.first_byte_from_slice(value)
      value_data = Utils.slice_from_slice(value, 1...value.size)
      case Decoder.get_basic_type(value_metadata)
      when VariantBasicType::Primitive
        primitive = Decoder.get_primitive_type(value_metadata)
        case primitive
        when VariantPrimitiveType::Null  then Null.new
        when VariantPrimitiveType::Int8  then Int8.new(Decoder.decode_int8(value_data))
        when VariantPrimitiveType::Int16 then Int16.new(Decoder.decode_int16(value_data))
        when VariantPrimitiveType::Int32 then Int32.new(Decoder.decode_int32(value_data))
        when VariantPrimitiveType::Int64 then Int64.new(Decoder.decode_int64(value_data))
        when VariantPrimitiveType::Decimal4
          integer, scale = Decoder.decode_decimal4(value_data)
          Decimal4.new(VariantDecimal4.try_new(integer, scale))
        when VariantPrimitiveType::Decimal8
          integer, scale = Decoder.decode_decimal8(value_data)
          Decimal8.new(VariantDecimal8.try_new(integer, scale))
        when VariantPrimitiveType::Decimal16
          integer, scale = Decoder.decode_decimal16(value_data)
          Decimal16.new(VariantDecimal16.try_new(integer, scale))
        when VariantPrimitiveType::Float        then Float.new(Decoder.decode_float(value_data))
        when VariantPrimitiveType::Double       then Double.new(Decoder.decode_double(value_data))
        when VariantPrimitiveType::BooleanTrue  then BooleanTrue.new
        when VariantPrimitiveType::BooleanFalse then BooleanFalse.new
        when VariantPrimitiveType::Date         then Date.new(Decoder.decode_date(value_data))
        when VariantPrimitiveType::TimestampMicros
          TimestampMicros.new(Decoder.decode_timestamp_micros(value_data))
        when VariantPrimitiveType::TimestampNtzMicros
          TimestampNtzMicros.new(Decoder.decode_timestampntz_micros(value_data))
        when VariantPrimitiveType::TimestampNanos
          TimestampNanos.new(Decoder.decode_timestamp_nanos(value_data))
        when VariantPrimitiveType::TimestampNtzNanos
          TimestampNtzNanos.new(Decoder.decode_timestampntz_nanos(value_data))
        when VariantPrimitiveType::Uuid   then Uuid.new(Decoder.decode_uuid(value_data))
        when VariantPrimitiveType::Binary then Binary.new(Decoder.decode_binary(value_data))
        when VariantPrimitiveType::String then String.new(Decoder.decode_long_string(value_data))
        when VariantPrimitiveType::Time   then Time.new(Decoder.decode_time_ntz(value_data))
        else
          raise InvalidArgumentError.new("unknown primitive type")
        end
      when VariantBasicType::ShortString
        ShortString.new(Decoder.decode_short_string(value_metadata, value_data))
      when VariantBasicType::Object
        Object.new(VariantObject.try_new_with_shallow_validation(metadata, value))
      when VariantBasicType::Array
        List.new(VariantList.try_new_with_shallow_validation(metadata, value))
      else
        raise InvalidArgumentError.new("unknown basic type")
      end
    end

    # True if this variant instance has already been fully validated.
    def is_fully_validated : ::Bool
      true
    end

    def fully_validated? : ::Bool
      is_fully_validated
    end

    # Recursively validates this variant value and returns it.
    def with_full_validation : Variant
      with_full_validation_at_depth(0)
    end

    # Same as `with_full_validation`, tracking the recursion depth.
    def with_full_validation_at_depth(depth : ::Int32) : Variant
      self
    end

    # Converts this variant to `true` if it is null, `false` otherwise.
    def as_null : ::Bool
      self.is_a?(Null)
    end

    # Converts this variant to a `Bool` if possible.
    def as_boolean : ::Bool?
      v = self
      case v
      when BooleanTrue  then true
      when BooleanFalse then false
      else                   nil
      end
    end

    # Converts this variant to a date `Time` if possible.
    def as_naive_date : ::Time?
      v = self
      case v
      when Date then v.value
      else           nil
      end
    end

    # Converts this variant to a `Time` if possible (micros variants and
    # microsecond-aligned nanos variants).
    def as_timestamp_micros : ::Time?
      v = self
      case v
      when TimestampMicros then v.value
      when TimestampNanos  then v.value.nanosecond % 1000 == 0 ? v.value : nil
      else                      nil
      end
    end

    # Converts this variant to a naive (no timezone) `Time` if possible.
    def as_timestamp_ntz_micros : ::Time?
      v = self
      case v
      when TimestampNtzMicros then v.value
      when TimestampNtzNanos  then v.value.nanosecond % 1000 == 0 ? v.value : nil
      else                         nil
      end
    end

    # Converts this variant to a `Time` if possible.
    def as_timestamp_nanos : ::Time?
      v = self
      case v
      when TimestampNanos, TimestampMicros then v.value
      else                                      nil
      end
    end

    # Converts this variant to a naive (no timezone) `Time` if possible.
    def as_timestamp_ntz_nanos : ::Time?
      v = self
      case v
      when TimestampNtzNanos, TimestampNtzMicros then v.value
      else                                            nil
      end
    end

    # Converts this variant to a `Bytes` if possible.
    def as_u8_slice : Bytes?
      v = self
      case v
      when Binary then v.value
      else             nil
      end
    end

    # Converts this variant to a `String` if possible (regular and short).
    def as_string : ::String?
      v = self
      case v
      when String      then v.value
      when ShortString then v.value.value
      else                  nil
      end
    end

    # Converts this variant to a `UUID` if possible.
    def as_uuid : UUID?
      v = self
      case v
      when Uuid then v.value
      else           nil
      end
    end

    # Converts this variant to a `Time::Span` if possible.
    def as_time_utc : ::Time::Span?
      v = self
      case v
      when Time then v.value
      else           nil
      end
    end

    # Converts this variant to an `Int8` if possible.
    def as_int8 : ::Int8?
      v = self
      case v
      when Int8      then v.value
      when Int16     then fit_i8(v.value)
      when Int32     then fit_i8(v.value)
      when Int64     then fit_i8(v.value)
      when Decimal4  then (i = v.value.as_integer; i ? fit_i8(i) : nil)
      when Decimal8  then (i = v.value.as_integer; i ? fit_i8(i) : nil)
      when Decimal16 then (i = v.value.as_integer; i ? fit_i8(i) : nil)
      else                nil
      end
    end

    # Converts this variant to an `Int16` if possible.
    def as_int16 : ::Int16?
      v = self
      case v
      when Int8      then v.value.to_i16
      when Int16     then v.value
      when Int32     then fit_i16(v.value)
      when Int64     then fit_i16(v.value)
      when Decimal4  then (i = v.value.as_integer; i ? fit_i16(i) : nil)
      when Decimal8  then (i = v.value.as_integer; i ? fit_i16(i) : nil)
      when Decimal16 then (i = v.value.as_integer; i ? fit_i16(i) : nil)
      else                nil
      end
    end

    # Converts this variant to an `Int32` if possible.
    def as_int32 : ::Int32?
      v = self
      case v
      when Int8      then v.value.to_i32
      when Int16     then v.value.to_i32
      when Int32     then v.value
      when Int64     then fit_i32(v.value)
      when Decimal4  then v.value.as_integer
      when Decimal8  then (i = v.value.as_integer; i ? fit_i32(i) : nil)
      when Decimal16 then (i = v.value.as_integer; i ? fit_i32(i) : nil)
      else                nil
      end
    end

    # Converts this variant to an `Int64` if possible.
    def as_int64 : ::Int64?
      v = self
      case v
      when Int8      then v.value.to_i64
      when Int16     then v.value.to_i64
      when Int32     then v.value.to_i64
      when Int64     then v.value
      when Decimal4  then (i = v.value.as_integer; i ? i.to_i64 : nil)
      when Decimal8  then v.value.as_integer
      when Decimal16 then (i = v.value.as_integer; i ? fit_i64(i) : nil)
      else                nil
      end
    end

    # Converts this variant to a `UInt8` if possible.
    def as_u8 : ::UInt8?
      v = self
      case v
      when Int8      then fit_u8(v.value)
      when Int16     then fit_u8(v.value)
      when Int32     then fit_u8(v.value)
      when Int64     then fit_u8(v.value)
      when Decimal4  then (i = v.value.as_integer; i ? fit_u8(i) : nil)
      when Decimal8  then (i = v.value.as_integer; i ? fit_u8(i) : nil)
      when Decimal16 then (i = v.value.as_integer; i ? fit_u8(i) : nil)
      else                nil
      end
    end

    # Converts this variant to a `UInt16` if possible.
    def as_u16 : ::UInt16?
      v = self
      case v
      when Int8      then fit_u16(v.value)
      when Int16     then fit_u16(v.value)
      when Int32     then fit_u16(v.value)
      when Int64     then fit_u16(v.value)
      when Decimal4  then (i = v.value.as_integer; i ? fit_u16(i) : nil)
      when Decimal8  then (i = v.value.as_integer; i ? fit_u16(i) : nil)
      when Decimal16 then (i = v.value.as_integer; i ? fit_u16(i) : nil)
      else                nil
      end
    end

    # Converts this variant to a `UInt32` if possible.
    def as_u32 : ::UInt32?
      v = self
      case v
      when Int8      then fit_u32(v.value)
      when Int16     then fit_u32(v.value)
      when Int32     then fit_u32(v.value)
      when Int64     then fit_u32(v.value)
      when Decimal4  then (i = v.value.as_integer; i ? fit_u32(i) : nil)
      when Decimal8  then (i = v.value.as_integer; i ? fit_u32(i) : nil)
      when Decimal16 then (i = v.value.as_integer; i ? fit_u32(i) : nil)
      else                nil
      end
    end

    # Converts this variant to a `UInt64` if possible.
    def as_u64 : ::UInt64?
      v = self
      case v
      when Int8      then fit_u64(v.value)
      when Int16     then fit_u64(v.value)
      when Int32     then fit_u64(v.value)
      when Int64     then fit_u64(v.value)
      when Decimal4  then (i = v.value.as_integer; i ? fit_u64(i) : nil)
      when Decimal8  then (i = v.value.as_integer; i ? fit_u64(i) : nil)
      when Decimal16 then (i = v.value.as_integer; i ? fit_u64(i) : nil)
      else                nil
      end
    end

    # Converts this variant to a `VariantDecimal4` if possible.
    def as_decimal4 : VariantDecimal4?
      v = self
      case v
      when Int8      then try_decimal4(v.value.to_i32, 0_u8)
      when Int16     then try_decimal4(v.value.to_i32, 0_u8)
      when Int32     then try_decimal4(v.value, 0_u8)
      when Int64     then (i = fit_i32(v.value); i ? try_decimal4(i, 0_u8) : nil)
      when Decimal4  then v.value
      when Decimal8  then v.value.to_decimal4
      when Decimal16 then v.value.to_decimal4
      else                nil
      end
    end

    # Converts this variant to a `VariantDecimal8` if possible.
    def as_decimal8 : VariantDecimal8?
      v = self
      case v
      when Int8      then try_decimal8(v.value.to_i64, 0_u8)
      when Int16     then try_decimal8(v.value.to_i64, 0_u8)
      when Int32     then try_decimal8(v.value.to_i64, 0_u8)
      when Int64     then try_decimal8(v.value, 0_u8)
      when Decimal4  then v.value.to_decimal8
      when Decimal8  then v.value
      when Decimal16 then v.value.to_decimal8
      else                nil
      end
    end

    # Converts this variant to a `VariantDecimal16` if possible.
    def as_decimal16 : VariantDecimal16?
      v = self
      case v
      when Int8      then try_decimal16(v.value.to_i128, 0_u8)
      when Int16     then try_decimal16(v.value.to_i128, 0_u8)
      when Int32     then try_decimal16(v.value.to_i128, 0_u8)
      when Int64     then try_decimal16(v.value.to_i128, 0_u8)
      when Decimal4  then v.value.to_decimal16
      when Decimal8  then v.value.to_decimal16
      when Decimal16 then v.value
      else                nil
      end
    end

    # Converts this variant to a `Float32` if possible.
    def as_f32 : ::Float32?
      v = self
      case v
      when Float then v.value
      else            nil
      end
    end

    # Converts this variant to a `Float64` if possible.
    def as_f64 : ::Float64?
      v = self
      case v
      when Double then v.value
      else             nil
      end
    end

    # Converts this variant to a `VariantObject` if possible.
    def as_object : VariantObject?
      v = self
      case v
      when Object then v.value
      else             nil
      end
    end

    # If this is an object and the requested field exists, retrieves its value.
    def get_object_field(field_name : ::String) : Variant?
      v = self
      case v
      when Object then v.value.get(field_name)
      else             nil
      end
    end

    # Converts this variant to a `VariantList` if possible.
    def as_list : VariantList?
      v = self
      case v
      when List then v.value
      else           nil
      end
    end

    # If this is a list and `index` is in bounds, retrieves its element.
    def get_list_element(index : ::Int32) : Variant?
      v = self
      case v
      when List then v.value.get(index)
      else           nil
      end
    end

    # Returns the metadata dictionary associated with this variant value.
    def metadata : VariantMetadata
      v = self
      case v
      when Object then v.value.metadata
      when List   then v.value.metadata
      else             EMPTY_VARIANT_METADATA
      end
    end

    # Returns a new variant obtained by following `path`, or `nil`.
    def get_path(path : VariantPath) : Variant?
      current : Variant? = self
      path.path.each do |element|
        node = current
        return nil if node.nil?

        if element.field?
          name = element.name
          return nil if name.nil?
          current = node.get_object_field(name)
        else
          index = element.index
          return nil if index.nil?
          current = node.get_list_element(index)
        end
        return nil if current.nil?
      end
      current
    end

    # Distinct subclass instances are unequal by default.
    def ==(other : Variant) : ::Bool
      other.class == self.class
    end

    def to_s(io : IO) : Nil
      io << self.class.name
    end

    # The variant protocol primitive (type_id=1): Null.
    class Null < Variant; end

    # Primitive (type_id=1): BOOLEAN (true).
    class BooleanTrue < Variant; end

    # Primitive (type_id=1): BOOLEAN (false).
    class BooleanFalse < Variant; end

    # Defines a primitive subtype holding a `value` of the given type.
    macro define_primitive(name, type)
      class {{name}} < Variant
        getter value : {{type}}

        def initialize(@value : {{type}})
        end

        def ==(other : Variant) : ::Bool
          other.is_a?({{name}}) && other.value == @value
        end

        def to_s(io : IO) : Nil
          io << {{name.stringify}} << '('
          io << @value
          io << ')'
        end
      end
    end

    define_primitive(Int8, ::Int8)
    define_primitive(Int16, ::Int16)
    define_primitive(Int32, ::Int32)
    define_primitive(Int64, ::Int64)
    define_primitive(Float, ::Float32)
    define_primitive(Double, ::Float64)
    define_primitive(Date, ::Time)
    define_primitive(TimestampMicros, ::Time)
    define_primitive(TimestampNtzMicros, ::Time)
    define_primitive(TimestampNanos, ::Time)
    define_primitive(TimestampNtzNanos, ::Time)
    define_primitive(Decimal4, VariantDecimal4)
    define_primitive(Decimal8, VariantDecimal8)
    define_primitive(Decimal16, VariantDecimal16)
    define_primitive(Binary, Bytes)
    define_primitive(String, ::String)
    define_primitive(ShortString, ParquetVariant::ShortString)
    define_primitive(Time, ::Time::Span)
    define_primitive(Uuid, UUID)

    # Object (type_id=3).
    class Object < Variant
      getter value : VariantObject

      def initialize(@value : VariantObject)
      end

      def is_fully_validated : ::Bool
        @value.is_fully_validated
      end

      def with_full_validation_at_depth(depth : ::Int32) : Variant
        @value.with_full_validation_at_depth(depth)
        self
      end

      def ==(other : Variant) : ::Bool
        other.is_a?(Object) && other.value == @value
      end

      def to_s(io : IO) : Nil
        io << '{'
        @value.iter.each_with_index do |(name, item), i|
          io << ", " if i > 0
          io << '"' << name << "\": " << item
        end
        io << '}'
      end
    end

    # Array (type_id=4).
    class List < Variant
      getter value : VariantList

      def initialize(@value : VariantList)
      end

      def is_fully_validated : ::Bool
        @value.is_fully_validated
      end

      def with_full_validation_at_depth(depth : ::Int32) : Variant
        @value.with_full_validation_at_depth(depth)
        self
      end

      def ==(other : Variant) : ::Bool
        other.is_a?(List) && other.value == @value
      end

      def to_s(io : IO) : Nil
        io << '['
        @value.iter.each_with_index do |item, i|
          io << ", " if i > 0
          io << item
        end
        io << ']'
      end
    end

    # --- Constructors from native Crystal values (upstream `From` impls) ---

    # Creates a null variant.
    def self.from_nil : Variant
      Null.new
    end

    def self.from(value : ::Bool) : Variant
      value ? BooleanTrue.new : BooleanFalse.new
    end

    def self.from(value : ::Int8) : Variant
      Int8.new(value)
    end

    def self.from(value : ::Int16) : Variant
      Int16.new(value)
    end

    def self.from(value : ::Int32) : Variant
      Int32.new(value)
    end

    def self.from(value : ::Int64) : Variant
      Int64.new(value)
    end

    def self.from(value : ::UInt8) : Variant
      value <= ::Int8::MAX ? Int8.new(value.to_i8) : Int16.new(value.to_i16)
    end

    def self.from(value : ::UInt16) : Variant
      value <= ::Int16::MAX ? Int16.new(value.to_i16) : Int32.new(value.to_i32)
    end

    def self.from(value : ::UInt32) : Variant
      value <= ::Int32::MAX ? Int32.new(value.to_i32) : Int64.new(value.to_i64)
    end

    def self.from(value : ::UInt64) : Variant
      if value <= ::Int64::MAX
        Int64.new(value.to_i64)
      else
        Decimal16.new(VariantDecimal16.try_new(value.to_i128, 0_u8))
      end
    end

    def self.from(value : ::Float32) : Variant
      Float.new(value)
    end

    def self.from(value : ::Float64) : Variant
      Double.new(value)
    end

    def self.from(value : ::String) : Variant
      if value.bytesize > MAX_SHORT_STRING_BYTES
        String.new(value)
      else
        ShortString.new(ParquetVariant::ShortString.new(value))
      end
    end

    def self.from(value : Bytes) : Variant
      Binary.new(value)
    end

    def self.from(value : UUID) : Variant
      Uuid.new(value)
    end

    def self.from(value : VariantDecimal4) : Variant
      Decimal4.new(value)
    end

    def self.from(value : VariantDecimal8) : Variant
      Decimal8.new(value)
    end

    def self.from(value : VariantDecimal16) : Variant
      Decimal16.new(value)
    end

    def self.from(value : VariantObject) : Variant
      Object.new(value)
    end

    def self.from(value : VariantList) : Variant
      List.new(value)
    end

    # Creates a date variant (upstream `From<NaiveDate>`).
    def self.from_date(value : ::Time) : Variant
      Date.new(value)
    end

    # Creates a UTC timestamp variant, choosing micros/nanos by nanosecond
    # alignment (upstream `From<DateTime<Utc>>`).
    def self.from_datetime_utc(value : ::Time) : Variant
      value.nanosecond % 1000 == 0 ? TimestampMicros.new(value) : TimestampNanos.new(value)
    end

    # Creates a naive timestamp variant, choosing micros/nanos by nanosecond
    # alignment (upstream `From<NaiveDateTime>`).
    def self.from_naive_datetime(value : ::Time) : Variant
      value.nanosecond % 1000 == 0 ? TimestampNtzMicros.new(value) : TimestampNtzNanos.new(value)
    end

    # Creates a time-of-day variant (upstream `From<NaiveTime>`).
    def self.from_time(value : ::Time::Span) : Variant
      Time.new(value)
    end

    private def try_decimal4(integer : ::Int32, scale : ::UInt8) : VariantDecimal4?
      VariantDecimal4.try_new(integer, scale)
    rescue InvalidArgumentError
      nil
    end

    private def try_decimal8(integer : ::Int64, scale : ::UInt8) : VariantDecimal8?
      VariantDecimal8.try_new(integer, scale)
    rescue InvalidArgumentError
      nil
    end

    private def try_decimal16(integer : ::Int128, scale : ::UInt8) : VariantDecimal16?
      VariantDecimal16.try_new(integer, scale)
    rescue InvalidArgumentError
      nil
    end

    private def fit_i8(value : ::Int) : ::Int8?
      return nil if value < -128 || value > 127
      value.to_i8
    end

    private def fit_i16(value : ::Int) : ::Int16?
      return nil if value < -32_768 || value > 32_767
      value.to_i16
    end

    private def fit_i32(value : ::Int) : ::Int32?
      return nil if value < ::Int32::MIN || value > ::Int32::MAX
      value.to_i32
    end

    private def fit_i64(value : ::Int) : ::Int64?
      return nil if value < ::Int64::MIN || value > ::Int64::MAX
      value.to_i64
    end

    private def fit_u8(value : ::Int) : ::UInt8?
      return nil if value < 0 || value > ::UInt8::MAX
      value.to_u8
    end

    private def fit_u16(value : ::Int) : ::UInt16?
      return nil if value < 0 || value > ::UInt16::MAX
      value.to_u16
    end

    private def fit_u32(value : ::Int) : ::UInt32?
      return nil if value < 0 || value > ::UInt32::MAX
      value.to_u32
    end

    private def fit_u64(value : ::Int) : ::UInt64?
      return nil if value < 0 || value > ::UInt64::MAX
      value.to_u64
    end
  end
end
