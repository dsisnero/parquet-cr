require "../errors"
require "../utils"
require "../decoder"
require "./metadata"
require "./decimal"

# Ported from `parquet-variant/src/variant/object.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `VariantObjectHeader` and `VariantObject` (the named-field object
# subtype). Divergences: `&[u8]` is `Bytes`; `Result`/`Option` map to
# raises/`nil`; `usize` is `Int32`; `Drop`/`Clone`/`Deref` have no Crystal
# analogue; `iter`/`iter_try` return eager `Array`s; the `PartialEq`
# implementation compares field name/value pairs recursively (order-independent,
# matching upstream).
module ParquetVariant
  # Header structure for `VariantObject`.
  struct VariantObjectHeader
    getter num_elements_size : OffsetSizeBytes
    getter field_id_size : OffsetSizeBytes
    getter field_offset_size : OffsetSizeBytes

    def initialize(
      @num_elements_size : OffsetSizeBytes,
      @field_id_size : OffsetSizeBytes,
      @field_offset_size : OffsetSizeBytes,
    )
    end

    # Width in bytes of the `num_elements` field (1 or 4).
    def num_elements_size_bytes : Int32
      @num_elements_size.value
    end

    # Width in bytes of each field id (1–4).
    def field_id_size_bytes : Int32
      @field_id_size.value
    end

    # Width in bytes of each field offset (1–4).
    def field_offset_size_bytes : Int32
      @field_offset_size.value
    end

    # Byte offset at which the field id array starts.
    def field_ids_start_byte : Int32
      NUM_HEADER_BYTES.to_i32 + num_elements_size_bytes
    end

    # Parses the object header byte.
    def self.try_new(header_byte : UInt8) : VariantObjectHeader
      value_header = header_byte >> 2
      field_offset_size_minus_one = value_header & 0x03
      field_id_size_minus_one = (value_header >> 2) & 0x03
      is_large = (value_header & 0x10) != 0
      num_elements_size =
        is_large ? OffsetSizeBytes::Four : OffsetSizeBytes::One
      new(
        num_elements_size,
        OffsetSizeBytes.try_new(field_id_size_minus_one),
        OffsetSizeBytes.try_new(field_offset_size_minus_one),
      )
    end
  end

  # A `Variant` object (a struct with named fields).
  #
  # See `VariantObject#Validation` (upstream) for the validation model.
  class VariantObject
    getter metadata : VariantMetadata
    property value : Bytes
    getter header : VariantObjectHeader
    getter num_elements : UInt32
    getter first_field_offset_byte : Int32
    getter first_value_byte : Int32

    def initialize(
      @metadata : VariantMetadata,
      @value : Bytes,
      @header : VariantObjectHeader,
      @num_elements : UInt32,
      @first_field_offset_byte : Int32,
      @first_value_byte : Int32,
      @validated : Bool,
    )
    end

    # Interprets `metadata`/`value` as a variant object, validating fully.
    def self.try_new(metadata : VariantMetadata, value : Bytes) : VariantObject
      try_new_with_shallow_validation(metadata, value).with_full_validation
    end

    # Interprets `metadata`/`value` as a variant object, performing only basic
    # (constant-cost) validation.
    def self.new(metadata : VariantMetadata, value : Bytes) : VariantObject
      try_new_with_shallow_validation(metadata, value)
    end

    # Performs basic (constant-cost) validation.
    def self.try_new_with_shallow_validation(
      metadata : VariantMetadata,
      value : Bytes,
    ) : VariantObject
      header_byte = Utils.first_byte_from_slice(value)
      header = VariantObjectHeader.try_new(header_byte)

      num_elements =
        header.num_elements_size.unpack_u32_at_offset(value, NUM_HEADER_BYTES.to_i32, 0).to_i64

      first_field_offset_byte = checked_i32(
        num_elements * header.field_id_size_bytes + header.field_ids_start_byte,
        "offset of variant object field offsets",
      )
      first_value_byte = checked_i32(
        (num_elements + 1) * header.field_offset_size_bytes + first_field_offset_byte,
        "offset of variant object field values",
      )

      obj = new(
        metadata, value, header, num_elements.to_u32,
        first_field_offset_byte, first_value_byte, false
      )

      # The last field_offset points to the byte after the end of the last value.
      last_offset = checked_i32(
        obj.get_offset(num_elements.to_i32).to_i64 + first_value_byte,
        "variant object size",
      )
      obj.value = Utils.slice_from_slice(value, 0...last_offset)
      obj
    end

    # Whether this instance is fully validated for panic-free infallible access.
    def is_fully_validated : Bool
      @validated
    end

    def fully_validated? : Bool
      @validated
    end

    # Performs a full validation of this variant object and returns it.
    def with_full_validation : VariantObject
      with_full_validation_at_depth(0)
    end

    # Same as `with_full_validation`, tracking the recursion depth.
    def with_full_validation_at_depth(depth : Int32) : VariantObject
      return self if @validated

      if depth >= MAX_NESTING_DEPTH
        raise InvalidArgumentError.new(
          "Variant nesting depth exceeds the maximum of #{MAX_NESTING_DEPTH}"
        )
      end

      @metadata = @metadata.with_full_validation

      field_id_buffer = Utils.slice_from_slice(
        @value, @header.field_ids_start_byte...@first_field_offset_byte
      )
      field_ids = Decoder.map_bytes_to_offsets(field_id_buffer, @header.field_id_size).to_a

      validate_field_ids(field_ids)

      field_offset_buffer = Utils.slice_from_slice(
        @value, @first_field_offset_byte...@first_value_byte
      )
      num_offsets = field_offset_buffer.size // @header.field_offset_size_bytes
      value_buffer = Utils.slice_from_slice(@value, @first_value_byte...@value.size)

      offsets = Decoder.map_bytes_to_offsets(field_offset_buffer, @header.field_offset_size).to_a
      offsets.first(Math.max(num_offsets - 1, 0)).each do |offset|
        value_bytes = Utils.slice_from_slice(value_buffer, offset...value_buffer.size)
        Variant.try_new_with_metadata_at_depth(@metadata, value_bytes, depth + 1)
      end

      @validated = true
      self
    end

    # Returns the number of key/value pairs in this object.
    def len : Int32
      @num_elements.to_i32
    end

    # Whether the object contains no key/value pairs.
    def empty? : Bool
      len == 0
    end

    # Gets a field's value by index in `0...len`, or `nil`.
    def field(i : Int32) : Variant?
      return nil unless i < len

      try_field_with_shallow_validation(i)
    end

    # Fallible version of `field` that performs full validation.
    def try_field(i : Int32) : Variant
      try_field_with_shallow_validation(i).with_full_validation
    end

    # Retrieves the `i`th field value, performing only basic validation.
    def try_field_with_shallow_validation(i : Int32) : Variant
      value_bytes = Utils.slice_from_slice(@value, @first_value_byte...@value.size)
      offset = get_offset(i)
      value_bytes = Utils.slice_from_slice(value_bytes, offset.to_i32...value_bytes.size)
      Variant.try_new_with_metadata_and_shallow_validation(@metadata, value_bytes)
    end

    # Gets a field's name by index in `0...len`, or `nil`.
    def field_name(i : Int32) : ::String?
      return nil unless i < len

      try_field_name(i)
    end

    # Retrieves the `i`th field name.
    def try_field_name(i : Int32) : ::String
      field_id_bytes = Utils.slice_from_slice(
        @value, @header.field_ids_start_byte...@first_field_offset_byte
      )
      field_id = @header.field_id_size.unpack_u32(field_id_bytes, i)
      @metadata.get(field_id.to_i32)
    end

    # Returns `{name, value}` pairs over the fields of this object.
    def iter : Array(Tuple(::String, Variant))
      (0...len).map do |i|
        {try_field_name(i), try_field_with_shallow_validation(i)}
      end
    end

    # Fallible iteration; errors are raised, so this matches `iter`.
    def iter_try : Array(Tuple(::String, Variant))
      iter
    end

    # Returns the value of the field named `name`, if any.
    def get(name : ::String) : Variant?
      cmp = ->(i : Int32) {
        field_name_i = field_name(i)
        field_name_i.nil? ? nil : (field_name_i <=> name)
      }
      result = Utils.try_binary_search_range_by(0...len, cmp)
      return nil if result.nil?

      found, index = result
      return nil unless found

      field(index)
    end

    def ==(other : VariantObject) : Bool
      return false unless @num_elements == other.num_elements

      iter.zip(other.iter).all? do |(name_a, value_a), (name_b, value_b)|
        name_a == name_b && value_a == value_b
      end
    end

    protected def get_offset(i : Int32) : UInt32
      field_offsets = Utils.slice_from_slice(
        @value, @first_field_offset_byte...@first_value_byte
      )
      @header.field_offset_size.unpack_u32(field_offsets, i)
    end

    private def validate_field_ids(field_ids : Array(Int32)) : Nil
      return if field_ids.empty?

      if @metadata.sorted?
        # A sorted dictionary with strictly ordered ids guarantees the
        # corresponding field names are lexicographically sorted and unique.
        dictionary_size = @metadata.len
        current_id = field_ids[0]
        field_ids[1..].each do |next_id|
          if current_id >= dictionary_size
            raise InvalidArgumentError.new("field id is not valid")
          end
          raise InvalidArgumentError.new("field names not sorted") if next_id <= current_id

          current_id = next_id
        end
        if current_id >= dictionary_size
          raise InvalidArgumentError.new("field id is not valid")
        end
      else
        # The dictionary cannot guarantee uniqueness/sortedness, so parse the
        # field names (which also verifies the ids are in bounds).
        current_field_name = @metadata.get(field_ids[0])
        field_ids[1..].each do |field_id|
          next_field_name = @metadata.get(field_id)
          raise InvalidArgumentError.new("field names not sorted") if next_field_name <= current_field_name

          current_field_name = next_field_name
        end
      end
    end

    private def self.checked_i32(value : Int64, msg : ::String) : Int32
      if value < 0 || value > Int32::MAX
        raise Utils.overflow_error(msg)
      end
      value.to_i32
    end
  end
end
