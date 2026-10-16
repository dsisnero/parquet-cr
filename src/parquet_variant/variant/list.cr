require "../errors"
require "../utils"
require "../decoder"
require "./metadata"

# Ported from `parquet-variant/src/variant/list.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `VariantListHeader` and `VariantList` (the array subtype). Divergences:
# `&[u8]` is `Bytes`; `Result`/`Option` map to raises/`nil`; `usize` is `Int32`;
# `Drop`/`Clone` have no Crystal analogue; `iter`/`iter_try` return eager
# `Array`s; the `PartialEq` implementation compares elements recursively.
module ParquetVariant
  # A parsed version of the variant array value header byte.
  struct VariantListHeader
    getter num_elements_size : OffsetSizeBytes
    getter offset_size : OffsetSizeBytes

    def initialize(
      @num_elements_size : OffsetSizeBytes,
      @offset_size : OffsetSizeBytes,
    )
    end

    # Width in bytes of the `num_elements` field (1 or 4).
    def num_elements_size_bytes : Int32
      @num_elements_size.value
    end

    # Width in bytes of each offset (1–4).
    def offset_size_bytes : Int32
      @offset_size.value
    end

    # Byte offset at which the offset array starts.
    def first_offset_byte : Int32
      NUM_HEADER_BYTES.to_i32 + num_elements_size_bytes
    end

    # Parses the array header byte.
    def self.try_new(header_byte : UInt8) : VariantListHeader
      value_header = header_byte >> 2
      is_large = (value_header & 0x04) != 0
      offset_size_minus_one = value_header & 0x03
      num_elements_size =
        is_large ? OffsetSizeBytes::Four : OffsetSizeBytes::One
      new(num_elements_size, OffsetSizeBytes.try_new(offset_size_minus_one))
    end
  end

  # A `Variant` array (a list).
  class VariantList
    getter metadata : VariantMetadata
    property value : Bytes
    getter header : VariantListHeader
    getter num_elements : UInt32
    getter first_value_byte : Int32

    def initialize(
      @metadata : VariantMetadata,
      @value : Bytes,
      @header : VariantListHeader,
      @num_elements : UInt32,
      @first_value_byte : Int32,
      @validated : Bool,
    )
    end

    # Interprets `metadata`/`value` as a variant array, validating fully.
    def self.try_new(metadata : VariantMetadata, value : Bytes) : VariantList
      try_new_with_shallow_validation(metadata, value).with_full_validation
    end

    # Interprets `metadata`/`value` as a variant array, performing only basic
    # (constant-cost) validation.
    def self.new(metadata : VariantMetadata, value : Bytes) : VariantList
      try_new_with_shallow_validation(metadata, value)
    end

    # Performs basic (constant-cost) validation.
    def self.try_new_with_shallow_validation(
      metadata : VariantMetadata,
      value : Bytes,
    ) : VariantList
      header_byte = Utils.first_byte_from_slice(value)
      header = VariantListHeader.try_new(header_byte)

      num_elements =
        header.num_elements_size.unpack_u32_at_offset(value, NUM_HEADER_BYTES.to_i32, 0).to_i64

      first_value_byte = checked_i32(
        (num_elements + 1) * header.offset_size_bytes + header.first_offset_byte,
        "offset of variant list values",
      )

      list = new(metadata, value, header, num_elements.to_u32, first_value_byte, false)

      first_offset = list.get_offset(0)
      unless first_offset == 0
        raise InvalidArgumentError.new("First offset is not zero: #{first_offset}")
      end

      last_offset = checked_i32(
        list.get_offset(num_elements.to_i32).to_i64 + first_value_byte,
        "variant array size",
      )
      list.value = Utils.slice_from_slice(value, 0...last_offset)
      list
    end

    # Whether this instance is fully validated for panic-free infallible access.
    def is_fully_validated : Bool
      @validated
    end

    def fully_validated? : Bool
      @validated
    end

    # Performs a full validation of this variant array and returns it.
    def with_full_validation : VariantList
      with_full_validation_at_depth(0)
    end

    # Same as `with_full_validation`, tracking the recursion depth.
    def with_full_validation_at_depth(depth : Int32) : VariantList
      return self if @validated

      if depth >= MAX_NESTING_DEPTH
        raise InvalidArgumentError.new(
          "Variant nesting depth exceeds the maximum of #{MAX_NESTING_DEPTH}"
        )
      end

      @metadata = @metadata.with_full_validation

      offset_buffer = Utils.slice_from_slice(
        @value, @header.first_offset_byte...@first_value_byte
      )
      value_buffer = Utils.slice_from_slice(@value, @first_value_byte...@value.size)

      offsets = Decoder.map_bytes_to_offsets(offset_buffer, @header.offset_size).to_a
      current_offset = offsets.shift? || 0

      offsets.each do |next_offset|
        value_bytes = Utils.slice_from_slice(value_buffer, current_offset...next_offset)
        Variant.try_new_with_metadata_at_depth(@metadata, value_bytes, depth + 1)
        current_offset = next_offset
      end

      @validated = true
      self
    end

    # Returns the length of this array.
    def len : Int32
      @num_elements.to_i32
    end

    # Whether this array has zero length.
    def empty? : Bool
      len == 0
    end

    # Returns the element by index in `0...len`, or `nil`.
    def get(index : Int32) : Variant?
      return nil unless index < len

      try_get_with_shallow_validation(index)
    end

    # Fallible version of `get` that performs full validation.
    def try_get(index : Int32) : Variant
      try_get_with_shallow_validation(index).with_full_validation
    end

    # Retrieves the `index`th element, performing only basic validation.
    def try_get_with_shallow_validation(index : Int32) : Variant
      byte_range = get_offset(index).to_i32...get_offset(index + 1).to_i32
      value_bytes = Utils.slice_from_slice_at_offset(@value, @first_value_byte, byte_range)
      Variant.try_new_with_metadata_and_shallow_validation(@metadata, value_bytes)
    end

    # Iterates over the values of this list.
    def iter : Array(Variant)
      (0...len).map { |i| try_get_with_shallow_validation(i) }
    end

    # Fallible iteration; errors are raised, so this matches `iter`.
    def iter_try : Array(Variant)
      iter
    end

    def ==(other : VariantList) : Bool
      return false unless @num_elements == other.num_elements

      iter.zip(other.iter).all? { |a, b| a == b }
    end

    protected def get_offset(index : Int32) : UInt32
      offset_bytes = Utils.slice_from_slice(
        @value, @header.first_offset_byte...@first_value_byte
      )
      @header.offset_size.unpack_u32(offset_bytes, index)
    end

    private def self.checked_i32(value : Int64, msg : ::String) : Int32
      if value < 0 || value > Int32::MAX
        raise Utils.overflow_error(msg)
      end
      value.to_i32
    end
  end
end
