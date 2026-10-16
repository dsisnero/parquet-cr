require "../errors"
require "../utils"
require "../decoder"

# Ported from `parquet-variant/src/variant/metadata.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `VariantMetadataHeader` and `VariantMetadata` (the field-name dictionary).
# Divergences: `&[u8]`/`&str` are `Bytes`/`String`; the zero-copy
# `borrowed_field_id` fast path cannot be modelled (Crystal `String.new(Bytes)`
# copies) so it always returns `nil`, and `get_entry` always uses the name
# search; `get` raises instead of returning a `Result`; `iter_try` is `iter`.
module ParquetVariant
  # The metadata header version (always 1).
  CORRECT_VERSION_VALUE = 1_u8
  # The metadata header is one byte.
  NUM_HEADER_BYTES = 1_u32

  # Header structure for `VariantMetadata`.
  struct VariantMetadataHeader
    getter version : UInt8
    getter is_sorted : Bool
    getter offset_size : OffsetSizeBytes

    def initialize(@version : UInt8, @is_sorted : Bool, @offset_size : OffsetSizeBytes)
    end

    # Tries to construct the header from `header_byte`.
    def self.try_new(header_byte : UInt8) : VariantMetadataHeader
      version = header_byte & 0x0F
      unless version == CORRECT_VERSION_VALUE
        raise InvalidArgumentError.new(
          "The version bytes in the header is not #{CORRECT_VERSION_VALUE}, got #{version.to_s(2)}"
        )
      end
      is_sorted = (header_byte & 0x10) != 0
      offset_size_minus_one = header_byte >> 6
      new(version, is_sorted, OffsetSizeBytes.try_new(offset_size_minus_one))
    end

    def offset_size_u32 : UInt32
      @offset_size.value.to_u32
    end

    def first_offset_byte : UInt32
      NUM_HEADER_BYTES + offset_size_u32
    end
  end

  # The `Variant` field-name metadata dictionary.
  class VariantMetadata
    property bytes : Bytes
    getter header : VariantMetadataHeader
    getter dictionary_size : UInt32
    getter first_value_byte : UInt32
    getter validated : Bool

    def initialize(
      @bytes : Bytes,
      @header : VariantMetadataHeader,
      @dictionary_size : UInt32,
      @first_value_byte : UInt32,
      @validated : Bool,
    )
    end

    # Interprets `bytes` as a fully-validated metadata dictionary.
    def self.try_new(bytes : Bytes) : VariantMetadata
      try_new_with_shallow_validation(bytes).with_full_validation
    end

    # Interprets `bytes` as a metadata dictionary with only basic validation.
    def self.new(bytes : Bytes) : VariantMetadata
      try_new_with_shallow_validation(bytes)
    end

    # Performs basic (constant-cost) validation.
    def self.try_new_with_shallow_validation(bytes : Bytes) : VariantMetadata
      header_byte = Utils.first_byte_from_slice(bytes)
      header = VariantMetadataHeader.try_new(header_byte)

      dictionary_size =
        header.offset_size.unpack_u32_at_offset(bytes, NUM_HEADER_BYTES.to_i32, 0)
      first_value_byte = (dictionary_size + 1) * header.offset_size_u32 + header.first_offset_byte

      metadata = new(bytes, header, dictionary_size, first_value_byte, false)

      first_offset = metadata.get_offset(0)
      unless first_offset == 0
        raise InvalidArgumentError.new("First offset is not zero: #{first_offset}")
      end

      last_offset = metadata.get_offset(dictionary_size.to_i32) + first_value_byte
      metadata.bytes = Utils.slice_from_slice(bytes, 0...last_offset.to_i32)
      metadata
    end

    # The number of metadata dictionary entries.
    def len : Int32
      @dictionary_size.to_i32
    end

    # Whether the dictionary has no entries.
    def empty? : Bool
      len == 0
    end

    # Whether this instance is fully validated.
    def fully_validated? : Bool
      @validated
    end

    # Performs full validation and returns `self`.
    def with_full_validation : VariantMetadata
      return self if @validated

      offset_bytes =
        Utils.slice_from_slice(@bytes, @header.first_offset_byte.to_i32...@first_value_byte.to_i32)
      value_buffer = Utils.string_from_slice(@bytes, 0, @first_value_byte.to_i32...@bytes.size)
      offsets = Decoder.map_bytes_to_offsets(offset_bytes, @header.offset_size).to_a
      current_offset = offsets.shift? || 0

      if @header.is_sorted
        prev_value = nil.as(String?)
        offsets.each do |next_offset|
          current_value = slice_value(value_buffer, current_offset, next_offset)
          if (prev = prev_value) && current_value <= prev
            raise InvalidArgumentError.new("dictionary values are not unique and ordered")
          end
          prev_value = current_value
          current_offset = next_offset
        end
      else
        offsets.each do |next_offset|
          slice_value(value_buffer, current_offset, next_offset)
          current_offset = next_offset
        end
      end

      @validated = true
      self
    end

    # Whether the dictionary keys are sorted and unique.
    def sorted? : Bool
      @header.is_sorted
    end

    # The variant protocol version.
    def version : UInt8
      @header.version
    end

    # The total size, in bytes, of the metadata.
    def size : Int32
      @bytes.size
    end

    # Returns the offset (into the value region) at index `i`.
    def get_offset(i : Int32) : UInt32
      offset_bytes =
        Utils.slice_from_slice(@bytes, @header.first_offset_byte.to_i32...@first_value_byte.to_i32)
      @header.offset_size.unpack_u32(offset_bytes, i)
    end

    # Returns dictionary entry `i`, raising if out of bounds or invalid.
    def get(i : Int32) : String
      start = get_offset(i)
      finish = get_offset(i + 1)
      if @validated
        value_bytes = Utils.slice_from_slice_at_offset(
          @bytes, @first_value_byte.to_i32, start.to_i32...finish.to_i32
        )
        String.new(value_bytes)
      else
        Utils.string_from_slice(@bytes, @first_value_byte.to_i32, start.to_i32...finish.to_i32)
      end
    end

    # The borrowed fast path is disabled; see the module comment.
    def borrowed_field_id(field_name : String) : UInt32?
      _ = field_name
      nil
    end

    # Attempts to resolve `field_name` to `{field_id, name}`, or `nil`.
    def get_entry(field_name : String) : Tuple(UInt32, String)?
      field_id =
        if sorted? && len > 10
          cmp = ->(i : Int32) { get_impl(i) <=> field_name }
          result = Utils.try_binary_search_range_by(0...len, cmp)
          return nil if result.nil?

          found, index = result
          return nil unless found

          index
        else
          (0...len).find { |i| get_impl(i) == field_name }
        end
      return nil if field_id.nil?

      {field_id.to_u32, get_impl(field_id)}
    end

    # Iterates over all dictionary entries, raising on invalid data.
    def iter : Iterator(String)
      (0...len).each.map { |i| get_impl(i) }
    end

    # Fallible iteration; `get` raises rather than returning a `Result`, so this
    # is the same as `iter`.
    def iter_try : Iterator(String)
      iter
    end

    def [](i : Int32) : String
      get_impl(i)
    end

    def ==(other : VariantMetadata) : Bool
      @bytes == other.bytes &&
        @header.version == other.header.version &&
        @header.is_sorted == other.header.is_sorted &&
        @header.offset_size == other.header.offset_size &&
        @dictionary_size == other.dictionary_size &&
        @first_value_byte == other.first_value_byte &&
        @validated == other.validated
    end

    private def get_impl(i : Int32) : String
      get(i)
    end

    # Slices the value region, validating bounds and UTF-8 character boundaries
    # (Crystal `byte_slice?` does not validate UTF-8).
    private def slice_value(value_buffer : String, from : Int32, to : Int32) : String
      if from < 0 || to > value_buffer.bytesize || to < from
        raise InvalidArgumentError.new("range #{from}..#{to} is invalid or out of bounds")
      end
      slice = String.new(value_buffer.to_slice[from, to - from])
      unless slice.valid_encoding?
        raise InvalidArgumentError.new("range #{from}..#{to} is invalid or out of bounds")
      end
      slice
    end
  end

  # The canonical byte slice corresponding to an empty metadata dictionary.
  EMPTY_VARIANT_METADATA_BYTES = Bytes[1_u8, 0_u8, 0_u8]

  # The empty metadata dictionary.
  EMPTY_VARIANT_METADATA = VariantMetadata.new(
    EMPTY_VARIANT_METADATA_BYTES,
    VariantMetadataHeader.new(CORRECT_VERSION_VALUE, false, OffsetSizeBytes::One),
    0_u32,
    3_u32,
    true,
  )
end
