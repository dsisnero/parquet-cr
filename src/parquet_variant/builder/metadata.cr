require "../errors"
require "../decoder"
require "../variant/metadata"

# Ported from `parquet-variant/src/builder/metadata.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `MetadataBuilder`, `WritableMetadataBuilder`, and
# `ReadOnlyMetadataBuilder` (the variant metadata dictionary builders).
# Divergences: the `MetadataBuilder` trait is a marker module; `IndexSet` is an
# insertion-ordered `Array(String)` + index map; `Extend`/`FromIterator` are
# `#extend`/`.from_iter`; errors are raised; `ReadOnlyMetadataBuilder`'s
# `borrowed_field_id` fast path is disabled (returns `nil`).
module ParquetVariant
  # Common behaviour shared by the metadata dictionary builders.
  module MetadataBuilder
    # Returns the number of bytes needed to store `value` (1–4).
    def self.int_size(value : Int32) : OffsetSizeBytes
      case value
      when 0..0xFF           then OffsetSizeBytes::One
      when 0x100..0xFFFF     then OffsetSizeBytes::Two
      when 0x10000..0xFFFFFF then OffsetSizeBytes::Three
      else                        OffsetSizeBytes::Four
      end
    end

    # Appends `value` as `nbytes` little-endian bytes to `buffer`.
    def self.write_offset(buffer : Array(UInt8), value : Int32, nbytes : Int32) : Nil
      nbytes.times { |i| buffer << ((value >> (8 * i)) & 0xFF).to_u8 }
      nil
    end
  end

  # A metadata builder that registers field names into a new dictionary.
  class WritableMetadataBuilder
    def initialize
      @names = [] of String
      @index = {} of String => Int32
      @is_sorted = false
      @metadata_buffer = [] of UInt8
    end

    # Creates a builder pre-populated with `field_names`.
    def self.from_iter(field_names : Enumerable(String)) : WritableMetadataBuilder
      builder = new
      builder.extend(field_names)
      builder
    end

    # Registers each of `field_names`, returning `self`.
    def extend(field_names : Enumerable(String)) : Nil
      field_names.each { |field_name| upsert_field_name(field_name) }
      nil
    end

    # Registers `field_name`, returning its (possibly newly-created) field id.
    def upsert_field_name(field_name : String) : UInt32
      if id = @index[field_name]?
        return id.to_u32
      end

      @names << field_name
      id = @names.size - 1
      @index[field_name] = id

      n = @names.size
      @is_sorted = n == 1 || (@is_sorted && @names[n - 2] < @names[n - 1])
      id.to_u32
    end

    def field_name(i : Int32) : String
      @names[i]
    end

    def num_field_names : Int32
      @names.size
    end

    # Reverts the field names to a previous size, discarding newer field ids.
    def truncate_field_names(new_size : Int32) : Nil
      @names[new_size..].each { |name| @index.delete(name) }
      @names = @names[0, new_size]
      nil
    end

    # The current length of the underlying metadata buffer.
    def offset : Int32
      @metadata_buffer.size
    end

    # Whether the current dictionary keys are sorted and unique.
    def sorted? : Bool
      @is_sorted
    end

    # Finalizes the dictionary, appending its bytes to the buffer and returning
    # the new buffer length.
    def finish : Int32
      nkeys = @names.size
      total_dict_size = @names.sum(&.bytesize)
      is_sorted = @is_sorted
      @is_sorted = false
      names = @names
      @names = [] of String
      @index.clear

      max_offset = Math.max(total_dict_size, nkeys)
      offset_size = MetadataBuilder.int_size(max_offset).value

      @metadata_buffer << (0x01_u8 | (is_sorted ? 0x10_u8 : 0_u8) | ((offset_size - 1) << 6).to_u8)
      MetadataBuilder.write_offset(@metadata_buffer, nkeys, offset_size)

      current_offset = 0
      names.each do |key|
        MetadataBuilder.write_offset(@metadata_buffer, current_offset, offset_size)
        current_offset += key.bytesize
      end
      MetadataBuilder.write_offset(@metadata_buffer, current_offset, offset_size)

      names.each { |key| @metadata_buffer.concat(key.to_slice) }

      @metadata_buffer.size
    end

    # Returns the buffer, consuming `self` (without finalizing).
    def into_inner : Bytes
      Bytes.new(@metadata_buffer.size) { |i| @metadata_buffer[i] }
    end
  end

  # A metadata builder that only resolves known field names (no new ids).
  class ReadOnlyMetadataBuilder
    def initialize(@metadata : VariantMetadata)
      @known_field_names = {} of String => UInt32
    end

    # Resolves `field_name` to its field id, raising if it is unknown.
    def try_upsert_field_name(field_name : String) : UInt32
      # The `borrowed_field_id` fast path is disabled (divergence).
      if field_id = @known_field_names[field_name]?
        return field_id
      end

      entry = @metadata.get_entry(field_name)
      if entry.nil?
        raise InvalidArgumentError.new("Field name '#{field_name}' not found in metadata dictionary")
      end

      field_id, name = entry
      @known_field_names[name] = field_id
      field_id
    end

    def field_name(field_id : Int32) : String
      @metadata[field_id]
    end

    def num_field_names : Int32
      @metadata.len
    end

    def truncate_field_names(new_size : Int32) : Nil
      unless @metadata.len == new_size
        raise InvalidArgumentError.new("Cannot truncate metadata to #{new_size}; length is #{@metadata.len}")
      end
      nil
    end

    def finish : Int32
      @metadata.bytes.size
    end
  end
end
