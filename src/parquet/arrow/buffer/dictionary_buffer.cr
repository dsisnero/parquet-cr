require "arrow-cr"
require "../../basic"
require "../../errors"
require "../record_reader/buffer"
require "./offset_buffer"

# Ported from `parquet/src/arrow/buffer/dictionary_buffer.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `DictionaryBuffer` (the byte-array `ValuesBuffer` that is either
# dictionary encoded or a flat `OffsetBuffer`), its arrow conversion
# (`into_array`, `pack_values_from_offsets[_impl]`) and the value hashing used to
# build a dictionary from flat values.
#
# Divergences: Rust's payload-carrying enum is a single class with a `Kind`
# (the variant is switched in place by `as_keys`/`spill_values`); buffer
# identity (`Arc` pointer comparison) is `Reference#same?`; upstream's
# `ahash::RandomState` hashing is replaced by a deterministic FNV-1a 64-bit hash
# (only equality of equal byte slices matters, and `pack_values_from_offsets_impl`
# takes pre-computed hashes so its behaviour is identical); upstream's
# `hash_scratch: MutableBuffer` parameter is dropped (Crystal returns the hash
# array); `into_array` takes the already-built `NullBuffer` and builds arrow-cr
# arrays positionally instead of through `ArrayDataBuilder`; `usize` counts are
# `Int32`. The `FixedSizeBinary` value target in the flat-values path is built as
# a real `FixedSizeBinaryArray` (upstream leaves the binary buffers on a
# `FixedSizeBinary` `ArrayData`, which is invalid).
module Parquet
  module Arrow
    module Buffer
      # An array of variable-length byte arrays that is potentially dictionary
      # encoded and can be converted into a corresponding arrow array.
      class DictionaryBuffer(K, V) < RecordReader::ValuesBuffer
        # Which representation is currently held.
        enum Kind
          # Dictionary encoded: `keys` indexes into the dictionary `values`.
          Dict
          # Flat: the values are stored directly.
          Values
        end

        getter kind : Kind
        # The dictionary keys (`Kind::Dict`).
        getter keys : Array(K)
        # The dictionary values array (`Kind::Dict`).
        getter dict_values : ::Arrow::ArrayBase?
        # The flat values (`Kind::Values`).
        getter values : OffsetBuffer(V)?

        # Creates a flat (empty) buffer storing its values in an
        # `OffsetBuffer` (upstream `ValuesBuffer::with_capacity`).
        def self.with_capacity(capacity : Int32) : DictionaryBuffer(K, V)
          _ = capacity
          new
        end

        def initialize
          @kind = Kind::Values
          @keys = [] of K
          @dict_values = nil
          @values = OffsetBuffer(V).new
        end

        # The number of values in this buffer.
        def len : Int32
          case @kind
          in .dict?   then @keys.size
          in .values? then @values.try(&.len) || 0
          end
        end

        def empty? : Bool
          len == 0
        end

        # Returns a mutable reference to the keys array (upstream `as_keys`).
        #
        # Returns `nil` if the dictionary needs to be recomputed.
        def as_keys(dictionary : ::Arrow::ArrayBase) : Array(K)?
          if @kind.values?
            values = @values || raise general_err!("values buffer is not initialized")
            return nil unless values.empty?

            @kind = Kind::Dict
            @keys = [] of K
            @dict_values = dictionary
            @values = nil
            return @keys
          end

          # Need to discard the fat pointer for the equality check, matching
          # upstream's `values_ptr == dict_ptr`.
          if (dict = @dict_values) && dict.same?(dictionary)
            @keys
          elsif @keys.empty?
            @dict_values = dictionary
            @keys
          else
            nil
          end
        end

        # Returns the flat values buffer, converting from the dictionary
        # representation when needed (upstream `spill_values`).
        def spill_values : OffsetBuffer(V)
          if @kind.values?
            return @values || raise general_err!("values buffer is not initialized")
          end

          dict = @dict_values || raise general_err!("dictionary values are not initialized")
          spilled = OffsetBuffer(V).new

          if dict.len == 0
            # If the dictionary is empty, zero pad the offsets.
            target = @keys.size + 1
            (spilled.offsets.size...target).each { spilled.offsets << V.new(0) }
          else
            # Note: at this point null positions will have arbitrary dictionary
            # keys and this will hydrate them to the corresponding byte array.
            # This is likely sub-optimal, as we would prefer zero length null
            # "slots", but spilling is already a degenerate case.
            dict_offsets, dict_values = DictionaryBuffer.offsets_and_values(V, dict)
            spilled.extend_from_dictionary(@keys, dict_offsets, dict_values)
          end

          @kind = Kind::Values
          @keys = [] of K
          @dict_values = nil
          @values = spilled
          spilled
        end

        # Converts this into an arrow array with the provided `data_type` and
        # null buffer (upstream `into_array`).
        def into_array(null_buffer : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
          unless data_type.kind.dictionary?
            raise general_err!("expected a dictionary data type, got #{data_type}")
          end

          case @kind
          in .dict?
            into_dictionary_array(null_buffer, data_type)
          in .values?
            values = @values || raise general_err!("values buffer is not initialized")
            key_type = data_type.key || raise general_err!("dictionary data type without a key type")
            value_type = data_type.value || raise general_err!("dictionary data type without a value type")
            hashes = DictionaryBuffer.hash_byte_slices(values.offsets, values.values)
            DictionaryBuffer.pack_values_from_offsets(
              key_type, value_type, values, hashes, null_buffer
            )
          end
        end

        def reserve_exact(additional : Int32) : Nil
          # Crystal arrays/buffers grow amortized (upstream reserves in the keys
          # or values buffers).
          _ = additional
          nil
        end

        def pad_nulls(
          read_offset : Int32,
          values_read : Int32,
          levels_read : Int32,
          valid_mask : Bytes,
        ) : Nil
          case @kind
          in .dict?
            keys = @keys
            target = read_offset + levels_read
            (keys.size...target).each { keys << K.new(0) }
            keys.truncate(target..) if keys.size > target
            RecordReader::ArrayValuesBuffer(K).new(keys, K.new(0))
              .pad_nulls(read_offset, values_read, levels_read, valid_mask)
          in .values?
            (@values || raise general_err!("values buffer is not initialized"))
              .pad_nulls(read_offset, values_read, levels_read, valid_mask)
          end
        end

        # The offsets and raw value bytes of a dictionary values array.
        def self.offsets_and_values(type : V.class, dict : ::Arrow::ArrayBase) : Tuple(Array(V), Bytes) forall V
          case dict
          when ::Arrow::GenericStringArray(V)
            {dict.offsets.to_slice.to_a, dict.value_data.as_slice}
          when ::Arrow::GenericBinaryArray(V)
            {dict.offsets.to_slice.to_a, dict.value_data.as_slice}
          else
            raise general_err!("unsupported dictionary values array type #{dict.data_type}")
          end
        end

        # Builds the dictionary-encoded array from `keys` and the dictionary
        # values array.
        private def into_dictionary_array(
          null_buffer : ::Arrow::NullBuffer?,
          data_type : ::Arrow::DataType,
        ) : ::Arrow::ArrayBase
          keys = @keys
          values = @dict_values || raise general_err!("dictionary values are not initialized")

          # Validate the keys unless the dictionary is empty.
          unless values.len == 0
            max = values.len
            keys.each do |key|
              index = key.to_i64
              if index < 0 || index >= max
                raise general_err!("dictionary key beyond bounds of dictionary: 0..#{values.len}")
              end
            end
          end

          value_type = data_type.value || raise general_err!("dictionary data type without a value type")
          values = normalize_dictionary_values(values, value_type)

          dict_keys = ::Arrow::PrimitiveArray(K).new(
            ::Arrow::ScalarBuffer(K).from(keys.dup), null_buffer
          )
          ::Arrow::DictionaryArray(K).new(data_type, dict_keys, values, false).as(::Arrow::ArrayBase)
        end

        # Upstream reinterprets a binary dictionary as a `FixedSizeBinaryArray`
        # when the target value type is fixed size.
        private def normalize_dictionary_values(
          values : ::Arrow::ArrayBase,
          value_type : ::Arrow::DataType,
        ) : ::Arrow::ArrayBase
          return values unless value_type.kind.fixed_size_binary?

          size = value_type.byte_width || raise general_err!("fixed size binary type without a size")
          binary = values
          unless binary.is_a?(::Arrow::GenericBinaryArray(Int32))
            raise general_err!("expected a binary dictionary, got #{binary.data_type}")
          end

          ::Arrow::FixedSizeBinaryArray.new(size, binary.values, binary.nulls).as(::Arrow::ArrayBase)
        end

        # Builds a dictionary array from a flat values buffer (upstream
        # `pack_values_from_offsets`).
        def self.pack_values_from_offsets(
          key_type : ::Arrow::DataType,
          value_type : ::Arrow::DataType,
          values : OffsetBuffer(V),
          hashes : Array(UInt64),
          null_buffer : ::Arrow::NullBuffer?,
        ) : ::Arrow::ArrayBase forall V
          case key_type.kind
          when .int8?    then pack_values_from_offsets_impl(Int8, key_type, value_type, values, hashes, null_buffer)
          when .int16?   then pack_values_from_offsets_impl(Int16, key_type, value_type, values, hashes, null_buffer)
          when .int32?   then pack_values_from_offsets_impl(Int32, key_type, value_type, values, hashes, null_buffer)
          when .int64?   then pack_values_from_offsets_impl(Int64, key_type, value_type, values, hashes, null_buffer)
          when .u_int8?  then pack_values_from_offsets_impl(UInt8, key_type, value_type, values, hashes, null_buffer)
          when .u_int16? then pack_values_from_offsets_impl(UInt16, key_type, value_type, values, hashes, null_buffer)
          when .u_int32? then pack_values_from_offsets_impl(UInt32, key_type, value_type, values, hashes, null_buffer)
          when .u_int64? then pack_values_from_offsets_impl(UInt64, key_type, value_type, values, hashes, null_buffer)
          else
            raise general_err!("unsupported dictionary key type #{key_type}")
          end
        end

        # Builds a dictionary array directly from a flat values buffer using
        # pre-computed hashes to deduplicate values in a single pass (upstream
        # `pack_values_from_offsets_impl`).
        #
        # Public in the port because upstream's test module (and this port's
        # specs) call it with hand-made hashes to exercise hash collisions.
        def self.pack_values_from_offsets_impl(
          type : K.class,
          key_type : ::Arrow::DataType,
          value_type : ::Arrow::DataType,
          offset_buffer : OffsetBuffer(V),
          hashes : Array(UInt64),
          null_buffer : ::Arrow::NullBuffer?,
        ) : ::Arrow::ArrayBase forall K, V
          offsets = offset_buffer.offsets
          bytes = offset_buffer.values
          dict_type = ::Arrow::DataType.dictionary(key_type, value_type)

          keys = [] of K
          unique_offsets = [] of V
          unique_offsets << V.new(0)
          unique_bytes = [] of UInt8

          # hash => (first input index, output index)
          dedup = {} of UInt64 => Tuple(Int32, Int32)
          # Tracks colliding values (same hash, different bytes) so repeated
          # occurrences are deduplicated rather than inserted as new entries.
          # Empty in the common case.
          collision_overflow = [] of Tuple(UInt64, Int32, Int32)

          hashes.each_with_index do |hash, input_idx|
            byte_start = offsets[input_idx].to_i32
            byte_end = offsets[input_idx + 1].to_i32
            value = bytes[byte_start...byte_end]

            output_idx =
              if entry = dedup[hash]?
                first_input_idx, existing_output_idx = entry
                first_start = offsets[first_input_idx].to_i32
                first_end = offsets[first_input_idx + 1].to_i32
                if bytes[first_start...first_end] == value
                  existing_output_idx
                else
                  # True hash collision: check the overflow list before
                  # inserting.
                  collision_output_idx = nil
                  collision_overflow.each do |overflow_hash, collision_input_idx, idx|
                    next unless overflow_hash == hash

                    collision_start = offsets[collision_input_idx].to_i32
                    collision_end = offsets[collision_input_idx + 1].to_i32
                    if bytes[collision_start...collision_end] == value
                      collision_output_idx = idx
                      break
                    end
                  end

                  if idx = collision_output_idx
                    idx
                  else
                    new_output_idx = unique_offsets.size - 1
                    unique_bytes.concat(value)
                    unique_offsets << checked_int(V, unique_bytes.size, "offset overflow building dictionary")
                    collision_overflow << {hash, input_idx, new_output_idx}
                    new_output_idx
                  end
                end
              else
                new_output_idx = unique_offsets.size - 1
                unique_bytes.concat(value)
                unique_offsets << checked_int(V, unique_bytes.size, "offset overflow building dictionary")
                dedup[hash] = {input_idx, new_output_idx}
                new_output_idx
              end

            keys << checked_int(K, output_idx, "dictionary key overflow")
          end

          num_unique = unique_offsets.size - 1
          values = build_values_array(value_type, unique_offsets, unique_bytes)

          dict_keys = ::Arrow::PrimitiveArray(K).new(
            ::Arrow::ScalarBuffer(K).from(keys), null_buffer
          )
          if values.len != num_unique
            raise general_err!(
              "expected #{num_unique} unique dictionary values, got #{values.len}"
            )
          end
          ::Arrow::DictionaryArray(K).new(dict_type, dict_keys, values, false).as(::Arrow::ArrayBase)
        end

        # Builds the (deduplicated) dictionary values array from its offsets and
        # bytes.
        private def self.build_values_array(
          value_type : ::Arrow::DataType,
          unique_offsets : Array(V),
          unique_bytes : Array(UInt8),
        ) : ::Arrow::ArrayBase forall V
          offsets = ::Arrow::OffsetBuffer(V).new(::Arrow::ScalarBuffer(V).from(unique_offsets), false)
          data = ::Arrow::Buffer.from_slice_ref(Bytes.new(unique_bytes.size) { |i| unique_bytes[i] })

          case value_type.kind
          when .utf8?, .large_utf8?
            ::Arrow::GenericStringArray(V).new(offsets, data, nil).as(::Arrow::ArrayBase)
          when .binary?, .large_binary?
            ::Arrow::GenericBinaryArray(V).new(offsets, data, nil).as(::Arrow::ArrayBase)
          when .fixed_size_binary?
            size = value_type.byte_width || raise general_err!("fixed size binary type without a size")
            unless unique_bytes.size % size == 0
              raise general_err!("misaligned fixed size binary dictionary")
            end
            ::Arrow::FixedSizeBinaryArray.new(size, data, nil).as(::Arrow::ArrayBase)
          else
            raise general_err!("unsupported dictionary value type #{value_type}")
          end
        end

        # Hashes each byte slice delimited by `offsets` (upstream
        # `hash_byte_slices`, which uses ahash; the port uses a deterministic
        # FNV-1a hash).
        def self.hash_byte_slices(offsets : Array(I), values : Array(UInt8)) : Array(UInt64) forall I
          count = Math.max(offsets.size - 1, 0)
          hashes = Array(UInt64).new(count)

          count.times do |idx|
            start = offsets[idx].to_i32
            finish = offsets[idx + 1].to_i32
            hashes << fnv1a_64(values[start...finish])
          end
          hashes
        end

        # FNV-1a 64-bit hash.
        private def self.fnv1a_64(bytes : Array(UInt8)) : UInt64
          hash = 0xcbf29ce484222325_u64
          bytes.each do |byte|
            hash ^= byte
            # Wrapping multiply (the prime is 2**40 + 435).
            hash = hash &* 0x00000100000001b3_u64
          end
          hash
        end

        # Converts `value` to `T`, raising `message` on overflow (upstream
        # `T::from_usize(..).ok_or_else(|| general_err!(..))`).
        private def self.checked_int(type : T.class, value : Int32, message : String) : T forall T
          T.new(value)
        rescue OverflowError
          raise general_err!(message)
        end
      end
    end
  end
end
