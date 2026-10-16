require "../../basic"
require "../../util/bit_util"
require "../../schema/types"
require "../../encodings/registry"

# Ported from `parquet/src/column/reader/decoder.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the level decoders (`DefinitionLevelDecoderImpl`,
# `RepetitionLevelDecoderImpl`) and the value decoder
# (`ColumnValueDecoderImpl<T>`).
#
# Divergence: Rust's `ColumnLevelDecoder`/`RepetitionLevelDecoder`/
# `DefinitionLevelDecoder`/`ColumnValueDecoder` traits (with associated `Buffer`
# types) become abstract classes; all buffers are `Array(Int16)`/`Array(T)`.
# `EncodingMask::is_set` → `set?`. The per-encoding decoder bucket array is sized
# `EncodingMask::MAX_ENCODING + 1`.
module Parquet
  module Column
    module Reader
      # Decodes level data.
      abstract class ColumnLevelDecoder
        abstract def set_data(encoding : Encoding, data : Bytes) : Nil
      end

      # Decodes repetition level data.
      abstract class RepetitionLevelDecoder < ColumnLevelDecoder
        abstract def read_rep_levels(dest : Array(Int16), num_records : Int32, num_levels : Int32) : Tuple(Int32, Int32)
        abstract def skip_rep_levels(num_records : Int32, num_levels : Int32) : Tuple(Int32, Int32)
        abstract def flush_partial : Bool
      end

      # Decodes definition level data.
      abstract class DefinitionLevelDecoder < ColumnLevelDecoder
        abstract def read_def_levels(dest : Array(Int16), num_levels : Int32) : Tuple(Int32, Int32)
        abstract def skip_def_levels(num_levels : Int32) : Tuple(Int32, Int32)
      end

      # Decodes value data for the data type `T`.
      abstract class ColumnValueDecoder(T)
        abstract def set_dict(buf : Bytes, num_values : UInt32, encoding : Encoding, is_sorted : Bool) : Nil
        abstract def set_data(encoding : Encoding, data : Bytes, num_levels : Int32, num_values : Int32?) : Nil
        abstract def read(dest : Array(T), num_values : Int32) : Int32
        abstract def skip_values(num_values : Int32) : Int32
      end

      # Bucket-based storage for decoder instances keyed by `Encoding`.
      ENCODING_SLOTS = EncodingMask::MAX_ENCODING + 1

      # Decodes levels using either RLE/bit-packing or (deprecated) bit packing.
      class LevelDecoder
        getter rle : Encodings::RleDecoder?
        getter reader : Util::BitReader?
        getter bit_width : UInt8

        private def initialize(@rle, @reader, @bit_width)
        end

        def self.new(encoding : Encoding, data : Bytes, bit_width : UInt8) : LevelDecoder
          case encoding
          when Encoding::RLE
            decoder = Encodings::RleDecoder.new(bit_width.to_i32)
            decoder.set_data(data)
            new(decoder, nil, bit_width)
          when Encoding::BIT_PACKED
            new(nil, Util::BitReader.new(data), bit_width)
          else
            raise "invalid level encoding: #{encoding}"
          end
        end

        # Reads up to `dest.size` levels into `dest`, returning the count read.
        def read(dest : Array(Int16)) : Int32
          if decoder = @rle
            decoder.get_batch(Int16, dest)
          else
            reader = @reader || raise "level decoder is not initialized"
            reader.get_batch(Int16, dest, @bit_width.to_i32)
          end
        end
      end

      # The default value for `T`, used to pre-size decode buffers.
      def self.default_value(type : T.class) : T forall T
        {% if T == Bool %}
          false
        {% elsif T == Int32 %}
          0_i32
        {% elsif T == Int64 %}
          0_i64
        {% elsif T == Float32 %}
          0_f32
        {% elsif T == Float64 %}
          0_f64
        {% elsif T == Int96 %}
          Int96.new
        {% elsif T == ByteArray %}
          ByteArray.new
        {% elsif T == FixedLenByteArray %}
          FixedLenByteArray.new(ByteArray.new)
        {% else %}
          {% raise "default_value: unsupported type #{T}" %}
        {% end %}
      end

      # An implementation of `ColumnValueDecoder` for `T`.
      class ColumnValueDecoderImpl(T) < ColumnValueDecoder(T)
        def initialize(@descr : Schema::ColumnDescriptor)
          @current_encoding = nil.as(Encoding?)
          @decoder_mask = EncodingMask.new(0)
          @decoders = Array(Encodings::Decoder(T)?).new(ENCODING_SLOTS, nil)
        end

        def set_dict(buf : Bytes, num_values : UInt32, encoding : Encoding, is_sorted : Bool) : Nil
          if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY
            encoding = Encoding::RLE_DICTIONARY
          end

          if @decoder_mask.set?(encoding)
            raise general_err!("Column cannot have more than one dictionary")
          end

          if encoding == Encoding::RLE_DICTIONARY
            dictionary = Encodings::PlainDecoder(T).new(@descr.type_length)
            dictionary.set_data(buf, num_values.to_i32)

            decoder = Encodings::DictDecoder(T).new
            decoder.set_dict(dictionary)
            @decoders[encoding.value] = decoder
            @decoder_mask.insert(encoding)
          else
            raise nyi_err!("Invalid/Unsupported encoding type for dictionary: #{encoding}")
          end

          _ = is_sorted
        end

        def set_data(encoding : Encoding, data : Bytes, num_levels : Int32, num_values : Int32?) : Nil
          encoding = Encoding::RLE_DICTIONARY if encoding == Encoding::PLAIN_DICTIONARY

          decoder =
            if encoding == Encoding::RLE_DICTIONARY
              @decoders[encoding.value] || raise "Decoder for dict should have been set"
            else
              slot = encoding.value
              if @decoders[slot].nil?
                @decoders[slot] = Encodings.get_decoder(T, @descr, encoding)
                @decoder_mask.insert(encoding)
              end
              @decoders[slot] || raise "decoder should have been inserted"
            end

          decoder.set_data(data, num_values || num_levels)
          @current_encoding = encoding
        end

        def read(dest : Array(T), num_values : Int32) : Int32
          encoding = @current_encoding || raise "current_encoding should be set"
          decoder = @decoders[encoding.value] || raise "decoder for encoding #{encoding} should be set"

          scratch = Array(T).new(num_values) { Reader.default_value(T) }
          read = decoder.get(scratch)
          dest.concat(scratch[0, read])
          read
        end

        def skip_values(num_values : Int32) : Int32
          encoding = @current_encoding || raise "current_encoding should be set"
          decoder = @decoders[encoding.value] || raise "decoder for encoding #{encoding} should be set"

          decoder.skip(num_values)
        end
      end

      SKIP_BUFFER_SIZE = 1024

      # An implementation of `DefinitionLevelDecoder` for `Int16`.
      class DefinitionLevelDecoderImpl < DefinitionLevelDecoder
        def initialize(max_level : Int16)
          @decoder = nil.as(LevelDecoder?)
          @bit_width = Util::BitUtil.num_required_bits(max_level.to_u64)
          @max_level = max_level
        end

        def set_data(encoding : Encoding, data : Bytes) : Nil
          @decoder = LevelDecoder.new(encoding, data, @bit_width)
        end

        def read_def_levels(dest : Array(Int16), num_levels : Int32) : Tuple(Int32, Int32)
          decoder = @decoder || raise "definition level decoder is not initialized"
          scratch = Array(Int16).new(num_levels, 0_i16)
          levels_read = decoder.read(scratch)
          dest.concat(scratch[0, levels_read])

          values_read = 0
          levels_read.times { |i| values_read += 1 if scratch[i] == @max_level }
          {values_read, levels_read}
        end

        def skip_def_levels(num_levels : Int32) : Tuple(Int32, Int32)
          level_skip = 0
          value_skip = 0
          while level_skip < num_levels
            remaining_levels = num_levels - level_skip
            to_read = Math.min(remaining_levels, SKIP_BUFFER_SIZE)
            buffer = [] of Int16
            values_read, levels_read = read_def_levels(buffer, to_read)
            break if levels_read == 0

            level_skip += levels_read
            value_skip += values_read
          end

          {value_skip, level_skip}
        end
      end

      REPETITION_LEVELS_BATCH_SIZE = 1024

      # An implementation of `RepetitionLevelDecoder` for `Int16`.
      class RepetitionLevelDecoderImpl < RepetitionLevelDecoder
        def initialize(max_level : Int16)
          @decoder = nil.as(LevelDecoder?)
          @bit_width = Util::BitUtil.num_required_bits(max_level.to_u64)
          @buffer = Array(Int16).new(REPETITION_LEVELS_BATCH_SIZE, 0_i16)
          @buffer_len = 0
          @buffer_offset = 0
          @has_partial = false
        end

        def set_data(encoding : Encoding, data : Bytes) : Nil
          @decoder = LevelDecoder.new(encoding, data, @bit_width)
          @buffer_len = 0
          @buffer_offset = 0
        end

        private def fill_buf : Nil
          decoder = @decoder || raise "repetition level decoder is not initialized"
          read = decoder.read(@buffer)
          @buffer_offset = 0
          @buffer_len = read
        end

        # Returns `(hit_limit, records_read, levels_read)`.
        private def count_records(records_to_read : Int32, num_levels : Int32) : Tuple(Bool, Int32, Int32)
          records_read = 0
          levels = Math.min(num_levels, @buffer_len - @buffer_offset)

          idx = 0
          while idx < levels
            if @buffer[@buffer_offset + idx] == 0 && (idx != 0 || @has_partial)
              records_read += 1
              return {false, records_read, idx} if records_read == records_to_read
            end
            idx += 1
          end

          {true, records_read, levels}
        end

        def read_rep_levels(dest : Array(Int16), num_records : Int32, num_levels : Int32) : Tuple(Int32, Int32)
          total_records_read = 0
          total_levels_read = 0

          while total_records_read < num_records && total_levels_read < num_levels
            if @buffer_len == @buffer_offset
              fill_buf
              break if @buffer_len == 0
            end

            partial, records_read, levels_read = count_records(
              num_records - total_records_read,
              num_levels - total_levels_read,
            )

            dest.concat(@buffer[@buffer_offset, levels_read])

            total_levels_read += levels_read
            total_records_read += records_read
            @buffer_offset += levels_read
            @has_partial = partial
          end
          {total_records_read, total_levels_read}
        end

        def skip_rep_levels(num_records : Int32, num_levels : Int32) : Tuple(Int32, Int32)
          total_records_read = 0
          total_levels_read = 0

          while total_records_read < num_records && total_levels_read < num_levels
            if @buffer_len == @buffer_offset
              fill_buf
              break if @buffer_len == 0
            end

            partial, records_read, levels_read = count_records(
              num_records - total_records_read,
              num_levels - total_levels_read,
            )

            total_levels_read += levels_read
            total_records_read += records_read
            @buffer_offset += levels_read
            @has_partial = partial
          end
          {total_records_read, total_levels_read}
        end

        def flush_partial : Bool
          partial = @has_partial
          @has_partial = false
          partial
        end
      end
    end
  end
end
