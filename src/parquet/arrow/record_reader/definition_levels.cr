require "arrow-cr"
require "../../basic"
require "../../column/reader/decoder"
require "../../errors"
require "../../schema/types"
require "../../util/bit_util"
require "../buffer/bit_util"

# Ported from `parquet/src/arrow/record_reader/definition_levels.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `DefinitionLevelBuffer` (+ its `Full`/`Mask` modes),
# `build_filtered_validity_bitmap`, `DefinitionLevelBufferDecoder`, and
# `PackedDecoder`.
#
# Divergences: `BufferInner` becomes a `Kind` field on `DefinitionLevelBuffer`;
# `PackedDecoder` is public (upstream keeps it private, exercised by its
# test module); `usize` counters are `Int32`; the `u64`-word `compress`
# fast path of `build_filtered_validity_bitmap` and the
# `UnalignedBitChunk`-based popcounts/`append_packed_range` chunk maths are
# scalar loops (`Buffer.count_set_bits`) with identical results.
module Parquet
  module Arrow
    module RecordReader
      # Decodes RLE/bit-packed data with a bit width of 1 straight into a
      # bitmask (`PackedDecoder`).
      #
      # Packed runs are already bitmask encoded and are appended directly; runs
      # of 1/0 bits are appended as runs.
      class PackedDecoder
        @data : Bytes
        @data_offset : Int32
        @rle_left : Int32
        @rle_value : Bool
        @packed_count : Int32
        @packed_offset : Int32

        def initialize
          @data = Bytes.empty
          @data_offset = 0
          @rle_left = 0
          @rle_value = false
          @packed_count = 0
          @packed_offset = 0
        end

        def set_data(encoding : Encoding, data : Bytes) : Nil
          @rle_left = 0
          @rle_value = false
          @packed_offset = 0
          @packed_count =
            case encoding
            when Encoding::RLE
              0
            when Encoding::BIT_PACKED
              data.size * 8
            else
              raise general_err!("invalid level encoding: #{encoding}")
            end
          @data = data
          @data_offset = 0
        end

        # Tries to consume `len` levels when they are all valid (max definition
        # level), returning the consumed count or `nil`.
        #
        # On `nil` the decoder may have advanced to the next RLE block when
        # `rle_left` was zero (it would have been loaded by the next read anyway).
        def try_consume_all_valid(len : Int32) : Int32?
          # If no active run and no packed data pending, parse the next RLE block
          if @rle_left == 0 && @packed_count == @packed_offset
            return nil if @data_offset >= @data.size
            next_rle_block
          end

          # The fast path only works for an active RLE run of true values that
          # covers the entire requested length.
          if @rle_left >= len && @rle_value
            @rle_left -= len
            return len
          end

          nil
        end

        # Decodes up to `len` levels into `buffer`, returning the number read.
        def read(buffer : ::Arrow::BooleanBufferBuilder, len : Int32) : Int32
          read_count = 0
          while read_count < len
            if @rle_left != 0
              to_read = Math.min(@rle_left, len - read_count)
              buffer.append_n(to_read, @rle_value)
              @rle_left -= to_read
              read_count += to_read
            elsif @packed_count != @packed_offset
              to_read = Math.min(@packed_count - @packed_offset, len - read_count)
              offset = @data_offset * 8 + @packed_offset
              buffer.append_packed_range(offset...offset + to_read, @data)
              @packed_offset += to_read
              read_count += to_read

              if @packed_offset == @packed_count
                @data_offset += @packed_count // 8
              end
            elsif @data_offset == @data.size
              break
            else
              next_rle_block
            end
          end
          read_count
        end

        # Skips `level_num` definition levels, returning the number of values and
        # levels skipped.
        def skip(level_num : Int32) : Tuple(Int32, Int32)
          skipped_value = 0
          skipped_level = 0
          while skipped_level < level_num
            if @rle_left != 0
              to_skip = Math.min(@rle_left, level_num - skipped_level)
              @rle_left -= to_skip
              skipped_level += to_skip
              skipped_value += to_skip if @rle_value
            elsif @packed_count != @packed_offset
              to_skip = Math.min(@packed_count - @packed_offset, level_num - skipped_level)
              offset = @data_offset * 8 + @packed_offset
              skipped_value += Parquet::Arrow::Buffer.count_set_bits(@data, offset, offset + to_skip)
              @packed_offset += to_skip
              skipped_level += to_skip

              if @packed_offset == @packed_count
                @data_offset += @packed_count // 8
              end
            elsif @data_offset == @data.size
              break
            else
              next_rle_block
            end
          end
          {skipped_value, skipped_level}
        end

        private def next_rle_block : Nil
          indicator_value = decode_header
          if indicator_value & 1 == 1
            @packed_count = (indicator_value >> 1).to_i32 * 8
            @packed_offset = 0
          else
            @rle_left = (indicator_value >> 1).to_i32
            if @data_offset >= @data.size
              raise eof_err!("unexpected end of file whilst decoding definition levels rle value")
            end
            byte = @data[@data_offset]
            @data_offset += 1
            @rle_value = byte != 0
          end
          nil
        end

        # Decodes a VLQ-encoded little-endian integer.
        private def decode_header : Int64
          offset = 0_i64
          value = 0_i64
          while offset < 10
            position = @data_offset + offset
            if position >= @data.size
              raise eof_err!("unexpected end of file whilst decoding definition levels rle header")
            end

            byte = @data[position.to_i32]
            value |= ((byte & 0x7F).to_i64 << (offset * 7))
            offset += 1
            if byte & 0x80 == 0
              @data_offset += offset.to_i32
              return value
            end
          end
          raise general_err!("too many bytes for VLQ")
        end
      end

      # Builds a filtered validity bitmap from definition/repetition levels.
      #
      # For each level where `d >= include_threshold` (when set) and
      # `r <= max_rep` (when `rep_filter` is set), appends one bit to `bitmap`:
      # set when `d >= value_level`, unset otherwise. Returns the number of bits
      # appended.
      #
      # This backs the compact bitmap (leaf readers with selective padding) and
      # the struct validity bitmap.
      def self.build_filtered_validity_bitmap(
        def_levels : Array(Int16),
        rep_filter : Tuple(Array(Int16), Int16)?,
        include_threshold : Int16?,
        value_level : Int16,
        bitmap : ::Arrow::BooleanBufferBuilder,
      ) : Int32
        item_count = 0
        def_levels.each_with_index do |def_level, idx|
          if threshold = include_threshold
            next if def_level < threshold
          end
          if filter = rep_filter
            next if filter[0][idx] > filter[1]
          end

          bitmap.append(def_level >= value_level)
          item_count += 1
        end
        item_count
      end

      # Definition level buffer (`DefinitionLevelBuffer`).
      class DefinitionLevelBuffer
        enum Kind
          # Compute levels and the null mask.
          Full
          # Only compute the null bitmask (requires `max_def_level == 1`).
          Mask
        end

        getter kind : Kind
        getter nulls : ::Arrow::BooleanBufferBuilder
        @levels : Array(Int16)
        # Note: `levels`/`nulls` may contain more elements than `@len`.
        @len : Int32

        def initialize(desc : Schema::ColumnDescriptor, null_mask_only : Bool)
          if null_mask_only
            unless desc.max_def_level == 1
              raise general_err!("max definition level must be 1 to only compute null bitmask")
            end
            unless desc.max_rep_level == 0
              raise general_err!("max repetition level must be 0 to only compute null bitmask")
            end

            @kind = Kind::Mask
            @levels = [] of Int16
          else
            @kind = Kind::Full
            @levels = [] of Int16
          end
          @nulls = ::Arrow::BooleanBufferBuilder.new(0)
          @len = 0
        end

        # Returns the built level data (full mode only).
        def consume_levels : Array(Int16)?
          return nil unless @kind.full?
          levels = @levels
          @levels = [] of Int16
          levels
        end

        # Returns the built null bitmask, or `nil` when all values are valid.
        def consume_bitmask : Bytes?
          @len = 0
          # Always finish so the builder resets for the next batch.
          buffer = @nulls.finish

          return nil if buffer.len == 0
          buffer.values
        end

        # Returns the raw definition levels accumulated so far (full mode only).
        def levels : Array(Int16)?
          @kind.full? ? @levels : nil
        end

        # The accumulated definition levels (full mode only; raises in mask mode).
        def levels_array : Array(Int16)
          @kind.full? ? @levels : raise general_err!("definition levels are not tracked in mask mode")
        end
      end

      # Writes definition levels (and the null bitmask) into a
      # `DefinitionLevelBuffer`.
      #
      # Divergence: upstream implements `DefinitionLevelDecoder` with
      # `type Buffer = DefinitionLevelBuffer`; Crystal's abstract decoder fixes
      # `Array(Int16)` as its buffer, so this is a standalone class (the record
      # reader consumes it directly rather than through `ColumnReaderImpl`).
      class DefinitionLevelBufferDecoder
        @max_level : Int16
        @packed : Bool
        @packed_decoder : PackedDecoder
        @fallback_decoder : Column::Reader::DefinitionLevelDecoderImpl?

        def initialize(@max_level : Int16, packed : Bool)
          @packed = packed
          @packed_decoder = PackedDecoder.new
          @fallback_decoder = packed ? nil : Column::Reader::DefinitionLevelDecoderImpl.new(max_level)
        end

        def set_data(encoding : Encoding, data : Bytes) : Nil
          if packed?
            @packed_decoder.set_data(encoding, data)
          else
            fallback = @fallback_decoder
            raise general_err!("definition level decoder is not initialized") unless fallback
            fallback.set_data(encoding, data)
          end
          nil
        end

        def read_def_levels(buffer : DefinitionLevelBuffer, num_levels : Int32) : Tuple(Int32, Int32)
          if buffer.kind.full?
            fallback = @fallback_decoder
            unless fallback
              raise general_err!("inconsistent null mask")
            end

            levels = buffer.levels_array
            start = levels.size
            values_read, levels_read = fallback.read_def_levels(levels, num_levels)

            (start...start + levels_read).each do |idx|
              buffer.nulls.append(levels[idx] == @max_level)
            end
            {values_read, levels_read}
          else
            unless packed?
              raise general_err!("inconsistent null mask")
            end

            # Fast path: all requested levels are valid, so skip RLE decoding and
            # append all-ones to the bitmap.
            if count = @packed_decoder.try_consume_all_valid(num_levels)
              buffer.nulls.append_n(count, true)
              return {count, count}
            end

            start = buffer.nulls.len
            levels_read = @packed_decoder.read(buffer.nulls, num_levels)
            values_read = Parquet::Arrow::Buffer.count_set_bits(
              buffer.nulls.as_slice, start, start + levels_read
            )
            {values_read, levels_read}
          end
        end

        def skip_def_levels(num_levels : Int32) : Tuple(Int32, Int32)
          if packed?
            @packed_decoder.skip(num_levels)
          else
            fallback = @fallback_decoder
            raise general_err!("definition level decoder is not initialized") unless fallback
            fallback.skip_def_levels(num_levels)
          end
        end

        private def packed? : Bool
          @packed
        end
      end
    end
  end
end
