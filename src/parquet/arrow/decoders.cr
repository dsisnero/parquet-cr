require "arrow-cr"
require "../basic"
require "../errors"
require "../encodings/rle"
require "../encodings/delta_bit_pack"

# Ported from `parquet/src/arrow/decoder/{mod,dictionary_index,delta_byte_array}.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Specialized decoders optimised for decoding parquet encodings into arrow
# format (`DictIndexDecoder` for `RLE_DICTIONARY` indices and
# `DeltaByteArrayDecoder` for `DELTA_BYTE_ARRAY`).
#
# Divergences: the upstream `read`/`skip` callbacks (`FnMut(&[i32])` /
# `FnMut(&[u8])`) are Crystal blocks; the index buffer is an `Array(Int32)` of
# `INDEX_BATCH_SIZE` entries; `usize` counts are `Int32`; `assert_eq!` on
# decoded lengths raises `GeneralError`.
module Parquet
  module Arrow
    # Specialized arrow decoders.
    module Decoders
      # Decoder for `Encoding::RLE_DICTIONARY` indices.
      class DictIndexDecoder
        # Number of indices decoded per batch.
        INDEX_BATCH_SIZE = 1024

        @decoder : Encodings::RleDecoder
        # Decoded indices (a chunk), and the current positions within it.
        @index_buf : Array(Int32)
        @index_buf_len : Int32
        @index_offset : Int32
        # Maximum number of remaining values (the null count is not always known,
        # e.g. for v1 data page values).
        @max_remaining_values : Int32

        # `num_levels` is the number of levels in the page and `num_values` the
        # number of non-null values when known.
        def initialize(data : Bytes, num_levels : Int32, num_values : Int32?)
          if data.empty?
            raise general_err!("dictionary index page is empty")
          end

          bit_width = data[0].to_i32
          if bit_width > Encodings::MAX_RLE_DICTIONARY_BIT_WIDTH
            raise general_err!(
              "Invalid or corrupted RLE bit width #{bit_width}. Max allowed is #{Encodings::MAX_RLE_DICTIONARY_BIT_WIDTH}"
            )
          end

          @decoder = Encodings::RleDecoder.new(bit_width)
          @decoder.set_data(data[1, data.size - 1])
          @index_buf = Array(Int32).new(INDEX_BATCH_SIZE, 0)
          @index_buf_len = 0
          @index_offset = 0
          @max_remaining_values = num_values || num_levels
        end

        # Reads up to `len` values, yielding each chunk of decoded dictionary
        # indices. Returns the number of values read.
        def read(len : Int32, & : Array(Int32) -> Nil) : Int32
          total_to_read = Math.min(len, @max_remaining_values)
          values_read = 0

          while values_read < total_to_read
            if @index_offset == @index_buf_len
              # The buffer is exhausted, so decode another chunk.
              read = @decoder.get_batch(Int32, @index_buf)
              break if read == 0

              @index_buf_len = read
              @index_offset = 0
            end

            available = @index_buf_len - @index_offset
            count = Math.min(available, total_to_read - values_read)

            yield @index_buf[@index_offset, count]

            @index_offset += count
            values_read += count
          end

          @max_remaining_values -= values_read
          values_read
        end

        # Skips up to `to_skip` values, returning the number skipped.
        def skip(to_skip : Int32) : Int32
          remaining = Math.min(to_skip, @max_remaining_values)
          values_skipped = 0

          while values_skipped < remaining
            if @index_offset == @index_buf_len
              # Skip in the decoder rather than reloading the buffer.
              skipped = @decoder.skip(remaining - values_skipped)
              break if skipped == 0

              @max_remaining_values -= skipped
              values_skipped += skipped
            else
              # Indices are still buffered, so skip within the buffer.
              skipped = Math.min(remaining - values_skipped, @index_buf_len - @index_offset)
              @index_offset += skipped
              @max_remaining_values -= skipped
              values_skipped += skipped
            end
          end

          values_skipped
        end
      end

      # Decoder for `Encoding::DELTA_BYTE_ARRAY`.
      class DeltaByteArrayDecoder
        @prefix_lengths : Array(Int32)
        @suffix_lengths : Array(Int32)
        @data : Bytes
        @length_offset : Int32
        @data_offset : Int32
        @last_value : Array(UInt8)

        def initialize(@data : Bytes)
          prefix = Encodings::DeltaBitPackDecoder(Int32).new
          prefix.set_data(@data, 0)
          num_prefix = prefix.values_left
          @prefix_lengths = Array(Int32).new(num_prefix, 0)
          unless prefix.get(@prefix_lengths) == num_prefix
            raise general_err!("failed to decode DELTA_BYTE_ARRAY prefix lengths")
          end

          suffix = Encodings::DeltaBitPackDecoder(Int32).new
          suffix.set_data(@data[prefix.get_offset, @data.size - prefix.get_offset], 0)
          num_suffix = suffix.values_left
          @suffix_lengths = Array(Int32).new(num_suffix, 0)
          unless suffix.get(@suffix_lengths) == num_suffix
            raise general_err!("failed to decode DELTA_BYTE_ARRAY suffix lengths")
          end

          if num_prefix != num_suffix
            raise general_err!(
              "inconsistent DELTA_BYTE_ARRAY lengths, prefixes: #{num_prefix}, suffixes: #{num_suffix}"
            )
          end

          @length_offset = 0
          @data_offset = prefix.get_offset + suffix.get_offset
          @last_value = [] of UInt8
        end

        # The number of values remaining.
        def remaining : Int32
          @prefix_lengths.size - @length_offset
        end

        # Reads up to `len` values, yielding each decoded byte slice. Returns the
        # number of values read.
        def read(len : Int32, & : Bytes -> Nil) : Int32
          to_read = Math.min(len, remaining)

          to_read.times do |offset|
            idx = @length_offset + offset
            prefix_length = @prefix_lengths[idx]
            suffix_length = @suffix_lengths[idx]

            if @data_offset + suffix_length > @data.size
              raise eof_err!("eof decoding byte array")
            end

            @last_value.truncate(0...prefix_length)
            @data[@data_offset, suffix_length].each { |byte| @last_value << byte }
            yield Bytes.new(@last_value.size) { |i| @last_value[i] }

            @data_offset += suffix_length
          end

          @length_offset += to_read
          to_read
        end

        # Skips up to `to_skip` values, returning the number skipped.
        def skip(to_skip : Int32) : Int32
          to_skip = Math.min(to_skip, remaining)

          to_skip.times do |offset|
            idx = @length_offset + offset
            prefix_length = @prefix_lengths[idx]
            suffix_length = @suffix_lengths[idx]

            if @data_offset + suffix_length > @data.size
              raise eof_err!("eof decoding byte array")
            end

            @last_value.truncate(0...prefix_length)
            @data[@data_offset, suffix_length].each { |byte| @last_value << byte }
            @data_offset += suffix_length
          end

          @length_offset += to_skip
          to_skip
        end
      end
    end
  end
end
