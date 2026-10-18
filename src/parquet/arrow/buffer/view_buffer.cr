require "arrow-cr"
require "../../errors"
require "../record_reader/buffer"
require "./bit_util"

# Ported from `parquet/src/arrow/buffer/view_buffer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ViewBuffer`, the byte-view (`Utf8View`/`BinaryView`) values buffer, so
# the byte-view array reader can use the arrow record reader.
#
# Divergences: views are an `Array(UInt128)` (upstream `Vec<u128>`) and blocks an
# `Array(Buffer)`; `into_array` constructs arrow-cr's `StringViewArray`/
# `BinaryViewArray` directly, and the null padding reuses
# `ArrayValuesBuffer(UInt128)` instead of upstream's `Vec<u128>` `ValuesBuffer`
# impl; `usize` counts are `Int32`.
module Parquet
  module Arrow
    module Buffer
      # A buffer of view type byte arrays that can be converted into a
      # `GenericByteViewArray` (upstream `ViewBuffer`).
      #
      # Note: this does not reuse arrow-cr's `GenericByteViewBuilder`, because the
      # record reader needs `pad_nulls` over the raw views.
      class ViewBuffer < RecordReader::ValuesBuffer
        getter views : Array(UInt128)
        getter buffers : Array(::Arrow::Buffer)

        def self.with_capacity(capacity : Int32) : ViewBuffer
          # Crystal arrays expose no capacity API, so the hint is ignored
          # (upstream pre-allocates `capacity` views).
          _ = capacity
          new
        end

        def initialize
          @views = [] of UInt128
          @buffers = [] of ::Arrow::Buffer
        end

        def empty? : Bool
          @views.empty?
        end

        # Appends a data block, returning its block id.
        def append_block(block : ::Arrow::Buffer) : UInt32
          block_id = @buffers.size.to_u32
          @buffers << block
          block_id
        end

        # Converts this into an arrow byte-view array.
        def into_array(null_buffer : ::Arrow::NullBuffer?, data_type : ::Arrow::DataType) : ::Arrow::ArrayBase
          views = ::Arrow::ScalarBuffer(UInt128).from(@views.dup)
          buffers = @buffers.dup

          case data_type.kind
          when .utf8_view?
            ::Arrow::StringViewArray.new(views, buffers, null_buffer)
          when .binary_view?
            ::Arrow::BinaryViewArray.new(views, buffers, null_buffer)
          else
            raise general_err!("Unsupported data type: #{data_type}")
          end
        end

        def reserve_exact(additional : Int32) : Nil
          _ = additional
        end

        # Pads nulls by shifting the view slots (upstream delegates to the
        # `Vec<u128>` `ValuesBuffer` impl).
        def pad_nulls(
          read_offset : Int32,
          values_read : Int32,
          levels_read : Int32,
          valid_mask : Bytes,
        ) : Nil
          RecordReader::ArrayValuesBuffer(UInt128).new(@views, 0_u128)
            .pad_nulls(read_offset, values_read, levels_read, valid_mask)
        end
      end
    end
  end
end
