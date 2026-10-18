require "arrow-cr"
require "../../basic"
require "../../errors"
require "../../encodings/rle"
require "../array_reader"
require "../buffer/dictionary_buffer"
require "../record_reader"

# Ported from `parquet/src/arrow/array_reader/byte_array_dictionary.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `make_byte_array_dictionary_reader`, `ByteArrayDictionaryReader`,
# `DictionaryDecoder` (+ `MaybeDictionaryDecoder`), and `convert_values_to_view`.
# The reader preserves the dictionary encoding found in the parquet data when it
# can: a single data page per read that is itself dictionary encoded.
#
# Divergences: upstream's `make_reader!` macro is a `case` over the key/value
# `DataType::Kind`s; the `MaybeDictionaryDecoder` enum is an abstract class with
# two subclasses (the variant never changes after construction); the fallback
# (non-dictionary encoding) path decodes through the shared
# `ColumnValueDecoderImpl(ByteArray)` into an `Array(ByteArray)` scratch and then
# pushes the values into the record reader's `OffsetBuffer`, because the port
# does not carry upstream's dedicated `ByteArrayDecoder`
# (`ByteArrayDecoderPlain`/`Dictionary`/`DeltaLength`/`DeltaByteArray`); since
# that decoder cannot stop at the end of the page buffer, an empty value payload
# is short-circuited to zero values (and the empty dictionary page to an empty
# dictionary), while a truncated non-empty payload still raises; the
# `convert_values_to_view` key type is passed explicitly (arrow-cr has no
# `AnyDictionaryArray`); `usize` counts are `Int32`.
module Parquet
  module Arrow
    # Returns an `ArrayReader` that decodes the provided byte array column,
    # attempting to preserve any dictionary encoding present in the parquet data
    # (upstream `make_byte_array_dictionary_reader`).
    #
    # It will be unable to preserve the dictionary encoding if a single read
    # spans across multiple column chunks, or if a column chunk contains
    # non-dictionary encoded pages. It is therefore recommended that if `pages`
    # contains data from multiple column chunks, the read batch size used is a
    # divisor of the row group size.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.make_byte_array_dictionary_reader(
      pages : Column::PageIterator,
      column_desc : Schema::ColumnDescriptor,
      arrow_type : ::Arrow::DataType?,
      batch_size : Int32,
      padding_threshold : Int16?,
    ) : ArrayReader
      # Check if the Arrow type is specified, else create it from the Parquet type.
      data_type = arrow_type || parquet_to_arrow_field(column_desc).data_type

      key_type = data_type.key
      value_type = data_type.value
      unless data_type.kind.dictionary? && key_type && value_type
        raise general_err!(
          "invalid non-dictionary data type for byte array dictionary reader - #{data_type}"
        )
      end

      key_kind = key_type.kind
      case value_type.kind
      when .binary?, .utf8?, .utf8_view?, .binary_view?, .fixed_size_binary?
        case key_kind
        when .u_int8?  then new_dictionary_reader(UInt8, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int8?    then new_dictionary_reader(Int8, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        when .u_int16? then new_dictionary_reader(UInt16, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int16?   then new_dictionary_reader(Int16, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        when .u_int32? then new_dictionary_reader(UInt32, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int32?   then new_dictionary_reader(Int32, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        when .u_int64? then new_dictionary_reader(UInt64, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int64?   then new_dictionary_reader(Int64, Int32, pages, column_desc, data_type, batch_size, padding_threshold)
        else
          raise general_err!(
            "unsupported data type for byte array dictionary reader - #{data_type}"
          )
        end
      when .large_binary?, .large_utf8?
        case key_kind
        when .u_int8?  then new_dictionary_reader(UInt8, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int8?    then new_dictionary_reader(Int8, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        when .u_int16? then new_dictionary_reader(UInt16, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int16?   then new_dictionary_reader(Int16, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        when .u_int32? then new_dictionary_reader(UInt32, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int32?   then new_dictionary_reader(Int32, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        when .u_int64? then new_dictionary_reader(UInt64, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        when .int64?   then new_dictionary_reader(Int64, Int64, pages, column_desc, data_type, batch_size, padding_threshold)
        else
          raise general_err!(
            "unsupported data type for byte array dictionary reader - #{data_type}"
          )
        end
      else
        raise general_err!(
          "unsupported data type for byte array dictionary reader - #{data_type}"
        )
      end
    end

    # Builds a `ByteArrayDictionaryReader(K, V)` over the shared record reader
    # (upstream `make_reader!`).
    private def self.new_dictionary_reader(
      key_type : K.class,
      value_type : V.class,
      pages : Column::PageIterator,
      column_desc : Schema::ColumnDescriptor,
      data_type : ::Arrow::DataType,
      batch_size : Int32,
      padding_threshold : Int16?,
    ) : ArrayReader forall K, V
      record_reader = RecordReader::GenericRecordReader(
        Buffer::DictionaryBuffer(K, V), DictionaryDecoder(K, V),
      ).new(column_desc, batch_size, -> { Buffer::DictionaryBuffer(K, V).new })

      if threshold = padding_threshold
        record_reader.set_padding_threshold(threshold)
      end

      ByteArrayDictionaryReader(K, V).try_new(pages, data_type, record_reader)
    end

    # An `ArrayReader` for dictionary encoded variable length byte arrays
    # (upstream `ByteArrayDictionaryReader`).
    class ByteArrayDictionaryReader(K, V) < ArrayReader
      # Type used by the dictionary buffer when it is different from the output
      # data type (`nil` when they match).
      getter buffer_type : ::Arrow::DataType?

      def self.try_new(
        pages : Column::PageIterator,
        data_type : ::Arrow::DataType,
        record_reader : RecordReader::GenericRecordReader(
          Buffer::DictionaryBuffer(K, V), DictionaryDecoder(K, V),
        ),
      ) : ByteArrayDictionaryReader(K, V)
        unless data_type.kind.dictionary?
          raise general_err!("Expected dictionary type, found #{data_type}")
        end

        key_type = data_type.key || raise general_err!("dictionary data type without a key type")
        value_type = data_type.value || raise general_err!("dictionary data type without a value type")

        # Byte-view targets are buffered as their non-view equivalent and
        # converted in `consume_batch`.
        buffer_type =
          case value_type.kind
          when .utf8_view?
            ::Arrow::DataType.dictionary(key_type, ::Arrow::DataType.utf8)
          when .binary_view?
            ::Arrow::DataType.dictionary(key_type, ::Arrow::DataType.binary)
          else
            nil
          end

        new(pages, data_type, buffer_type, record_reader)
      end

      def initialize(
        @pages : Column::PageIterator,
        @data_type : ::Arrow::DataType,
        @buffer_type : ::Arrow::DataType?,
        @record_reader : RecordReader::GenericRecordReader(
          Buffer::DictionaryBuffer(K, V), DictionaryDecoder(K, V),
        ),
      )
        @def_levels_buffer = nil.as(Array(Int16)?)
        @rep_levels_buffer = nil.as(Array(Int16)?)
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        Arrow.read_records_from_pages(@record_reader, @pages, batch_size)
      end

      def skip_records(num_records : Int32) : Int32
        Arrow.skip_records_from_pages(@record_reader, @pages, num_records)
      end

      def consume_batch : ::Arrow::ArrayBase
        # Advance the def & rep level buffers.
        @def_levels_buffer = @record_reader.consume_def_levels
        @rep_levels_buffer = @record_reader.consume_rep_levels

        if @record_reader.num_values == 0
          # Once the record reader has been consumed, its values buffer has been
          # replaced by a fresh one. If `consume_batch` gets called again, return
          # an empty array rather than using the wrong buffer variant.
          return ::Arrow.new_empty_array(@data_type)
        end

        len = @record_reader.values_written
        buffer = @record_reader.consume_record_data
        null_buffer = Arrow.null_buffer_from(@record_reader.consume_compact_bitmap, len)

        array =
          case buffer_type = @buffer_type
          when nil
            buffer.into_array(null_buffer, @data_type)
          else
            Arrow.convert_values_to_view(K, buffer.into_array(null_buffer, buffer_type), @data_type)
          end

        @record_reader.reset
        array
      end

      def def_levels : Array(Int16)?
        @def_levels_buffer
      end

      def rep_levels : Array(Int16)?
        @rep_levels_buffer
      end

      def max_def_level : Int16
        @record_reader.max_def_level
      end
    end

    # Converts a dictionary array with string or binary typed values to one with
    # string or binary view typed values (upstream `convert_values_to_view`).
    #
    # `K` is the dictionary key type (arrow-cr has no `AnyDictionaryArray`, so
    # the caller passes the key type explicitly). The result's data type is
    # rebuilt from the converted values, since arrow-cr's
    # `DictionaryArray#with_values` keeps the stale data type.
    def self.convert_values_to_view(
      type : K.class,
      array : ::Arrow::ArrayBase,
      to_type : ::Arrow::DataType,
    ) : ::Arrow::ArrayBase forall K
      to_value_type = to_type.value
      unless to_type.kind.dictionary? && to_value_type
        raise general_err!(
          "Cannot convert #{array.data_type} dictionary values to non-dictionary type #{to_type}"
        )
      end

      dict = array.as(::Arrow::DictionaryArray(K))
      values = dict.values

      new_values =
        case to_value_type.kind
        when .utf8_view?
          strings = values.as(::Arrow::GenericStringArray(Int32))
          options = Array(String?).new(strings.len) do |i|
            strings.valid?(i) ? strings.value(i) : nil
          end
          ::Arrow::StringViewArray.from_options(options).as(::Arrow::ArrayBase)
        when .binary_view?
          binaries = values.as(::Arrow::GenericBinaryArray(Int32))
          options = Array(Bytes?).new(binaries.len) do |i|
            binaries.valid?(i) ? binaries.value(i) : nil
          end
          ::Arrow::BinaryViewArray.from_options(options).as(::Arrow::ArrayBase)
        else
          raise general_err!("Cannot convert dictionary values to #{to_value_type}")
        end

      keys = dict.keys
      ::Arrow::DictionaryArray(K).new(
        ::Arrow::DataType.dictionary(keys.data_type, new_values.data_type),
        keys, new_values, false
      ).as(::Arrow::ArrayBase)
    end

    # If the data is dictionary encoded, decodes the key data directly so the
    # dictionary encoding can be preserved; otherwise the values are decoded into
    # the (spilled) values buffer and a fresh dictionary is computed when the
    # batch is converted to arrow (upstream `MaybeDictionaryDecoder`).
    abstract class MaybeDictionaryDecoder
    end

    # `RLE_DICTIONARY`/`PLAIN_DICTIONARY` data pages: dictionary indices are
    # decoded straight into the record reader's keys buffer.
    class DictionaryIndexDecoder < MaybeDictionaryDecoder
      getter decoder : Encodings::RleDecoder
      # A maximum, as the null count is not always known (e.g. value data from a
      # v1 data page).
      property max_remaining_values : Int32

      def initialize(@decoder : Encodings::RleDecoder, @max_remaining_values : Int32)
      end
    end

    # Any other encoding falls back to decoding the values and recomputing a
    # dictionary.
    class FallbackByteArrayDecoder < MaybeDictionaryDecoder
      getter decoder : Column::Reader::ColumnValueDecoderImpl(Parquet::ByteArray)
      # Whether the page carried no value data at all.
      #
      # Upstream's `ByteArrayDecoder` variants stop at the end of the page
      # buffer, so a page with no value bytes decodes to no values. The shared
      # `ColumnValueDecoderImpl`/`PlainDecoder` instead reports "not enough
      # bytes" (it never reads past the values the page claims), so the empty
      # case is short-circuited here. A *truncated* (non-empty) page still
      # raises.
      getter? empty_data : Bool

      def initialize(
        @decoder : Column::Reader::ColumnValueDecoderImpl(Parquet::ByteArray),
        @empty_data : Bool,
      )
      end
    end

    # A `ColumnValueDecoder` for dictionary encoded variable length byte arrays
    # (upstream `DictionaryDecoder`).
    class DictionaryDecoder(K, V) < Column::Reader::ColumnValueDecoder
      @dict : ::Arrow::ArrayBase?
      @decoder : MaybeDictionaryDecoder?
      @validate_utf8 : Bool
      @value_type : ::Arrow::DataType

      def initialize(@descr : Schema::ColumnDescriptor)
        @validate_utf8 = @descr.converted_type == ConvertedType::UTF8
        @value_type = DictionaryDecoder.value_type_impl(V, @validate_utf8)
        @dict = nil
        @decoder = nil
      end

      # The arrow data type decoded from the parquet byte array type (`V` is
      # `Int32` for `BYTE_ARRAY` and `Int64` for `FIXED_LEN_BYTE_ARRAY`/large
      # byte arrays).
      def self.value_type_impl(type : V.class, validate_utf8 : Bool) : ::Arrow::DataType forall V
        {% if V == Int64 %}
          validate_utf8 ? ::Arrow::DataType.large_utf8 : ::Arrow::DataType.large_binary
        {% else %}
          validate_utf8 ? ::Arrow::DataType.utf8 : ::Arrow::DataType.binary
        {% end %}
      end

      # The arrow data type of the decoded values.
      getter value_type : ::Arrow::DataType

      def set_dict(buf : Bytes, num_values : UInt32, encoding : Encoding, is_sorted : Bool) : Nil
        unless encoding == Encoding::PLAIN || encoding == Encoding::RLE_DICTIONARY || encoding == Encoding::PLAIN_DICTIONARY
          raise nyi_err!("Invalid/Unsupported encoding type for dictionary: #{encoding}")
        end

        unless fits_index_type?(num_values.to_i32)
          raise general_err!("dictionary too large for index type")
        end

        len = num_values.to_i32
        buffer = Buffer::OffsetBuffer(V).new

        # Upstream decodes the dictionary page with `ByteArrayDecoderPlain`,
        # which stops at the end of the page buffer: an empty dictionary page
        # (e.g. an all-null column) decodes to no values. The shared
        # `ColumnValueDecoderImpl(ByteArray)` instead reports "not enough bytes",
        # so the empty case is short-circuited (a truncated page still raises).
        unless buf.empty?
          decoder = Column::Reader::ColumnValueDecoderImpl(Parquet::ByteArray).new(@descr)
          decoder.set_data(Encoding::PLAIN, buf, len, len)

          values = [] of Parquet::ByteArray
          decoder.read(values, len)
          values.each { |value| buffer.try_push(value.data, @validate_utf8) }
        end

        @dict = buffer.into_array(nil, @value_type)
        _ = is_sorted
        nil
      end

      def set_data(encoding : Encoding, data : Bytes, num_levels : Int32, num_values : Int32?) : Nil
        decoder =
          case encoding
          when Encoding::RLE_DICTIONARY, Encoding::PLAIN_DICTIONARY
            bit_width = data[0]? || raise general_err!("dictionary index page is empty")
            if bit_width > Encodings::MAX_RLE_DICTIONARY_BIT_WIDTH
              raise general_err!(
                "Invalid or corrupted RLE bit width #{bit_width}. Max allowed is #{Encodings::MAX_RLE_DICTIONARY_BIT_WIDTH}"
              )
            end

            index_decoder = Encodings::RleDecoder.new(bit_width.to_i32)
            index_decoder.set_data(data[1..])
            DictionaryIndexDecoder.new(index_decoder, num_values || num_levels).as(MaybeDictionaryDecoder)
          else
            fallback = Column::Reader::ColumnValueDecoderImpl(Parquet::ByteArray).new(@descr)
            fallback.set_data(encoding, data, num_levels, num_values)
            FallbackByteArrayDecoder.new(fallback, data.empty?).as(MaybeDictionaryDecoder)
          end

        @decoder = decoder
        nil
      end

      def read(out_buffer : Buffer::DictionaryBuffer(K, V), num_values : Int32) : Int32
        decoder = @decoder || raise general_err!("decoder set")

        case decoder
        when FallbackByteArrayDecoder
          return 0 if decoder.empty_data?

          values = out_buffer.spill_values
          initial = values.values.size

          scratch = [] of Parquet::ByteArray
          read = decoder.decoder.read(scratch, num_values)
          scratch.each { |value| values.try_push(value.data, @validate_utf8) }
          values.check_valid_utf8(initial) if @validate_utf8
          read
        when DictionaryIndexDecoder
          len = Math.min(num_values, decoder.max_remaining_values)

          dict = @dict || raise general_err!("missing dictionary page for column")
          if dict.len == 0
            return 0 # All data must be NULL
          end

          if keys = out_buffer.as_keys(dict)
            # Happy path - just copy the keys.
            # Keys will be validated on conversion to arrow.
            scratch = Array(K).new(len) { K.new(0) }
            read = decoder.decoder.get_batch(K, scratch)
            keys.concat(scratch[0, read])
            decoder.max_remaining_values -= read
            read
          else
            # Sad path - need to recompute the dictionary.
            #
            # This either means we crossed into a new column chunk whilst reading
            # this batch, or encountered non-dictionary encoded data.
            values = out_buffer.spill_values
            scratch = Array(K).new(len) { K.new(0) }
            read = decoder.decoder.get_batch(K, scratch)

            dict_offsets, dict_values = Buffer::DictionaryBuffer.offsets_and_values(V, dict)
            values.extend_from_dictionary(scratch[0, read], dict_offsets, dict_values)
            decoder.max_remaining_values -= read
            read
          end
        else
          raise general_err!("unexpected dictionary decoder")
        end
      end

      def skip_values(num_values : Int32) : Int32
        decoder = @decoder || raise general_err!("decoder set")

        case decoder
        when FallbackByteArrayDecoder
          return 0 if decoder.empty_data?

          decoder.decoder.skip_values(num_values)
        when DictionaryIndexDecoder
          count = Math.min(num_values, decoder.max_remaining_values)
          decoder.max_remaining_values -= count
          decoder.decoder.skip(count)
        else
          raise general_err!("unexpected dictionary decoder")
        end
      end

      # Whether `num_values` dictionary entries fit in the key type `K`
      # (upstream `K::from_usize(num_values).is_none()`).
      private def fits_index_type?(num_values : Int32) : Bool
        K.new(num_values)
        true
      rescue OverflowError
        false
      end
    end
  end
end
