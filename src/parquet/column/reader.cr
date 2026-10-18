require "../basic"
require "../errors"
require "../schema/types"
require "../encodings/registry"
require "../util/bit_util"
require "./page"
require "./reader/decoder"
require "../arrow/record_reader/definition_levels"

# Ported from `parquet/src/column/reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the `ColumnReader` dispatch (`get_column_reader`/
# `get_typed_column_reader`) and the typed `ColumnReaderImpl(B, CV)` (upstream
# `GenericColumnReader<R, D, V>`).
#
# Divergences: the Rust `ColumnReader` enum is an abstract `ColumnReader` class
# with the single generic `ColumnReaderImpl(B, CV)` subclass (instead of one
# variant per physical type); `GenericColumnReader<R, D, V>`'s repetition
# (`R`) and definition (`D`) decoders are fixed to the ported
# `RepetitionLevelDecoderImpl` and the classic-`DefinitionLevelDecoder`/
# arrow-`DefinitionLevelBufferDecoder` pair; the class is parameterized by the
# values **buffer** (`B`) and the value decoder (`CV`) rather than by the
# physical type, which is what lets the arrow record reader drive it with the
# `OffsetBuffer`/`DictionaryBuffer` value buffers; the classic
# `GenericColumnReader::new` type-parameter inference is replaced by the
# `column_reader(type, ...)` helper (Crystal cannot infer a method type
# parameter that only appears in the return type); buffer lengths are `Int32`.
# The arrow record reader's pluggable decoder path (`new_with_decoders` +
# `read_records_with_reservation`) accepts an explicit values decoder plus the
# arrow definition-level buffer/decoder and a reservation callback.
module Parquet
  module Column
    # Column reader for a Parquet column chunk.
    abstract class ColumnReader
    end

    # Typed value reader for a particular primitive column.
    class ColumnReaderImpl(B, CV) < ColumnReader
      @arrow_def_level_buffer : Arrow::RecordReader::DefinitionLevelBuffer?
      @arrow_def_level_decoder : Arrow::RecordReader::DefinitionLevelBufferDecoder?

      # Creates a reader over the given decoders (upstream
      # `GenericColumnReader::new`/`new_with_decoders`).
      #
      # `arrow_def_level_buffer`/`arrow_def_level_decoder` replace the classic
      # definition-level decoder for the arrow record reader; they are `nil` for
      # the classic path, as are the classic decoders for columns whose maximum
      # definition/repetition level is 0.
      def initialize(
        @descr : Schema::ColumnDescriptor,
        @page_reader : PageReader,
        @values_decoder : CV,
        @def_level_decoder : Reader::DefinitionLevelDecoder? = nil,
        @rep_level_decoder : Reader::RepetitionLevelDecoder? = nil,
        @arrow_def_level_buffer : Arrow::RecordReader::DefinitionLevelBuffer? = nil,
        @arrow_def_level_decoder : Arrow::RecordReader::DefinitionLevelBufferDecoder? = nil,
      )
        @num_buffered_values = 0
        @num_decoded_values = 0
        @has_record_delimiter = false
      end

      # Creates a reader that decodes definition levels into an arrow
      # `DefinitionLevelBuffer` (upstream
      # `GenericColumnReader::new_with_decoders`).
      #
      # `def_level_buffer`/`def_level_buffer_decoder` are `nil` for columns whose
      # maximum definition level is 0.
      def self.new_with_decoders(
        descr : Schema::ColumnDescriptor,
        page_reader : PageReader,
        values_decoder : CV,
        def_level_buffer : Arrow::RecordReader::DefinitionLevelBuffer?,
        def_level_buffer_decoder : Arrow::RecordReader::DefinitionLevelBufferDecoder?,
      ) : ColumnReaderImpl(B, CV)
        new(
          descr, page_reader, values_decoder,
          rep_level_decoder: (descr.max_rep_level != 0) ? Reader::RepetitionLevelDecoderImpl.new(descr.max_rep_level) : nil,
          arrow_def_level_buffer: def_level_buffer,
          arrow_def_level_decoder: def_level_buffer_decoder,
        )
      end

      # Read up to `max_records` whole records.
      #
      # Returns `(records_read, values_read, levels_read)`.
      def read_records(
        max_records : Int32,
        def_levels : Array(Int16)?,
        rep_levels : Array(Int16)?,
        values : B,
      ) : Tuple(Int32, Int32, Int32)
        read_records_with_reservation(max_records, def_levels, rep_levels, values) do |_values, _values_to_read, _levels_to_read, _def_levels|
        end
      end

      # Read up to `max_records` whole records, invoking `reserve_values` once
      # the level decoders know how many output slots a batch needs (upstream
      # `GenericColumnReader::read_records_with_reservation`).
      #
      # The callback receives the values buffer, the number of values/levels to
      # read, and the arrow definition level buffer when one is in use.
      #
      # Returns `(records_read, values_read, levels_read)`.
      # ameba:disable Metrics/CyclomaticComplexity
      def read_records_with_reservation(
        max_records : Int32,
        def_levels : Array(Int16)?,
        rep_levels : Array(Int16)?,
        values : B,
        &
      ) : Tuple(Int32, Int32, Int32)
        total_records_read = 0
        total_levels_read = 0
        total_values_read = 0

        while total_records_read < max_records && has_next
          remaining_records = max_records - total_records_read
          remaining_levels = @num_buffered_values - @num_decoded_values

          records_read, levels_to_read =
            if reader = @rep_level_decoder
              dest = rep_levels || raise general_err!("must specify repetition levels")
              records, levels = reader.read_rep_levels(dest, remaining_records, remaining_levels)

              if records == 0 && levels == 0
                raise general_err!("Insufficient repetition levels read from column")
              end
              if levels == remaining_levels && @has_record_delimiter
                Column.check_partial_record_fits(records, remaining_records)
                records += reader.flush_partial ? 1 : 0
              end
              {records, levels}
            else
              min = Math.min(remaining_records, remaining_levels)
              {min, min}
            end

          arrow_buffer = @arrow_def_level_buffer
          values_to_read =
            if reader = @arrow_def_level_decoder
              buffer = arrow_buffer || raise general_err!("must specify definition levels")
              values_read, levels_read = reader.read_def_levels(buffer, levels_to_read)
              if levels_read != levels_to_read
                raise general_err!(
                  "insufficient definition levels read from column - expected #{levels_to_read}, got #{levels_read}"
                )
              end
              values_read
            elsif reader = @def_level_decoder
              dest = def_levels || raise general_err!("must specify definition levels")
              values_read, levels_read = reader.read_def_levels(dest, levels_to_read)
              if levels_read != levels_to_read
                raise general_err!(
                  "insufficient definition levels read from column - expected #{levels_to_read}, got #{levels_read}"
                )
              end
              values_read
            else
              levels_to_read
            end

          yield values, values_to_read, levels_to_read, arrow_buffer

          values_read = @values_decoder.read(values, values_to_read)
          if values_read != values_to_read
            raise general_err!(
              "insufficient values read from column - expected: #{values_to_read}, got: #{values_read}"
            )
          end

          @num_decoded_values += levels_to_read
          total_records_read += records_read
          total_levels_read += levels_to_read
          total_values_read += values_read
        end

        {total_records_read, total_values_read, total_levels_read}
      end

      # Skips over `num_records` records, returning the number skipped.
      # ameba:disable Metrics/CyclomaticComplexity
      def skip_records(num_records : Int32) : Int32
        remaining_records = num_records

        while remaining_records != 0
          if @num_buffered_values == @num_decoded_values
            metadata = @page_reader.peek_next_page
            return num_records - remaining_records if metadata.nil?

            if metadata.is_dict
              read_dictionary_page
              next
            end

            rows = metadata.num_rows
            rows = metadata.num_levels if rows.nil? && @rep_level_decoder.nil?

            if !rows.nil? && rows <= remaining_records
              @page_reader.skip_next_page
              remaining_records -= rows
              next
            end

            return num_records - remaining_records unless read_new_page
          end

          remaining_levels = @num_buffered_values - @num_decoded_values

          records_read, rep_levels_read =
            if decoder = @rep_level_decoder
              records, levels = decoder.skip_rep_levels(remaining_records, remaining_levels)
              if levels == remaining_levels && @has_record_delimiter
                Column.check_partial_record_fits(records, remaining_records)
                records += decoder.flush_partial ? 1 : 0
              end
              {records, levels}
            else
              levels = Math.min(remaining_levels, remaining_records)
              {levels, levels}
            end

          @num_decoded_values += rep_levels_read
          remaining_records -= records_read

          next if @num_buffered_values == @num_decoded_values

          values_read, def_levels_read =
            if decoder = @def_level_decoder
              decoder.skip_def_levels(rep_levels_read)
            else
              {rep_levels_read, rep_levels_read}
            end

          if rep_levels_read != def_levels_read
            raise general_err!(
              "levels mismatch, read #{rep_levels_read} repetition levels and #{def_levels_read} definition levels"
            )
          end

          values = @values_decoder.skip_values(values_read)
          if values != values_read
            raise general_err!("skipped #{values} values, expected #{values_read}")
          end
        end

        num_records - remaining_records
      end

      # Returns whether there is more data to read from this column.
      def has_next : Bool
        if @num_buffered_values == 0 || @num_buffered_values == @num_decoded_values
          return false unless read_new_page

          @num_buffered_values != 0
        else
          true
        end
      end

      private def read_dictionary_page : Nil
        case page = @page_reader.get_next_page
        when Page::DictionaryPage
          @values_decoder.set_dict(page.buf, page.num_values, page.encoding, page.is_sorted)
        else
          raise general_err!("Invalid page. Expecting dictionary page")
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def read_new_page : Bool
        loop do
          page = @page_reader.get_next_page
          return false if page.nil?

          case page
          when Page::DictionaryPage
            @values_decoder.set_dict(page.buf, page.num_values, page.encoding, page.is_sorted)
          when Page::DataPage
            @num_buffered_values = page.num_values.to_i32
            @num_decoded_values = 0

            max_rep_level = @descr.max_rep_level
            max_def_level = @descr.max_def_level
            offset = 0

            if max_rep_level > 0
              bytes_read, level_data = Column.parse_v1_level(
                max_rep_level,
                page.num_values,
                page.rep_level_encoding,
                page.buf[offset, page.buf.size - offset],
              )
              offset += bytes_read
              @has_record_delimiter = @page_reader.at_record_boundary
              (@rep_level_decoder || raise general_err!("repetition level decoder is not initialized"))
                .set_data(page.rep_level_encoding, level_data)
            end

            if max_def_level > 0
              bytes_read, level_data = Column.parse_v1_level(
                max_def_level,
                page.num_values,
                page.def_level_encoding,
                page.buf[offset, page.buf.size - offset],
              )
              offset += bytes_read
              if decoder = @arrow_def_level_decoder
                decoder.set_data(page.def_level_encoding, level_data)
              else
                (@def_level_decoder || raise general_err!("definition level decoder is not initialized"))
                  .set_data(page.def_level_encoding, level_data)
              end
            end

            @values_decoder.set_data(
              page.encoding,
              page.buf[offset, page.buf.size - offset],
              page.num_values.to_i32,
              nil,
            )
            return true
          when Page::DataPageV2
            if page.num_nulls > page.num_values
              raise general_err!(
                "more nulls than values in page, contained #{page.num_values} values and #{page.num_nulls} nulls"
              )
            end

            @num_buffered_values = page.num_values.to_i32
            @num_decoded_values = 0

            if @descr.max_rep_level > 0
              @has_record_delimiter = @page_reader.at_record_boundary
              (@rep_level_decoder || raise general_err!("repetition level decoder is not initialized"))
                .set_data(Encoding::RLE, page.buf[0, page.rep_levels_byte_len.to_i32])
            end

            if @descr.max_def_level > 0
              if decoder = @arrow_def_level_decoder
                decoder.set_data(
                  Encoding::RLE,
                  page.buf[page.rep_levels_byte_len.to_i32, page.def_levels_byte_len.to_i32],
                )
              else
                (@def_level_decoder || raise general_err!("definition level decoder is not initialized"))
                  .set_data(
                    Encoding::RLE,
                    page.buf[page.rep_levels_byte_len.to_i32, page.def_levels_byte_len.to_i32],
                  )
              end
            end

            value_offset = (page.rep_levels_byte_len + page.def_levels_byte_len).to_i32
            @values_decoder.set_data(
              page.encoding,
              page.buf[value_offset, page.buf.size - value_offset],
              page.num_values.to_i32,
              (page.num_values - page.num_nulls).to_i32,
            )
            return true
          end
        end
      end
    end

    # Gets a column reader for `descr`, reading pages from `col_page_reader`.
    def self.get_column_reader(descr : Schema::ColumnDescriptor, col_page_reader : PageReader) : ColumnReader
      case descr.physical_type
      when Parquet::Type::BOOLEAN              then column_reader(Bool, descr, col_page_reader)
      when Parquet::Type::INT32                then column_reader(Int32, descr, col_page_reader)
      when Parquet::Type::INT64                then column_reader(Int64, descr, col_page_reader)
      when Parquet::Type::INT96                then column_reader(Int96, descr, col_page_reader)
      when Parquet::Type::FLOAT                then column_reader(Float32, descr, col_page_reader)
      when Parquet::Type::DOUBLE               then column_reader(Float64, descr, col_page_reader)
      when Parquet::Type::BYTE_ARRAY           then column_reader(ByteArray, descr, col_page_reader)
      when Parquet::Type::FIXED_LEN_BYTE_ARRAY then column_reader(FixedLenByteArray, descr, col_page_reader)
      else
        raise general_err!("Cannot get column reader for physical type #{descr.physical_type}")
      end
    end

    # Creates a `ColumnReaderImpl` with the classic decoders for the physical
    # value type `T` (upstream `GenericColumnReader::new`).
    #
    # Crystal cannot infer a method type parameter that only appears in the
    # return type, so the classic path is driven by an explicit value type
    # rather than the class's own type arguments.
    def self.column_reader(
      type : T.class,
      descr : Schema::ColumnDescriptor,
      page_reader : PageReader,
    ) : ColumnReaderImpl(Array(T), Reader::ColumnValueDecoderImpl(T)) forall T
      ColumnReaderImpl(Array(T), Reader::ColumnValueDecoderImpl(T)).new(
        descr,
        page_reader,
        Reader::ColumnValueDecoderImpl(T).new(descr),
        (descr.max_def_level != 0) ? Reader::DefinitionLevelDecoderImpl.new(descr.max_def_level) : nil,
        (descr.max_rep_level != 0) ? Reader::RepetitionLevelDecoderImpl.new(descr.max_rep_level) : nil,
      )
    end

    # Up-casts `col_reader` to a typed `ColumnReaderImpl` (upstream
    # `get_typed_column_reader`).
    def self.get_typed_column_reader(
      type : T.class,
      col_reader : ColumnReader,
    ) : ColumnReaderImpl(Array(T), Reader::ColumnValueDecoderImpl(T)) forall T
      col_reader.as(ColumnReaderImpl(Array(T), Reader::ColumnValueDecoderImpl(T)))
    rescue TypeCastError
      raise general_err!(
        "Failed to convert column reader into a typed column reader for `#{Encodings.physical_type(T)}` type"
      )
    end

    # Checks that a partial record can be flushed into the caller's record budget.
    def self.check_partial_record_fits(records_read : Int32, remaining_records : Int32) : Nil
      if remaining_records <= records_read
        raise general_err!(
          "page ended after #{records_read} record(s), which is already all of the " \
          "#{remaining_records} record(s) asked for, so there is no partial record to flush"
        )
      end
    end

    # Parses the level data of a Data Page v1, returning `(bytes_read, data)`.
    def self.parse_v1_level(max_level : Int16, num_buffered_values : UInt32, encoding : Encoding, buf : Bytes) : Tuple(Int32, Bytes)
      case encoding
      when Encoding::RLE
        i32_size = 4
        if i32_size <= buf.size
          data_size = Util::BitUtil.read_num_bytes(Int32, i32_size, buf)
          if data_size < 0
            raise general_err!("invalid level length")
          end
          finish = i32_size + data_size
          if finish <= buf.size
            return {finish, buf[i32_size, data_size]}
          end
        end
        raise general_err!("not enough data to read levels")
      when Encoding::BIT_PACKED
        bit_width = Util::BitUtil.num_required_bits(max_level.to_u64)
        num_bytes = Util::BitUtil.ceil((num_buffered_values * bit_width).to_i64, 8_i64).to_i32
        {num_bytes, buf[0, num_bytes]}
      else
        raise general_err!("invalid level encoding: #{encoding}")
      end
    end
  end
end
