require "digest/crc32"
require "../errors"
require "../thrift"
require "../basic"
require "../format"
require "../compression_codecs"
require "./metadata/column_chunk"
require "./properties"
require "./statistics"
require "./chunk_reader"
require "../column/page"
require "../encryption/decrypt"

# Ported from `parquet/src/file/serialized_reader.rs` (`decode_page` and
# `SerializedPageReader`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the serialized page reader (the offset-stream state) and `decode_page`.
# Divergence: the offset-index-driven `Pages` state is not ported; `R:
# ChunkReader` is a concrete generic parameter. The `crc`-feature page-checksum
# verification is always performed (Crystal has no feature flags). Page
# decryption is also wired through the core `SerializedRowGroupReader` (upstream
# only attaches the crypto context in the arrow reader).
module Parquet
  module File
    # Decodes a `Page` from the provided `buffer`.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.decode_page(
      page_header : Format::PageHeader,
      buffer : Bytes,
      physical_type : Parquet::Type,
      decompressor : CompressionCodecs::Codec?,
    ) : Column::Page
      if expected_crc = page_header.crc
        # Upstream compares `crc::checksum(buffer) != expected_crc as u32`: the
        # thrift field is a signed `i32`, so the conversion truncates rather
        # than raising on negative checksums (which occur for real files).
        if Digest::CRC32.checksum(buffer) != expected_crc.to_u32!
          raise general_err!("Page CRC checksum mismatch")
        end
      end

      offset = 0
      can_decompress = true

      if header_v2 = page_header.data_page_header_v2
        def_len = header_v2.definition_levels_byte_length
        rep_len = header_v2.repetition_levels_byte_length
        if def_len < 0 || rep_len < 0 || def_len + rep_len > page_header.uncompressed_page_size
          raise general_err!(
            "DataPage v2 header contains implausible values for definition_levels_byte_length " \
            "(#{def_len}) and repetition_levels_byte_length (#{rep_len}) given DataPage header " \
            "provides uncompressed_page_size (#{page_header.uncompressed_page_size})"
          )
        end
        offset = def_len + rep_len
        compressed_flag = header_v2.is_compressed
        can_decompress = compressed_flag.nil? ? true : compressed_flag
      end

      if (dec = decompressor) && can_decompress
        uncompressed_page_size = page_header.uncompressed_page_size
        if offset > buffer.size || offset > uncompressed_page_size
          raise general_err!("Invalid page header")
        end

        decompressed_size = uncompressed_page_size - offset
        decompressed = [] of UInt8
        decompressed.concat(buffer[0, offset])
        if decompressed_size > 0
          dec.decompress(buffer[offset, buffer.size - offset], decompressed, decompressed_size)
        end

        if decompressed.size != uncompressed_page_size
          raise general_err!(
            "Actual decompressed size doesn't match the expected one (#{decompressed.size} vs #{uncompressed_page_size})"
          )
        end
        buffer = Slice.new(decompressed.to_unsafe, decompressed.size).dup
      end

      case page_header.type
      when PageType::DICTIONARY_PAGE
        dict_header = page_header.dictionary_page_header || raise general_err!("Missing dictionary page header")
        sorted_flag = dict_header.is_sorted
        is_sorted = sorted_flag.nil? ? false : sorted_flag
        Column::Page::DictionaryPage.new(buffer, dict_header.num_values.to_u32!, dict_header.encoding, is_sorted)
      when PageType::DATA_PAGE
        header = page_header.data_page_header || raise general_err!("Missing V1 data page header")
        Column::Page::DataPage.new(
          buffer,
          header.num_values.to_u32!,
          header.encoding,
          header.definition_level_encoding,
          header.repetition_level_encoding,
          Statistics.from_thrift_page_stats(physical_type, header.statistics),
        )
      when PageType::DATA_PAGE_V2
        header = page_header.data_page_header_v2 || raise general_err!("Missing V2 data page header")
        compressed_flag = header.is_compressed
        is_compressed = compressed_flag.nil? ? true : compressed_flag
        Column::Page::DataPageV2.new(
          buffer,
          header.num_values.to_u32!,
          header.encoding,
          header.num_nulls.to_u32!,
          header.num_rows.to_u32!,
          header.definition_levels_byte_length.to_u32!,
          header.repetition_levels_byte_length.to_u32!,
          is_compressed,
          Statistics.from_thrift_page_stats(physical_type, header.statistics),
        )
      else
        raise general_err!("Page type #{page_header.type} is not supported")
      end
    end

    # An `IO` wrapper that counts the bytes read through it.
    private class CountingIO < IO
      getter count : Int32

      def initialize(@io : IO)
        @count = 0
      end

      def read(slice : Bytes) : Int32
        read = @io.read(slice)
        @count += read if read > 0
        read
      end

      def write(slice : Bytes) : Nil
        @io.write(slice)
      end
    end

    # A serialized implementation of `PageReader` for a column chunk.
    #
    # Yields either the sequential offset-stream pages (`Values`) or the pages
    # named by the column chunk's offset index (`Pages`), mirroring upstream
    # `SerializedPageReaderState`.
    class SerializedPageReader(R) < Column::PageReader
      include Iterator(Column::Page)

      # Reader state (upstream `SerializedPageReaderState`).
      abstract class State
      end

      # Walks the column chunk's page headers sequentially.
      class Values < State
        # The current byte offset in the reader.
        property offset : UInt64
        # The number of bytes left in the column chunk.
        property remaining_bytes : UInt64
        # A page header that was already read by `peek_next_page`.
        property next_page_header : Format::PageHeader?
        # The index of the next data page within this column chunk.
        property page_index : Int32
        # Whether the next page is expected to be a dictionary page.
        # ameba:disable Naming/QueryBoolMethods
        property require_dictionary : Bool

        def initialize(@offset : UInt64, @remaining_bytes : UInt64, @page_index : Int32, @require_dictionary : Bool)
          @next_page_header = nil
        end
      end

      # Consumes the column chunk's offset-index `PageLocation` queue.
      class Pages < State
        # Remaining page locations.
        getter page_locations : Deque(Metadata::PageLocation)
        # The synthesized dictionary page location, if the chunk has one.
        property dictionary_page : Metadata::PageLocation?
        # The total number of rows in this column chunk.
        getter total_rows : Int32
        # The index of the next data page within this column chunk.
        property page_index : Int32

        def initialize(
          @page_locations : Deque(Metadata::PageLocation),
          @dictionary_page : Metadata::PageLocation?,
          @total_rows : Int32,
          @page_index : Int32,
        )
        end
      end

      @state : State

      def initialize(
        @reader : R,
        @decompressor : CompressionCodecs::Codec?,
        @physical_type : Parquet::Type,
        @state : State,
        @read_stats : Bool,
      )
        @crypto_context = nil.as(Encryption::CryptoContext?)
      end

      # Adds the crypto context needed to decrypt this column's pages.
      def add_crypto_context(
        rg_idx : Int32,
        column_idx : Int32,
        parquet_meta_data : Metadata::ParquetMetaData,
        column_chunk_metadata : Metadata::ColumnChunkMetaData,
      ) : self
        file_decryptor = parquet_meta_data.file_decryptor
        return self unless file_decryptor

        crypto_metadata = column_chunk_metadata.crypto_metadata
        return self unless crypto_metadata

        @crypto_context = Encryption::CryptoContext.for_column(
          file_decryptor, crypto_metadata, rg_idx, column_idx
        )
        self
      end

      # Creates a reader for a column chunk.
      #
      # When `page_locations` (the chunk's offset index) is given, pages are read
      # by location instead of walking the offset stream.
      def self.new_for_column(
        reader : R,
        meta : Metadata::ColumnChunkMetaData,
        total_rows : Int32,
        page_locations : Array(Metadata::PageLocation)?,
        props : Properties::ReaderProperties,
      ) : SerializedPageReader(R) forall R
        decompressor = CompressionCodecs.create_codec(Compression.from_codec(meta.compression), props.codec_options)
        start = (meta.dictionary_page_offset || meta.data_page_offset).to_u64
        length = meta.total_compressed_size.to_u64

        state =
          if locations = page_locations
            # If the offset of the first page doesn't match the start of the
            # column chunk then the preceding space must contain a dictionary
            # page.
            dictionary_page =
              if first = locations.first?
                unless first.offset.to_u64 == start
                  Metadata::PageLocation.new(start.to_i64, (first.offset.to_u64 - start).to_i32, 0_i64)
                end
              end

            Pages.new(Deque(Metadata::PageLocation).new(locations), dictionary_page, total_rows, 0)
          else
            Values.new(start, length, 0, !meta.dictionary_page_offset.nil?)
          end

        new(reader, decompressor, meta.column_type, state, props.read_page_stats)
      end

      # ameba:disable Naming/AccessorMethodName
      def get_next_page : Column::Page?
        case state = @state
        when Values
          loop do
            return nil if state.remaining_bytes == 0

            read = @reader.get_read(state.offset)
            header =
              if buffered = state.next_page_header
                state.next_page_header = nil
                buffered
              else
                header_len, parsed = read_page_header_len(read, state.page_index, state.require_dictionary)
                verify_page_header_len(header_len, state.remaining_bytes)
                state.offset += header_len.to_u64
                state.remaining_bytes -= header_len.to_u64
                parsed
              end

            verify_page_size(header.compressed_page_size, header.uncompressed_page_size, state.remaining_bytes)

            data_len = header.compressed_page_size
            data_start = state.offset
            state.offset += data_len.to_u64
            state.remaining_bytes -= data_len.to_u64

            next if header.type == PageType::INDEX_PAGE

            buffer = @reader.get_bytes(data_start, data_len)
            buffer = decrypt_page_data(buffer, state.page_index, state.require_dictionary)
            page = File.decode_page(header, buffer, @physical_type, @decompressor)
            if page.is_data_page?
              state.page_index += 1
            elsif page.is_dictionary_page?
              state.require_dictionary = false
            end
            return page
          end
        when Pages
          front, is_dictionary_page =
            if dictionary = state.dictionary_page
              state.dictionary_page = nil
              {dictionary, true}
            else
              case location = state.page_locations.shift?
              when Nil
                return nil
              else
                {location, false}
              end
            end

          buffer = @reader.get_bytes(front.offset.to_u64, front.compressed_page_size)
          header_len, header = read_page_header_len_from_bytes(buffer, state.page_index, is_dictionary_page)
          buffer = buffer[header_len, buffer.size - header_len]
          buffer = decrypt_page_data(buffer, state.page_index, is_dictionary_page)

          state.page_index += 1 unless is_dictionary_page

          File.decode_page(header, buffer, @physical_type, @decompressor)
        else
          raise general_err!("unknown serialized page reader state")
        end
      end

      # Upstream `impl Iterator for SerializedPageReader` (`Item =
      # Result<Page>`). Crystal's iterator protocol uses `Iterator::Stop`
      # instead of `Option`, and errors raise.
      def next : Column::Page | Iterator::Stop
        get_next_page || Iterator::Stop::INSTANCE
      end

      def peek_next_page : Column::PageMetadata?
        case state = @state
        when Values
          loop do
            return nil if state.remaining_bytes == 0

            if buffered = state.next_page_header
              return Column::PageMetadata.from_thrift(buffered)
            end

            read = @reader.get_read(state.offset)
            header_len, header = read_page_header_len(read, state.page_index, state.require_dictionary)
            verify_page_header_len(header_len, state.remaining_bytes)
            state.offset += header_len.to_u64
            state.remaining_bytes -= header_len.to_u64

            next if header.type == PageType::INDEX_PAGE

            state.next_page_header = header
            return Column::PageMetadata.from_thrift(header)
          end
        when Pages
          if state.dictionary_page
            return Column::PageMetadata.new(nil, nil, true)
          end

          if front = state.page_locations.first?
            next_rows =
              if state.page_locations.size > 1
                state.page_locations[1].first_row_index
              else
                state.total_rows.to_i64
              end
            return Column::PageMetadata.new((next_rows - front.first_row_index).to_i32, nil, false)
          end

          nil
        else
          raise general_err!("unknown serialized page reader state")
        end
      end

      def skip_next_page : Nil
        case state = @state
        when Values
          if buffered = state.next_page_header
            verify_page_size(buffered.compressed_page_size, buffered.uncompressed_page_size, state.remaining_bytes)
            data_page_size = buffered.compressed_page_size.to_u64
            state.offset += data_page_size
            state.remaining_bytes -= data_page_size
            state.next_page_header = nil
          else
            read = @reader.get_read(state.offset)
            header_len, header = read_page_header_len(read, state.page_index, state.require_dictionary)
            verify_page_header_len(header_len, state.remaining_bytes)
            verify_page_size(header.compressed_page_size, header.uncompressed_page_size, state.remaining_bytes)
            data_page_size = header.compressed_page_size.to_u64
            state.offset += header_len.to_u64 + data_page_size
            state.remaining_bytes -= header_len.to_u64 + data_page_size
          end

          if state.require_dictionary
            state.require_dictionary = false
          else
            state.page_index += 1
          end
        when Pages
          if state.dictionary_page
            state.dictionary_page = nil
          elsif state.page_locations.shift?
            state.page_index += 1
          end
        else
          raise general_err!("unknown serialized page reader state")
        end
        nil
      end

      # Upstream: the offset-index state always starts at a record boundary; the
      # offset-stream state inspects the next page's metadata.
      def at_record_boundary : Bool
        case @state
        when Pages
          true
        else
          super
        end
      end

      # Returns the offset of the next page, if any.
      #
      # Upstream only compiles this for tests (`#[cfg(test)]`); the port exposes
      # it so a caller can tell whether the next page was already read.
      def peek_next_page_offset : UInt64?
        case state = @state
        when Values
          loop do
            return nil if state.remaining_bytes == 0

            if header = state.next_page_header
              return state.offset if page_metadata?(header)

              # For an unknown page type (e.g., INDEX_PAGE), skip and read next.
              state.next_page_header = nil
              next
            end

            read = @reader.get_read(state.offset)
            header_len, header = read_page_header_len(read, state.page_index, state.require_dictionary)
            state.offset += header_len.to_u64
            state.remaining_bytes -= header_len.to_u64

            next unless page_metadata?(header)

            state.next_page_header = header
            return state.offset
          end
        when Pages
          if page = state.dictionary_page
            page.offset.to_u64
          elsif page = state.page_locations.first?
            page.offset.to_u64
          end
        else
          raise general_err!("unknown serialized page reader state")
        end
      end

      private def page_metadata?(header : Format::PageHeader) : Column::PageMetadata?
        Column::PageMetadata.from_thrift(header)
      rescue Parquet::ParquetError
        nil
      end

      private def read_page_header_len(
        input : IO,
        page_index : Int32,
        dictionary_page : Bool,
      ) : Tuple(Int32, Format::PageHeader)
        counting = CountingIO.new(input)
        header = read_page_header(counting, page_index, dictionary_page)
        {counting.count, header}
      end

      private def read_page_header_len_from_bytes(
        buffer : Bytes,
        page_index : Int32,
        dictionary_page : Bool,
      ) : Tuple(Int32, Format::PageHeader)
        input = IO::Memory.new(buffer)
        header = read_page_header(input, page_index, dictionary_page)
        {input.pos.to_i32, header}
      end

      private def read_page_header(
        input : IO,
        page_index : Int32,
        dictionary_page : Bool,
      ) : Format::PageHeader
        if context = page_crypto_context(page_index, dictionary_page)
          buffer =
            begin
              Encryption.read_and_decrypt(
                context.data_decryptor, input, context.create_page_header_aad
              )
            rescue
              raise general_err!(
                "Error decrypting page header for column #{context.column_ordinal}, " \
                "decryption key may be wrong"
              )
            end
          protocol = Thrift::ThriftSliceInputProtocol.new(buffer)
          if @read_stats
            Format::PageHeader.read_thrift(protocol)
          else
            Format.read_page_header_without_stats(protocol)
          end
        else
          protocol = Thrift::ThriftReadInputProtocol.new(input)
          if @read_stats
            Format::PageHeader.read_thrift(protocol)
          else
            Format.read_page_header_without_stats(protocol)
          end
        end
      end

      private def decrypt_page_data(
        buffer : Bytes,
        page_index : Int32,
        dictionary_page : Bool,
      ) : Bytes
        return buffer unless context = page_crypto_context(page_index, dictionary_page)
        context.data_decryptor.decrypt(buffer, context.create_page_aad)
      end

      private def page_crypto_context(
        page_index : Int32,
        dictionary_page : Bool,
      ) : Encryption::CryptoContext?
        context = @crypto_context
        return nil unless context
        dictionary_page ? context.for_dictionary_page : context.with_page_ordinal(page_index)
      end

      private def verify_page_header_len(header_len : Int32, remaining_bytes : UInt64) : Nil
        if header_len.to_u64 > remaining_bytes
          raise eof_err!("Invalid page header")
        end
      end

      private def verify_page_size(compressed_size : Int32, uncompressed_size : Int32, remaining_bytes : UInt64) : Nil
        if compressed_size < 0 || compressed_size.to_u64 > remaining_bytes || uncompressed_size < 0
          raise eof_err!("Invalid page header")
        end
      end
    end
  end
end
