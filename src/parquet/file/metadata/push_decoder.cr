# Ported from `parquet/src/file/metadata/push_decoder.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the sans-IO `ParquetMetaDataPushDecoder` state machine and
# `range_for_page_index` (the latter lives in `Metadata`; see `parser.cr`).
# Divergences: `with_file_decryption_properties` routes the footer decode
# through `parquet_metadata_with_encryption`; the `try_new_with_*` constructors
# set state via protected setters rather than the Rust state-replacement idiom.
require "./thrift/encryption"

module Parquet
  module File
    module Metadata
      # A push (sans-IO) decoder for `ParquetMetaData`.
      #
      # Feed byte ranges via `push_range`/`push_ranges`, then call `try_decode`
      # in a loop: it returns `DecodeResult::NeedsData` (with the ranges to
      # fetch), `DecodeResult::Data` once decoded, or `DecodeResult::Finished`.
      class ParquetMetaDataPushDecoder
        enum State
          ReadingFooter
          ReadingMetadata
          ReadingPageIndex
          Finished
        end

        @state : State
        @footer_tail : FooterTail?
        @metadata : ParquetMetaData?
        @column_index_policy : PageIndexPolicy
        @offset_index_policy : PageIndexPolicy
        @buffers : Util::PushBuffers
        @metadata_parser : MetadataParser
        @metadata_options : ParquetMetaDataOptions?
        @file_decryption_properties : ::Parquet::Encryption::FileDecryptionProperties?

        # Creates a decoder for a file of the given length.
        def self.try_new(file_len : UInt64) : ParquetMetaDataPushDecoder
          if file_len < ::Parquet::File::FOOTER_SIZE
            raise general_err!(
              "Parquet files are at least #{::Parquet::File::FOOTER_SIZE} bytes long, but file length is #{file_len}"
            )
          end
          new(file_len)
        end

        # Begins decoding from a known footer tail.
        def self.try_new_with_footer_tail(file_len : UInt64, footer_tail : FooterTail) : ParquetMetaDataPushDecoder
          decoder = try_new(file_len)
          decoder.state = State::ReadingMetadata
          decoder.footer_tail = footer_tail
          decoder
        end

        # Creates a decoder with `metadata` already known (to populate page
        # indexes afterward).
        def self.try_new_with_metadata(file_len : UInt64, metadata : ParquetMetaData) : ParquetMetaDataPushDecoder
          decoder = try_new(file_len)
          decoder.state = State::ReadingPageIndex
          decoder.metadata = metadata
          decoder
        end

        def initialize(file_len : UInt64)
          @state = State::ReadingFooter
          @footer_tail = nil
          @metadata = nil
          @column_index_policy = PageIndexPolicy::Optional
          @offset_index_policy = PageIndexPolicy::Optional
          @buffers = Util::PushBuffers.new(file_len)
          @metadata_parser = MetadataParser.new
          @metadata_options = nil
          @file_decryption_properties = nil
        end

        protected setter state : State
        protected setter footer_tail : FooterTail?
        protected setter metadata : ParquetMetaData?

        # Sets the policy for both the column and offset index.
        def with_page_index_policy(policy : PageIndexPolicy) : self
          @column_index_policy = policy
          @offset_index_policy = policy
          self
        end

        # Sets the policy for the column index.
        def with_column_index_policy(policy : PageIndexPolicy) : self
          @column_index_policy = policy
          self
        end

        # Sets the policy for the offset index.
        def with_offset_index_policy(policy : PageIndexPolicy) : self
          @offset_index_policy = policy
          self
        end

        # Sets the options used when decoding the metadata.
        def with_metadata_options(options : ParquetMetaDataOptions?) : self
          @metadata_options = options
          @metadata_parser = @metadata_parser.with_metadata_options(options)
          self
        end

        # Sets the properties used to decrypt an encrypted footer / column metadata.
        def with_file_decryption_properties(
          properties : ::Parquet::Encryption::FileDecryptionProperties?,
        ) : self
          @file_decryption_properties = properties
          self
        end

        # Pushes all the ranges and buffers into the decoder's buffer.
        def push_ranges(ranges : Array(Range(UInt64, UInt64)), buffers : Array(Bytes)) : Nil
          ensure_not_finished
          @buffers.push_ranges(ranges, buffers)
        end

        # Pushes a single range and buffer into the decoder's buffer.
        def push_range(range : Range(UInt64, UInt64), buffer : Bytes) : Nil
          ensure_not_finished
          @buffers.push_range(range, buffer)
        end

        # Clears any staged byte ranges currently buffered.
        def clear_all_ranges : Nil
          @buffers.clear_all_ranges
        end

        # Tries to decode the metadata from the pushed data.
        def try_decode : Parquet::DecodeResult(ParquetMetaData)
          file_len = @buffers.file_len
          footer_len = ::Parquet::File::FOOTER_SIZE.to_u64

          loop do
            case @state
            when State::ReadingFooter
              footer_start = file_len > footer_len ? file_len - footer_len : 0_u64
              footer_range = footer_start...file_len
              unless @buffers.has_range?(footer_range)
                return Parquet::DecodeResult(ParquetMetaData).needs_data([footer_range])
              end
              @footer_tail = FooterTail.try_from(get_bytes(footer_range))
              @state = State::ReadingMetadata
            when State::ReadingMetadata
              footer_tail = @footer_tail.as(FooterTail)
              metadata_len = footer_tail.metadata_length.to_u64
              metadata_start = file_len - footer_len - metadata_len
              metadata_range = metadata_start...(metadata_start + metadata_len)
              unless @buffers.has_range?(metadata_range)
                return Parquet::DecodeResult(ParquetMetaData).needs_data([metadata_range])
              end
              @metadata =
                if properties = @file_decryption_properties
                  Metadata.parquet_metadata_with_encryption(
                    properties, footer_tail.encrypted_footer?, get_bytes(metadata_range), @metadata_options
                  )
                else
                  @metadata_parser.decode_metadata(get_bytes(metadata_range), footer_tail.encrypted_footer?)
                end
              @state = State::ReadingPageIndex
            when State::ReadingPageIndex
              metadata = @metadata.as(ParquetMetaData)
              range = Metadata.range_for_page_index(metadata, @column_index_policy, @offset_index_policy)
              if range.nil?
                @state = State::Finished
                return Parquet::DecodeResult(ParquetMetaData).data(metadata)
              end
              unless @buffers.has_range?(range)
                return Parquet::DecodeResult(ParquetMetaData).needs_data([range])
              end
              Metadata.parse_page_index(
                metadata, @column_index_policy, @offset_index_policy, get_bytes(range), range.begin
              )
              @state = State::Finished
              return Parquet::DecodeResult(ParquetMetaData).data(metadata)
            when State::Finished
              return Parquet::DecodeResult(ParquetMetaData).finished
            end
          end
        end

        private def ensure_not_finished : Nil
          if @state == State::Finished
            raise general_err!(
              "ParquetMetaDataPushDecoder: cannot push data after decoding is finished"
            )
          end
        end

        # Returns the bytes for the given range from the internal buffer.
        private def get_bytes(range : Range(UInt64, UInt64)) : Bytes
          raw_len = range.end - range.begin
          if raw_len > Int32::MAX
            raise general_err!(
              "ParquetMetaDataPushDecoder: Range length too large to fit in usize: #{raw_len}"
            )
          end
          @buffers.get_bytes(range.begin, raw_len.to_i32)
        end
      end
    end
  end
end
