require "../basic"
require "../format"
require "../file/statistics"

# Ported from `parquet/src/column/page.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `Page` (the data/v2-data/dictionary page representation),
# `CompressedPage`, `PageWriteSpec`, `PageMetadata`, and the `PageReader`/
# `PageWriter` interfaces.
#
# Divergence: Rust's `Page` enum becomes an abstract `Page` class with nested
# `DataPage`/`DataPageV2`/`DictionaryPage` subclasses; `usize` sizes are `Int64`
# for `uncompressed_size` (so the i32-overflow check is reachable) and `Int32`
# elsewhere; the `PageReader` iterator impl and the `PageIterator` trait are not
# ported; `is_dict` keeps its upstream name; encryption-only
# `with_new_compressed_buffer` is not ported.
module Parquet
  module Column
    # Parquet page definition (1-to-1 with the equivalent Thrift definitions,
    # except `buf` which holds the uncompressed bytes).
    abstract class Page
      # Returns the `PageType` for this page.
      abstract def page_type : PageType

      # Returns the internal byte buffer for this page.
      abstract def buffer : Bytes

      # Returns the number of values in this page.
      abstract def num_values : UInt32

      # Returns this page's `Encoding`.
      abstract def encoding : Encoding

      # Returns optional `Statistics`.
      abstract def statistics : Statistics?

      # Returns whether this page is any version of a data page.
      # ameba:disable Naming/PredicateName
      def is_data_page? : Bool
        is_a?(DataPage) || is_a?(DataPageV2)
      end

      # Returns whether this page is a dictionary page.
      # ameba:disable Naming/PredicateName
      def is_dictionary_page? : Bool
        is_a?(DictionaryPage)
      end

      # Data page, Parquet format v1.
      class DataPage < Page
        getter buf : Bytes
        getter num_values : UInt32
        getter encoding : Encoding
        getter def_level_encoding : Encoding
        getter rep_level_encoding : Encoding
        getter statistics : Statistics?

        def initialize(
          @buf : Bytes,
          @num_values : UInt32,
          @encoding : Encoding,
          @def_level_encoding : Encoding,
          @rep_level_encoding : Encoding,
          @statistics : Statistics? = nil,
        )
        end

        def page_type : PageType
          PageType::DATA_PAGE
        end

        def buffer : Bytes
          @buf
        end
      end

      # Data page, Parquet format v2.
      class DataPageV2 < Page
        getter buf : Bytes
        getter num_values : UInt32
        getter encoding : Encoding
        getter num_nulls : UInt32
        getter num_rows : UInt32
        getter def_levels_byte_len : UInt32
        getter rep_levels_byte_len : UInt32
        # ameba:disable Naming/QueryBoolMethods
        getter is_compressed : Bool
        getter statistics : Statistics?

        def initialize(
          @buf : Bytes,
          @num_values : UInt32,
          @encoding : Encoding,
          @num_nulls : UInt32,
          @num_rows : UInt32,
          @def_levels_byte_len : UInt32,
          @rep_levels_byte_len : UInt32,
          @is_compressed : Bool,
          @statistics : Statistics? = nil,
        )
        end

        def page_type : PageType
          PageType::DATA_PAGE_V2
        end

        def buffer : Bytes
          @buf
        end
      end

      # Dictionary page.
      class DictionaryPage < Page
        getter buf : Bytes
        getter num_values : UInt32
        getter encoding : Encoding
        # ameba:disable Naming/QueryBoolMethods
        getter is_sorted : Bool

        def initialize(@buf : Bytes, @num_values : UInt32, @encoding : Encoding, @is_sorted : Bool)
        end

        def page_type : PageType
          PageType::DICTIONARY_PAGE
        end

        def buffer : Bytes
          @buf
        end

        def statistics : Statistics?
          nil
        end
      end
    end

    # A page with a potentially compressed buffer and its uncompressed size.
    class CompressedPage
      def initialize(@compressed_page : Page, @uncompressed_size : Int64)
      end

      # Returns page type.
      def page_type : PageType
        @compressed_page.page_type
      end

      # Returns the underlying page with a potentially compressed buffer.
      def compressed_page : Page
        @compressed_page
      end

      # Returns uncompressed size in bytes.
      def uncompressed_size : Int64
        @uncompressed_size
      end

      # Returns compressed size in bytes.
      def compressed_size : Int32
        @compressed_page.buffer.size
      end

      # Returns the number of values in the page.
      def num_values : UInt32
        @compressed_page.num_values
      end

      # Returns the encoding for values in the page.
      def encoding : Encoding
        @compressed_page.encoding
      end

      # Returns the slice of the compressed buffer in the page.
      def data : Bytes
        @compressed_page.buffer
      end

      # Returns a copy of this page with a new compressed buffer (used by page
      # encryption).
      def with_new_compressed_buffer(buf : Bytes) : CompressedPage
        page =
          case p = @compressed_page
          when Page::DataPage
            Page::DataPage.new(
              buf, p.num_values, p.encoding, p.def_level_encoding, p.rep_level_encoding, p.statistics
            )
          when Page::DataPageV2
            Page::DataPageV2.new(
              buf, p.num_values, p.encoding, p.num_nulls, p.num_rows,
              p.def_levels_byte_len, p.rep_levels_byte_len, p.is_compressed, p.statistics
            )
          when Page::DictionaryPage
            Page::DictionaryPage.new(buf, p.num_values, p.encoding, p.is_sorted)
          else
            raise general_err!("Unsupported page type for re-buffering")
          end
        CompressedPage.new(page, @uncompressed_size)
      end

      # Returns the number of heap bytes this page currently holds.
      def memory_usage : Int32
        @compressed_page.buffer.size
      end

      # Returns the thrift page header.
      def to_thrift_header : Format::PageHeader
        uncompressed_size = self.uncompressed_size
        compressed_size = self.compressed_size
        if uncompressed_size > Int32::MAX
          raise general_err!("Page uncompressed size overflow: #{uncompressed_size}")
        end
        if compressed_size > Int32::MAX
          raise general_err!("Page compressed size overflow: #{compressed_size}")
        end

        num_values = self.num_values
        encoding = self.encoding
        page_type = self.page_type

        page_header = Format::PageHeader.new(
          page_type,
          uncompressed_size.to_i32,
          compressed_size.to_i32,
          nil,
          nil,
          nil,
          nil,
          nil,
        )

        case page = @compressed_page
        when Page::DataPage
          page_header.data_page_header = Format::DataPageHeader.new(
            num_values.to_i32,
            encoding,
            page.def_level_encoding,
            page.rep_level_encoding,
            Statistics.page_stats_to_thrift(page.statistics),
          )
        when Page::DataPageV2
          page_header.data_page_header_v2 = Format::DataPageHeaderV2.new(
            num_values.to_i32,
            page.num_nulls.to_i32,
            page.num_rows.to_i32,
            encoding,
            page.def_levels_byte_len.to_i32,
            page.rep_levels_byte_len.to_i32,
            page.is_compressed,
            Statistics.page_stats_to_thrift(page.statistics),
          )
        when Page::DictionaryPage
          page_header.dictionary_page_header = Format::DictionaryPageHeader.new(
            num_values.to_i32,
            encoding,
            page.is_sorted,
          )
        end
        page_header
      end
    end

    # Contains page write metrics.
    class PageWriteSpec
      property page_type : PageType
      property uncompressed_size : Int64
      property compressed_size : Int64
      property num_values : UInt32
      property offset : UInt64
      property bytes_written : UInt64

      def initialize
        @page_type = PageType::DATA_PAGE
        @uncompressed_size = 0_i64
        @compressed_size = 0_i64
        @num_values = 0_u32
        @offset = 0_u64
        @bytes_written = 0_u64
      end
    end

    # Contains metadata for a page.
    class PageMetadata
      getter num_rows : Int32?
      getter num_levels : Int32?
      # ameba:disable Naming/QueryBoolMethods
      getter is_dict : Bool

      def initialize(@num_rows : Int32?, @num_levels : Int32?, @is_dict : Bool)
      end

      # Builds `PageMetadata` from a thrift `PageHeader`.
      def self.from_thrift(header : Format::PageHeader) : PageMetadata
        case header.type
        when PageType::DATA_PAGE
          page_header = header.data_page_header || raise general_err!("Data page header is required for a data page")
          PageMetadata.new(nil, page_header.num_values, false)
        when PageType::DICTIONARY_PAGE
          PageMetadata.new(nil, nil, true)
        when PageType::DATA_PAGE_V2
          page_header = header.data_page_header_v2 || raise general_err!("Data page v2 header is required for a data page v2")
          PageMetadata.new(page_header.num_rows, page_header.num_values, false)
        else
          raise general_err!("page type #{header.type} cannot be converted to PageMetadata")
        end
      end
    end

    # API for reading pages from a column chunk.
    abstract class PageReader
      # Gets the next page, or `nil` if there are no pages left.
      # ameba:disable Naming/AccessorMethodName
      abstract def get_next_page : Page?

      # Gets metadata about the next page.
      abstract def peek_next_page : PageMetadata?

      # Skips reading the next page.
      abstract def skip_next_page : Nil

      # Returns `true` if the next page can be assumed to start a new record.
      def at_record_boundary : Bool
        metadata = peek_next_page
        return true if metadata.nil?

        !metadata.num_rows.nil?
      end
    end

    # An iterator over pages of one specific column in a Parquet file.
    #
    # Divergence: Rust's `PageIterator` marker trait
    # (`Iterator<Item = Result<Box<dyn PageReader>>>`) is a Crystal module that
    # includes `Iterator(PageReader)`.
    module PageIterator
      include Iterator(PageReader)
    end

    # API for writing pages in a column chunk.
    abstract class PageWriter
      # Writes a page into the output stream/sink, returning its write metrics.
      abstract def write_page(page : CompressedPage) : PageWriteSpec

      # Closes resources and flushes the underlying sink.
      abstract def close : Nil

      # Whether this writer resolves the final page layout itself at flush.
      def defers_dictionary_ordering : Bool
        false
      end

      # The number of bytes this writer currently holds in memory for pages.
      def buffered_memory_size : Int32
        0
      end
    end
  end
end
