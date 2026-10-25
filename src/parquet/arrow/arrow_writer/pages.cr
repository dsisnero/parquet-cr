require "../../file/serialized_writer"
require "../../column/page_store"

module Parquet::Arrow
  # Serialized pages retained through opaque store keys, with dictionary first.
  class ArrowPageWriter < Column::PageWriter
    getter store : Column::PageStore
    getter dictionary_len = 0
    @keys = [] of Column::PageKey
    @dictionary_keys = [] of Column::PageKey
    @length = 0_i64

    def initialize(@store, @encryptor : Column::PageEncryptor? = nil)
    end

    def write_page(page : Column::CompressedPage) : Column::PageWriteSpec
      sink = IO::Memory.new
      writer = File::SerializedPageWriter.new(File::TrackedWrite.new(sink), @encryptor)
      spec = writer.write_page(page)
      spec.offset = @length.to_u64
      bytes = sink.to_slice.dup
      # Encryption may change data length, so derive the header boundary from
      # its serialized thrift header size for plaintext; encrypted output keeps
      # the complete page as a single blob.
      if @encryptor
        keys = page.page_type.dictionary_page? ? @dictionary_keys : @keys
        keys << @store.put(bytes)
      else
        header_size = (spec.uncompressed_size - page.uncompressed_size).to_i32
        keys = page.page_type.dictionary_page? ? @dictionary_keys : @keys
        keys << @store.put(bytes[0, header_size])
        keys << @store.put(bytes[header_size, bytes.size - header_size])
      end
      @dictionary_len += bytes.size if page.page_type.dictionary_page?
      @length += bytes.size
      spec
    end

    def defers_dictionary_ordering : Bool
      true
    end

    def buffered_memory_size : Int32
      @store.memory_size.to_i32
    end

    def close : Nil
    end

    def each_blob(& : Bytes ->) : Nil
      (@dictionary_keys + @keys).each { |key| yield @store.take(key) }
    end
  end

  # Completed encoded column with metadata that can be adjusted before splicing.
  class ArrowColumnChunk
    getter close : Column::Writer::ColumnCloseResult

    def initialize(@pages : ArrowPageWriter, @close)
    end

    def close_mut : Column::Writer::ColumnCloseResult
      @close
    end

    def append_to_row_group(writer : File::SerializedRowGroupWriter) : Nil
      @close.update_dictionary_location(@pages.dictionary_len)
      writer.append_column_from_pages(self, @close)
    end

    def each(& : Bytes ->) : Nil
      @pages.each_blob { |blob| yield blob }
    end
  end
end
