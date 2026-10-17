require "../errors"
require "../thrift"
require "../basic"
require "../format"
require "./tracked_write"
require "../column/page"
require "../column/page_encryption"

# Ported from `parquet/src/file/writer.rs` (`SerializedPageWriter`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the serialized page writer: serializes a `CompressedPage` (thrift page
# header + bytes) into a `TrackedWrite`, returning its `PageWriteSpec`, and
# encrypts page headers/data when a `PageEncryptor` is configured.
# Divergence: `W: Write` becomes the concrete `TrackedWrite`.
module Parquet
  module File
    # Writes pages into a `TrackedWrite` sink.
    class SerializedPageWriter < Column::PageWriter
      def initialize(@sink : TrackedWrite, @page_encryptor : Column::PageEncryptor? = nil)
      end

      # Sets the page encryptor used to encrypt page headers/data.
      def with_page_encryptor(page_encryptor : Column::PageEncryptor?) : self
        @page_encryptor = page_encryptor
        self
      end

      # Writes `page`, returning its write metrics.
      def write_page(page : Column::CompressedPage) : Column::PageWriteSpec
        if page_encryptor = @page_encryptor
          page = page_encryptor.encrypt_compressed_page(page)
        end

        page_type = page.page_type
        start_pos = @sink.bytes_written.to_u64

        page_header = page.to_thrift_header
        header_size = serialize_page_header(page_header)

        @sink.write(page.data)

        spec = Column::PageWriteSpec.new
        spec.page_type = page_type
        spec.uncompressed_size = page.uncompressed_size + header_size
        spec.compressed_size = page.compressed_size.to_i64 + header_size
        spec.offset = start_pos
        spec.bytes_written = @sink.bytes_written.to_u64 - start_pos
        spec.num_values = page.num_values

        if page_encryptor = @page_encryptor
          if page.page_type.data_page? || page.page_type.data_page_v2?
            page_encryptor.increment_page
          end
        end

        spec
      end

      # Flushes the underlying sink.
      def close : Nil
        @sink.flush
        nil
      end

      private def serialize_page_header(header : Format::PageHeader) : Int32
        start_pos = @sink.bytes_written
        if page_encryptor = @page_encryptor
          page_encryptor.encrypt_page_header(header, @sink)
        else
          protocol = Thrift::ThriftCompactOutputProtocol.new(@sink)
          header.write_thrift(protocol)
        end
        (@sink.bytes_written - start_pos).to_i32
      end
    end
  end
end
