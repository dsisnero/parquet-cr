require "../errors"
require "../thrift"
require "../basic"
require "../format"
require "./tracked_write"
require "../column/page"

# Ported from `parquet/src/file/writer.rs` (`SerializedPageWriter`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the serialized page writer: serializes a `CompressedPage` (thrift page
# header + bytes) into a `TrackedWrite`, returning its `PageWriteSpec`.
# Divergence: `W: Write` becomes the concrete `TrackedWrite`; page encryption is
# not ported.
module Parquet
  module File
    # Writes pages into a `TrackedWrite` sink.
    class SerializedPageWriter < Column::PageWriter
      def initialize(@sink : TrackedWrite)
      end

      # Writes `page`, returning its write metrics.
      def write_page(page : Column::CompressedPage) : Column::PageWriteSpec
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
        spec
      end

      # Flushes the underlying sink.
      def close : Nil
        @sink.flush
        nil
      end

      private def serialize_page_header(header : Format::PageHeader) : Int32
        start_pos = @sink.bytes_written
        protocol = Thrift::ThriftCompactOutputProtocol.new(@sink)
        header.write_thrift(protocol)
        (@sink.bytes_written - start_pos).to_i32
      end
    end
  end
end
