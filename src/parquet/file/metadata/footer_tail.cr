# Ported from `parquet/src/file/metadata/footer_tail.rs` and
# `parquet/src/file/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
module Parquet
  module File
    # Size of the Parquet footer tail: 4 bytes length + 4 bytes magic.
    FOOTER_SIZE = 8

    # Magic bytes for a regular Parquet footer.
    PARQUET_MAGIC = Bytes[0x50_u8, 0x41_u8, 0x52_u8, 0x31_u8]

    # Magic bytes for an encrypted Parquet footer.
    PARQUET_MAGIC_ENCRYPTED_FOOTER = Bytes[0x50_u8, 0x41_u8, 0x52_u8, 0x45_u8]

    module Metadata
      # Parsed Parquet footer tail (last 8 bytes of a file).
      #
      # Layout: 4 bytes metadata length, 4 bytes magic (`PAR1` or `PARE`).
      struct FooterTail
        getter metadata_length : UInt32
        getter? encrypted_footer : Bool

        def initialize(@metadata_length : UInt32, @encrypted_footer : Bool)
        end

        # Decodes the footer tail from exactly `FOOTER_SIZE` bytes.
        def self.try_new(slice : Bytes) : FooterTail
          try_from(slice)
        end

        # Decodes the footer tail from a byte slice.
        def self.try_from(bytes : Bytes) : FooterTail
          if bytes.size != FOOTER_SIZE
            raise general_err!("Invalid footer length #{bytes.size}, expected #{FOOTER_SIZE}")
          end

          magic = bytes[4, 4]
          encrypted_footer = if magic == Parquet::File::PARQUET_MAGIC_ENCRYPTED_FOOTER
                               true
                             elsif magic == Parquet::File::PARQUET_MAGIC
                               false
                             else
                               raise general_err!("Invalid Parquet file. Corrupt footer")
                             end

          metadata_length = IO::Memory.new(bytes[0, 4]).read_bytes(UInt32, IO::ByteFormat::LittleEndian)
          new(metadata_length, encrypted_footer)
        end
      end
    end
  end
end
