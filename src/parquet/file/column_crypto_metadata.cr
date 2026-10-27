require "../errors"
require "../macros"
require "../thrift"

# Ported from `parquet/src/file/column_crypto_metadata.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `EncryptionWithColumnKey` and the `ColumnCryptoMetaData` union (column
# chunk encryption metadata). Divergences: the payload-carrying Rust enum is a
# `struct` with a `Kind` + payload (Crystal has no enums with payloads), matching
# the `ColumnOrder`/`Compression` convention; native `HeapSize` lives in metadata/memory.cr.
module Parquet
  module File
    # Encryption metadata for a column chunk encrypted with a column-specific key.
    define_thrift_struct(EncryptionWithColumnKey) do
      required 1, path_in_schema, Array(String)
      optional 2, key_metadata, Bytes
    end

    # Column chunk crypto metadata: either the footer key or a column key.
    struct ColumnCryptoMetaData
      # Which union variant this value holds.
      enum Kind
        EncryptionWithFooterKey
        EncryptionWithColumnKey
      end

      getter kind : Kind
      getter encryption_with_column_key : EncryptionWithColumnKey?

      private def initialize(@kind : Kind, @encryption_with_column_key : EncryptionWithColumnKey? = nil)
      end

      # The `ENCRYPTION_WITH_FOOTER_KEY` variant.
      ENCRYPTION_WITH_FOOTER_KEY = new(Kind::EncryptionWithFooterKey)

      # The `ENCRYPTION_WITH_COLUMN_KEY` variant.
      def self.encryption_with_column_key(value : EncryptionWithColumnKey) : ColumnCryptoMetaData
        new(Kind::EncryptionWithColumnKey, value)
      end

      # Returns the payload, raising if this is not the column-key variant.
      def encryption_with_column_key! : EncryptionWithColumnKey
        @encryption_with_column_key ||
          raise general_err!("ColumnCryptoMetaData is not ENCRYPTION_WITH_COLUMN_KEY")
      end

      def self.thrift_element_type : ::Parquet::Thrift::ElementType
        ::Parquet::Thrift::ElementType::Struct
      end

      def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : ColumnCryptoMetaData
        field_ident = prot.read_field_begin(0_i16)
        if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop
          raise general_err!("Received empty union from remote ColumnCryptoMetaData")
        end

        ret =
          case field_ident.id
          when 1
            prot.skip_empty_struct
            ENCRYPTION_WITH_FOOTER_KEY
          when 2
            encryption_with_column_key(EncryptionWithColumnKey.read_thrift(prot))
          else
            raise general_err!("Unexpected ColumnCryptoMetaData #{field_ident.id}")
          end

        end_ident = prot.read_field_begin(field_ident.id)
        unless end_ident.field_type == ::Parquet::Thrift::FieldType::Stop
          raise general_err!("Received multiple fields for union from remote ColumnCryptoMetaData")
        end

        ret
      end

      def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
        case @kind
        when Kind::EncryptionWithFooterKey
          prot.write_empty_struct(1_i16, 0_i16)
        when Kind::EncryptionWithColumnKey
          prot.write_field_begin(::Parquet::Thrift::FieldType::Struct, 2_i16, 0_i16)
          encryption_with_column_key!.write_thrift(prot)
        end
        prot.write_struct_end
      end
    end
  end
end
