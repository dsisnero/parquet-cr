require "../../../errors"
require "../../../macros"
require "../../../thrift"

# Ported from `parquet/src/file/metadata/thrift/encryption.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the encryption Thrift IDL: `AesGcmV1`, `AesGcmCtrV1`,
# `EncryptionAlgorithm`, and `FileCryptoMetaData`. Divergences: the payload-
# carrying `EncryptionAlgorithm` enum is a `struct` with a `Kind` + payload (see
# `ColumnOrder`); `HeapSize` is not ported; the encrypted-footer reading helpers
# (`parquet_metadata_with_encryption`, `row_group_from_encrypted_thrift`,
# `get_file_decryptor`) depend on the unported decryption-properties layer and
# are deferred (see `plans/parity.md`).
module Parquet
  module File
    module Metadata
      # AES-GCM (12-byte nonce) encryption parameters.
      define_thrift_struct(AesGcmV1) do
        optional 1, aad_prefix, Bytes
        optional 2, aad_file_unique, Bytes
        optional 3, supply_aad_prefix, Bool
      end

      # AES-GCM with a blocking CTR (4-byte nonce) encryption parameters.
      define_thrift_struct(AesGcmCtrV1) do
        optional 1, aad_prefix, Bytes
        optional 2, aad_file_unique, Bytes
        optional 3, supply_aad_prefix, Bool
      end

      # The encryption algorithm used for a file/column (union).
      struct EncryptionAlgorithm
        # Which union variant this value holds.
        enum Kind
          AesGcmV1
          AesGcmCtrV1
        end

        getter kind : Kind
        getter aes_gcm_v1 : AesGcmV1?
        getter aes_gcm_ctr_v1 : AesGcmCtrV1?

        private def initialize(
          @kind : Kind,
          @aes_gcm_v1 : AesGcmV1? = nil,
          @aes_gcm_ctr_v1 : AesGcmCtrV1? = nil,
        )
        end

        # The `AES_GCM_V1` variant.
        def self.aes_gcm_v1(value : AesGcmV1) : EncryptionAlgorithm
          new(Kind::AesGcmV1, value, nil)
        end

        # The `AES_GCM_CTR_V1` variant.
        def self.aes_gcm_ctr_v1(value : AesGcmCtrV1) : EncryptionAlgorithm
          new(Kind::AesGcmCtrV1, nil, value)
        end

        def self.thrift_element_type : ::Parquet::Thrift::ElementType
          ::Parquet::Thrift::ElementType::Struct
        end

        def self.read_thrift(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : EncryptionAlgorithm
          field_ident = prot.read_field_begin(0_i16)
          if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop
            raise general_err!("Received empty union from remote EncryptionAlgorithm")
          end

          ret =
            case field_ident.id
            when 1 then aes_gcm_v1(AesGcmV1.read_thrift(prot))
            when 2 then aes_gcm_ctr_v1(AesGcmCtrV1.read_thrift(prot))
            else
              raise general_err!("Unexpected EncryptionAlgorithm #{field_ident.id}")
            end

          end_ident = prot.read_field_begin(field_ident.id)
          unless end_ident.field_type == ::Parquet::Thrift::FieldType::Stop
            raise general_err!("Received multiple fields for union from remote EncryptionAlgorithm")
          end

          ret
        end

        def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
          case @kind
          when Kind::AesGcmV1
            prot.write_field_begin(::Parquet::Thrift::FieldType::Struct, 1_i16, 0_i16)
            (@aes_gcm_v1 || raise general_err!("EncryptionAlgorithm AES_GCM_V1 payload is missing")).write_thrift(prot)
          when Kind::AesGcmCtrV1
            prot.write_field_begin(::Parquet::Thrift::FieldType::Struct, 2_i16, 0_i16)
            (@aes_gcm_ctr_v1 || raise general_err!("EncryptionAlgorithm AES_GCM_CTR_V1 payload is missing")).write_thrift(prot)
          end
          prot.write_struct_end
        end
      end

      # Crypto metadata for files with an encrypted footer.
      define_thrift_struct(FileCryptoMetaData) do
        required 1, encryption_algorithm, EncryptionAlgorithm
        optional 2, key_metadata, Bytes
      end
    end
  end
end
