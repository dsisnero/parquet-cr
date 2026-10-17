require "../../../errors"
require "../../../macros"
require "../../../thrift"
require "../../../encryption/decrypt"
require "../../../encryption/modules"
require "../../column_crypto_metadata"
require "../column_chunk"
require "../row_group"
require "../parquet_metadata"

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

      # Builds a `FileDecryptor` from the file's encryption algorithm and the
      # provided decryption properties.
      def self.get_file_decryptor(
        encryption_algorithm : EncryptionAlgorithm,
        footer_key_metadata : Bytes?,
        file_decryption_properties : ::Parquet::Encryption::FileDecryptionProperties,
      ) : ::Parquet::Encryption::FileDecryptor
        algorithm = encryption_algorithm.aes_gcm_v1
        if algorithm.nil?
          raise nyi_err!("The AES_GCM_CTR_V1 encryption algorithm is not yet supported")
        end

        aad_file_unique = algorithm.aad_file_unique
        raise general_err!("AAD unique file identifier is not set") if aad_file_unique.nil?

        aad_prefix = file_decryption_properties.aad_prefix ||
                     algorithm.aad_prefix || Bytes.empty

        ::Parquet::Encryption::FileDecryptor.new(
          file_decryption_properties, footer_key_metadata, aad_file_unique, aad_prefix
        )
      end

      # Decrypts the per-column metadata of a row group (populated after the
      # footer is decrypted).
      def self.row_group_from_encrypted_thrift(
        rg : RowGroupMetaData,
        decryptor : ::Parquet::Encryption::FileDecryptor?,
        options : ParquetMetaDataOptions?,
      ) : RowGroupMetaData
        _ = options
        schema_descr = rg.schema_descr
        if schema_descr.num_columns != rg.columns.size
          raise general_err!(
            "Column count mismatch. Schema has #{schema_descr.num_columns} columns while Row Group has #{rg.columns.size}"
          )
        end

        total_byte_size = rg.total_byte_size
        num_rows = rg.num_rows
        columns = [] of ColumnChunkMetaData

        rg.columns.each_with_index do |column, i|
          d = schema_descr.columns[i]
          encrypted = column.encrypted_column_metadata
          file_decryptor = decryptor

          if encrypted && file_decryptor
            metadata = column.crypto_metadata
            if metadata.nil?
              raise general_err!(
                "No crypto_metadata is set for column '#{d.path.string}', which has encrypted metadata"
              )
            end

            column_decryptor =
              if metadata.kind.encryption_with_column_key?
                crypto_metadata = metadata.encryption_with_column_key!
                column_name = crypto_metadata.path_in_schema.join(".")
                begin
                  file_decryptor.get_column_metadata_decryptor(column_name, crypto_metadata.key_metadata)
                rescue
                  # We don't have the key for this column, so we can't decrypt
                  # its metadata; keep the column as-is.
                  columns << column
                  next
                end
              else
                file_decryptor.footer_decryptor
              end

            rg_ordinal = rg.ordinal
            if rg_ordinal.nil?
              raise general_err!(
                "Row group ordinal is required to decrypt column metadata for column '#{d.path.string}', but the file's row-group ordinals are inconsistent"
              )
            end
            if rg_ordinal < 0
              raise general_err!(
                "Row group ordinal #{rg_ordinal} is invalid (must be non-negative) for decrypting column metadata for column '#{d.path.string}'"
              )
            end

            column_aad = ::Parquet::Encryption.create_module_aad(
              file_decryptor.file_aad, ::Parquet::Encryption::ModuleType::ColumnMetaData,
              rg_ordinal, i, nil
            )

            decrypted = begin
              column_decryptor.decrypt(encrypted, column_aad)
            rescue
              raise general_err!(
                "Unable to decrypt column '#{d.path.string}', perhaps the column key is wrong?"
              )
            end

            column.encrypted_column_metadata = nil
            column_prot = ::Parquet::Thrift::ThriftSliceInputProtocol.new(decrypted)
            mask = read_column_metadata(column_prot, column, i)
            validate_column_metadata(mask)
            columns << column
          else
            columns << column
          end
        end

        result = RowGroupMetaData.new(schema_descr)
        result.columns = columns
        result.num_rows = num_rows
        result.sorting_columns = rg.sorting_columns
        result.total_byte_size = total_byte_size
        result.file_offset = rg.file_offset
        result.ordinal = rg.ordinal
        result
      end

      # Decodes `ParquetMetaData` from a thrift-encoded footer that may be
      # encrypted (encrypted footer or plaintext footer with encrypted columns).
      def self.parquet_metadata_with_encryption(
        file_decryption_properties : ::Parquet::Encryption::FileDecryptionProperties?,
        encrypted_footer : Bool,
        buf : Bytes,
        options : ParquetMetaDataOptions?,
      ) : ParquetMetaData
        file_decryptor = nil.as(::Parquet::Encryption::FileDecryptor?)
        footer_buf = buf

        if encrypted_footer
          prot = ::Parquet::Thrift::ThriftSliceInputProtocol.new(buf)
          if file_decryption_properties.nil?
            raise general_err!(
              "Parquet file has an encrypted footer but decryption properties were not provided"
            )
          end
          properties = file_decryption_properties

          crypto_metadata =
            begin
              FileCryptoMetaData.read_thrift(prot)
            rescue error
              raise general_err!("Could not parse crypto metadata: #{error.message}")
            end

          supply_aad_prefix =
            if algorithm = crypto_metadata.encryption_algorithm.aes_gcm_v1
              algorithm.supply_aad_prefix || false
            else
              false
            end
          if supply_aad_prefix && properties.aad_prefix.nil?
            raise general_err!(
              "Parquet file was encrypted with an AAD prefix that is not stored in the file, " \
              "but no AAD prefix was provided in the file decryption properties"
            )
          end

          decryptor = get_file_decryptor(
            crypto_metadata.encryption_algorithm, crypto_metadata.key_metadata, properties
          )
          aad_footer = ::Parquet::Encryption.create_footer_aad(decryptor.file_aad)
          decrypted_footer =
            begin
              decryptor.footer_decryptor.decrypt(prot.as_slice, aad_footer)
            rescue
              raise general_err!(
                "Provided footer key and AAD were unable to decrypt parquet footer"
              )
            end

          footer_buf = decrypted_footer
          file_decryptor = decryptor
        end

        parquet_meta =
          begin
            parquet_metadata_from_bytes(footer_buf, options)
          rescue error
            raise general_err!("Could not parse metadata: #{error.message}")
          end

        file_metadata = parquet_meta.file_metadata
        row_groups = parquet_meta.row_groups

        # A plaintext footer may still carry an encryption algorithm and a
        # footer signature.
        if (algorithm = file_metadata.encryption_algorithm) && (properties = file_decryption_properties)
          value = get_file_decryptor(
            algorithm, file_metadata.footer_signing_key_metadata, properties
          )
          if properties.check_plaintext_footer_integrity && !encrypted_footer
            value.verify_plaintext_footer_signature(buf)
          end
          file_decryptor = value
        end

        decrypted_row_groups = row_groups.map do |row_group|
          row_group_from_encrypted_thrift(row_group, file_decryptor, options)
        end

        ParquetMetaDataBuilder.new(file_metadata)
          .set_row_groups(decrypted_row_groups)
          .set_file_decryptor(file_decryptor)
          .build
      end
    end
  end
end
