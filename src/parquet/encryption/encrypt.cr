require "random/secure"
require "./ciphers"
require "./modules"
require "../errors"
require "../file/column_crypto_metadata"
require "../schema/types"
require "../thrift"

# Ported from `parquet/src/encryption/encrypt.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `EncryptionKey`, `FileEncryptionProperties`, `EncryptionPropertiesBuilder`,
# `FileEncryptor`, and the thrift-encryption helpers
# (`encrypt_thrift_object[_to_vec]`, `write_signed_plaintext_thrift_object`,
# `get_column_crypto_metadata`).
#
# Divergences: `Vec<u8>` is `Bytes`; `Arc<FileEncryptionProperties>` is just
# `FileEncryptionProperties` (no `Arc`); `Result` errors raise
# `Parquet::GeneralError`; `HashMap` is a `Hash`; the `SystemRandom` AAD seed is
# `Random::Secure`; `Box<dyn BlockEncryptor>` is the `BlockEncryptor` abstract
# class; `EncryptionKey` is public (upstream private) because it appears in the
# public `FileEncryptionProperties#column_keys_map`/`#column_keys`.
module Parquet
  module Encryption
    # A key plus optional retrieval metadata.
    struct EncryptionKey
      getter key : Bytes
      getter key_metadata : Bytes?

      def initialize(@key : Bytes, @key_metadata : Bytes? = nil)
      end

      # Returns a copy with `metadata` set.
      def with_metadata(metadata : Bytes) : EncryptionKey
        self.class.new(@key, metadata)
      end
    end

    # Defines how data in a Parquet file should be encrypted.
    class FileEncryptionProperties
      # The AAD prefix, if set.
      getter aad_prefix : Bytes?
      # Per-column encryption keys, in insertion order.
      getter column_keys_map : Hash(String, EncryptionKey)

      def initialize(
        @encrypt_footer : Bool,
        @footer_key : EncryptionKey,
        @column_keys_map : Hash(String, EncryptionKey),
        @aad_prefix : Bytes?,
        @store_aad_prefix : Bool,
      )
      end

      # Creates a builder with the given footer key.
      def self.builder(footer_key : Bytes) : EncryptionPropertiesBuilder
        EncryptionPropertiesBuilder.new(footer_key)
      end

      # Whether the footer should be encrypted.
      def encrypt_footer : Bool
        @encrypt_footer
      end

      # The footer encryption key.
      def footer_key : Bytes
        @footer_key.key
      end

      # Retrieval metadata of the footer key, if set.
      def footer_key_metadata : Bytes?
        @footer_key.key_metadata
      end

      # The column names, keys, and (for keys that have it) metadata.
      def column_keys : Tuple(Array(String), Array(Bytes), Array(Bytes))
        names = Array(String).new(@column_keys_map.size)
        keys = Array(Bytes).new(@column_keys_map.size)
        metadata = Array(Bytes).new
        @column_keys_map.each do |name, key|
          names << name
          keys << key.key
          if meta = key.key_metadata
            metadata << meta
          end
        end
        {names, keys, metadata}
      end

      # Whether the AAD prefix should be stored in the file.
      def store_aad_prefix : Bool
        @store_aad_prefix && !@aad_prefix.nil?
      end

      # Whether uniform encryption (no per-column keys) is configured.
      def uniform_encryption? : Bool
        @column_keys_map.empty?
      end

      # Returns the key for `name`, or `nil`.
      def column_key_for(name : String) : EncryptionKey?
        @column_keys_map[name]?
      end

      # Checks that every column with an encryption key is present in `schema`.
      def validate_encrypted_column_names(schema : Schema::SchemaDescriptor) : Nil
        column_paths = schema.columns.map(&.path.string).to_set
        encryption_columns = @column_keys_map.keys.to_set
        return if encryption_columns.subset_of?(column_paths)

        missing = (encryption_columns - column_paths).to_a.sort
        raise general_err!(
          "The following columns with encryption keys specified were not found in the schema: #{missing.join(", ")}"
        )
      end
    end

    # Builder for `FileEncryptionProperties`.
    class EncryptionPropertiesBuilder
      def initialize(footer_key : Bytes)
        @encrypt_footer = true
        @footer_key = EncryptionKey.new(footer_key)
        @column_keys = Hash(String, EncryptionKey).new
        @aad_prefix = nil.as(Bytes?)
        @store_aad_prefix = false
      end

      # Sets whether the footer is stored in plaintext (default encrypted).
      def with_plaintext_footer(plaintext_footer : Bool) : self
        @encrypt_footer = !plaintext_footer
        self
      end

      # Sets the footer key retrieval metadata.
      def with_footer_key_metadata(metadata : Bytes) : self
        @footer_key = @footer_key.with_metadata(metadata)
        self
      end

      # Sets the key used to encrypt a column.
      def with_column_key(column_name : String, key : Bytes) : self
        @column_keys[column_name] = EncryptionKey.new(key)
        self
      end

      # Sets the key and retrieval metadata used to encrypt a column.
      def with_column_key_and_metadata(column_name : String, key : Bytes, metadata : Bytes) : self
        @column_keys[column_name] = EncryptionKey.new(key, metadata)
        self
      end

      # Sets keys for multiple columns (raising if the counts differ).
      def with_column_keys(column_names : Enumerable(String), keys : Enumerable(Bytes)) : self
        names = column_names.to_a
        key_list = keys.to_a
        if names.size != key_list.size
          raise general_err!(
            "The number of column names (#{names.size}) does not match the number of keys (#{key_list.size})"
          )
        end
        names.each_with_index do |name, i|
          @column_keys[name] = EncryptionKey.new(key_list[i])
        end
        self
      end

      # Sets the AAD prefix.
      def with_aad_prefix(aad_prefix : Bytes) : self
        @aad_prefix = aad_prefix
        self
      end

      # Sets whether the AAD prefix is stored in the file.
      def with_aad_prefix_storage(store_aad_prefix : Bool) : self
        @store_aad_prefix = store_aad_prefix
        self
      end

      # Builds the encryption properties.
      def build : FileEncryptionProperties
        FileEncryptionProperties.new(
          @encrypt_footer, @footer_key, @column_keys, @aad_prefix, @store_aad_prefix
        )
      end
    end

    # The encryption configuration for a single Parquet file.
    class FileEncryptor
      getter properties : FileEncryptionProperties
      getter aad_file_unique : Bytes
      getter file_aad : Bytes

      def initialize(@properties : FileEncryptionProperties)
        @aad_file_unique = Random::Secure.random_bytes(8)
        prefix = @properties.aad_prefix
        @file_aad = prefix.nil? ? @aad_file_unique : prefix + @aad_file_unique
      end

      # Whether data for `column_path` should be encrypted.
      def column_encrypted?(column_path : String) : Bool
        if @properties.uniform_encryption?
          true
        else
          !@properties.column_key_for(column_path).nil?
        end
      end

      # Returns the `BlockEncryptor` for the footer.
      def footer_encryptor : BlockEncryptor
        GcmBlockEncryptor.new(@properties.footer_key)
      end

      # Returns the `BlockEncryptor` for `column_path`, raising if unencrypted.
      def column_encryptor(column_path : String) : BlockEncryptor
        return footer_encryptor if @properties.uniform_encryption?

        key = @properties.column_key_for(column_path)
        if key.nil?
          raise general_err!("Column '#{column_path}' is not encrypted")
        end
        GcmBlockEncryptor.new(key.key)
      end
    end

    # Serializes `object` and appends its encrypted bytes to `sink`.
    def self.encrypt_thrift_object(
      object,
      encryptor : BlockEncryptor,
      sink : IO,
      module_aad : Bytes,
    ) : Nil
      sink.write(encrypt_thrift_object_to_vec(object, encryptor, module_aad))
    end

    # Serializes `object` and returns its encrypted (`[size, nonce, ciphertext,
    # tag]`) bytes.
    def self.encrypt_thrift_object_to_vec(
      object,
      encryptor : BlockEncryptor,
      module_aad : Bytes,
    ) : Bytes
      io = IO::Memory.new
      protocol = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
      object.write_thrift(protocol)
      encryptor.encrypt(io.to_slice.dup, module_aad)
    end

    # Writes `object` in plaintext, followed by the nonce and authentication tag
    # of its encrypted form (the "signed plaintext" footer format).
    def self.write_signed_plaintext_thrift_object(
      object,
      encryptor : BlockEncryptor,
      sink : IO,
      module_aad : Bytes,
    ) : Nil
      io = IO::Memory.new
      protocol = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
      object.write_thrift(protocol)
      buffer = io.to_slice.dup

      sink.write(buffer)

      encrypted = encryptor.encrypt(buffer, module_aad)
      nonce = encrypted[SIZE_LEN, NONCE_LEN]
      tag = encrypted[encrypted.size - TAG_LEN, TAG_LEN]
      sink.write(nonce)
      sink.write(tag)
    end

    # Returns the crypto metadata for `column`.
    def self.get_column_crypto_metadata(
      properties : FileEncryptionProperties,
      column : Schema::ColumnDescriptor,
    ) : File::ColumnCryptoMetaData?
      if properties.uniform_encryption?
        # Uniform encryption
        File::ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY
      else
        key = properties.column_key_for(column.path.string)
        return nil if key.nil?

        File::ColumnCryptoMetaData.encryption_with_column_key(
          File::EncryptionWithColumnKey.new(column.path.parts, key.key_metadata)
        )
      end
    end
  end
end
