require "./ciphers"
require "./modules"
require "../errors"
require "../file/column_crypto_metadata"

# Ported from `parquet/src/encryption/decrypt.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `KeyRetriever`, `read_and_decrypt`, `CryptoContext`, `DecryptionKeys`/
# `ExplicitDecryptionKeys`, `FileDecryptionProperties`,
# `DecryptionPropertiesBuilder`, `DecryptionPropertiesBuilderWithRetriever`, and
# `FileDecryptor`.
#
# Divergences: `Cow<'_, Vec<u8>>` degrades to `Bytes`; `Arc<dyn BlockDecryptor>`
# is the `BlockDecryptor` abstract class; `Arc<dyn KeyRetriever>` is the
# `KeyRetriever` abstract class; `Result` errors raise `Parquet::GeneralError`;
# `HashMap` is a `Hash`; `usize` ordinals are `Int32`; the payload-carrying
# `DecryptionKeys` enum is a struct with a `Kind`; `Send`/`Sync` bounds are omitted. Native `HeapSize` lives in
# file/metadata/memory.cr.
module Parquet
  module Encryption
    # Retrieves a decryption key using its metadata.
    abstract class KeyRetriever
      # Returns the decryption key for `key_metadata`.
      abstract def retrieve_key(key_metadata : Bytes) : Bytes
    end

    # Reads a length-prefixed encrypted block from `input` and decrypts it.
    def self.read_and_decrypt(decryptor : BlockDecryptor, input : IO, aad : Bytes) : Bytes
      length_bytes = Bytes.new(4)
      input.read_fully(length_bytes)
      ciphertext_length = IO::ByteFormat::LittleEndian.decode(UInt32, length_bytes).to_i32

      # The first four bytes are a zero placeholder; the decryptor reads the
      # nonce/ciphertext/tag from offset four.
      ciphertext = Bytes.new(4 + ciphertext_length)
      input.read_fully(ciphertext[4, ciphertext_length])
      decryptor.decrypt(ciphertext, aad)
    end

    # The context required to decrypt Parquet modules (data pages, etc.).
    class CryptoContext
      getter row_group_idx : Int32
      getter column_ordinal : Int32
      getter page_ordinal : Int32?
      getter? dictionary_page : Bool
      getter file_aad : Bytes

      def initialize(
        @row_group_idx : Int32,
        @column_ordinal : Int32,
        @page_ordinal : Int32?,
        @dictionary_page : Bool,
        @data_decryptor : BlockDecryptor,
        @metadata_decryptor : BlockDecryptor,
        @file_aad : Bytes,
      )
      end

      # Builds a context for a column from its crypto metadata.
      def self.for_column(
        file_decryptor : FileDecryptor,
        column_crypto_metadata : File::ColumnCryptoMetaData,
        row_group_idx : Int32,
        column_ordinal : Int32,
      ) : CryptoContext
        data_decryptor, metadata_decryptor =
          case column_crypto_metadata.kind
          when File::ColumnCryptoMetaData::Kind::EncryptionWithFooterKey
            {file_decryptor.footer_decryptor, file_decryptor.footer_decryptor}
          else
            column_key = column_crypto_metadata.encryption_with_column_key!
            column_name =
              if column_key.path_in_schema.size == 1
                column_key.path_in_schema[0]
              else
                column_key.path_in_schema.join('.')
              end
            key_metadata = column_key.key_metadata
            {
              file_decryptor.get_column_data_decryptor(column_name, key_metadata),
              file_decryptor.get_column_metadata_decryptor(column_name, key_metadata),
            }
          end

        new(
          row_group_idx, column_ordinal, nil, false,
          data_decryptor, metadata_decryptor, file_decryptor.file_aad
        )
      end

      def with_page_ordinal(page_ordinal : Int32) : CryptoContext
        CryptoContext.new(
          @row_group_idx, @column_ordinal, page_ordinal, false,
          @data_decryptor, @metadata_decryptor, @file_aad
        )
      end

      def for_dictionary_page : CryptoContext
        CryptoContext.new(
          @row_group_idx, @column_ordinal, @page_ordinal, true,
          @data_decryptor, @metadata_decryptor, @file_aad
        )
      end

      def data_decryptor : BlockDecryptor
        @data_decryptor
      end

      def metadata_decryptor : BlockDecryptor
        @metadata_decryptor
      end

      def create_page_header_aad : Bytes
        module_type = @dictionary_page ? ModuleType::DictionaryPageHeader : ModuleType::DataPageHeader
        Encryption.create_module_aad(@file_aad, module_type, @row_group_idx, @column_ordinal, @page_ordinal)
      end

      def create_page_aad : Bytes
        module_type = @dictionary_page ? ModuleType::DictionaryPage : ModuleType::DataPage
        Encryption.create_module_aad(@file_aad, module_type, @row_group_idx, @column_ordinal, @page_ordinal)
      end

      def create_column_index_aad : Bytes
        Encryption.create_module_aad(@file_aad, ModuleType::ColumnIndex, @row_group_idx, @column_ordinal, @page_ordinal)
      end

      def create_offset_index_aad : Bytes
        Encryption.create_module_aad(@file_aad, ModuleType::OffsetIndex, @row_group_idx, @column_ordinal, @page_ordinal)
      end
    end

    # Explicit footer/column keys.
    struct ExplicitDecryptionKeys
      getter footer_key : Bytes
      getter column_keys : Hash(String, Bytes)

      def initialize(@footer_key : Bytes, @column_keys : Hash(String, Bytes))
      end

      def ==(other : ExplicitDecryptionKeys) : Bool
        @footer_key == other.footer_key && @column_keys == other.column_keys
      end
    end

    # The keys used to decrypt a file, either explicit or via a retriever.
    struct DecryptionKeys
      # Which key source is configured.
      enum Kind
        Explicit
        ViaRetriever
      end

      getter kind : Kind
      getter explicit : ExplicitDecryptionKeys?
      getter retriever : KeyRetriever?

      private def initialize(@kind : Kind, @explicit : ExplicitDecryptionKeys? = nil, @retriever : KeyRetriever? = nil)
      end

      def self.explicit(value : ExplicitDecryptionKeys) : DecryptionKeys
        new(Kind::Explicit, value, nil)
      end

      def self.via_retriever(value : KeyRetriever) : DecryptionKeys
        new(Kind::ViaRetriever, nil, value)
      end

      def explicit! : ExplicitDecryptionKeys
        @explicit || raise general_err!("DecryptionKeys is not explicit")
      end

      def retriever! : KeyRetriever
        @retriever || raise general_err!("DecryptionKeys has no key retriever")
      end

      def ==(other : DecryptionKeys) : Bool
        if @kind.explicit? && other.kind.explicit?
          explicit! == other.explicit!
        else
          @kind == other.kind
        end
      end
    end

    # Keys and AAD data required to decrypt a Parquet file.
    class FileDecryptionProperties
      getter aad_prefix : Bytes?
      getter keys : DecryptionKeys

      def initialize(
        @keys : DecryptionKeys,
        @aad_prefix : Bytes?,
        @footer_signature_verification : Bool,
      )
      end

      # Creates a builder using an explicit footer key.
      def self.builder(footer_key : Bytes) : DecryptionPropertiesBuilder
        DecryptionPropertiesBuilder.new(footer_key)
      end

      # Creates a builder using a `KeyRetriever`.
      def self.with_key_retriever(key_retriever : KeyRetriever) : DecryptionPropertiesBuilderWithRetriever
        DecryptionPropertiesBuilderWithRetriever.new(key_retriever)
      end

      # Whether footer signature verification is enabled for plaintext footers.
      def check_plaintext_footer_integrity : Bool
        @footer_signature_verification
      end

      # The footer key (explicit) or the retrieved key.
      def footer_key(key_metadata : Bytes?) : Bytes
        case @keys.kind
        when DecryptionKeys::Kind::Explicit
          @keys.explicit!.footer_key
        else
          @keys.retriever!.retrieve_key(key_metadata || Bytes.empty)
        end
      end

      # The column key (explicit) or the retrieved key; raises if unset.
      def column_key(column_name : String, key_metadata : Bytes?) : Bytes
        case @keys.kind
        when DecryptionKeys::Kind::Explicit
          key = @keys.explicit!.column_keys[column_name]?
          if key.nil?
            raise general_err!("No column decryption key set for encrypted column '#{column_name}'")
          end
          key
        else
          @keys.retriever!.retrieve_key(key_metadata || Bytes.empty)
        end
      end

      # The configured column names and keys (empty when using a retriever).
      def column_keys : Tuple(Array(String), Array(Bytes))
        names = [] of String
        keys = [] of Bytes
        if explicit = @keys.explicit
          explicit.column_keys.each do |name, key|
            names << name
            keys << key
          end
        end
        {names, keys}
      end

      # Whether these properties use a key retriever.
      def uses_key_retriever? : Bool
        @keys.kind.via_retriever?
      end

      def ==(other : FileDecryptionProperties) : Bool
        @keys == other.keys && @aad_prefix == other.aad_prefix &&
          @footer_signature_verification == other.check_plaintext_footer_integrity
      end

      # Avoids leaking keys in logs (upstream's custom `Debug` impl).
      def to_s(io : IO) : Nil
        io << "FileDecryptionProperties { }"
      end
    end

    # Builder for `FileDecryptionProperties` with explicit keys.
    class DecryptionPropertiesBuilder
      def initialize(footer_key : Bytes)
        @footer_key = footer_key
        @column_keys = Hash(String, Bytes).new
        @aad_prefix = nil.as(Bytes?)
        @footer_signature_verification = true
      end

      # Finalizes the builder.
      def build : FileDecryptionProperties
        keys = DecryptionKeys.explicit(ExplicitDecryptionKeys.new(@footer_key, @column_keys))
        FileDecryptionProperties.new(keys, @aad_prefix, @footer_signature_verification)
      end

      # Sets the expected AAD prefix.
      def with_aad_prefix(value : Bytes) : self
        @aad_prefix = value
        self
      end

      # Sets the decryption key for a column.
      def with_column_key(column_name : String, decryption_key : Bytes) : self
        @column_keys[column_name] = decryption_key
        self
      end

      # Sets decryption keys for multiple columns (raising if counts differ).
      def with_column_keys(column_names : Enumerable(String), keys : Enumerable(Bytes)) : self
        names = column_names.to_a
        key_list = keys.to_a
        if names.size != key_list.size
          raise general_err!(
            "The number of column names (#{names.size}) does not match the number of keys (#{key_list.size})"
          )
        end
        names.each_with_index { |name, i| @column_keys[name] = key_list[i] }
        self
      end

      # Disables plaintext-footer signature verification.
      def disable_footer_signature_verification : self
        @footer_signature_verification = false
        self
      end
    end

    # Builder for `FileDecryptionProperties` using a `KeyRetriever`.
    class DecryptionPropertiesBuilderWithRetriever
      def initialize(@key_retriever : KeyRetriever)
        @aad_prefix = nil.as(Bytes?)
        @footer_signature_verification = true
      end

      # Finalizes the builder.
      def build : FileDecryptionProperties
        FileDecryptionProperties.new(
          DecryptionKeys.via_retriever(@key_retriever),
          @aad_prefix, @footer_signature_verification
        )
      end

      # Sets the expected AAD prefix.
      def with_aad_prefix(value : Bytes) : self
        @aad_prefix = value
        self
      end

      # Disables plaintext-footer signature verification.
      def disable_footer_signature_verification : self
        @footer_signature_verification = false
        self
      end
    end

    # The decryption configuration for a single Parquet file.
    class FileDecryptor
      getter footer_decryptor : BlockDecryptor
      getter file_aad : Bytes
      protected getter decryption_properties : FileDecryptionProperties

      def initialize(
        @decryption_properties : FileDecryptionProperties,
        footer_key_metadata : Bytes?,
        aad_file_unique : Bytes,
        aad_prefix : Bytes,
      )
        @file_aad = aad_prefix + aad_file_unique
        footer_key = @decryption_properties.footer_key(footer_key_metadata)
        footer_decryptor =
          begin
            GcmBlockDecryptor.new(footer_key)
          rescue error : GeneralError
            message = (error.message || "").sub("Parquet error: ", "")
            raise general_err!("Invalid footer key. #{message}")
          end
        @footer_decryptor = footer_decryptor
      end

      # Verifies the signature of a plaintext footer.
      def verify_plaintext_footer_signature(plaintext_footer : Bytes) : Nil
        tag = plaintext_footer[plaintext_footer.size - TAG_LEN, TAG_LEN]
        aad = Encryption.create_footer_aad(@file_aad)
        computed_tag = @footer_decryptor.compute_plaintext_tag(aad, plaintext_footer)
        if computed_tag != tag
          raise general_err!(
            "Footer signature verification failed. Computed: #{computed_tag.inspect}, Expected: #{tag.inspect}"
          )
        end
      end

      # Returns the data decryptor for a column.
      def get_column_data_decryptor(column_name : String, key_metadata : Bytes?) : BlockDecryptor
        column_key = @decryption_properties.column_key(column_name, key_metadata)
        GcmBlockDecryptor.new(column_key)
      end

      # Returns the metadata decryptor for a column.
      def get_column_metadata_decryptor(column_name : String, key_metadata : Bytes?) : BlockDecryptor
        get_column_data_decryptor(column_name, key_metadata)
      end

      def ==(other : FileDecryptor) : Bool
        @decryption_properties == other.decryption_properties && @file_aad == other.file_aad
      end
    end
  end
end
