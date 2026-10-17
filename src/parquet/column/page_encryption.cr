require "../errors"
require "../basic"
require "../format"
require "./page"
require "../encryption/ciphers"
require "../encryption/encrypt"
require "../encryption/modules"

# Ported from `parquet/src/column/page_encryption.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `PageEncryptor`, which encrypts page headers and page data for encrypted
# columns.
#
# Divergences: `Arc<FileEncryptor>`/`Box<dyn BlockEncryptor>` are the concrete
# classes; `usize` indices are `Int32`.
module Parquet
  module Column
    # Encrypts page headers and page data for encrypted columns.
    class PageEncryptor
      # Creates a page encryptor if `column_path` is encrypted.
      def self.create_if_column_encrypted(
        file_encryptor : ::Parquet::Encryption::FileEncryptor?,
        row_group_index : Int32,
        column_index : Int32,
        column_path : String,
      ) : PageEncryptor?
        encryptor = file_encryptor
        return nil if encryptor.nil? || !encryptor.column_encrypted?(column_path)

        new(
          encryptor,
          encryptor.column_encryptor(column_path),
          row_group_index,
          column_index,
        )
      end

      def initialize(
        @file_encryptor : ::Parquet::Encryption::FileEncryptor,
        @block_encryptor : ::Parquet::Encryption::BlockEncryptor,
        @row_group_index : Int32,
        @column_index : Int32,
        @page_index : Int32 = 0,
      )
      end

      # Advances the page index after a data page has been written.
      def increment_page : Nil
        @page_index += 1
        nil
      end

      # Encrypts the compressed data of `page` (framed as
      # `[size, nonce, ciphertext, tag]`).
      def encrypt_compressed_page(page : CompressedPage) : CompressedPage
        module_type =
          if page.page_type.data_page? || page.page_type.data_page_v2?
            ::Parquet::Encryption::ModuleType::DataPage
          else
            ::Parquet::Encryption::ModuleType::DictionaryPage
          end
        aad = module_aad(module_type)
        page.with_new_compressed_buffer(@block_encryptor.encrypt(page.data, aad))
      end

      # Encrypts the thrift `page_header` into `sink`.
      def encrypt_page_header(header : Format::PageHeader, sink : IO) : Nil
        module_type =
          case header.type
          when PageType::DATA_PAGE, PageType::DATA_PAGE_V2
            ::Parquet::Encryption::ModuleType::DataPageHeader
          when PageType::DICTIONARY_PAGE
            ::Parquet::Encryption::ModuleType::DictionaryPageHeader
          else
            raise general_err!("Unsupported page type for page header encryption: #{header.type}")
          end
        ::Parquet::Encryption.encrypt_thrift_object(
          header, @block_encryptor, sink, module_aad(module_type)
        )
      end

      private def module_aad(module_type : ::Parquet::Encryption::ModuleType) : Bytes
        ::Parquet::Encryption.create_module_aad(
          @file_encryptor.file_aad, module_type,
          @row_group_index, @column_index, @page_index
        )
      end
    end
  end
end
