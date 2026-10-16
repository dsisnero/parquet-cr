require "openssl"
require "random/secure"
require "../errors"

# Adds the AES-GCM EVP cipher accessors (not bound by Crystal's `LibCrypto`).
lib LibCrypto
  fun evp_aes_128_gcm = EVP_aes_128_gcm : EVP_CIPHER
  fun evp_aes_256_gcm = EVP_aes_256_gcm : EVP_CIPHER
end

# Ported from `parquet/src/encryption/ciphers.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the AES-GCM block encryptor/decryptor used by Parquet Modular
# Encryption.
#
# Divergences: Rust's `ring` AEAD/RNG are replaced by a minimal AES-GCM with
# AAD built directly on OpenSSL's EVP interface (`AesGcm`), because Crystal's
# `OpenSSL::Cipher` exposes no AAD setter; errors raise `Parquet::GeneralError`;
# upstream's `HeapSize` trait (`heap_size`) and `Send`/`Sync` bounds have no
# Crystal analogue; the `ring`-prefixed names drop the prefix (`GcmBlockEncryptor`
# / `GcmBlockDecryptor`).
module Parquet
  module Encryption
    # Low 96 bits (12 bytes) of a 128-bit value.
    RIGHT_TWELVE = 0x0000_0000_ffff_ffff_ffff_ffff_ffff_ffff_u128

    # Length of the AES-GCM nonce, in bytes.
    NONCE_LEN = 12
    # Length of the AES-GCM authentication tag, in bytes.
    TAG_LEN = 16
    # Length of the little-endian ciphertext-length prefix, in bytes.
    SIZE_LEN = 4

    # `EVP_CTRL_AEAD_SET_IVLEN` (OpenSSL).
    private EVP_CTRL_AEAD_SET_IVLEN = 0x9

    # Minimal AES-GCM (with additional authenticated data) over OpenSSL EVP.
    module AesGcm
      # Resolves the EVP cipher for `key` (AES-128-GCM or AES-256-GCM).
      def self.cipher_for(key : Bytes) : LibCrypto::EVP_CIPHER
        case key.size
        when 16 then LibCrypto.evp_aes_128_gcm
        when 32 then LibCrypto.evp_aes_256_gcm
        else
          raise GeneralError.new(
            "Error creating AES-GCM cipher with unsupported key length: #{key.size}"
          )
        end
      end

      # Encrypts `plaintext` with `aad`, returning `{ciphertext, tag}`.
      def self.seal(key : Bytes, nonce : Bytes, aad : Bytes, plaintext : Bytes) : Tuple(Bytes, Bytes)
        cipher = cipher_for(key)
        ctx = LibCrypto.evp_cipher_ctx_new
        begin
          raise general_err unless LibCrypto.evp_cipherinit_ex(ctx, cipher, nil, key.to_unsafe, nil, 1) == 1
          raise general_err unless LibCrypto.evp_cipher_ctx_ctrl(ctx, EVP_CTRL_AEAD_SET_IVLEN, nonce.size, nil) == 1
          raise general_err unless LibCrypto.evp_cipherinit_ex(ctx, nil, nil, nil, nonce.to_unsafe, 1) == 1

          length = 0
          unless aad.empty?
            raise general_err unless LibCrypto.evp_cipherupdate(ctx, nil, pointerof(length), aad.to_unsafe, aad.size) == 1
          end

          buffer = Bytes.new(plaintext.size + TAG_LEN)
          unless LibCrypto.evp_cipherupdate(ctx, buffer.to_unsafe, pointerof(length), plaintext.to_unsafe, plaintext.size) == 1
            raise general_err
          end
          total = length
          final_length = 0
          unless LibCrypto.evp_cipherfinal_ex(ctx, buffer.to_unsafe + total, pointerof(final_length)) == 1
            raise general_err
          end
          total += final_length

          tag = Bytes.new(TAG_LEN)
          unless LibCrypto.evp_cipher_ctx_ctrl(ctx, LibCrypto::EVP_CTRL_GCM_GET_TAG, TAG_LEN, tag.to_unsafe.as(Void*)) == 1
            raise general_err
          end

          {buffer[0, total], tag}
        ensure
          LibCrypto.evp_cipher_ctx_free(ctx)
        end
      end

      # Decrypts `ciphertext` with `aad` and `tag`, returning the plaintext.
      def self.open(key : Bytes, nonce : Bytes, aad : Bytes, ciphertext : Bytes, tag : Bytes) : Bytes
        cipher = cipher_for(key)
        ctx = LibCrypto.evp_cipher_ctx_new
        begin
          raise general_err unless LibCrypto.evp_cipherinit_ex(ctx, cipher, nil, key.to_unsafe, nil, 0) == 1
          raise general_err unless LibCrypto.evp_cipher_ctx_ctrl(ctx, EVP_CTRL_AEAD_SET_IVLEN, nonce.size, nil) == 1
          raise general_err unless LibCrypto.evp_cipherinit_ex(ctx, nil, nil, nil, nonce.to_unsafe, 0) == 1

          length = 0
          unless aad.empty?
            raise general_err unless LibCrypto.evp_cipherupdate(ctx, nil, pointerof(length), aad.to_unsafe, aad.size) == 1
          end

          buffer = Bytes.new(ciphertext.size + TAG_LEN)
          unless LibCrypto.evp_cipherupdate(ctx, buffer.to_unsafe, pointerof(length), ciphertext.to_unsafe, ciphertext.size) == 1
            raise general_err
          end
          total = length
          unless LibCrypto.evp_cipher_ctx_ctrl(ctx, LibCrypto::EVP_CTRL_GCM_SET_TAG, tag.size, tag.to_unsafe.as(Void*)) == 1
            raise general_err
          end
          final_length = 0
          # `EVP_CipherFinal_ex` returns 0 when the tag does not verify.
          unless LibCrypto.evp_cipherfinal_ex(ctx, buffer.to_unsafe + total, pointerof(final_length)) == 1
            raise GeneralError.new("AES-GCM authentication tag verification failed")
          end
          total += final_length

          buffer[0, total]
        ensure
          LibCrypto.evp_cipher_ctx_free(ctx)
        end
      end

      private def self.general_err : GeneralError
        GeneralError.new("AES-GCM cipher operation failed")
      end
    end

    # Decrypts AES-GCM blocks (`BlockDecryptor` upstream).
    abstract class BlockDecryptor
      # Decrypts `more_ciphertext` (a `[size, nonce, ciphertext, tag]` block)
      # using `aad`.
      abstract def decrypt(length_and_ciphertext : Bytes, aad : Bytes) : Bytes

      # Recomputes the authentication tag over `plaintext` (whose trailing
      # `nonce`+`tag` frame the nonce) using `aad`.
      abstract def compute_plaintext_tag(aad : Bytes, plaintext : Bytes) : Bytes
    end

    # AES-GCM block decryptor.
    class GcmBlockDecryptor < BlockDecryptor
      @key : Bytes

      def initialize(key_bytes : Bytes)
        check_key_length(key_bytes)
        @key = key_bytes.dup
      end

      def decrypt(length_and_ciphertext : Bytes, aad : Bytes) : Bytes
        nonce = length_and_ciphertext[SIZE_LEN, NONCE_LEN]
        frame = length_and_ciphertext[SIZE_LEN + NONCE_LEN, length_and_ciphertext.size - SIZE_LEN - NONCE_LEN]
        ciphertext = frame[0, frame.size - TAG_LEN]
        tag = frame[frame.size - TAG_LEN, TAG_LEN]
        AesGcm.open(@key, nonce, aad, ciphertext, tag)
      end

      def compute_plaintext_tag(aad : Bytes, plaintext : Bytes) : Bytes
        end_index = plaintext.size - NONCE_LEN - TAG_LEN
        nonce = plaintext[end_index, NONCE_LEN]
        _, tag = AesGcm.seal(@key, nonce, aad, plaintext[0, end_index])
        tag
      end

      private def check_key_length(key_bytes : Bytes) : Nil
        return if key_bytes.size == 16 || key_bytes.size == 32

        raise GeneralError.new(
          "Error creating GcmBlockDecryptor with unsupported key length: #{key_bytes.size}"
        )
      end
    end

    # Encrypts AES-GCM blocks (`BlockEncryptor` upstream).
    abstract class BlockEncryptor
      # Encrypts `plaintext` with `aad`, returning the framed
      # `[size, nonce, ciphertext, tag]` block.
      abstract def encrypt(plaintext : Bytes, aad : Bytes) : Bytes
    end

    # AES-GCM block encryptor with an advancing counter nonce.
    class GcmBlockEncryptor < BlockEncryptor
      @key : Bytes
      @nonce_sequence : CounterNonce

      def initialize(key_bytes : Bytes)
        unless key_bytes.size == 16 || key_bytes.size == 32
          raise GeneralError.new(
            "Error creating GcmBlockEncryptor with unsupported key length: #{key_bytes.size}"
          )
        end
        @key = key_bytes.dup
        @nonce_sequence = CounterNonce.new
      end

      def encrypt(plaintext : Bytes, aad : Bytes) : Bytes
        ciphertext_length = NONCE_LEN + plaintext.size + TAG_LEN
        nonce = @nonce_sequence.advance
        ciphertext, tag = AesGcm.seal(@key, nonce, aad, plaintext)

        io = IO::Memory.new(SIZE_LEN + ciphertext_length)
        io.write_bytes(ciphertext_length.to_u32, IO::ByteFormat::LittleEndian)
        io.write(nonce)
        io.write(ciphertext)
        io.write(tag)
        io.to_slice
      end
    end

    # A counter-based 96-bit nonce sequence seeded from a random start.
    class CounterNonce
      @start : UInt128
      @counter : UInt128

      def initialize
        buffer = Random::Secure.random_bytes(16)
        @start = IO::Memory.new(buffer).read_bytes(UInt128, IO::ByteFormat::SystemEndian) & RIGHT_TWELVE
        @counter = @start &+ 1
      end

      # The current nonce bytes (low 12 bytes of the counter, little-endian).
      def bytes : Bytes
        io = IO::Memory.new(NONCE_LEN)
        io.write_bytes(@counter, IO::ByteFormat::LittleEndian)
        io.to_slice[0, NONCE_LEN]
      end

      # Advances the counter, returning the nonce used for this block.
      def advance : Bytes
        if (@counter & RIGHT_TWELVE) == (@start & RIGHT_TWELVE)
          raise GeneralError.new("Counter nonce sequence exhausted")
        end

        bytes = self.bytes
        @counter = @counter &+ 1
        bytes
      end
    end
  end
end
