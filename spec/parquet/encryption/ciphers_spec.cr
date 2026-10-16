require "../../spec_helper"

# Ported from `parquet/src/encryption/ciphers.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `ring` is replaced by an OpenSSL EVP AES-GCM (`AesGcm`) with the
# same `[size, nonce, ciphertext, tag]` framing; the `ring`-prefixed types are
# `GcmBlockEncryptor`/`GcmBlockDecryptor`.

private alias E = Parquet::Encryption

describe Parquet::Encryption::GcmBlockEncryptor do
  # Upstream `test_round_trip`.
  it "round-trips an AES-GCM block" do
    key = Bytes.new(16)
    encryptor = E::GcmBlockEncryptor.new(key)
    decryptor = E::GcmBlockDecryptor.new(key)

    plaintext = "hello, world!".to_slice
    aad = "some aad".to_slice

    ciphertext = encryptor.encrypt(plaintext, aad)
    decryptor.decrypt(ciphertext, aad).should eq(plaintext)
  end

  it "round-trips with an AES-256 key" do
    key = Bytes.new(32) { |i| i.to_u8 }
    encryptor = E::GcmBlockEncryptor.new(key)
    decryptor = E::GcmBlockDecryptor.new(key)

    plaintext = "a longer payload for aes-256".to_slice
    aad = "different aad".to_slice

    ciphertext = encryptor.encrypt(plaintext, aad)
    expected_size = IO::Memory.new
    expected_size.write_bytes((E::NONCE_LEN + plaintext.size + E::TAG_LEN).to_u32, IO::ByteFormat::LittleEndian)
    ciphertext[0, 4].should eq(expected_size.to_slice)
    decryptor.decrypt(ciphertext, aad).should eq(plaintext)
  end

  it "advances the nonce on each block" do
    key = Bytes.new(16)
    encryptor = E::GcmBlockEncryptor.new(key)
    aad = Bytes.empty

    first = encryptor.encrypt("one".to_slice, aad)
    second = encryptor.encrypt("one".to_slice, aad)

    first[E::SIZE_LEN, E::NONCE_LEN].should_not eq(second[E::SIZE_LEN, E::NONCE_LEN])
  end

  it "rejects unsupported key lengths" do
    expect_raises(Parquet::GeneralError) { E::GcmBlockEncryptor.new(Bytes.new(8)) }
    expect_raises(Parquet::GeneralError) { E::GcmBlockDecryptor.new(Bytes.new(24)) }
  end
end

describe Parquet::Encryption::GcmBlockDecryptor do
  it "rejects a tampered authentication tag" do
    key = Bytes.new(16)
    encryptor = E::GcmBlockEncryptor.new(key)
    decryptor = E::GcmBlockDecryptor.new(key)

    ciphertext = encryptor.encrypt("secret".to_slice, "aad".to_slice)
    ciphertext[ciphertext.size - 1] ^= 0xFF_u8

    expect_raises(Parquet::GeneralError) { decryptor.decrypt(ciphertext, "aad".to_slice) }
  end

  it "rejects a mismatched AAD" do
    key = Bytes.new(16)
    encryptor = E::GcmBlockEncryptor.new(key)
    decryptor = E::GcmBlockDecryptor.new(key)

    ciphertext = encryptor.encrypt("secret".to_slice, "aad".to_slice)
    expect_raises(Parquet::GeneralError) { decryptor.decrypt(ciphertext, "other".to_slice) }
  end

  # Upstream `RingGcmBlockDecryptor::compute_plaintext_tag`.
  it "recomputes the plaintext tag" do
    key = Bytes.new(16)
    decryptor = E::GcmBlockDecryptor.new(key)
    nonce = Bytes.new(12) { |i| (i + 1).to_u8 }
    aad = "aad".to_slice
    plaintext = "the plaintext".to_slice

    _, tag = E::AesGcm.seal(key, nonce, aad, plaintext)
    frame = plaintext + nonce + Bytes.new(E::TAG_LEN)
    decryptor.compute_plaintext_tag(aad, frame).should eq(tag)
  end
end
