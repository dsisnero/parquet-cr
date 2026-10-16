# Ported from the `Encoder`/`Decoder` traits in
# `parquet/src/encodings/encoding/mod.rs` and `parquet/src/encodings/decoding.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# An encoder/decoder for the data type `T`. Divergence: Rust traits become
# abstract generic classes; `Encoder#put_spaced` (Rust `#[cfg(test)]` trait
# default) and `Decoder#get_spaced` (trait default) are concrete methods on the
# base class.
module Parquet
  module Encodings
    # An encoder for the data type `T`.
    abstract class Encoder(T)
      # Encodes `values`.
      abstract def put(values : Array(T)) : Nil

      # Returns the encoding type of this encoder.
      abstract def encoding : Encoding

      # Returns an estimate of the encoded data, in bytes.
      abstract def estimated_data_encoded_size : Int32

      # Returns an estimate of the memory use of this encoder, in bytes.
      abstract def estimated_memory_size : Int32

      # Flushes the underlying byte buffer and returns it.
      abstract def flush_buffer : Bytes

      # Encodes values selected by `valid_bits`, returning the count encoded.
      def put_spaced(values : Array(T), valid_bits : Bytes) : Int32
        buffer = [] of T
        values.each_with_index do |item, i|
          buffer << item if Util::BitUtil.get_bit(valid_bits, i)
        end
        put(buffer)
        buffer.size
      end
    end

    # A decoder for the data type `T`.
    abstract class Decoder(T)
      # Sets `data`, which should contain `num_values` values to decode.
      abstract def set_data(data : Bytes, num_values : Int32) : Nil

      # Consumes values and writes the results to `buffer`, returning the count.
      abstract def get(buffer : Array(T)) : Int32

      # Returns the number of values left in this decoder stream.
      abstract def values_left : Int32

      # Returns the encoding for this decoder.
      abstract def encoding : Encoding

      # Skips the specified number of values.
      abstract def skip(num_values : Int32) : Int32

      # Consumes values into `buffer`, leaving spaces for `null_count` nulls
      # identified by `valid_bits`.
      def get_spaced(buffer : Array(T), null_count : Int32, valid_bits : Bytes) : Int32
        raise general_err!("Cannot get_spaced: buffer.len #{buffer.size} < null_count #{null_count}") if buffer.size < null_count
        return get(buffer) if null_count == 0

        num_values = buffer.size
        values_to_read = num_values - null_count
        values_read = get(buffer)
        if values_read != values_to_read
          raise general_err!(
            "Number of values read: #{values_read}, doesn't match expected: #{values_to_read}"
          )
        end
        values_to_move = values_read
        (num_values - 1).downto(0) do |i|
          if Util::BitUtil.get_bit(valid_bits, i)
            values_to_move -= 1
            buffer[i], buffer[values_to_move] = buffer[values_to_move], buffer[i]
          end
        end

        num_values
      end
    end
  end
end
