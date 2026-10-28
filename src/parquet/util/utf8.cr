require "../errors"

module Parquet::Util
  # Equivalent to upstream util::utf8::check_valid_utf8. Crystal exposes a
  # validity predicate rather than Rust's failing byte offset/error length.
  def self.check_valid_utf8(bytes : Bytes) : Nil
    unless String.new(bytes).valid_encoding?
      raise general_err!("encountered non UTF-8 data: invalid utf-8 sequence")
    end
  end
end
