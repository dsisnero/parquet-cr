require "../spec_helper"

# Ported from `parquet/src/errors.rs` `mod tests` and the `Display`
# implementation (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe Parquet::ParquetError do
  # Upstream: `error_macros_format_inline_args`. The single-argument arms of the
  # error macros must format their argument so inline arguments are not emitted
  # as literal text.
  it "formats inline args in error macros" do
    expected = 1
    actual = 2

    general_err!("expected #{expected}, got #{actual}").message
      .should eq("Parquet error: expected 1, got 2")
    nyi_err!("not yet: #{expected}").message
      .should eq("NYI: not yet: 1")
    eof_err!("eof at #{actual}").message
      .should eq("EOF: eof at 2")
  end

  it "renders every variant with the upstream Display format" do
    general_err!("boom").message.should eq("Parquet error: boom")
    nyi_err!("later").message.should eq("NYI: later")
    eof_err!("short").message.should eq("EOF: short")
    arrow_err!("bad arrow").message.should eq("Arrow: bad arrow")

    Parquet::IndexOutOfBoundError.new(3_u64, 2_u64).message
      .should eq("Index 3 out of bound: 2")
    Parquet::NeedMoreDataError.new(64_u64).message
      .should eq("NeedMoreData: 64")
    Parquet::NeedMoreDataRangeError.new(4_u64..10_u64).message
      .should eq("NeedMoreDataRange: 4..10")

    cause = ArgumentError.new("inner")
    Parquet::ExternalError.new(cause).message.should eq("External: inner")
  end

  it "wraps integer overflow as a general error" do
    # Upstream: `From<TryFromIntError>` -> `General("Integer overflow: {e}")`,
    # which `Display` renders with the `Parquet error: ` prefix.
    Parquet.overflow_error(OverflowError.new("overflow")).message
      .should eq("Parquet error: Integer overflow: overflow")
  end

  it "wraps IO errors as external errors" do
    error = Parquet.from_io_error(IO::Error.new("disk"))
    error.should be_a(Parquet::ExternalError)
    error.message.should eq("External: disk")
  end

  it "is rescuable as ParquetError" do
    expect_raises(Parquet::ParquetError, "EOF: missing") do
      raise eof_err!("missing")
    end
  end
end
