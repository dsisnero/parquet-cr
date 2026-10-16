require "../spec_helper"

# Ported from `parquet-variant/src/path.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `VariantPath::try_from` -> `VariantPath.parse`; `from_iter` is a
# class method; Rust's `usize` index is `Int32`; `ArrowError::ParseError` is
# `ParquetVariant::ParseError`.

private alias VP = ParquetVariant::VariantPath
private alias VPE = ParquetVariant::VariantPathElement

private def field(name : String) : VPE
  VPE.field(name)
end

private def index(value : Int32) : VPE
  VPE.index(value)
end

private def expect_parse_error(message : String, path : String) : Nil
  error = expect_raises(ParquetVariant::ParseError) do
    VP.parse(path)
  end
  error.message.should eq("Parser error: #{message}")
end

describe ParquetVariant::VariantPath do
  # Upstream `test_variant_path_empty`.
  it "is empty when constructed from no elements" do
    VP.from_iter([] of VPE).empty?.should be_true
  end

  # Upstream `test_variant_path_empty_str`.
  it "is empty when parsed from an empty string" do
    VP.parse("").empty?.should be_true
  end

  # Upstream `test_variant_path_non_empty`.
  it "is not empty with one element" do
    VP.from_iter([field("a")]).empty?.should be_false
  end

  # Upstream `test_variant_path_dot_notation_with_array_index`.
  it "parses dot notation with an array index" do
    path = VP.parse("city.store.books[3].title")
    expected = VP.parse("city").join("store").join("books").join(3).join("title")
    path.should eq(expected)
  end

  # Upstream `test_variant_path_dot_notation_with_only_array_index`.
  it "parses a lone array index" do
    VP.parse("[3]").should eq(VP.from(3))
  end

  # Upstream `test_variant_path_dot_notation_with_starting_array_index`.
  it "parses a leading array index followed by a field" do
    VP.parse("[3].title").should eq(VP.from(3).join("title"))
  end

  # Upstream `test_variant_path_field_in_bracket`.
  it "parses fields and indexes inside brackets" do
    VP.parse("foo[0].bar").should eq(VP.from_iter([field("foo"), index(0), field("bar")]))
    VP.parse("foo.bar[42]").should eq(VP.from_iter([field("foo"), field("bar"), index(42)]))
    VP.parse("foo.bar['abc'][\"def\"]").should eq(
      VP.from_iter([field("foo"), field("bar"), field("abc"), field("def")])
    )
    VP.parse("foo['0'].bar[\"1\"]").should eq(
      VP.from_iter([field("foo"), field("0"), field("bar"), field("1")])
    )
  end

  # Upstream `test_invalid_path_parse`.
  it "rejects invalid paths" do
    expect_parse_error("Unexpected leading '.'", ".foo.bar")
    expect_parse_error("Unexpected trailing '.'", "foo.bar.")
    expect_parse_error("Unclosed '[' at byte 7", "foo.bar[2.baz")
    expect_parse_error("Unclosed '[' at byte 7", "foo.bar[2\\].fds")
    expect_parse_error("Unclosed '[' at byte 7", "foo.bar[fdafa\\")
    expect_parse_error("Unexpected ']' at byte 7", "foo.bar]baz")
    expect_parse_error(
      "Invalid token in bracket request: `123abc`. Expected a quoted string or a number(e.g., `['field']` or `[123]`)",
      "foo.bar[123abc]"
    )
    expect_parse_error(
      "Invalid token in bracket request: `abc`. Expected a quoted string or a number(e.g., `['field']` or `[123]`)",
      "foo.bar[abc]"
    )

    error = expect_raises(ParquetVariant::ParseError) { VP.parse("[9999999999]") }
    error.message.not_nil!.should contain("Invalid token in bracket request")
  end

  # Upstream `path[1]` / `push` / `path` accessors.
  it "exposes path elements" do
    path = VP.parse("foo").join("bar").join("baz")
    path[1].should eq(field("bar"))
    path.path.should eq([field("foo"), field("bar"), field("baz")])
  end
end
