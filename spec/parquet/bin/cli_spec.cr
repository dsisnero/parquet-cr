require "../../spec_helper"

# Ported from `parquet/src/bin/cli/mod.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias Cli = Parquet::Bin::Cli

describe Parquet::Bin::Cli do
  # Upstream `parse_column_path_splits_unescaped_dots`.
  it "splits unescaped dots" do
    Cli.parse_column_path("a.b.c").should eq(["a", "b", "c"])
  end

  # Upstream `parse_column_path_allows_escaped_dots`.
  it "allows escaped dots" do
    Cli.parse_column_path(%q(a\.b.c)).should eq(["a.b", "c"])
  end

  # Upstream `parse_column_path_allows_escaped_backslashes`.
  it "allows escaped backslashes" do
    Cli.parse_column_path(%q(a\\b.c)).should eq([%q(a\b), "c"])
  end

  # Upstream `parse_column_path_invalid_escape_sequence`.
  it "rejects an invalid escape sequence" do
    expect_raises(Parquet::GeneralError, /Invalid escape sequence/) do
      Cli.parse_column_path(%q(a.\b.c))
    end
  end

  # Upstream `parse_column_path_incomplete_escape_sequence`.
  it "rejects an incomplete escape sequence" do
    expect_raises(Parquet::GeneralError, /incomplete escape sequence/) do
      Cli.parse_column_path(%q(a.b.c\))
    end
  end
end
