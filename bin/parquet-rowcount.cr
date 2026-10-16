require "../src/parquet-cr"
require "option_parser"

# `parquet-rowcount` entry point: prints the row count of each Parquet file.
# Ported from `parquet/src/bin/parquet-rowcount.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

files = [] of String

OptionParser.parse do |parser|
  parser.banner = "Usage: parquet-rowcount <file> [<file> ...]"
  parser.on("-h", "--help", "Print help") { puts parser; exit 0 }
  parser.unknown_args { |before, after| files = before + after }
end

if files.empty?
  STDERR.puts "Usage: parquet-rowcount <file> [<file> ...]"
  exit 1
end

Parquet::Bin::ParquetRowCount.run(STDERR, files)
