require "../src/parquet-cr"
require "option_parser"

# `parquet-layout` entry point: prints the physical layout of a Parquet file as
# JSON. Ported from `parquet/src/bin/parquet-layout.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

file_name = ""

OptionParser.parse do |parser|
  parser.banner = "Usage: parquet-layout <file>"
  parser.on("-h", "--help", "Print help") { puts parser; exit 0 }
  parser.unknown_args { |before, after| file_name = (before + after).first? || "" }
end

if file_name.empty?
  STDERR.puts "Usage: parquet-layout <file>"
  exit 1
end

Parquet::Bin::ParquetLayout.run(STDOUT, file_name)
