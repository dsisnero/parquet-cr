require "../src/parquet-cr"
require "option_parser"

# `parquet-concat` entry point: concatenates one or more Parquet files. Ported
# from `parquet/src/bin/parquet-concat.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

positional = [] of String

OptionParser.parse do |parser|
  parser.banner = "Usage: parquet-concat <output> <input> [<input> ...]"
  parser.on("-h", "--help", "Print help") { puts parser; exit 0 }
  parser.unknown_args { |before, after| positional = before + after }
end

if positional.size < 2
  STDERR.puts "Usage: parquet-concat <output> <input> [<input> ...]"
  exit 1
end

Parquet::Bin::ParquetConcat.run(positional[0], positional[1..])
