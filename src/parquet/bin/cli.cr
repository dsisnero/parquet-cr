require "../errors"

# Ported from `parquet/src/bin/cli/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Shared helpers for the CLI tools. Divergence: upstream returns
# `Result<Vec<String>, String>`; Crystal raises `Parquet::GeneralError`.
module Parquet
  module Bin
    # Shared helpers for the `parquet-*` CLI tools.
    module Cli
      # Parses a dot-separated Parquet column path, using `\` as an escape
      # character for literal dots and backslashes.
      def self.parse_column_path(path : String) : Array(String)
        parts = [] of String
        current = String.new
        escaped = false

        path.each_char do |char|
          if escaped
            case char
            when '.', '\\'
              current += char
            else
              raise general_err!("Invalid escape sequence \\#{char} in column path #{path}")
            end
            escaped = false
            next
          end

          case char
          when '\\'
            escaped = true
          when '.'
            parts << current
            current = String.new
          else
            current += char
          end
        end

        if escaped
          raise general_err!("Column path #{path} ends with an incomplete escape sequence")
        end

        parts << current
        parts
      end
    end
  end
end
