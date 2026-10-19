require "base64"
require "../parquet_variant"

# Ported from `parquet-variant-json/src/to_json.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): the value formatting helpers used
# when rendering a variant as JSON.
#
# Divergences: chrono's `NaiveDate`/`NaiveDateTime`/`NaiveTime` are `Time`/
# `Time::Span` (Crystal has no date-only or time-of-day types), and float
# rendering strips Crystal's `.0` suffix so whole floats render like Rust's
# `Display` (`0` rather than `0.0`).
module ParquetVariantJson
  # JSON value formatting for variant payloads.
  module Formatting
    # Upstream `DATE_FORMAT`.
    DATE_FORMAT = "%Y-%m-%d"

    # Formats a date as `YYYY-MM-DD` (upstream `format_date_string`).
    def self.date_string(date : Time) : String
      date.to_s(DATE_FORMAT)
    end

    # Formats a naive timestamp with `precision` fractional digits (upstream
    # `format_timestamp_ntz_string`).
    def self.timestamp_ntz_string(time : Time, precision : Int32) : String
      base = time.to_s("%Y-%m-%dT%H:%M:%S")
      digits = time.nanosecond.to_s.rjust(9, '0')[0, precision]
      "#{base}.#{digits}"
    end

    # Formats a timestamp like chrono's `DateTime::to_rfc3339`: the shortest of
    # 0/3/6/9 fractional digits followed by a `+HH:MM` offset (upstream calls
    # `to_rfc3339` directly).
    def self.rfc3339(time : Time) : String
      base = time.to_s("%Y-%m-%dT%H:%M:%S")
      nanos = time.nanosecond
      fraction =
        if nanos == 0
          ""
        elsif nanos % 1_000_000 == 0
          "." + (nanos // 1_000_000).to_s.rjust(3, '0')
        elsif nanos % 1_000 == 0
          "." + (nanos // 1_000).to_s.rjust(6, '0')
        else
          "." + nanos.to_s.rjust(9, '0')
        end
      "#{base}#{fraction}#{offset_string(time)}"
    end

    # Formats a time-of-day, trimming trailing zeros from the microseconds and
    # always keeping a fraction (upstream `format_time_ntz_str`).
    def self.time_string(span : Time::Span) : String
      total_seconds = span.total_seconds.to_i64 % (24 * 60 * 60)
      hours = total_seconds // 3600
      minutes = (total_seconds % 3600) // 60
      seconds = total_seconds % 60
      base = "#{pad2(hours)}:#{pad2(minutes)}:#{pad2(seconds)}"

      micros = span.nanoseconds // 1000
      return "#{base}.0" if micros == 0

      "#{base}.#{micros.to_s.rjust(6, '0').rstrip('0')}"
    end

    # Base64-encodes binary data (upstream `format_binary_base64`).
    def self.binary_base64(bytes : Bytes) : String
      Base64.strict_encode(bytes)
    end

    # Formats a `Float32` like Rust's `Display` for `f32`.
    def self.float32_string(value : Float32) : String
      return "NaN" if value.nan?
      return value > 0 ? "inf" : "-inf" if value.infinite?

      strip_whole_suffix(value.to_s)
    end

    # Formats a `Float64` like Rust's `Display` for `f64`.
    def self.float64_string(value : Float64) : String
      return "NaN" if value.nan?
      return value > 0 ? "inf" : "-inf" if value.infinite?

      strip_whole_suffix(value.to_s)
    end

    private def self.strip_whole_suffix(value : String) : String
      value.ends_with?(".0") ? value[0...-2] : value
    end

    private def self.pad2(value : Int64) : String
      value.to_s.rjust(2, '0')
    end

    private def self.offset_string(time : Time) : String
      offset = time.offset
      sign = offset < 0 ? '-' : '+'
      total = offset.abs
      "#{sign}#{pad2((total // 3600).to_i64)}:#{pad2(((total % 3600) // 60).to_i64)}"
    end
  end
end
