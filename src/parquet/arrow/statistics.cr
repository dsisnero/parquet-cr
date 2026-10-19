require "arrow-cr"
require "../errors"
require "../basic"
require "../file/metadata"
require "../file/metadata/page_index"
require "../file/page_index/column_index"
require "../file/statistics"
require "../schema/types"
require "./mod"
require "./buffer/bit_util"

# Ported from `parquet/src/arrow/arrow_reader/statistics.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `StatisticsConverter` maps the parquet statistics embedded in row-group and
# data-page metadata to arrow arrays, so callers (e.g. a query engine) can prune
# files, row groups and pages: `row_group_mins`/`row_group_maxes` and friends for
# column-chunk statistics, `data_page_mins`/`data_page_maxes` and friends for
# the page index (`ColumnIndexMetaData`).
#
# Divergences:
# - the upstream module-private free functions (`min_statistics`,
#   `max_statistics`, `min_page_statistics`, `max_page_statistics`,
#   `null_counts_page_statistics`, `nan_counts_page_statistics`, the
#   `from_bytes_to_*` decoders and the `make_stats_iterator!` adapters) live in
#   `StatisticsExtractors` (`Statistics` would shadow `Parquet::Statistics`);
#   the per-type iterator structs are replaced by extracting a canonical
#   `StatValue` and building the arrow array from it, which behaves the same;
# - `usize` counts are `Int32`, and the `IntoIterator` parameters are
#   `Enumerable`/`Array`;
# - `row_group_row_counts`/`data_page_row_counts` return a nilable
#   `PrimitiveArray(UInt64)` (upstream `Option<UInt64Array>`);
# - `Float16` uses arrow-cr's `Arrow::Float16` (Crystal has no native half
#   float) and `i256` is `Arrow::I256`;
# - the page null/NaN-count helpers append one value per count instead of
#   upstream's `len`-sized validity (identical when a chunk's null counts match
#   its page count, which the writer guarantees);
# - the `TryFrom`/`with_precision_and_scale` validation of decimal arrays is
#   implicit in `with_data_type` (arrow-cr validates the data type when it is
#   constructed), so a malformed precision/scale surfaces earlier;
# - `StatisticsConverter` keeps its `physical_type` private (upstream field is
#   private too) and `arrow_field`/`parquet_column_index` are getters instead of
#   borrowed accessors.
module Parquet
  module Arrow
    # Canonical statistic value, one variant per parquet physical representation
    # that can be extracted from statistics (upstream's per-physical-type
    # iterator items).
    alias StatValue = Bool | Int32 | Int64 | Float32 | Float64 | Bytes

    # The statistics extraction helpers of `arrow_reader/statistics.rs`
    # (upstream module-private free functions).
    module StatisticsExtractors
      # Converts big-endian statistics bytes to `Int32`
      # (upstream `from_bytes_to_i32`).
      def self.from_bytes_to_i32(bytes : Bytes) : Int32
        IO::ByteFormat::BigEndian.decode(Int32, Buffer.sign_extend_be(bytes, 4))
      end

      # Converts big-endian statistics bytes to `Int64`
      # (upstream `from_bytes_to_i64`).
      def self.from_bytes_to_i64(bytes : Bytes) : Int64
        IO::ByteFormat::BigEndian.decode(Int64, Buffer.sign_extend_be(bytes, 8))
      end

      # Converts big-endian statistics bytes to `Int128`
      # (upstream `from_bytes_to_i128`).
      def self.from_bytes_to_i128(bytes : Bytes) : Int128
        IO::ByteFormat::BigEndian.decode(Int128, Buffer.sign_extend_be(bytes, 16))
      end

      # Converts big-endian statistics bytes to `I256`
      # (upstream `from_bytes_to_i256`).
      def self.from_bytes_to_i256(bytes : Bytes) : ::Arrow::I256
        ::Arrow::I256.from_be_bytes(Buffer.sign_extend_be(bytes, 32))
      end

      # Converts little-endian FLOAT16 statistics bytes to `Float16`
      # (upstream `from_bytes_to_f16`).
      def self.from_bytes_to_f16(bytes : Bytes) : ::Arrow::Float16?
        return nil unless bytes.size == 2

        bits = (bytes[1].to_u16 << 8) | bytes[0].to_u16
        ::Arrow::Float16.from_bits(bits)
      end

      # Extracts the min (`max` = false) or max (`max` = true) value of a
      # parquet `Statistics`, if it is present and of a usable physical type.
      #
      # Upstream's `make_stats_iterator!` adapters do the same per physical
      # type; INT96 statistics are ignored, as upstream does.
      def self.stats_value(stats : ::Parquet::Statistics?, max : Bool) : StatValue?
        return nil unless stats

        case value = stats.value
        when ::Parquet::ValueStatistics(Bool)
          max ? value.max_opt : value.min_opt
        when ::Parquet::ValueStatistics(Int32)
          max ? value.max_opt : value.min_opt
        when ::Parquet::ValueStatistics(Int64)
          max ? value.max_opt : value.min_opt
        when ::Parquet::ValueStatistics(Float32)
          max ? value.max_opt : value.min_opt
        when ::Parquet::ValueStatistics(Float64)
          max ? value.max_opt : value.min_opt
        when ::Parquet::ValueStatistics(ByteArray)
          (max ? value.max_opt : value.min_opt).try(&.data)
        when ::Parquet::ValueStatistics(FixedLenByteArray)
          (max ? value.max_opt : value.min_opt).try(&.data)
        else
          nil
        end
      end

      # Extracts the per-page min/max values from the `ColumnIndexMetaData` of
      # each row group `(len, index)` chunk, appending `len` nulls when the
      # statistics are absent or of an unusable type (upstream
      # `get_data_page_statistics!`).
      def self.page_values(
        chunks : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)),
        max : Bool,
      ) : Array(StatValue?)
        values = [] of StatValue?

        chunks.each do |(len, index)|
          if index.nil?
            len.times { values << nil }
            next
          end

          case index.kind
          when ::Parquet::Type::BOOLEAN
            inner = index.inner.as(::Parquet::File::PageIndex::PrimitiveColumnIndex(Bool))
            (max ? inner.max_values_iter : inner.min_values_iter).each { |value| values << value }
          when ::Parquet::Type::INT32
            inner = index.inner.as(::Parquet::File::PageIndex::PrimitiveColumnIndex(Int32))
            (max ? inner.max_values_iter : inner.min_values_iter).each { |value| values << value }
          when ::Parquet::Type::INT64
            inner = index.inner.as(::Parquet::File::PageIndex::PrimitiveColumnIndex(Int64))
            (max ? inner.max_values_iter : inner.min_values_iter).each { |value| values << value }
          when ::Parquet::Type::FLOAT
            inner = index.inner.as(::Parquet::File::PageIndex::PrimitiveColumnIndex(Float32))
            (max ? inner.max_values_iter : inner.min_values_iter).each { |value| values << value }
          when ::Parquet::Type::DOUBLE
            inner = index.inner.as(::Parquet::File::PageIndex::PrimitiveColumnIndex(Float64))
            (max ? inner.max_values_iter : inner.min_values_iter).each { |value| values << value }
          when ::Parquet::Type::BYTE_ARRAY, ::Parquet::Type::FIXED_LEN_BYTE_ARRAY
            inner = index.inner.as(::Parquet::File::PageIndex::ByteArrayColumnIndex)
            (max ? inner.max_values_iter : inner.min_values_iter).each { |value| values << value }
          else
            # INT96 page statistics are not extracted (as upstream).
            len.times { values << nil }
          end
        end

        values
      end

      # Upstream `min_statistics`: extracts row-group minimums as an arrow array.
      def self.min_statistics(
        data_type : ::Arrow::DataType,
        statistics : Array(::Parquet::Statistics?),
        physical_type : ::Parquet::Type?,
      ) : ::Arrow::ArrayBase
        build_stats_array(data_type, statistics.map { |stats| stats_value(stats, false) }, physical_type)
      end

      # Upstream `max_statistics`: extracts row-group maximums as an arrow array.
      def self.max_statistics(
        data_type : ::Arrow::DataType,
        statistics : Array(::Parquet::Statistics?),
        physical_type : ::Parquet::Type?,
      ) : ::Arrow::ArrayBase
        build_stats_array(data_type, statistics.map { |stats| stats_value(stats, true) }, physical_type)
      end

      # Upstream `min_page_statistics`: extracts data-page minimums.
      def self.min_page_statistics(
        data_type : ::Arrow::DataType,
        chunks : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)),
        physical_type : ::Parquet::Type?,
      ) : ::Arrow::ArrayBase
        build_stats_array(data_type, page_values(chunks, false), physical_type)
      end

      # Upstream `max_page_statistics`: extracts data-page maximums.
      def self.max_page_statistics(
        data_type : ::Arrow::DataType,
        chunks : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)),
        physical_type : ::Parquet::Type?,
      ) : ::Arrow::ArrayBase
        build_stats_array(data_type, page_values(chunks, true), physical_type)
      end

      # Upstream `null_counts_page_statistics`: the per-page null counts.
      def self.null_counts_page_statistics(
        chunks : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)),
      ) : ::Arrow::PrimitiveArray(UInt64)
        counts_page_statistics(chunks, &.null_counts)
      end

      # Upstream `nan_counts_page_statistics`: the per-page NaN counts.
      def self.nan_counts_page_statistics(
        chunks : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)),
      ) : ::Arrow::PrimitiveArray(UInt64)
        counts_page_statistics(chunks, &.nan_counts)
      end

      private def self.counts_page_statistics(
        chunks : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)),
        & : ::Parquet::File::PageIndex::ColumnIndexMetaData -> Array(Int64)?
      ) : ::Arrow::PrimitiveArray(UInt64)
        values = [] of UInt64?
        chunks.each do |(len, index)|
          counts = index.try { |chunk| yield chunk }
          if counts
            counts.each { |count| values << count.to_u64 }
          else
            len.times { values << nil }
          end
        end
        ::Arrow::PrimitiveArray(UInt64).from_options(values)
      end

      # Builds the arrow statistics array for `data_type` from canonical
      # statistic values (upstream `get_statistics!`/`get_data_page_statistics!`).
      def self.build_stats_array(
        data_type : ::Arrow::DataType,
        values : Array(StatValue?),
        physical_type : ::Parquet::Type?,
      ) : ::Arrow::ArrayBase
        kind = data_type.kind

        case kind
        when .boolean?
          ::Arrow::BooleanArray.from_options(values.map { |value| value.as?(Bool) })
        when .int8?
          primitive(Int8, data_type, values) { |value| narrow_i8(value.as?(Int32)) }
        when .int16?
          primitive(Int16, data_type, values) { |value| narrow_i16(value.as?(Int32)) }
        when .int32?
          primitive(Int32, data_type, values) { |value| value.as?(Int32) }
        when .int64?
          primitive(Int64, data_type, values) { |value| value.as?(Int64) }
        when .u_int8?
          primitive(UInt8, data_type, values) { |value| narrow_u8(value.as?(Int32)) }
        when .u_int16?
          primitive(UInt16, data_type, values) { |value| narrow_u16(value.as?(Int32)) }
        when .u_int32?
          # Upstream casts (`*x as u32`), i.e. wrapping semantics.
          primitive(UInt32, data_type, values) { |value| value.as?(Int32).try(&.to_u32!) }
        when .u_int64?
          primitive(UInt64, data_type, values) { |value| value.as?(Int64).try(&.to_u64!) }
        when .float16?
          primitive(::Arrow::Float16, data_type, values) do |value|
            value.as?(Bytes).try { |bytes| from_bytes_to_f16(bytes) }
          end
        when .float32?
          primitive(Float32, data_type, values) { |value| value.as?(Float32) }
        when .float64?
          primitive(Float64, data_type, values) { |value| value.as?(Float64) }
        when .date32?
          primitive(Int32, data_type, values) { |value| value.as?(Int32) }
        when .date64?
          # Date64 statistics depend on the physical type: INT32 days are
          # scaled to milliseconds, INT64 values are already milliseconds.
          case physical_type
          when ::Parquet::Type::INT32
            primitive(Int64, data_type, values) do |value|
              value.as?(Int32).try { |days| days.to_i64 * 24 * 60 * 60 * 1000 }
            end
          when ::Parquet::Type::INT64
            primitive(Int64, data_type, values) { |value| value.as?(Int64) }
          else
            ::Arrow.new_null_array(data_type, values.size)
          end
        when .timestamp?
          primitive(Int64, data_type, values) { |value| value.as?(Int64) }
        when .time32?
          unit = data_type.time_unit
          if unit == ::Arrow::TimeUnit::Second || unit == ::Arrow::TimeUnit::Millisecond
            primitive(Int32, data_type, values) { |value| value.as?(Int32) }
          else
            ::Arrow.new_null_array(data_type, values.size)
          end
        when .time64?
          unit = data_type.time_unit
          if unit == ::Arrow::TimeUnit::Microsecond || unit == ::Arrow::TimeUnit::Nanosecond
            primitive(Int64, data_type, values) { |value| value.as?(Int64) }
          else
            ::Arrow.new_null_array(data_type, values.size)
          end
        when .binary?
          ::Arrow::GenericBinaryArray(Int32).from_options(values.map { |value| value.as?(Bytes) })
        when .large_binary?
          ::Arrow::GenericBinaryArray(Int64).from_options(values.map { |value| value.as?(Bytes) })
        when .utf8?
          ::Arrow::GenericStringArray(Int32).from_options(utf8_values(values))
        when .large_utf8?
          ::Arrow::GenericStringArray(Int64).from_options(utf8_values(values))
        when .utf8_view?
          ::Arrow::StringViewArray.from_options(utf8_values(values))
        when .binary_view?
          ::Arrow::BinaryViewArray.from_options(values.map { |value| value.as?(Bytes) })
        when .fixed_size_binary?
          fixed_size_binary_array(data_type.byte_width || 0, values)
        when .decimal32?
          primitive(Int32, data_type, values) { |value| narrow_i32(decimal_value(value, 4)) }
        when .decimal64?
          primitive(Int64, data_type, values) { |value| narrow_i64(decimal_value(value, 8)) }
        when .decimal128?
          primitive(Int128, data_type, values) { |value| decimal_value(value, 16) }
        when .decimal256?
          primitive(::Arrow::I256, data_type, values) do |value|
            case value
            when Int32 then ::Arrow::I256.from_i128(value.to_i128)
            when Int64 then ::Arrow::I256.from_i128(value.to_i128)
            when Bytes then from_bytes_to_i256(value)
            end
          end
        when .dictionary?
          # Statistics are stored as the dictionary's value type.
          value_type = data_type.value
          raise general_err!("Dictionary type missing a value type") unless value_type

          build_stats_array(value_type, values, physical_type)
        else
          # Unknown how to extract statistics, so return a null array.
          ::Arrow.new_null_array(data_type, values.size)
        end
      end

      # Builds a `PrimitiveArray` of `t` carrying `data_type`.
      private def self.primitive(
        t : T.class,
        data_type : ::Arrow::DataType,
        values : Array(StatValue?),
        & : StatValue? -> T?
      ) : ::Arrow::ArrayBase forall T
        ::Arrow::PrimitiveArray(T).from_options(values.map { |value| yield value }).with_data_type(data_type)
      end

      # Converts canonical values to UTF-8 strings, dropping invalid UTF-8 (as
      # upstream's `std::str::from_utf8` check does).
      private def self.utf8_values(values : Array(StatValue?)) : Array(String?)
        values.map do |value|
          bytes = value.as?(Bytes)
          next nil unless bytes

          string = String.new(bytes)
          string.valid_encoding? ? string : nil
        end
      end

      # Builds a `FixedSizeBinaryArray`, dropping values whose width does not
      # match `size`.
      private def self.fixed_size_binary_array(size : Int32, values : Array(StatValue?)) : ::Arrow::ArrayBase
        rows = values.map do |value|
          bytes = value.as?(Bytes)
          bytes && bytes.size == size ? bytes : nil
        end

        buffer = Bytes.new(size * rows.size)
        rows.each_with_index do |bytes, index|
          next unless bytes

          bytes.copy_to(buffer[index * size, size])
        end
        nulls =
          if rows.any?(Nil)
            ::Arrow::NullBuffer.new(::Arrow::BooleanBuffer.from_slice(rows.map { |row| !row.nil? }))
          end
        ::Arrow::FixedSizeBinaryArray.new(size, ::Arrow::Buffer.from_slice_ref(buffer), nulls)
      end

      # Decimal statistics accept INT32/INT64 statistics and (fixed-length) byte
      # array statistics, sign-extended to `width` bytes (upstream
      # `make_decimal_stats_iterator!`).
      private def self.decimal_value(value : StatValue?, width : Int32) : Int128?
        case value
        when Int32
          value.to_i128
        when Int64
          value.to_i128
        when Bytes
          case width
          when  4 then from_bytes_to_i32(value).to_i128
          when  8 then from_bytes_to_i64(value).to_i128
          when 16 then from_bytes_to_i128(value)
          end
        end
      end

      # Narrows an extracted decimal value to the `Decimal32` width.
      private def self.narrow_i32(value : Int128?) : Int32?
        return nil unless value
        Int32::MIN <= value <= Int32::MAX ? value.to_i32 : nil
      end

      # Narrows an extracted decimal value to the `Decimal64` width.
      private def self.narrow_i64(value : Int128?) : Int64?
        return nil unless value
        Int64::MIN <= value <= Int64::MAX ? value.to_i64 : nil
      end

      # `i8::try_from` for canonical INT32 statistics.
      private def self.narrow_i8(value : Int32?) : Int8?
        return nil unless value
        Int8::MIN <= value <= Int8::MAX ? value.to_i8 : nil
      end

      # `i16::try_from` for canonical INT32 statistics.
      private def self.narrow_i16(value : Int32?) : Int16?
        return nil unless value
        Int16::MIN <= value <= Int16::MAX ? value.to_i16 : nil
      end

      # `u8::try_from` for canonical INT32 statistics.
      private def self.narrow_u8(value : Int32?) : UInt8?
        return nil unless value
        0 <= value <= UInt8::MAX ? value.to_u8 : nil
      end

      # `u16::try_from` for canonical INT32 statistics.
      private def self.narrow_u16(value : Int32?) : UInt16?
        return nil unless value
        0 <= value <= UInt16::MAX ? value.to_u16 : nil
      end
    end

    # Converts parquet statistics to arrow arrays (upstream
    # `StatisticsConverter`).
    #
    # This is used to prune parquet files, row groups and data pages based on
    # the statistics embedded in parquet metadata.
    #
    # The converter uses the parquet schema and the arrow schema to convert the
    # underlying statistics value (stored as a parquet value) into the
    # corresponding arrow value. The schemas do not have to be identical (the
    # columns may be in different orders and one or the other schema may have
    # additional columns); `Parquet::Arrow.parquet_column` matches the column in
    # the parquet schema to the column in the arrow schema.
    class StatisticsConverter
      # The index of the matched column in the parquet schema, if any.
      getter parquet_column_index : Int32?
      # The field (with data type) of the column in the arrow schema.
      getter arrow_field : ::Arrow::Field
      # Whether missing null counts are treated as zero nulls.
      getter? missing_null_counts_as_zero : Bool

      @physical_type : ::Parquet::Type?

      private def initialize(
        @parquet_column_index : Int32?,
        @arrow_field : ::Arrow::Field,
        @physical_type : ::Parquet::Type?,
      )
        @missing_null_counts_as_zero = true
      end

      # Creates a converter for `column_name` (upstream `try_new`).
      #
      # If the column is not present in the parquet file, the returned arrays
      # will be null. This can happen if the column is in the arrow schema but
      # not in the parquet schema due to schema evolution.
      #
      # This constructor only supports top-level, non-nested columns. For nested
      # fields (e.g. fields within a struct), use `from_column_index`.
      #
      # Raises if the column is not found in the arrow schema.
      def self.try_new(
        column_name : String,
        arrow_schema : ::Arrow::Schema,
        parquet_schema : Schema::SchemaDescriptor,
      ) : StatisticsConverter
        found = arrow_schema.column_with_name(column_name)
        unless found
          raise arrow_err!("Column '#{column_name}' not found in schema for statistics conversion")
        end
        _, arrow_field = found

        parquet_index = nil
        if match = Parquet::Arrow.parquet_column(parquet_schema, arrow_schema, column_name)
          parquet_idx, matched_field = match
          # Sanity check that the matching field matches the arrow field.
          unless matched_field == arrow_field
            raise arrow_err!(
              "Matched column '#{matched_field.inspect}' does not match original matched column '#{arrow_field.inspect}'"
            )
          end
          parquet_index = parquet_idx
        end

        physical_type = parquet_index.try { |index| parquet_schema.column(index).physical_type }
        new(parquet_index, arrow_field, physical_type)
      end

      # Creates a converter from a parquet leaf column index directly
      # (upstream `from_column_index`).
      #
      # Unlike `try_new`, this constructor bypasses schema resolution, which is
      # useful for nested fields (e.g. struct fields) where the caller has
      # already resolved the mapping from the arrow field to the parquet leaf
      # column.
      #
      # Raises if `parquet_column_index` is out of bounds.
      def self.from_column_index(
        parquet_column_index : Int32,
        arrow_field : ::Arrow::Field,
        parquet_schema : Schema::SchemaDescriptor,
      ) : StatisticsConverter
        column_count = parquet_schema.columns.size
        if parquet_column_index >= column_count || parquet_column_index < 0
          raise arrow_err!(
            "Parquet column index #{parquet_column_index} out of bounds, column count #{column_count}"
          )
        end

        physical_type = parquet_schema.column(parquet_column_index).physical_type
        new(parquet_column_index, arrow_field, physical_type)
      end

      # Treats missing null counts as missing rather than as zero nulls.
      #
      # By default, the converter treats missing null counts as though the null
      # count is known to be `0`.
      def with_missing_null_counts_as_zero(missing_null_counts_as_zero : Bool) : self
        @missing_null_counts_as_zero = missing_null_counts_as_zero
        self
      end

      # Returns a `UInt64Array` with row counts for each row group.
      #
      # The returned array has no nulls, and has one value for each row group:
      # the number of rows in the row group. Returns nil when the column is not
      # present in the parquet file.
      def row_group_row_counts(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::PrimitiveArray(UInt64)?
        return nil unless @parquet_column_index

        counts = row_groups.map do |row_group|
          row_count = row_group.num_rows
          if row_count < 0
            raise arrow_err!("Parquet row count #{row_count} too large to convert to u64")
          end
          row_count.to_u64
        end
        ::Arrow::PrimitiveArray(UInt64).from_options(counts)
      end

      # Extracts the minimum values from row-group statistics.
      #
      # The returned array contains one value for each row group, in the same
      # order as `row_groups`. Each value is either the minimum value for the
      # column or a null value when the statistics cannot be extracted.
      def row_group_mins(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::ArrayBase
        row_group_stats(row_groups) { |statistics| StatisticsExtractors.min_statistics(@arrow_field.data_type, statistics, @physical_type) }
      end

      # Extracts the maximum values from row-group statistics. See
      # `row_group_mins` for details.
      def row_group_maxes(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::ArrayBase
        row_group_stats(row_groups) { |statistics| StatisticsExtractors.max_statistics(@arrow_field.data_type, statistics, @physical_type) }
      end

      # Extracts the `max_value_exact` flags from row-group statistics.
      def row_group_is_max_value_exact(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::BooleanArray
        row_group_flags(row_groups, &.max_is_exact)
      end

      # Extracts the `min_value_exact` flags from row-group statistics.
      def row_group_is_min_value_exact(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::BooleanArray
        row_group_flags(row_groups, &.min_is_exact)
      end

      # Extracts the null counts from row-group statistics.
      def row_group_null_counts(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::PrimitiveArray(UInt64)
        row_group_counts(row_groups) do |statistics|
          if missing_null_counts_as_zero?
            statistics.null_count_opt || 0_u64
          else
            statistics.null_count_opt
          end
        end
      end

      # Extracts the NaN counts from row-group statistics.
      def row_group_nan_counts(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::PrimitiveArray(UInt64)
        row_group_counts(row_groups, &.nan_count_opt)
      end

      # Extracts the distinct counts from row-group statistics.
      def row_group_distinct_counts(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
      ) : ::Arrow::PrimitiveArray(UInt64)
        row_group_counts(row_groups, &.distinct_count_opt)
      end

      # Extracts the minimum values from data-page statistics
      # (upstream `data_page_mins`).
      #
      # `page_index` is the parquet page index (read from `ParquetMetaData`) and
      # `row_group_indices` are the row groups whose column page index is used.
      # The returned array has one value per page index entry, in row-group
      # order.
      def data_page_mins(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        row_group_indices : Enumerable(Int32),
      ) : ::Arrow::ArrayBase
        data_page_stats(page_index, row_group_indices) do |chunks|
          StatisticsExtractors.min_page_statistics(@arrow_field.data_type, chunks, @physical_type)
        end
      end

      # Extracts the maximum values from data-page statistics. See
      # `data_page_mins` for details.
      def data_page_maxes(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        row_group_indices : Enumerable(Int32),
      ) : ::Arrow::ArrayBase
        data_page_stats(page_index, row_group_indices) do |chunks|
          StatisticsExtractors.max_page_statistics(@arrow_field.data_type, chunks, @physical_type)
        end
      end

      # Returns a `UInt64Array` with null counts for each data page.
      def data_page_null_counts(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        row_group_indices : Enumerable(Int32),
      ) : ::Arrow::PrimitiveArray(UInt64)
        data_page_counts(page_index, row_group_indices) do |chunks|
          StatisticsExtractors.null_counts_page_statistics(chunks)
        end
      end

      # Returns a `UInt64Array` with NaN counts for each data page.
      def data_page_nan_counts(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        row_group_indices : Enumerable(Int32),
      ) : ::Arrow::PrimitiveArray(UInt64)
        data_page_counts(page_index, row_group_indices) do |chunks|
          StatisticsExtractors.nan_counts_page_statistics(chunks)
        end
      end

      # Returns a `UInt64Array` with row counts for each data page, or nil when
      # the column is not present in the parquet file (there is then no page
      # location to derive per-page row counts from).
      def data_page_row_counts(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        row_group_metadatas : Array(::Parquet::File::Metadata::RowGroupMetaData),
        row_group_indices : Enumerable(Int32),
      ) : ::Arrow::PrimitiveArray(UInt64)?
        parquet_index = @parquet_column_index
        return nil unless parquet_index

        values = [] of UInt64?
        row_group_indices.each do |row_group_index|
          offset_index = page_index.offset_index(row_group_index, parquet_index)
          next unless offset_index

          locations = offset_index.page_locations
          next if locations.empty?

          (0...(locations.size - 1)).each do |index|
            values << (locations[index + 1].first_row_index - locations[index].first_row_index).to_u64
          end
          # The last page runs to the end of the row group.
          last = locations.last
          values << (row_group_metadatas[row_group_index].num_rows - last.first_row_index).to_u64
        end
        ::Arrow::PrimitiveArray(UInt64).from_options(values)
      end

      private def row_group_stats(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
        & : Array(::Parquet::Statistics?) -> ::Arrow::ArrayBase
      ) : ::Arrow::ArrayBase
        groups = row_groups.to_a
        parquet_index = @parquet_column_index
        unless parquet_index
          return ::Arrow.new_null_array(@arrow_field.data_type, groups.size)
        end

        statistics = groups.map(&.column(parquet_index).statistics)
        yield statistics
      end

      private def row_group_flags(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
        & : ::Parquet::Statistics -> Bool
      ) : ::Arrow::BooleanArray
        groups = row_groups.to_a
        parquet_index = @parquet_column_index
        unless parquet_index
          return ::Arrow::BooleanArray.from_options(Array(Bool?).new(groups.size, nil))
        end

        ::Arrow::BooleanArray.from_options(groups.map do |row_group|
          statistics = row_group.column(parquet_index).statistics
          statistics.try { |value| yield value }
        end)
      end

      private def row_group_counts(
        row_groups : Enumerable(::Parquet::File::Metadata::RowGroupMetaData),
        & : ::Parquet::Statistics -> UInt64?
      ) : ::Arrow::PrimitiveArray(UInt64)
        groups = row_groups.to_a
        parquet_index = @parquet_column_index
        unless parquet_index
          return ::Arrow::PrimitiveArray(UInt64).from_options(Array(UInt64?).new(groups.size, nil))
        end

        ::Arrow::PrimitiveArray(UInt64).from_options(groups.map do |row_group|
          statistics = row_group.column(parquet_index).statistics
          statistics.try { |value| yield value }
        end)
      end

      private def data_page_chunks(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        parquet_index : Int32,
        row_group_indices : Enumerable(Int32),
      ) : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?))
        row_group_indices.map do |row_group_index|
          num_data_pages = page_index.num_data_pages(row_group_index, parquet_index) || 0
          {num_data_pages, page_index.column_index(row_group_index, parquet_index)}
        end
      end

      private def data_page_stats(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        row_group_indices : Enumerable(Int32),
        & : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)) -> ::Arrow::ArrayBase
      ) : ::Arrow::ArrayBase
        parquet_index = @parquet_column_index
        unless parquet_index
          return ::Arrow.new_null_array(@arrow_field.data_type, row_group_indices.size)
        end

        yield data_page_chunks(page_index, parquet_index, row_group_indices)
      end

      private def data_page_counts(
        page_index : ::Parquet::File::Metadata::PageIndexProvider,
        row_group_indices : Enumerable(Int32),
        & : Array(Tuple(Int32, ::Parquet::File::PageIndex::ColumnIndexMetaData?)) -> ::Arrow::PrimitiveArray(UInt64)
      ) : ::Arrow::PrimitiveArray(UInt64)
        parquet_index = @parquet_column_index
        unless parquet_index
          return ::Arrow::PrimitiveArray(UInt64).from_options(Array(UInt64?).new(row_group_indices.size, nil))
        end

        yield data_page_chunks(page_index, parquet_index, row_group_indices)
      end
    end
  end
end
