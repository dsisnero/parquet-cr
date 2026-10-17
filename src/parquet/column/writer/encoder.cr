require "../../basic"
require "../../errors"
require "../../encodings/registry"
require "../../encodings/dict"
require "../../file/properties"
require "../../bloom_filter"
require "../../schema/types"
require "../../geospatial"

# Ported from `parquet/src/column/writer/encoder.rs` and the statistics/encoding
# helper functions from `parquet/src/column/writer/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ColumnValueEncoder`/`ColumnValueEncoderImpl<T>`, `DictionaryPage`,
# `DataPageValues<T>`, and the min/max comparison + fallback-encoding helpers the
# encoder uses.
#
# Divergences: the `ColumnValueEncoder` trait (with associated `T`/`Values`
# types) is an abstract `ColumnValueEncoder(T)` class; `ColumnValues` is
# `Array(T)`; `usize` becomes `Int32`; `f16`/float `total_cmp` are hand-rolled
# IEEE total-order keys; `is_nan`/`compare_greater` dispatch on `T.class`;
# `update_geo_stats_accumulator` is a private generic helper over `Array(T)`
# using `AsBytes` (upstream iterates `&T::T`); Rust `panic!`/`assert!` become
# Crystal exceptions.
module Parquet
  module Column
    module Writer
      # The encoded data for a dictionary page.
      class DictionaryPage
        getter buf : Bytes
        getter num_values : Int32
        # ameba:disable Naming/QueryBoolMethods
        getter is_sorted : Bool

        def initialize(@buf : Bytes, @num_values : Int32, @is_sorted : Bool)
        end
      end

      # The encoded values for a data page, with optional statistics.
      class DataPageValues(T)
        getter buf : Bytes
        getter num_values : Int32
        getter encoding : Encoding
        getter min_value : T?
        getter max_value : T?
        getter nan_count : UInt64?
        getter variable_length_bytes : Int64?

        def initialize(
          @buf : Bytes,
          @num_values : Int32,
          @encoding : Encoding,
          @min_value : T?,
          @max_value : T?,
          @nan_count : UInt64?,
          @variable_length_bytes : Int64?,
        )
        end
      end

      # Returns `true` if `a > b` for big-endian signed decimal byte arrays.
      def self.compare_greater_byte_array_decimals(a : Bytes, b : Bytes) : Bool
        a_length = a.size
        b_length = b.size

        return a_length > 0 if a_length == 0 || b_length == 0

        first_a = a[0]
        first_b = b[0]

        if (0x80 & first_a) != (0x80 & first_b) || (a_length == b_length && first_a != first_b)
          return first_a.to_i8! > first_b.to_i8!
        end

        extension = (first_a.to_i8! < 0) ? 0xFF_u8 : 0x00_u8

        if a_length != b_length
          not_equal =
            if a_length > b_length
              lead_length = a_length - b_length
              (0...lead_length).any? { |i| a[i] != extension }
            else
              lead_length = b_length - a_length
              (0...lead_length).any? { |i| b[i] != extension }
            end

          if not_equal
            negative_values = first_a.to_i8! < 0
            a_longer = a_length > b_length
            return negative_values ? !a_longer : a_longer
          end
        end

        tail_length = Math.min(a_length, b_length)
        (a[a_length - tail_length, tail_length]) > (b[b_length - tail_length, tail_length])
      end

      # IEEE-754 total-order key for an `f32`.
      def self.f32_total_key(value : Float32) : UInt32
        bits = value.unsafe_as(UInt32)
        (bits & 0x8000_0000_u32) != 0 ? ~bits : (bits | 0x8000_0000_u32)
      end

      # IEEE-754 total-order key for an `f64`.
      def self.f64_total_key(value : Float64) : UInt64
        bits = value.unsafe_as(UInt64)
        (bits & 0x8000_0000_0000_0000_u64) != 0 ? ~bits : (bits | 0x8000_0000_0000_0000_u64)
      end

      # IEEE-754 total-order key for a little-endian half float (`f16`).
      def self.f16_total_key(bytes : Bytes) : UInt16
        bits = bytes[0].to_u16 | (bytes[1].to_u16 << 8)
        (bits & 0x8000_u16) != 0 ? ~bits : (bits | 0x8000_u16)
      end

      # `true` if `a > b` for half-float byte arrays (IEEE total order).
      def self.compare_greater_f16(a : Bytes, b : Bytes) : Bool
        f16_total_key(a) > f16_total_key(b)
      end

      # `true` if `a > b` as unsigned integers.
      def self.compare_greater_unsigned_int(a : Int32, b : Int32) : Bool
        a.unsafe_as(UInt32) > b.unsafe_as(UInt32)
      end

      def self.compare_greater_unsigned_int(a : Int64, b : Int64) : Bool
        a.unsafe_as(UInt64) > b.unsafe_as(UInt64)
      end

      # `true` if `val` is a NaN (float, or half-float stored as FLBA in total order).
      # ameba:disable Naming/PredicateName
      def self.is_nan(basic_type_info : Schema::BasicTypeInfo, val : T) : Bool forall T
        {% if T == Float32 || T == Float64 %}
          val.nan?
        {% elsif T == FixedLenByteArray %}
          if basic_type_info.sort_order == SortOrder::TOTAL_ORDER
            bytes = val.as_bytes
            uval = bytes[0].to_u16 | (bytes[1].to_u16 << 8)
            (uval & 0x7FFF_u16) > 0x7C00_u16
          else
            false
          end
        {% else %}
          false
        {% end %}
      end

      # Evaluate `a > b` according to the underlying logical type.
      def self.compare_greater(basic_type_info : Schema::BasicTypeInfo, a : T, b : T) : Bool forall T
        {% if T == Float32 %}
          f32_total_key(a) > f32_total_key(b)
        {% elsif T == Float64 %}
          f64_total_key(a) > f64_total_key(b)
        {% elsif T == Int32 || T == Int64 %}
          if basic_type_info.sort_order == SortOrder::UNSIGNED
            compare_greater_unsigned_int(a, b)
          else
            a > b
          end
        {% elsif T == FixedLenByteArray %}
          if basic_type_info.sort_order == SortOrder::TOTAL_ORDER
            compare_greater_f16(a.as_bytes, b.as_bytes)
          elsif decimal?(basic_type_info)
            compare_greater_byte_array_decimals(a.as_bytes, b.as_bytes)
          else
            a > b
          end
        {% elsif T == ByteArray %}
          if decimal?(basic_type_info)
            compare_greater_byte_array_decimals(a.as_bytes, b.as_bytes)
          else
            a > b
          end
        {% elsif T == Bool %}
          # Rust `bool: Ord` (`false < true`).
          a && !b
        {% else %}
          a > b
        {% end %}
      end

      private def self.decimal?(basic_type_info : Schema::BasicTypeInfo) : Bool
        basic_type_info.converted_type == ConvertedType::DECIMAL ||
          basic_type_info.logical_type_ref.is_a?(LogicalType::Decimal)
      end

      # Get min and max values for `values`, plus the NaN count.
      def self.get_min_max(basic_type_info : Schema::BasicTypeInfo, values : Array(T)) : Tuple(T, T, UInt64)? forall T
        return nil if values.empty?

        first = values[0]
        min_max_nan = is_nan(basic_type_info, first)
        nan_count = min_max_nan ? 1_u64 : 0_u64

        min = first
        max = first

        values[1..].each do |val|
          val_is_nan = is_nan(basic_type_info, val)
          if !min_max_nan && val_is_nan
            nan_count += 1
          elsif min_max_nan && !val_is_nan
            min = val
            max = val
            min_max_nan = false
          else
            nan_count += 1 if val_is_nan
            if compare_greater(basic_type_info, min, val)
              min = val
            elsif compare_greater(basic_type_info, val, max)
              max = val
            end
          end
        end

        {min, max, nan_count}
      end

      # Conditionally lower `min` to `val`.
      def self.update_min(descr : Schema::ColumnDescriptor, val : T, min : T?) : T? forall T
        current = min
        return val if current.nil?

        info = descr.get_basic_info
        min_nan = is_nan(info, current)
        val_nan = is_nan(info, val)
        if !min_nan && val_nan
          current
        elsif min_nan && !val_nan
          val
        else
          compare_greater(info, current, val) ? val : current
        end
      end

      # Conditionally raise `max` to `val`.
      def self.update_max(descr : Schema::ColumnDescriptor, val : T, max : T?) : T? forall T
        current = max
        return val if current.nil?

        info = descr.get_basic_info
        max_nan = is_nan(info, current)
        val_nan = is_nan(info, val)
        if !max_nan && val_nan
          current
        elsif max_nan && !val_nan
          val
        else
          compare_greater(info, val, current) ? val : current
        end
      end

      # Variable-length bytes for a slice (`ByteArray` only, `nil` otherwise).
      def self.variable_length_bytes(type : T.class, values : Array(T)) : Int64? forall T
        {% if T == ByteArray %}
          total = 0_i64
          values.each { |value| total += value.len }
          total
        {% else %}
          nil
        {% end %}
      end

      # Returns the encoding for a column when none is provided in properties.
      def self.fallback_encoding(physical : Parquet::Type, props : File::Properties::WriterProperties) : Encoding
        version = props.writer_version
        case {physical, version}
        when {Parquet::Type::BOOLEAN, File::Properties::WriterVersion::PARQUET_2_0}
          Encoding::RLE
        when {Parquet::Type::INT32, File::Properties::WriterVersion::PARQUET_2_0}
          Encoding::DELTA_BINARY_PACKED
        when {Parquet::Type::INT64, File::Properties::WriterVersion::PARQUET_2_0}
          Encoding::DELTA_BINARY_PACKED
        when {Parquet::Type::BYTE_ARRAY, File::Properties::WriterVersion::PARQUET_2_0}
          Encoding::DELTA_BYTE_ARRAY
        when {Parquet::Type::FIXED_LEN_BYTE_ARRAY, File::Properties::WriterVersion::PARQUET_2_0}
          Encoding::DELTA_BYTE_ARRAY
        else
          Encoding::PLAIN
        end
      end

      # Returns `true` if dictionary encoding is supported for `physical`.
      def self.has_dictionary_support(physical : Parquet::Type, props : File::Properties::WriterProperties) : Bool
        case {physical, props.writer_version}
        when {Parquet::Type::BOOLEAN, File::Properties::WriterVersion::PARQUET_1_0},
             {Parquet::Type::BOOLEAN, File::Properties::WriterVersion::PARQUET_2_0}
          false
        when {Parquet::Type::FIXED_LEN_BYTE_ARRAY, File::Properties::WriterVersion::PARQUET_1_0}
          false
        else
          true
        end
      end

      # Plain-encoded byte cost of a single value.
      def self.plain_encoded_byte_size(type : T.class, value : T) : Int32 forall T
        {% if T == ByteArray %}
          4 + value.len
        {% elsif T == FixedLenByteArray %}
          value.len
        {% else %}
          sizeof(T)
        {% end %}
      end

      # How many values fit in `byte_budget` bytes (contiguous range).
      def self.count_values_within_byte_budget(type : T.class, values : Array(T), offset : Int32, len : Int32, byte_budget : Int32) : Int32? forall T
        finish = Math.min(offset + len, values.size)
        start = Math.min(offset, finish)
        count_within_budget(T, finish - start, byte_budget) { |i| values[start + i] }
      end

      # How many gathered values fit in `byte_budget` bytes.
      def self.count_values_within_byte_budget_gather(type : T.class, values : Array(T), indices : Array(Int32), byte_budget : Int32) : Int32? forall T
        count_within_budget(T, indices.size, byte_budget) { |i| values[indices[i]]? }
      end

      private def self.count_within_budget(type : T.class, n : Int32, byte_budget : Int32, & : Int32 -> T?) : Int32? forall T
        physical = Encodings.physical_type(T)
        if physical != Parquet::Type::BYTE_ARRAY && physical != Parquet::Type::FIXED_LEN_BYTE_ARRAY
          per = Math.max(sizeof(T), 1)
          return Math.min(Math.max(byte_budget // per, 1), n)
        end

        cum = 0
        n.times do |i|
          if value = yield(i)
            cum += plain_encoded_byte_size(T, value)
          end
          return i + 1 if cum > byte_budget
        end
        n
      end

      # Creates a bloom filter for the column, returning it and the fold FPP.
      def self.create_bloom_filter(column_props : File::Properties::ResolvedColumnProperties) : Tuple(BloomFilter::Sbbf?, Float64)
        if bf_props = column_props.bloom_filter_properties
          {BloomFilter::Sbbf.new_with_ndv_fpp(bf_props.ndv, bf_props.fpp), bf_props.fpp}
        else
          {nil, 0.0}
        end
      end

      # A generic encoder of values to data and dictionary pages.
      abstract class ColumnValueEncoder(T)
        abstract def write(values : Array(T), offset : Int32, len : Int32) : Nil
        abstract def write_gather(values : Array(T), indices : Array(Int32)) : Nil
        abstract def num_values : Int32
        abstract def has_dictionary : Bool
        abstract def estimated_memory_size : Int32
        abstract def estimated_dict_page_size : Int32?
        abstract def estimated_data_page_size : Int32
        abstract def flush_dict_page : DictionaryPage?
        abstract def flush_data_page : DataPageValues(T)
        abstract def flush_bloom_filter : BloomFilter::Sbbf?
        abstract def flush_geospatial_statistics : Geospatial::GeospatialStatistics?

        # Returns `true` if the encoder compresses each value against the
        # previous one within the page.
        def compresses_against_previous_value : Bool
          false
        end
      end

      # An implementation of `ColumnValueEncoder` for `T`.
      class ColumnValueEncoderImpl(T) < ColumnValueEncoder(T)
        def self.try_new(
          descr : Schema::ColumnDescriptor,
          props : File::Properties::WriterProperties,
          column_props : File::Properties::ResolvedColumnProperties,
        ) : ColumnValueEncoderImpl(T)
          physical = Encodings.physical_type(T)
          dict_supported = column_props.dictionary_enabled &&
                           Writer.has_dictionary_support(physical, props)
          dict_encoder = dict_supported ? Encodings::DictEncoder(T).new(descr) : nil
          encoding = column_props.encoding || Writer.fallback_encoding(physical, props)
          encoder = Encodings.get_encoder(T, encoding, descr)
          bloom_filter, bloom_filter_target_fpp = Writer.create_bloom_filter(column_props)
          geo_stats_accumulator = Geospatial.try_new_geo_stats_accumulator(descr)

          new(
            encoder,
            dict_encoder,
            descr,
            column_props.statistics_enabled,
            bloom_filter,
            bloom_filter_target_fpp,
            geo_stats_accumulator,
          )
        end

        private def initialize(
          @encoder : Encodings::Encoder(T),
          @dict_encoder : Encodings::DictEncoder(T)?,
          @descr : Schema::ColumnDescriptor,
          @statistics_enabled : File::Properties::EnabledStatistics,
          @bloom_filter : BloomFilter::Sbbf?,
          @bloom_filter_target_fpp : Float64,
          @geo_stats_accumulator : Geospatial::GeoStatsAccumulator?,
        )
          @num_values = 0
          @min_value = nil.as(T?)
          @max_value = nil.as(T?)
          @nan_count = nil.as(UInt64?)
          @variable_length_bytes = nil.as(Int64?)
        end

        def write(values : Array(T), offset : Int32, len : Int32) : Nil
          @num_values += len

          if offset + len > values.size
            raise general_err!("Expected to write #{len} values, but have only #{values.size - offset}")
          end

          write_slice(values[offset, len])
        end

        def write_gather(values : Array(T), indices : Array(Int32)) : Nil
          @num_values += indices.size
          slice = indices.map { |index| values[index] }
          write_slice(slice)
        end

        def num_values : Int32
          @num_values
        end

        def has_dictionary : Bool
          !@dict_encoder.nil?
        end

        def compresses_against_previous_value : Bool
          @dict_encoder.nil? && @encoder.encoding == Encoding::DELTA_BYTE_ARRAY
        end

        def estimated_memory_size : Int32
          encoder_size = @encoder.estimated_memory_size
          dict_encoder_size = @dict_encoder.try(&.estimated_memory_size) || 0
          bloom_filter_size = @bloom_filter.try(&.estimated_memory_size) || 0
          encoder_size + dict_encoder_size + bloom_filter_size
        end

        def estimated_dict_page_size : Int32?
          encoder = @dict_encoder
          encoder ? encoder.dict_encoded_size : nil
        end

        def estimated_data_page_size : Int32
          if encoder = @dict_encoder
            encoder.estimated_data_encoded_size
          else
            @encoder.estimated_data_encoded_size
          end
        end

        def flush_bloom_filter : BloomFilter::Sbbf?
          sbbf = @bloom_filter
          return nil if sbbf.nil?

          @bloom_filter = nil
          sbbf.fold_to_target_fpp(@bloom_filter_target_fpp)
          sbbf
        end

        def flush_dict_page : DictionaryPage?
          encoder = @dict_encoder
          return nil if encoder.nil?

          if @num_values != 0
            raise general_err!("Must flush data pages before flushing dictionary")
          end

          if bloom_filter = @bloom_filter
            encoder.uniques.each { |value| bloom_filter.insert(value) }
          end

          buf = encoder.write_dict
          num_values = encoder.num_entries
          is_sorted = encoder.sorted?
          @dict_encoder = nil
          DictionaryPage.new(buf, num_values, is_sorted)
        end

        def flush_data_page : DataPageValues(T)
          buf, encoding =
            if encoder = @dict_encoder
              {encoder.write_indices, Encoding::RLE_DICTIONARY}
            else
              {@encoder.flush_buffer, @encoder.encoding}
            end

          result = DataPageValues(T).new(
            buf,
            @num_values,
            encoding,
            @min_value,
            @max_value,
            @nan_count,
            @variable_length_bytes,
          )

          @num_values = 0
          @min_value = nil
          @max_value = nil
          @nan_count = nil
          @variable_length_bytes = nil
          result
        end

        def flush_geospatial_statistics : Geospatial::GeospatialStatistics?
          @geo_stats_accumulator.try(&.finish)
        end

        private def write_slice(slice : Array(T)) : Nil
          if @statistics_enabled != File::Properties::EnabledStatistics::None &&
             @descr.converted_type != ConvertedType::INTERVAL
            # INTERVAL, Geometry, and Geography have undefined sort order, so
            # don't write min/max statistics for them.
            if accumulator = @geo_stats_accumulator
              update_geo_stats_accumulator(accumulator, slice)
            elsif min_max = Writer.get_min_max(@descr.get_basic_info, slice)
              min, max, nan_count = min_max
              @min_value = Writer.update_min(@descr, min, @min_value)
              @max_value = Writer.update_max(@descr, max, @max_value)
              if floating_point_column?
                @nan_count = (@nan_count || 0_u64) + nan_count
              end
            end

            if var_bytes = Writer.variable_length_bytes(T, slice)
              @variable_length_bytes = (@variable_length_bytes || 0_i64) + var_bytes
            end
          end

          if encoder = @dict_encoder
            encoder.put(slice)
          else
            if bloom_filter = @bloom_filter
              slice.each { |value| bloom_filter.insert(value) }
            end
            @encoder.put(slice)
          end
        end

        # Feeds one slice of WKB-encoded values to the accumulator.
        private def update_geo_stats_accumulator(
          accumulator : Geospatial::GeoStatsAccumulator,
          slice : Array(T),
        ) : Nil
          return unless accumulator.is_valid

          slice.each { |value| accumulator.update_wkb(value.as_bytes) }
          nil
        end

        private def floating_point_column? : Bool
          physical = Encodings.physical_type(T)
          physical == Parquet::Type::FLOAT ||
            physical == Parquet::Type::DOUBLE ||
            @descr.logical_type_ref.is_a?(LogicalType::Float16)
        end
      end
    end
  end
end
