require "../column/reader"
require "./api"

# Ported from `parquet/src/record/triplet.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `TripletIter` and `TypedTripletIter<T>`. Divergence: Rust's
# payload-carrying `TripletIter` enum is an abstract class with the single
# generic `TypedTripletIter(T)` subclass; `usize` is `Int32`.
module Parquet
  module Record
    # High level wrapper on a column reader, providing per-element access.
    abstract class TripletIter
      # Creates a triplet iterator for `reader` on column `descr`.
      def self.new(
        descr : Schema::ColumnDescriptor,
        reader : Column::ColumnReader,
        batch_size : Int32,
      ) : TripletIter
        case descr.physical_type
        when Parquet::Type::BOOLEAN              then TypedTripletIter(Bool).new(descr, batch_size, reader)
        when Parquet::Type::INT32                then TypedTripletIter(Int32).new(descr, batch_size, reader)
        when Parquet::Type::INT64                then TypedTripletIter(Int64).new(descr, batch_size, reader)
        when Parquet::Type::INT96                then TypedTripletIter(Int96).new(descr, batch_size, reader)
        when Parquet::Type::FLOAT                then TypedTripletIter(Float32).new(descr, batch_size, reader)
        when Parquet::Type::DOUBLE               then TypedTripletIter(Float64).new(descr, batch_size, reader)
        when Parquet::Type::BYTE_ARRAY           then TypedTripletIter(ByteArray).new(descr, batch_size, reader)
        when Parquet::Type::FIXED_LEN_BYTE_ARRAY then TypedTripletIter(FixedLenByteArray).new(descr, batch_size, reader)
        else
          raise general_err!("Cannot create triplet iterator for physical type #{descr.physical_type}")
        end
      end

      # Advances to the next triplet, returning `false` when exhausted.
      abstract def read_next : Bool
      # Whether more values/levels exist.
      abstract def has_next? : Bool
      # Returns the current definition level.
      abstract def current_def_level : Int16
      # Returns the maximum definition level.
      abstract def max_def_level : Int16
      # Returns the current repetition level.
      abstract def current_rep_level : Int16
      # Returns the maximum repetition level.
      abstract def max_rep_level : Int16
      # Returns the column descriptor.
      abstract def column_descr : Schema::ColumnDescriptor
      # Builds the current value as a `Field`.
      abstract def current_field : Field

      # Returns `true` if the current value is null.
      def null? : Bool
        current_def_level < max_def_level
      end

      # Returns the current value as a `Field` (`Field::Null` when null).
      def current_value : Field
        return Field::Null.new if null?

        current_field
      end
    end

    # Typed triplet iterator wrapping a `ColumnReaderImpl`.
    class TypedTripletIter(T) < TripletIter
      getter column_descr : Schema::ColumnDescriptor
      @reader : Column::ColumnReaderImpl(Array(T), Column::Reader::ColumnValueDecoderImpl(T))
      @batch_size : Int32
      @max_def_level : Int16
      @max_rep_level : Int16
      @values : Array(T)
      @def_levels : Array(Int16)?
      @rep_levels : Array(Int16)?
      @curr_triplet_index : Int32
      @triplets_left : Int32
      @has_next : Bool

      def initialize(
        descr : Schema::ColumnDescriptor,
        batch_size : Int32,
        column_reader : Column::ColumnReader,
      )
        raise ArgumentError.new("Expected positive batch size, found: #{batch_size}") unless batch_size > 0

        @column_descr = descr
        @reader = Column.get_typed_column_reader(T, column_reader)
        @batch_size = batch_size
        @max_def_level = descr.max_def_level
        @max_rep_level = descr.max_rep_level
        @def_levels = @max_def_level == 0 ? nil : Array(Int16).new(batch_size, 0_i16)
        @rep_levels = @max_rep_level == 0 ? nil : Array(Int16).new(batch_size, 0_i16)
        @values = [] of T
        @curr_triplet_index = 0
        @triplets_left = 0
        @has_next = false
      end

      def max_def_level : Int16
        @max_def_level
      end

      def max_rep_level : Int16
        @max_rep_level
      end

      # Returns the current raw value (raises if the value is null).
      #
      # Divergence: renamed from upstream `current_value` to avoid clashing with
      # the base `TripletIter#current_value` (which returns a `Field`).
      def typed_value : T
        unless current_def_level == @max_def_level
          raise general_err!(
            "Cannot extract value, max definition level: #{@max_def_level}, current level: #{current_def_level}"
          )
        end
        @values[@curr_triplet_index]
      end

      def current_def_level : Int16
        return 0_i16 unless @has_next

        if levels = @def_levels
          levels[@curr_triplet_index]
        else
          @max_def_level
        end
      end

      def current_rep_level : Int16
        return 0_i16 unless @has_next

        if levels = @rep_levels
          levels[@curr_triplet_index]
        else
          @max_rep_level
        end
      end

      def has_next? : Bool
        @has_next
      end

      def read_next : Bool
        @curr_triplet_index += 1

        while @curr_triplet_index >= @triplets_left
          @values.clear
          @def_levels.try(&.clear)
          @rep_levels.try(&.clear)

          records_read, values_read, levels_read =
            @reader.read_records(@batch_size, @def_levels, @rep_levels, @values)

          if records_read == 0 && values_read == 0 && levels_read == 0
            @curr_triplet_index = 0
            @has_next = false
            return false
          end

          if levels_read == 0 || values_read == levels_read
            @curr_triplet_index = 0
            @triplets_left = values_read
          elsif values_read < levels_read
            idx = values_read
            def_levels = @def_levels || raise general_err!("expected definition levels")
            while @values.size < levels_read
              @values << Column::Reader.default_value(T)
            end
            (0...levels_read).each do |i|
              if def_levels[levels_read - i - 1] == @max_def_level
                idx -= 1
                @values.swap(levels_read - i - 1, idx)
              end
            end
            @curr_triplet_index = 0
            @triplets_left = levels_read
          else
            raise general_err!(
              "Spacing of values/levels is wrong, values_read: #{values_read}, levels_read: #{levels_read}"
            )
          end
        end

        @has_next = true
        true
      end

      def current_field : Field
        {% if T == Bool %}
          Field.convert_bool(@column_descr, typed_value)
        {% elsif T == Int32 %}
          Field.convert_int32(@column_descr, typed_value)
        {% elsif T == Int64 %}
          Field.convert_int64(@column_descr, typed_value)
        {% elsif T == Int96 %}
          Field.convert_int96(@column_descr, typed_value)
        {% elsif T == Float32 %}
          Field.convert_float(@column_descr, typed_value)
        {% elsif T == Float64 %}
          Field.convert_double(@column_descr, typed_value)
        {% elsif T == ByteArray %}
          Field.convert_byte_array(@column_descr, typed_value)
        {% elsif T == FixedLenByteArray %}
          Field.convert_byte_array(@column_descr, typed_value.value)
        {% else %}
          {% raise "current_field: unsupported type #{T}" %}
        {% end %}
      end
    end
  end
end
