require "arrow-cr"
require "../basic"
require "../errors"
require "../schema/types"
require "../column/reader"
require "./record_reader/buffer"
require "./record_reader/definition_levels"

# Ported from `parquet/src/arrow/record_reader/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `GenericRecordReader<V, CV>` (upstream's `RecordReader<T>` is the
# `V = Vec<T::T>`, `CV = ColumnValueDecoderImpl<T>` instantiation) and
# `packed_null_mask`. The upstream `read_records`/`skip_records` helpers live in
# `array_reader/primitive_array.cr` (`read_records_from_pages`/
# `skip_records_from_pages`).
#
# Divergences: the arrow values buffers are the `ValuesBuffer` subclasses
# (`ArrayValuesBuffer(T)`, `Buffer::OffsetBuffer(I)`,
# `Buffer::DictionaryBuffer(K, V)`) rather than upstream's `Vec` impls, and the
# value decoder is a type *parameter* (`CV`) rather than the associated
# `ColumnValueDecoder::Buffer`, so a record reader's `(V, CV)` pair is chosen at
# construction; `V::with_capacity(0)` becomes a `->V` factory because Crystal
# has no `Default` bound; `usize` counts are `Int32`; `consume_bitmap`/
# `consume_compact_bitmap` return `Bytes?` (upstream `Option<Buffer>`); the
# upstream `debug_assert!`s (reservation accounting and compact-bitmap length)
# are dropped.
module Parquet
  module Arrow
    module RecordReader
      # A stateful column reader that delimits semantic records.
      class GenericRecordReader(V, CV)
        getter column_desc : Schema::ColumnDescriptor

        @values : V?
        @def_levels : DefinitionLevelBuffer?
        @rep_levels : Array(Int16)?
        @column_reader : Column::ColumnReaderImpl(V, CV)?
        # Number of buffered levels / null-padded values.
        @num_values : Int32
        # Number of buffered records.
        @num_records : Int32
        # Capacity hint for pre-allocating buffers based on batch size.
        @capacity_hint : Int32
        # Number of values in the values buffer (differs from `num_values` under
        # selective padding, where parent-level padding is excluded).
        @values_written : Int32
        # Definition-level threshold for selective null padding.
        @padding_threshold : Int16?
        # Compact bitmap accumulated during selective padding.
        @compact_bitmap : ::Arrow::BooleanBufferBuilder?

        # Creates a record reader (upstream `GenericRecordReader::new`).
        #
        # `new_values` lazily creates the values buffer (upstream
        # `V::with_capacity(0)`).
        def initialize(
          @column_desc : Schema::ColumnDescriptor,
          @capacity_hint : Int32,
          @new_values : -> V,
        )
          desc = @column_desc
          @values = nil
          @def_levels = desc.max_def_level > 0 ? DefinitionLevelBuffer.new(desc, packed_null_mask(desc)) : nil
          @rep_levels = desc.max_rep_level > 0 ? [] of Int16 : nil
          @column_reader = nil
          @num_values = 0
          @num_records = 0
          @values_written = 0
          @padding_threshold = nil
          @compact_bitmap = nil
        end

        # Sets the current page reader.
        # ameba:disable Naming/AccessorMethodName
        def set_page_reader(page_reader : Column::PageReader) : Nil
          desc = @column_desc
          def_level_decoder =
            if desc.max_def_level != 0
              DefinitionLevelBufferDecoder.new(desc.max_def_level, packed_null_mask(desc))
            end

          @column_reader = Column::ColumnReaderImpl(V, CV).new_with_decoders(
            desc, page_reader, CV.new(desc), @def_levels, def_level_decoder
          )
          nil
        end

        # Tries to read `num_records` of column data into the internal buffer,
        # returning the number of records actually read.
        def read_records(num_records : Int32) : Int32
          reader = @column_reader
          return 0 unless reader

          records_read = 0
          loop do
            records_to_read = num_records - records_read
            records_read += read_one_batch(records_to_read)
            break if records_read == num_records || !reader.has_next
          end
          records_read
        end

        # Tries to skip the next `num_records` rows, returning the number skipped.
        def skip_records(num_records : Int32) : Int32
          reader = @column_reader
          return 0 unless reader
          reader.skip_records(num_records)
        end

        # The number of records stored in the buffer.
        def num_records : Int32
          @num_records
        end

        # The number of values stored in the buffer.
        def num_values : Int32
          @num_values
        end

        # Returns the definition level data (upstream `consume_def_levels`).
        def consume_def_levels : Array(Int16)?
          @def_levels.try(&.consume_levels)
        end

        # Returns the repetition level data (upstream `consume_rep_levels`).
        def consume_rep_levels : Array(Int16)?
          levels = @rep_levels
          @rep_levels = [] of Int16 if levels
          levels
        end

        # Returns the currently stored values buffer (upstream
        # `consume_record_data`).
        #
        # Takes the buffer, leaving `nil`; the next read lazily allocates a new
        # one via the constructor's factory.
        def consume_record_data : V
          values = @values
          @values = nil
          values || @new_values.call
        end

        # Resets the buffer state (call after consuming data).
        def reset : Nil
          @num_values = 0
          @num_records = 0
          @values_written = 0
          @compact_bitmap = nil
          nil
        end

        # The maximum definition level of the column being read.
        def max_def_level : Int16
          @column_desc.max_def_level
        end

        # Enables selective null padding: only entries with `def >= threshold`
        # (item-level nulls inside non-null lists) are padded, while
        # list-level padding entries are skipped.
        # ameba:disable Naming/AccessorMethodName
        def set_padding_threshold(threshold : Int16) : Nil
          @padding_threshold = threshold
          nil
        end

        # The number of values in the values buffer.
        def values_written : Int32
          @padding_threshold ? @values_written : @num_values
        end

        # Consumes the compact null bitmap built during selective padding.
        def consume_compact_bitmap : Bytes?
          unless @padding_threshold
            return consume_bitmap
          end

          @def_levels.try(&.consume_bitmask)
          bitmap = @compact_bitmap
          @compact_bitmap = nil
          bitmap.try(&.finish.values)
        end

        # Returns the null bitmap data for nullable columns.
        def consume_bitmap : Bytes?
          mask = @def_levels.try(&.consume_bitmask)

          # Only return the bitmap for nullable columns: marking nulls on a
          # required array can fail validation even when masked off at a higher
          # level.
          @column_desc.self_type.optional? ? mask : nil
        end

        # ameba:disable Metrics/CyclomaticComplexity
        private def read_one_batch(batch_size : Int32) : Int32
          return 0 if batch_size == 0

          @capacity_hint = batch_size if batch_size > @capacity_hint

          padding_threshold = @padding_threshold
          max_def = @column_desc.max_def_level
          physical_values_to_read = 0
          output_slots_to_read = 0

          values = @values || (@values = @new_values.call)
          reader = @column_reader || raise general_err!("column reader is not initialized")

          if padding_threshold.nil?
            values.reserve_exact(Math.max(@capacity_hint - @num_values, 0))
          end

          records_read, values_read, levels_read = reader.read_records_with_reservation(
            batch_size, nil, @rep_levels, values
          ) do |buffer, values_to_read, levels_to_read, def_levels|
            if threshold = padding_threshold
              def_buffer = def_levels || raise general_err!(
                "Definition levels should exist when data is less than levels!"
              )
              all_levels = def_buffer.levels_array
              batch_levels = all_levels[all_levels.size - levels_to_read, levels_to_read]
              bitmap = @compact_bitmap || (@compact_bitmap = ::Arrow::BooleanBufferBuilder.new(0))

              output_slots = RecordReader.build_filtered_validity_bitmap(
                batch_levels, nil, threshold, max_def, bitmap
              )
            else
              output_slots = levels_to_read
            end

            buffer.reserve_exact(output_slots_to_read + output_slots - physical_values_to_read)

            output_slots_to_read += output_slots
            physical_values_to_read += values_to_read
          end

          if padding_threshold
            item_count = output_slots_to_read
            bitmap = @compact_bitmap || (@compact_bitmap = ::Arrow::BooleanBufferBuilder.new(0))

            # Pad values to item positions (only when there are gaps).
            if values_read < item_count
              values.reserve_exact(item_count - values_read)
              values.pad_nulls(@values_written, values_read, item_count, bitmap.as_slice)
            end

            @values_written += item_count
          elsif values_read < levels_read
            def_buffer = @def_levels || raise general_err!(
              "Definition levels should exist when data is less than levels!"
            )
            values.reserve_exact(levels_read - values_read)
            values.pad_nulls(@num_values, values_read, levels_read, def_buffer.nulls.as_slice)
          end

          @num_records += records_read
          @num_values += levels_read
          records_read
        end

        # Whether the null bitmap can be decoded straight into a bitmask: only
        # possible when nulls are at the leaf level (max def level 1) of an
        # optional scalar column.
        private def packed_null_mask(descr : Schema::ColumnDescriptor) : Bool
          descr.max_def_level == 1 && descr.max_rep_level == 0 && descr.self_type.optional?
        end
      end

      # Creates a record reader over `ArrayValuesBuffer(T)` and the classic
      # value decoder (upstream `RecordReader<T>`).
      #
      # Crystal cannot infer a method type parameter that only appears in the
      # return type, so the classic path is driven by an explicit value type.
      def self.typed_record_reader(
        column_desc : Schema::ColumnDescriptor,
        capacity_hint : Int32,
        pad_default : T,
      ) : GenericRecordReader(ArrayValuesBuffer(T), Column::Reader::ColumnValueDecoderImpl(T)) forall T
        GenericRecordReader(ArrayValuesBuffer(T), Column::Reader::ColumnValueDecoderImpl(T)).new(
          column_desc, capacity_hint, -> { ArrayValuesBuffer(T).new(pad_default) }
        )
      end
    end
  end
end
