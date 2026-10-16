require "../writer"

# Ported from `parquet/src/column/writer/byte_budget_chunker.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Picks byte-budget-aware mini-batch sizes for one column.
# Divergence: `SubBatchStrategy` (whose payloads are `usize`) is a struct
# carrying a `Kind` + `Int32` size; `usize` fields are `Int32`; `pick_sub_batch`
# takes the value type `T` explicitly (Crystal has no associated `Values` type
# on the encoder here).
module Parquet
  module Column
    module Writer
      # How to cut mini-batch windows in one chunk.
      struct SubBatchStrategy
        enum Kind
          Values
          Levels
        end

        getter kind : Kind
        getter size : Int32

        def initialize(@kind : Kind, @size : Int32)
        end

        def self.values(size : Int32) : SubBatchStrategy
          new(Kind::Values, size)
        end

        def self.levels(size : Int32) : SubBatchStrategy
          new(Kind::Levels, size)
        end
      end

      # Picks byte-budget-aware mini-batch sizes for one column.
      class ByteBudgetChunker
        @page_byte_limit : Int32
        @dict_page_byte_limit : Int32
        @max_def_level : Int16
        @static_always_fits : Bool
        @static_dict_always_fits : Bool

        def initialize(
          descr : Schema::ColumnDescriptor,
          column_props : File::Properties::ResolvedColumnProperties,
          base_batch_size : Int32,
        )
          @page_byte_limit = column_props.data_page_size_limit
          @dict_page_byte_limit = column_props.dictionary_page_size_limit
          @max_def_level = descr.max_def_level

          static_bytes_per_value = self.class.static_bytes_per_value(descr)
          @static_always_fits = self.class.static_always_fits(static_bytes_per_value, base_batch_size, @page_byte_limit)
          @static_dict_always_fits = self.class.static_always_fits(static_bytes_per_value, base_batch_size, @dict_page_byte_limit)
        end

        # Decides how many values at the start of a chunk belong in one
        # mini-batch; `nil` means the whole chunk fits in one mini-batch.
        def pick_sub_batch(
          type : T.class,
          encoder,
          values : Array(T),
          value_indices : Array(Int32)?,
          chunk_def : LevelDataRef,
          values_offset : Int32,
          chunk_size : Int32,
        ) : SubBatchStrategy? forall T
          return nil if chunk_size == 0

          budget, value_exact =
            if encoder.has_dictionary
              return nil if @static_dict_always_fits

              used = encoder.estimated_dict_page_size
              return nil if used.nil?

              {Math.max(@dict_page_byte_limit - used, 0), false}
            else
              return nil if @static_always_fits

              {@page_byte_limit, encoder.compresses_against_previous_value}
            end

          byte_budget_sub_batch(
            type, encoder, values, value_indices, chunk_def,
            values_offset, chunk_size, budget, value_exact,
          )
        end

        private def byte_budget_sub_batch(
          type : T.class,
          encoder,
          values : Array(T),
          value_indices : Array(Int32)?,
          chunk_def : LevelDataRef,
          values_offset : Int32,
          chunk_size : Int32,
          budget : Int32,
          value_exact : Bool,
        ) : SubBatchStrategy? forall T
          _ = encoder
          vals_in_chunk = chunk_def.value_count(chunk_size, @max_def_level)
          return nil if vals_in_chunk == 0

          fit =
            if indices = value_indices
              finish = Math.min(values_offset + vals_in_chunk, indices.size)
              start = Math.min(values_offset, finish)
              Writer.count_values_within_byte_budget_gather(T, values, indices[start, finish - start], budget)
            else
              Writer.count_values_within_byte_budget(T, values, values_offset, vals_in_chunk, budget)
            end

          return nil if fit.nil?

          values_fit = fit
          return nil if values_fit >= vals_in_chunk

          values_fit = Math.max(values_fit, 1)
          if value_exact
            SubBatchStrategy.values(values_fit)
          else
            levels_per_subbatch =
              if vals_in_chunk == chunk_size
                values_fit
              else
                Math.max((values_fit * chunk_size + vals_in_chunk - 1) // vals_in_chunk, 1)
              end
            SubBatchStrategy.levels(Math.min(chunk_size, levels_per_subbatch))
          end
        end

        def self.static_bytes_per_value(descr : Schema::ColumnDescriptor) : Int32?
          case descr.physical_type
          when Parquet::Type::BOOLEAN                      then 1
          when Parquet::Type::INT32, Parquet::Type::FLOAT  then 4
          when Parquet::Type::INT64, Parquet::Type::DOUBLE then 8
          when Parquet::Type::INT96                        then 12
          when Parquet::Type::FIXED_LEN_BYTE_ARRAY         then Math.max(descr.type_length, 0)
          else                                                  nil
          end
        end

        def self.static_always_fits(static_bytes_per_value : Int32?, base_batch_size : Int32, limit : Int32) : Bool
          if bytes = static_bytes_per_value
            bytes * base_batch_size <= limit
          else
            false
          end
        end
      end
    end
  end
end
