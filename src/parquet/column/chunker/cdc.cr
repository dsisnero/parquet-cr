require "../../errors"
require "../../file/properties"
require "../../schema/types"
require "../writer"
require "../chunker"
require "./cdc_generated"

# Ported from `parquet/src/column/chunker/cdc.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ContentDefinedChunker`, the FastCDC-inspired content-defined chunker
# that the parquet writer uses to place data-page boundaries (deduplication in
# content-addressable storage). See `Parquet::Column::Chunker::CdcChunk` for the
# produced chunk descriptors.
#
# Divergences: `new` raises `GeneralError` (upstream returns `Result<Self>`);
# `usize` becomes `Int32`; the compile-time-width `roll_fixed::<N>(&[u8; N])`
# becomes `roll_fixed(bytes : Bytes)` sharing `roll`'s byte loop (Crystal has no
# const-generic array width, and the behavior is identical); the `usize` closure
# parameter is an `Int32`; the `FnMut(&mut Self, usize)` callback becomes a block
# receiving the chunker and the value offset; `debug_assertions` maps to
# `flag?(:release)` (chunks are validated in non-release builds); Rust
# `assert!`/`expect` become `GeneralError`s. `roll`/`roll_fixed`/`roll_level`/
# `need_new_chunk` are public because the `calculate` callback drives them
# (upstream keeps them private but its child test module can still reach them).
# `get_arrow_chunks` is deferred with the arrow layer (it dispatches on
# `arrow_array::Array` data types).
module Parquet
  module Column
    module Chunker
      # CDC (Content-Defined Chunking) divides data into variable-sized chunks
      # based on content rather than fixed-size boundaries.
      #
      # For example, given this sequence of values in a column:
      #
      # ```text
      # File1:    [1,2,3,   4,5,6,   7,8,9]
      #            chunk1   chunk2   chunk3
      # ```
      #
      # If a value is inserted between 3 and 4:
      #
      # ```text
      # File2:    [1,2,3,0,   4,5,6,   7,8,9]
      #            new-chunk  chunk2  chunk3
      # ```
      #
      # The chunking process adjusts to maintain stable boundaries across data
      # modifications. Each chunk defines a new parquet data page.
      #
      # This implements a [FastCDC]-inspired algorithm using gear hashing: the
      # input is fed byte-by-byte into a rolling hash, and a boundary is committed
      # once the hash matches a mask 8 consecutive times (each against a different
      # pre-computed gear hash table), which normalizes chunk sizes around the
      # midpoint between `min_chunk_size` and `max_chunk_size`.
      #
      # For nested data (lists, maps, structs) chunk boundaries are restricted to
      # top-level record boundaries (`rep_level == 0`) so that a nested row is
      # never split across chunks. The rolling-hash state is maintained across the
      # entire column and is not reset between pages or row groups.
      #
      # Boundaries are deterministically calculated exclusively based on the data
      # itself, so the same data always produces the same chunks given the same
      # configuration.
      #
      # Ported from the C++ implementation in apache/arrow#45360
      # (`cpp/src/parquet/chunker_internal.cc`).
      #
      # [FastCDC]: https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia
      # [central-limit-theorem normalization]: https://www.cidrdb.org/cidr2023/papers/p43-low.pdf
      class ContentDefinedChunker
        # Maximum definition level for this column.
        getter max_def_level : Int16
        # Maximum repetition level for this column.
        getter max_rep_level : Int16
        # Definition level at the nearest REPEATED ancestor.
        getter repeated_ancestor_def_level : Int16

        # Minimum chunk size in bytes.
        #
        # The rolling hash will not be updated until this size is reached for each
        # chunk. All data sent through the hash function counts towards the chunk
        # size, including definition and repetition levels if present.
        getter min_chunk_size : Int64
        # Maximum chunk size in bytes.
        #
        # A new chunk is created whenever the chunk size exceeds this value. The
        # chunk size distribution approximates a normal distribution between
        # `min_chunk_size` and `max_chunk_size`.
        getter max_chunk_size : Int64
        # Mask for matching against the rolling hash.
        getter rolling_hash_mask : UInt64
        # Current chunk size in bytes.
        getter chunk_size : Int64

        # Creates a chunker for `descr`, raising `GeneralError` when `options`
        # describe an impossible chunk size distribution.
        def initialize(
          descr : ::Parquet::Schema::ColumnDescriptor,
          options : ::Parquet::File::Properties::CdcOptions,
        )
          @max_def_level = descr.max_def_level
          @max_rep_level = descr.max_rep_level
          @repeated_ancestor_def_level = descr.repeated_ancestor_def_level
          @min_chunk_size = options.min_chunk_size.to_i64
          @max_chunk_size = options.max_chunk_size.to_i64
          @rolling_hash_mask = ContentDefinedChunker.calculate_mask(
            @min_chunk_size, @max_chunk_size, options.norm_level
          )
          @rolling_hash = 0_u64
          @has_matched = false
          @nth_run = 0
          @chunk_size = 0_i64
        end

        # Calculate the mask used to determine chunk boundaries from the rolling
        # hash.
        #
        # The mask is calculated so that the expected chunk size distribution
        # approximates a normal distribution between min and max chunk sizes.
        def self.calculate_mask(min_chunk_size : Int64, max_chunk_size : Int64, norm_level : Int32) : UInt64
          if min_chunk_size < 0
            raise general_err!("min_chunk_size must be non-negative")
          end
          if max_chunk_size <= min_chunk_size
            raise general_err!("max_chunk_size must be greater than min_chunk_size")
          end

          # Rust `i64::midpoint` for non-negative bounds.
          avg_chunk_size = min_chunk_size + (max_chunk_size - min_chunk_size) // 2
          # Target size after subtracting the min-size skip window and dividing by
          # the number of hash tables (for central-limit-theorem normalization).
          target_size = (avg_chunk_size - min_chunk_size) // NUM_GEARHASH_TABLES

          # floor(log2(target_size)) — equivalent to C++ NumRequiredBits(target_size) - 1
          mask_bits = target_size > 0 ? target_size.to_u64.bit_length - 1 : 0

          effective_bits = mask_bits - norm_level

          unless 1 <= effective_bits <= 63
            raise general_err!(
              "The number of bits in the CDC mask must be between 1 and 63, got #{effective_bits}"
            )
          end

          # Create the mask by setting the top `effective_bits` bits.
          UInt64::MAX << (64 - effective_bits)
        end

        # Feed raw bytes into the rolling hash.
        #
        # The byte count always accumulates toward `chunk_size`, but the actual
        # hash update is skipped until `min_chunk_size` has been reached. This
        # "skip window" is the FastCDC optimization that prevents boundaries from
        # appearing too early in a chunk.
        def roll(bytes : Bytes) : Nil
          @chunk_size += bytes.size
          return if @chunk_size < @min_chunk_size

          bytes.each do |byte|
            @rolling_hash = (@rolling_hash << 1) &+ GEARHASH_TABLE[@nth_run][byte]
            @has_matched = true if (@rolling_hash & @rolling_hash_mask) == 0
          end
          nil
        end

        # Feed fixed-width bytes into the rolling hash.
        #
        # Upstream takes `&[u8; N]` (N known at compile time) so the inner loop can
        # be unrolled; Crystal has no const-generic array width, so this shares
        # `roll`'s byte loop.
        def roll_fixed(bytes : Bytes) : Nil
          roll(bytes)
        end

        # Feed a definition or repetition level (i16) into the rolling hash.
        def roll_level(level : Int16) : Nil
          bits = level.unsafe_as(UInt16)
          roll(StaticArray[bits.to_u8!, (bits >> 8).to_u8!].to_slice)
        end

        # Check whether a new chunk boundary should be created.
        #
        # A boundary is created when either of two conditions holds:
        #
        # 1. **CLT normalization**: the rolling hash has matched the mask
        #    (`has_matched`) and this is the 8th consecutive such match
        #    (`nth_run` reaches `NUM_GEARHASH_TABLES`). Each match advances to the
        #    next gear hash table, so 8 independent matches are required; a single
        #    table would yield exponentially distributed chunk sizes.
        #
        # 2. **Hard size limit**: `chunk_size` has reached `max_chunk_size`.
        #
        # Note: when `max_chunk_size` forces a boundary, `nth_run` is **not**
        # reset, so the CLT sequence continues from where it left off in the next
        # chunk. This matches the C++ behavior.
        def need_new_chunk : Bool
          if @has_matched
            @has_matched = false
            @nth_run += 1
            if @nth_run >= NUM_GEARHASH_TABLES
              @nth_run = 0
              @chunk_size = 0
              return true
            end
          end
          if @chunk_size >= @max_chunk_size
            @chunk_size = 0
            return true
          end
          false
        end

        # Compute chunk boundaries for the given column data.
        #
        # The chunking state is maintained across the entire column without being
        # reset between pages and row groups, so chunking can continue across
        # write calls.
        #
        # The block is called once per (def_level, rep_level, value) triplet with
        # this chunker and the value offset, and must feed the value's bytes into
        # the rolling hash (e.g. `c.roll_fixed(bytes)`). Whenever the rolling hash
        # matches the mask, a boundary becomes pending; `need_new_chunk` is
        # evaluated after each triplet.
        def calculate(
          def_levels : ::Parquet::Column::Writer::LevelDataRef,
          rep_levels : ::Parquet::Column::Writer::LevelDataRef,
          num_levels : Int32,
          &roll_value : ContentDefinedChunker, Int32 ->
        ) : Array(CdcChunk)
          has_def_levels = max_def_level > 0
          has_rep_levels = max_rep_level > 0

          chunks = [] of CdcChunk
          prev_offset = 0
          prev_value_offset = 0
          value_offset = 0

          if !has_rep_levels && !has_def_levels
            # Fastest path: non-nested, non-null data. Every level corresponds to
            # exactly one non-null value, so value_offset == level_offset and
            # num_values == num_levels.
            (0...num_levels).each do |offset|
              roll_value.call(self, offset)
              if need_new_chunk
                chunks << CdcChunk.new(
                  prev_offset, offset - prev_offset, prev_offset, offset - prev_offset
                )
                prev_offset = offset
              end
            end
            prev_value_offset = prev_offset
            value_offset = num_levels
          elsif !has_rep_levels
            # Non-nested data with nulls. value_offset only increments for non-null
            # values (def == max_def), so it diverges from the level offset when
            # nulls are present.
            (0...num_levels).each do |offset|
              def_level = def_levels.value_at(offset) ||
                          raise general_err!("def_levels required when max_def_level > 0")
              roll_level(def_level)
              if def_level == max_def_level
                # For non-nested data the leaf array has one slot per level (nulls
                # are array elements), so `offset` is the correct array index.
                roll_value.call(self, offset)
              end
              # Check the boundary before incrementing value_offset so that
              # num_values reflects only entries in the completed chunk.
              if need_new_chunk
                chunks << CdcChunk.new(
                  prev_offset,
                  offset - prev_offset,
                  prev_value_offset,
                  value_offset - prev_value_offset,
                )
                prev_offset = offset
                prev_value_offset = value_offset
              end
              value_offset += 1 if def_level == max_def_level
            end
          else
            # Nested data with nulls. Two counters are needed:
            #
            #   leaf_offset: index into the leaf values array for hashing,
            #     incremented for all leaf slots (def >= repeated_ancestor_def_level),
            #     including null elements.
            #
            #   value_offset: index into non_null_indices for chunk boundaries,
            #     incremented only for non-null leaf values (def == max_def_level).
            #
            # These diverge when nullable elements exist inside lists.
            leaf_offset = 0

            (0...num_levels).each do |offset|
              def_level = def_levels.value_at(offset) ||
                          raise general_err!("def_levels required for nested data")
              rep_level = rep_levels.value_at(offset) ||
                          raise general_err!("rep_levels required for nested data")

              roll_level(def_level)
              roll_level(rep_level)
              roll_value.call(self, leaf_offset) if def_level == max_def_level

              # Check the boundary before incrementing value_offset so that
              # num_values reflects only entries in the completed chunk, and only
              # at top-level record boundaries.
              if rep_level == 0 && need_new_chunk
                levels_to_write = offset - prev_offset
                if levels_to_write > 0
                  chunks << CdcChunk.new(
                    prev_offset,
                    levels_to_write,
                    prev_value_offset,
                    value_offset - prev_value_offset,
                  )
                  prev_offset = offset
                  prev_value_offset = value_offset
                end
              end
              value_offset += 1 if def_level == max_def_level
              leaf_offset += 1 if def_level >= repeated_ancestor_def_level
            end
          end

          # Add the last chunk if we have any levels left.
          if prev_offset < num_levels
            chunks << CdcChunk.new(
              prev_offset,
              num_levels - prev_offset,
              prev_value_offset,
              value_offset - prev_value_offset,
            )
          end

          {% unless flag?(:release) %}
            validate_chunks(chunks, num_levels, value_offset)
          {% end %}

          chunks
        end

        # Validates that `chunks` cover `[0, num_levels)` / `[0, total_values)`
        # contiguously (upstream runs this under `debug_assertions`).
        def validate_chunks(chunks : Array(CdcChunk), num_levels : Int32, total_values : Int32) : Nil
          raise general_err!("chunks must be non-empty") if chunks.empty?

          first = chunks[0]
          unless first.level_offset == 0
            raise general_err!("first chunk must start at level 0")
          end
          unless first.value_offset == 0
            raise general_err!("first chunk must start at value 0")
          end

          sum_levels = first.num_levels
          sum_values = first.num_values
          (1...chunks.size).each do |i|
            chunk = chunks[i]
            prev = chunks[i - 1]
            raise general_err!("chunk must have levels") unless chunk.num_levels > 0

            unless chunk.level_offset == prev.level_offset + prev.num_levels
              raise general_err!("level offsets must be contiguous")
            end
            unless chunk.value_offset == prev.value_offset + prev.num_values
              raise general_err!("value offsets must be contiguous")
            end

            sum_levels += chunk.num_levels
            sum_values += chunk.num_values
          end

          raise general_err!("chunks must cover all levels") unless sum_levels == num_levels
          raise general_err!("chunks must cover all values") unless sum_values == total_values

          last = chunks[chunks.size - 1]
          unless last.level_offset + last.num_levels == num_levels
            raise general_err!("last chunk must end at num_levels")
          end
          nil
        end
      end
    end
  end
end
