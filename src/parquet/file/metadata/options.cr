# Ported from `parquet/src/file/metadata/options.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ParquetStatisticsPolicy` and `ParquetMetaDataOptions`. Divergence:
# Rust `with_*(mut self) -> Self` consumes and returns self; the Crystal methods
# mutate and return `self` (same call-chain behavior). The `Arc<HashSet<usize>>`
# payload is a plain `Set(Int32)`.
module Parquet
  module File
    module Metadata
      # Controls decoding of some Parquet statistics fields.
      struct ParquetStatisticsPolicy
        enum Kind
          KeepAll
          SkipAll
          SkipExcept
        end

        getter kind : Kind
        getter keep : Set(Int32)

        def initialize(@kind : Kind, @keep : Set(Int32) = Set(Int32).new)
        end

        # Decode the relevant statistics for all columns.
        KeepAll = new(Kind::KeepAll)
        # Skip decoding the relevant statistics for all columns.
        SkipAll = new(Kind::SkipAll)

        # Creates a policy to skip all columns except those in `keep`.
        # Returns `SkipAll` when `keep` is empty.
        def self.skip_except(keep : Array(Int32)) : ParquetStatisticsPolicy
          keep.empty? ? SkipAll : new(Kind::SkipExcept, Set.new(keep))
        end

        # Whether the policy for `col_index` is to skip the statistics.
        def skip?(col_index : Int32) : Bool
          case @kind
          when Kind::KeepAll
            false
          when Kind::SkipAll
            true
          else
            !@keep.includes?(col_index)
          end
        end

        def_equals @kind, @keep
      end

      # Options that control what parts of the Parquet file footer metadata are
      # decoded by `ParquetMetaDataReader`/`ParquetMetaDataPushDecoder`.
      class ParquetMetaDataOptions
        getter schema_descr : Parquet::Schema::SchemaDescriptor?
        getter? encoding_stats_as_mask : Bool
        getter encoding_stats_policy : ParquetStatisticsPolicy
        getter column_stats_policy : ParquetStatisticsPolicy
        getter size_stats_policy : ParquetStatisticsPolicy

        def initialize
          @schema_descr = nil
          @encoding_stats_as_mask = true
          @encoding_stats_policy = ParquetStatisticsPolicy::KeepAll
          @column_stats_policy = ParquetStatisticsPolicy::KeepAll
          @size_stats_policy = ParquetStatisticsPolicy::KeepAll
        end

        # Returns an optional schema to use when decoding.
        def schema : Parquet::Schema::SchemaDescriptor?
          @schema_descr
        end

        # ameba:disable Naming/AccessorMethodName
        def set_schema(val : Parquet::Schema::SchemaDescriptor) : Nil
          @schema_descr = val
        end

        def with_schema(val : Parquet::Schema::SchemaDescriptor) : self
          @schema_descr = val
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_encoding_stats_as_mask(val : Bool) : Nil
          @encoding_stats_as_mask = val
        end

        def with_encoding_stats_as_mask(val : Bool) : self
          @encoding_stats_as_mask = val
          self
        end

        # Whether to skip decoding `encoding_stats` for `col_index`.
        def skip_encoding_stats(col_index : Int32) : Bool
          @encoding_stats_policy.skip?(col_index)
        end

        # ameba:disable Naming/AccessorMethodName
        def set_encoding_stats_policy(policy : ParquetStatisticsPolicy) : Nil
          @encoding_stats_policy = policy
        end

        def with_encoding_stats_policy(policy : ParquetStatisticsPolicy) : self
          @encoding_stats_policy = policy
          self
        end

        # Whether to skip decoding `statistics` for `col_index`.
        def skip_column_stats(col_index : Int32) : Bool
          @column_stats_policy.skip?(col_index)
        end

        # ameba:disable Naming/AccessorMethodName
        def set_column_stats_policy(policy : ParquetStatisticsPolicy) : Nil
          @column_stats_policy = policy
        end

        def with_column_stats_policy(policy : ParquetStatisticsPolicy) : self
          @column_stats_policy = policy
          self
        end

        # Whether to skip decoding `size_statistics` for `col_index`.
        def skip_size_stats(col_index : Int32) : Bool
          @size_stats_policy.skip?(col_index)
        end

        # ameba:disable Naming/AccessorMethodName
        def set_size_stats_policy(policy : ParquetStatisticsPolicy) : Nil
          @size_stats_policy = policy
        end

        def with_size_stats_policy(policy : ParquetStatisticsPolicy) : self
          @size_stats_policy = policy
          self
        end
      end
    end
  end
end
