require "arrow-cr"
require "../errors"
require "./mod"

# Ported from `parquet/src/arrow/arrow_reader/filter.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `ArrowPredicate` is the predicate a `RowFilter` evaluates during a parquet
# read; `ArrowPredicateFn` adapts a block, and `RowFilter` groups predicates
# (a row is returned only when every predicate accepts it).
#
# Divergences: the `Send + 'static` bounds are dropped (Crystal has no `Send`),
# the `FnMut(RecordBatch) -> Result<BooleanArray, ArrowError>` closure is a
# `RecordBatch -> BooleanArray` block (errors raise), and the trait becomes an
# abstract class. `evaluate` is called with a batch containing only the columns
# named by the predicate's `ProjectionMask`.
module Parquet
  module Arrow
    # A predicate operating on an `Arrow::RecordBatch`
    # (upstream `ArrowPredicate`).
    #
    # See `RowFilter` for information on applying filters during the parquet
    # decoding process.
    abstract class ArrowPredicate
      # Returns the `ProjectionMask` that describes the columns required to
      # evaluate this predicate.
      #
      # All projected columns will be provided in the `batch` passed to
      # `#evaluate`. The projection mask should be as small as possible because
      # any columns needed for the overall projection mask are decoded again
      # after a predicate is applied.
      abstract def projection : ProjectionMask

      # Evaluates this predicate for the given `Arrow::RecordBatch` containing
      # the columns identified by `#projection`.
      #
      # Must return an `Arrow::BooleanArray` that has the same length as the
      # input `batch`, where each row indicates whether the row should be
      # returned:
      #
      # * `true`: the row should be returned
      # * `false` or `null`: the row should not be returned
      abstract def evaluate(batch : ::Arrow::RecordBatch) : ::Arrow::BooleanArray
    end

    # An `ArrowPredicate` created from a block and a `ProjectionMask`
    # (upstream `ArrowPredicateFn`).
    #
    # The block is passed `Arrow::RecordBatch`es with only the columns
    # specified in the `ProjectionMask`.
    class ArrowPredicateFn < ArrowPredicate
      # Creates a new predicate that invokes the block on the columns specified
      # in `projection`.
      def initialize(@projection : ProjectionMask, &@f : ::Arrow::RecordBatch -> ::Arrow::BooleanArray)
      end

      def projection : ProjectionMask
        @projection
      end

      def evaluate(batch : ::Arrow::RecordBatch) : ::Arrow::BooleanArray
        @f.call(batch)
      end
    end

    # Filter applied *during* the parquet read process (upstream `RowFilter`).
    #
    # `RowFilter` applies predicates in order, after decoding only the columns
    # required. As predicates eliminate rows, fewer rows from subsequent
    # columns may be required, thus potentially reducing IO and decode. This
    # process is also known as *push down* filtering and *late materialization*.
    #
    # A `RowFilter` consists of a list of `ArrowPredicate`s. Only the rows for
    # which all the predicates evaluate to `true` will be returned. Any
    # `RowSelection` provided to the reader will be applied prior to the first
    # predicate, and each predicate in turn will then be used to compute a more
    # refined `RowSelection` used when evaluating the subsequent predicates.
    #
    # Once all predicates have been evaluated, the final `RowSelection` is
    # applied to the top-level projection to produce the final output
    # `Arrow::RecordBatch`.
    class RowFilter
      # The predicates, applied in order.
      getter predicates : Array(ArrowPredicate)

      # Creates a new `RowFilter` from an array of `ArrowPredicate`s.
      def initialize(@predicates : Array(ArrowPredicate))
      end

      # Creates a new `RowFilter` from individual predicates (a Crystal
      # convenience; upstream takes a `Vec<Box<dyn ArrowPredicate>>`).
      def initialize(*predicates : ArrowPredicate)
        @predicates = [] of ArrowPredicate
        predicates.each { |predicate| @predicates << predicate }
      end

      def to_s(io : IO) : Nil
        io << "RowFilter { " << @predicates.size << " predicates: }"
      end
    end
  end
end
