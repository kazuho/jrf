# frozen_string_literal: true

module Jrf
  module Reducers
    module_function

    class Reduce
      def initialize(initial, finish_fn: nil, &step_fn)
        @acc = initial
        @step_fn = step_fn
        @finish_fn = finish_fn || ->(acc) { acc }
      end

      def step(value)
        @acc = @step_fn.call(@acc, value)
      end

      def finish
        @finish_fn.call(@acc)
      end
    end

    # A reducer whose partial accumulators can be merged across parallel workers.
    #
    # Contract:
    # - `identity` is the neutral element for `merge_fn`: merge(identity, x) == x
    # - `initial` is always set to `identity` (the accumulator starts from the neutral element)
    # - Any bias (e.g. sum's `initial:` keyword) is applied in `finish_fn`, not in the starting accumulator
    class DecomposableReduce < Reduce
      attr_reader :merge_fn

      def initialize(identity, merge:, finish_fn: nil, &step_fn)
        super(identity, finish_fn: finish_fn, &step_fn)
        @merge_fn = merge
      end

      # Returns the raw accumulator without applying finish_fn.
      def partial
        @acc
      end

      # Merges another partial accumulator into this one.
      def merge_partial(other_acc)
        @acc = @merge_fn.call(@acc, other_acc)
      end
    end

    def reduce(initial, finish: nil, &step_fn)
      Reduce.new(initial, finish_fn: finish, &step_fn)
    end

    def decomposable_reduce(identity, merge:, finish: nil, &step_fn)
      DecomposableReduce.new(identity, merge: merge, finish_fn: finish, &step_fn)
    end
  end
end
