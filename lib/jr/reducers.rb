# frozen_string_literal: true

module Jr
  module Reducers
    module_function

    Event = Struct.new(:factory, :value)

    class Reduce
      def initialize(initial, &step_fn)
        @acc = initial
        @step_fn = step_fn
      end

      def step(value)
        @acc = @step_fn.call(@acc, value)
      end

      def finish
        @acc
      end
    end

    def reduce(initial, &step_fn)
      Reduce.new(initial, &step_fn)
    end

    def event(value, initial:, &step_fn)
      Event.new(-> { reduce(initial, &step_fn) }, value)
    end
  end
end
