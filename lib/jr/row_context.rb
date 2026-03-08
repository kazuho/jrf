# frozen_string_literal: true
require_relative "control"
require_relative "reducers"

module Jr
  class RowContext
    def initialize(obj = nil)
      @obj = obj
    end

    def reset(obj)
      @obj = obj
      self
    end

    def _
      @obj
    end

    def flat
      Control::Flat.new(@obj)
    end

    def sum(value, initial: 0)
      Reducers.sum_event(value, initial: initial)
    end

    def min(value)
      Reducers.min_event(value)
    end

    def max(value)
      Reducers.max_event(value)
    end

    def reduce(value, initial:, &block)
      raise ArgumentError, "reduce requires a block" unless block

      Reducers.event(value, initial: initial, &block)
    end
  end
end
