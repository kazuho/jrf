# frozen_string_literal: true

require_relative "control"
require_relative "reducers"

module Jrf
  class RowContext
    MISSING = Object.new

    attr_writer :__jrf_current_stage

    class << self
      def define_reducer(name, &definition)
        define_method(name) do |*args, **kwargs, &block|
          spec = definition.call(self, *args, **kwargs, block: block)
          @__jrf_current_stage.step_reduce(
            spec.fetch(:value),
            initial: reducer_initial_value(spec.fetch(:initial)),
            finish: spec[:finish],
            &spec.fetch(:step)
          )
        end
      end
    end

    def initialize(obj = nil)
      @obj = obj
      @__jrf_current_stage = nil
      @__jrf_current_input = obj
    end

    def reset(obj)
      @obj = obj
      @__jrf_current_input = obj
      self
    end

    def _
      @obj
    end

    def flat
      Control::Flat.new(current_input)
    end

    def select(predicate)
      predicate ? current_input : Control::DROPPED
    end

    define_reducer(:sum) do |_ctx, value, initial: 0, block: nil|
      { value: value, initial: initial, step: ->(acc, v) { v.nil? ? acc : (acc + v) } }
    end

    define_reducer(:count) do |_ctx, value = MISSING, block: nil|
      if value.equal?(MISSING)
        { value: nil, initial: 0, step: ->(acc, _v) { acc + 1 } }
      else
        { value: value, initial: 0, step: ->(acc, v) { v.nil? ? acc : (acc + 1) } }
      end
    end

    define_reducer(:min) do |_ctx, value, block: nil|
      { value: value, initial: nil, step: ->(acc, v) { v.nil? ? acc : (acc.nil? || v < acc ? v : acc) } }
    end

    define_reducer(:max) do |_ctx, value, block: nil|
      { value: value, initial: nil, step: ->(acc, v) { v.nil? ? acc : (acc.nil? || v > acc ? v : acc) } }
    end

    define_reducer(:average) do |_ctx, value, block: nil|
      {
        value: value,
        initial: -> { [0.0, 0] },
        finish: ->((sum, count)) { [count.zero? ? nil : (sum / count)] },
        step: ->(acc, v) {
          return acc if v.nil?

          acc[0] += v
          acc[1] += 1
          acc
        }
      }
    end

    define_reducer(:stdev) do |_ctx, value, sample: false, block: nil|
      {
        value: value,
        initial: [0, 0.0, 0.0],
        finish: ->((count, mean, m2)) {
          return [nil] if count.zero?
          return [nil] if sample && count < 2

          denom = sample ? (count - 1) : count
          [Math.sqrt(m2 / denom)]
        },
        step: ->(acc, x) {
          return acc if x.nil?

          count, mean, m2 = acc
          count += 1
          delta = x - mean
          mean += delta / count
          delta2 = x - mean
          m2 += delta * delta2
          acc[0] = count
          acc[1] = mean
          acc[2] = m2
          acc
        }
      }
    end

    define_reducer(:sort) do |ctx, key = MISSING, block: nil|
      if block
        {
          value: ctx.send(:current_input),
          initial: -> { [] },
          finish: ->(rows) { rows.sort(&block) },
          step: ->(rows, row) { rows << row }
        }
      else
        current = ctx.send(:current_input)
        resolved_key = key.equal?(MISSING) ? current : key
        {
          value: [resolved_key, current],
          initial: -> { [] },
          finish: ->(pairs) { pairs.sort_by(&:first).map(&:last) },
          step: ->(pairs, pair) { pairs << pair }
        }
      end
    end

    define_reducer(:group) do |ctx, value = MISSING, block: nil|
      resolved_value = value.equal?(MISSING) ? ctx.send(:current_input) : value
      { value: resolved_value, initial: -> { [] }, step: ->(acc, v) { acc << v } }
    end

    define_reducer(:percentile) do |ctx, value, percentage, block: nil|
      {
        value: value,
        initial: {config: -> {
          scalar = !percentage.is_a?(Enumerable)
          percentages = scalar ? [percentage] : percentage.to_a
          percentages.each { |p| ctx.send(:validate_percentile!, p) }
          [scalar, percentages]
        }, values: []},
        finish: ->(state) {
          scalar, percentages = state.fetch(:config)
          sorted = state.fetch(:values).sort
          if scalar
            [ctx.send(:percentile_value, sorted, percentages.first)]
          else
            [percentages.map { |p| ctx.send(:percentile_value, sorted, p) }]
          end
        },
        step: ->(state, v) {
          config = state.fetch(:config)
          state[:config] = config.call if config.respond_to?(:call)
          return state if v.nil?

          state.fetch(:values) << v
          state
        }
      }
    end

    def reduce(initial, &block)
      raise ArgumentError, "reduce requires a block" unless block

      @__jrf_current_stage.step_reduce(current_input, initial: initial, &block)
    end

    def map(&block)
      raise ArgumentError, "map requires a block" unless block

      @__jrf_current_stage.step_map(:map, current_input, &block)
    end

    def map_values(&block)
      raise ArgumentError, "map_values requires a block" unless block

      @__jrf_current_stage.step_map(:map_values, current_input, &block)
    end

    def apply(&block)
      raise ArgumentError, "apply requires a block" unless block

      @__jrf_current_stage.step_apply(current_input, &block)
    end

    def group_by(key, &block)
      block ||= proc { group }
      @__jrf_current_stage.step_group_by(key, &block)
    end

    private

    def current_input
      @__jrf_current_input
    end

    def __jrf_with_current_input(value)
      saved_input = current_input
      @__jrf_current_input = value
      yield
    ensure
      @__jrf_current_input = saved_input
    end

    def reducer_initial_value(initial)
      return initial.call if initial.respond_to?(:call)
      return initial.dup if initial.is_a?(Array) || initial.is_a?(Hash)

      initial
    end

    def validate_percentile!(value)
      unless value.is_a?(Numeric) && value >= 0 && value <= 1
        raise ArgumentError, "percentile must be numeric in [0, 1]"
      end
    end

    def percentile_value(sorted_values, percentile)
      return nil if sorted_values.empty?

      idx = (percentile.to_f * sorted_values.length).ceil - 1
      idx = 0 if idx.negative?
      idx = sorted_values.length - 1 if idx >= sorted_values.length
      sorted_values[idx]
    end
  end
end
