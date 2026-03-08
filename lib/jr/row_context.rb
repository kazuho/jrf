# frozen_string_literal: true
require_relative "control"
require_relative "reducers"

module Jr
  class RowContext
    MISSING = Object.new
    ReducerToken = Struct.new(:index)

    class << self
      def define_reducer(name, &definition)
        define_method(name) do |*args, **kwargs, &block|
          spec = definition.call(self, *args, **kwargs, block: block)
          create_reducer(
            spec.fetch(:value),
            initial: reducer_initial_value(spec.fetch(:initial)),
            finish: spec[:finish],
            emit_many: spec.fetch(:emit_many, false),
            &spec.fetch(:step)
          )
        end
      end
    end

    def initialize(obj = nil)
      @obj = obj
      @__jr_stage = nil
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

    define_reducer(:sum) do |_ctx, value, initial: 0, block: nil|
      { value: value, initial: initial, step: ->(acc, v) { acc + v } }
    end

    define_reducer(:min) do |_ctx, value, block: nil|
      { value: value, initial: nil, step: ->(acc, v) { acc.nil? || v < acc ? v : acc } }
    end

    define_reducer(:max) do |_ctx, value, block: nil|
      { value: value, initial: nil, step: ->(acc, v) { acc.nil? || v > acc ? v : acc } }
    end

    define_reducer(:average) do |_ctx, value, block: nil|
      {
        value: value,
        initial: -> { [0.0, 0] },
        finish: ->((sum, count)) { count.zero? ? nil : (sum / count) },
        step: ->(acc, v) {
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
          return nil if count.zero?
          return nil if sample && count < 2

          denom = sample ? (count - 1) : count
          Math.sqrt(m2 / denom)
        },
        step: ->(acc, x) {
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
          value: ctx._,
          initial: -> { [] },
          emit_many: true,
          finish: ->(rows) { rows.sort(&block) },
          step: ->(rows, row) { rows << row }
        }
      else
        resolved_key = key.equal?(MISSING) ? ctx._ : key
        {
          value: [resolved_key, ctx._],
          initial: -> { [] },
          emit_many: true,
          finish: ->(pairs) { pairs.sort_by(&:first).map(&:last) },
          step: ->(pairs, pair) { pairs << pair }
        }
      end
    end

    define_reducer(:group) do |ctx, value = MISSING, block: nil|
      resolved_value = value.equal?(MISSING) ? ctx._ : value
      { value: resolved_value, initial: -> { [] }, step: ->(acc, v) { acc << v } }
    end

    define_reducer(:percentile) do |ctx, value, percentage, block: nil|
      percentages = percentage.is_a?(Array) ? percentage : [percentage]
      percentages.each { |p| ctx.send(:validate_percentile!, p) }
      scalar = !percentage.is_a?(Array)

      finish =
        if scalar
          ->(values) { ctx.send(:percentile_value, values.sort, percentages.first) }
        else
          ->(values) {
            sorted = values.sort
            percentages.map do |p|
              { "percentile" => p, "value" => ctx.send(:percentile_value, sorted, p) }
            end
          }
        end

      {
        value: value,
        initial: -> { [] },
        emit_many: !scalar,
        finish: finish,
        step: ->(acc, v) { acc << v }
      }
    end

    def reduce(initial, &block)
      raise ArgumentError, "reduce requires a block" unless block

      create_reducer(@obj, initial: initial, &block)
    end

    def __jr_begin_stage__(stage, probing: false)
      @__jr_stage = stage
      stage[:reducer_cursor] = 0
      stage[:reducer_called] = false
      stage[:reducer_probing] = probing
    end

    def __jr_reducer_called?
      @__jr_stage && @__jr_stage[:reducer_called]
    end

  private

    def create_reducer(value, initial:, emit_many: false, finish: nil, &step_fn)
      raise "internal error: reducer used outside stage context" unless @__jr_stage

      reducers = (@__jr_stage[:reducers] ||= [])
      idx = @__jr_stage[:reducer_cursor] || 0
      reducers[idx] ||= Reducers.reduce(initial, finish: finish, &step_fn)
      reducers[idx].step(value) unless @__jr_stage[:reducer_probing]
      @__jr_stage[:reducer_cursor] = idx + 1
      @__jr_stage[:reducer_called] = true
      @__jr_stage[:reducer_emit_many] = emit_many if @__jr_stage[:reducer_emit_many].nil?
      ReducerToken.new(idx)
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
