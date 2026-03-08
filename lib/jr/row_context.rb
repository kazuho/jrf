# frozen_string_literal: true
require_relative "control"
require_relative "reducers"

module Jr
  class RowContext
    ReducerToken = Struct.new(:index)

    class << self
      def define_reducer(name, initial:, finish: nil, emit_many: false, &step_fn)
        define_method(name) do |value = @obj|
          create_reducer(
            value,
            initial: reducer_initial_value(initial),
            finish: finish,
            emit_many: emit_many,
            &step_fn
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

    def sum(value, initial: 0)
      create_reducer(value, initial: initial) { |acc, v| acc + v }
    end

    define_reducer(:min, initial: nil) { |acc, v| acc.nil? || v < acc ? v : acc }
    define_reducer(:max, initial: nil) { |acc, v| acc.nil? || v > acc ? v : acc }

    def average(value)
      create_reducer(
        value,
        initial: [0.0, 0],
        finish: ->((sum, count)) { count.zero? ? nil : (sum / count) }
      ) do |acc, v|
        acc[0] += v
        acc[1] += 1
        acc
      end
    end

    def stdev(value, sample: false)
      create_reducer(
        value,
        initial: [0, 0.0, 0.0],
        finish: ->((count, mean, m2)) {
          return nil if count.zero?
          return nil if sample && count < 2

          denom = sample ? (count - 1) : count
          Math.sqrt(m2 / denom)
        }
      ) do |acc, x|
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
      end
    end

    def sort(key = @obj, &compare)
      if compare
        create_reducer(
          @obj,
          initial: [],
          emit_many: true,
          finish: ->(rows) { rows.sort(&compare) }
        ) do |rows, row|
          rows << row
        end
      else
        create_reducer(
          [key, @obj],
          initial: [],
          emit_many: true,
          finish: ->(pairs) { pairs.sort_by(&:first).map(&:last) }
        ) do |pairs, pair|
          pairs << pair
        end
      end
    end

    def group(value = @obj)
      create_reducer(value, initial: []) { |acc, v| acc << v }
    end

    def percentile(value, percentage)
      percentages = percentage.is_a?(Array) ? percentage : [percentage]
      percentages.each { |p| validate_percentile!(p) }

      finish =
        if percentage.is_a?(Array)
          ->(values) {
            sorted = values.sort
            percentages.map do |p|
              { "percentile" => p, "value" => percentile_value(sorted, p) }
            end
          }
        else
          ->(values) {
            percentile_value(values.sort, percentages.first)
          }
        end

      create_reducer(
        value,
        initial: [],
        emit_many: percentage.is_a?(Array),
        finish: finish
      ) { |acc, v| acc << v }
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
