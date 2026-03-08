# frozen_string_literal: true
require_relative "control"
require_relative "reducers"

module Jr
  class RowContext
    ReducerToken = Struct.new(:index)

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
      __jr_register_reducer__(value: value, initial: initial) { |acc, v| acc + v }
    end

    def min(value)
      __jr_register_reducer__(value: value, initial: nil) { |acc, v| acc.nil? || v < acc ? v : acc }
    end

    def max(value)
      __jr_register_reducer__(value: value, initial: nil) { |acc, v| acc.nil? || v > acc ? v : acc }
    end

    def sort(key = @obj, &compare)
      if compare
        __jr_register_reducer__(
          value: @obj,
          initial: [],
          emit_many: true,
          finish: ->(rows) { rows.sort(&compare) }
        ) do |rows, row|
          rows << row
        end
      else
        __jr_register_reducer__(
          value: [key, @obj],
          initial: [],
          emit_many: true,
          finish: ->(pairs) { pairs.sort_by(&:first).map(&:last) }
        ) do |pairs, pair|
          pairs << pair
        end
      end
    end

    def group(value = @obj)
      __jr_register_reducer__(value: value, initial: []) { |acc, v| acc << v }
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

      __jr_register_reducer__(
        value: value,
        initial: [],
        emit_many: percentage.is_a?(Array),
        finish: finish
      ) { |acc, v| acc << v }
    end

    NO_KW = Object.new

    def reduce(*args, initial: NO_KW, &block)
      raise ArgumentError, "reduce requires a block" unless block

      value, init =
        if initial != NO_KW
          raise ArgumentError, "reduce(value, initial: ...): value is required" unless args.size == 1
          [args[0], initial]
        elsif args.size == 1
          [@obj, args[0]]
        elsif args.size == 2
          [args[0], args[1]]
        else
          raise ArgumentError, "reduce expects reduce(initial), reduce(value, initial: ...), or reduce(value, initial)"
        end

      __jr_register_reducer__(value: value, initial: init, &block)
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

    def __jr_register_reducer__(value:, initial:, emit_many: false, finish: nil, &step_fn)
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
