# frozen_string_literal: true

require_relative "control"
require_relative "reducers"

module Jrf
  class Stage
    ReducerToken = Struct.new(:index)

    attr_reader :method_name, :src

    def initialize(ctx, method_name, src: nil)
      @ctx = ctx
      @method_name = method_name
      @src = src
      @reducers = []
      @cursor = 0
      @template = nil
      @mode = nil # nil=unknown, :reducer, :passthrough
      @probing = false
    end

    def call(input, probing: false)
      @ctx.reset(input)
      @cursor = 0
      @probing = probing
      @ctx.__jrf_current_stage = self
      result = @ctx.public_send(@method_name)

      if @mode.nil? && @reducers.any?
        @mode = :reducer
        @template = result
      elsif @mode.nil? && !probing
        @mode = :passthrough
      end

      (@mode == :reducer) ? Control::DROPPED : result
    end

    def allocate_reducer(value, initial:, finish: nil, &step_fn)
      idx = @cursor
      finish_rows = finish || ->(acc) { [acc] }
      @reducers[idx] ||= Reducers.reduce(initial, finish: finish_rows, &step_fn)
      @reducers[idx].step(value) unless @probing
      @cursor += 1
      ReducerToken.new(idx)
    end

    def reducer?
      @mode == :reducer
    end

    def finish
      return [] unless @mode == :reducer && @reducers.any?

      if @template.is_a?(ReducerToken)
        @reducers.fetch(@template.index).finish
      else
        [finish_template(@template)]
      end
    end

    private

    def finish_template(template)
      if template.is_a?(ReducerToken)
        rows = @reducers.fetch(template.index).finish
        rows.length == 1 ? rows.first : rows
      elsif template.is_a?(Array)
        template.map { |v| finish_template(v) }
      elsif template.is_a?(Hash)
        template.transform_values { |v| finish_template(v) }
      else
        template
      end
    end
  end
end
