# frozen_string_literal: true

require_relative "control"
require_relative "reducers"

module Jrf
  class Stage
    ReducerToken = Struct.new(:index)

    attr_reader :method_name, :src

    def self.resolve_template(template, reducers)
      if template.is_a?(ReducerToken)
        rows = reducers.fetch(template.index).finish
        rows.length == 1 ? rows.first : rows
      elsif template.is_a?(Array)
        template.map { |v| resolve_template(v, reducers) }
      elsif template.is_a?(Hash)
        template.transform_values { |v| resolve_template(v, reducers) }
      else
        template
      end
    end

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

    def allocate_map(type, collection, &block)
      idx = @cursor
      map_reducer = (@reducers[idx] ||= MapReducer.new(type))

      unless @probing
        saved_obj = @ctx._

        case type
        when :array
          raise TypeError, "map expects Array, got #{collection.class}" unless collection.is_a?(Array)
          collection.each_with_index do |v, i|
            @ctx.reset(v)
            with_scoped_reducers(map_reducer.slots[i] ||= []) do
              result = block.call(v)
              map_reducer.templates[i] ||= result
            end
          end
        when :hash
          raise TypeError, "map_values expects Hash, got #{collection.class}" unless collection.is_a?(Hash)
          collection.each do |k, v|
            @ctx.reset(v)
            with_scoped_reducers(map_reducer.slots[k] ||= []) do
              result = block.call(v)
              map_reducer.templates[k] ||= result
            end
          end
        end

        @ctx.reset(saved_obj)
      end

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
        [self.class.resolve_template(@template, @reducers)]
      end
    end

    private

    def with_scoped_reducers(reducer_list)
      saved_reducers = @reducers
      saved_cursor = @cursor
      @reducers = reducer_list
      @cursor = 0
      yield
    ensure
      @reducers = saved_reducers
      @cursor = saved_cursor
    end

    class MapReducer
      attr_reader :slots, :templates

      def initialize(type)
        @type = type
        @slots = {}
        @templates = {}
      end

      def finish
        case @type
        when :array
          keys = @slots.keys.sort
          [keys.map { |k| Stage.resolve_template(@templates[k], @slots[k]) }]
        when :hash
          result = {}
          @slots.each { |k, reducers| result[k] = Stage.resolve_template(@templates[k], reducers) }
          [result]
        end
      end
    end
  end
end
