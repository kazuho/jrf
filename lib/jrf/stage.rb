# frozen_string_literal: true

require_relative "control"
require_relative "reducers"

module Jrf
  class Stage
    ReducerToken = Struct.new(:index)

    attr_reader :src

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

    def initialize(ctx, block, src: nil)
      @ctx = ctx
      @block = block
      @src = src
      @reducers = []
      @cursor = 0
      @template = nil
      @mode = nil # nil=unknown, :reducer, :passthrough
      @map_transforms = {}
    end

    def call(input)
      @ctx.reset(input)
      @cursor = 0
      @ctx.__jrf_current_stage = self
      result = @ctx.instance_eval(&@block)

      if @mode.nil? && @reducers.any?
        @mode = :reducer
        @template = result
      elsif @mode.nil?
        @mode = :passthrough
      end

      (@mode == :reducer) ? Control::DROPPED : result
    end

    def allocate_reducer(value, initial:, finish: nil, &step_fn)
      idx = @cursor
      finish_rows = finish || ->(acc) { [acc] }
      @reducers[idx] ||= Reducers.reduce(initial, finish: finish_rows, &step_fn)
      @reducers[idx].step(value)
      @cursor += 1
      ReducerToken.new(idx)
    end

    def allocate_map(type, collection, &block)
      idx = @cursor
      @cursor += 1

      # Transformation mode (detected on first call)
      if @map_transforms[idx]
        case type
        when :array then return collection.map(&block)
        when :hash then return collection.transform_values(&block)
        end
      end

      map_reducer = (@reducers[idx] ||= MapReducer.new(type))

      case type
      when :array
        raise TypeError, "map expects Array, got #{collection.class}" unless collection.is_a?(Array)
        collection.each_with_index do |v, i|
          slot = map_reducer.slot(i)
          with_scoped_reducers(slot.reducers) do
            result = block.call(v)
            slot.template ||= result
          end
        end
      when :hash
        raise TypeError, "map_values expects Hash, got #{collection.class}" unless collection.is_a?(Hash)
        collection.each do |k, v|
          slot = map_reducer.slot(k)
          with_scoped_reducers(slot.reducers) do
            result = block.call(v)
            slot.template ||= result
          end
        end
      end

      # Detect transformation: no reducers were allocated in any slot
      if @mode.nil? && map_reducer.slots.values.all? { |s| s.reducers.empty? }
        @map_transforms[idx] = true
        @reducers[idx] = nil
        case type
        when :array
          return map_reducer.slots.sort_by { |k, _| k }.map { |_, s| s.template }
        when :hash
          return map_reducer.slots.transform_values(&:template)
        end
      end

      ReducerToken.new(idx)
    end

    def allocate_group_by(key, &block)
      idx = @cursor
      map_reducer = (@reducers[idx] ||= MapReducer.new(:hash))

      row = @ctx._
      slot = map_reducer.slot(key)
      with_scoped_reducers(slot.reducers) do
        result = block.call(row)
        slot.template ||= result
      end

      @cursor += 1
      ReducerToken.new(idx)
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
      attr_reader :slots

      def initialize(type)
        @type = type
        @slots = {}
      end

      def slot(key)
        @slots[key] ||= SlotState.new
      end

      def finish
        case @type
        when :array
          keys = @slots.keys.sort
          [keys.map { |k| Stage.resolve_template(@slots[k].template, @slots[k].reducers) }]
        when :hash
          result = {}
          @slots.each { |k, s| result[k] = Stage.resolve_template(s.template, s.reducers) }
          [result]
        end
      end

      class SlotState
        attr_reader :reducers
        attr_accessor :template

        def initialize
          @reducers = []
          @template = nil
        end
      end
    end
  end
end
