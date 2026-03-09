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
      @used_reducer = false
    end

    def call(input)
      @ctx.reset(input)
      @cursor = 0
      @used_reducer = false
      @ctx.__jrf_current_stage = self
      result = @ctx.public_send(@method_name)
      @template ||= result if @used_reducer

      @used_reducer ? Control::DROPPED : result
    end

    def allocate_reducer(value, initial:, finish: nil, &step_fn)
      idx = @cursor
      finish_rows = finish || ->(acc) { [acc] }
      @reducers[idx] ||= Reducers.reduce(initial, finish: finish_rows, &step_fn)
      @reducers[idx].step(value)
      @used_reducer = true
      @cursor += 1
      ReducerToken.new(idx)
    end

    def allocate_map(type, collection, &block)
      idx = @cursor
      map_reducer = (@reducers[idx] ||= MapReducer.new(type))
      result =
        case type
        when :array
          raise TypeError, "map expects Array, got #{collection.class}" unless collection.is_a?(Array)
          collection.each_with_index.map do |v, i|
            slot = map_reducer.slot(i)
            slot_result, reducer_used = with_scoped_reducers(slot.reducers) { block.call(v) }
            slot.template ||= slot_result if reducer_used
            slot.value = slot_result unless reducer_used
            slot.output
          end
        when :hash
          raise TypeError, "map_values expects Hash, got #{collection.class}" unless collection.is_a?(Hash)
          collection.each_with_object({}) do |(k, v), acc|
            slot = map_reducer.slot(k)
            slot_result, reducer_used = with_scoped_reducers(slot.reducers) { block.call(v) }
            slot.template ||= slot_result if reducer_used
            slot.value = slot_result unless reducer_used
            acc[k] = slot.output
          end
        end

      @cursor += 1
      @used_reducer ? ReducerToken.new(idx) : result
    end

    def allocate_group_by(key, &block)
      idx = @cursor
      map_reducer = (@reducers[idx] ||= MapReducer.new(:hash))
      row = @ctx._
      slot = map_reducer.slot(key)
      result, reducer_used = with_scoped_reducers(slot.reducers) { block.call(row) }
      slot.template ||= result
      slot.value = result unless reducer_used
      @cursor += 1
      ReducerToken.new(idx)
    end

    def reducer?
      !@reducers.empty?
    end

    def finish
      return [] if @reducers.empty? || @template.nil?

      rows = if @template.is_a?(ReducerToken)
        @reducers.fetch(@template.index).finish
      else
        [self.class.resolve_template(@template, @reducers)]
      end
      @reducers = []
      @template = nil
      rows
    end

    private

    def with_scoped_reducers(reducer_list)
      saved_reducers = @reducers
      saved_cursor = @cursor
      saved_used_reducer = @used_reducer
      @reducers = reducer_list
      @cursor = 0
      @used_reducer = false
      result = yield
      reducer_used = @used_reducer
      [result, reducer_used]
    ensure
      @reducers = saved_reducers
      @cursor = saved_cursor
      @used_reducer = saved_used_reducer || reducer_used
    end

    class MapReducer
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
          [keys.map { |k| @slots.fetch(k).output }]
        when :hash
          result = {}
          @slots.each { |k, slot| result[k] = slot.output }
          [result]
        end
      end

      class SlotState
        attr_reader :reducers
        attr_accessor :template, :value

        def initialize
          @reducers = []
        end

        def output
          if @template
            Stage.resolve_template(@template, @reducers)
          else
            @value
          end
        end
      end
    end
  end
end
