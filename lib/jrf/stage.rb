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

    def allocate_map(builtin, collection, &block)
      idx = @cursor
      @cursor += 1

      if collection.is_a?(Array)
        raise TypeError, "map_values expects Hash, got Array" if builtin == :map_values
      elsif !collection.is_a?(Hash)
        raise TypeError, "#{builtin} expects #{builtin == :map_values ? "Hash" : "Array or Hash"}, got #{collection.class}"
      end

      # Transformation mode (detected on first call)
      if @map_transforms[idx]
        return transform_collection(builtin, collection, &block)
      end

      map_reducer = (@reducers[idx] ||= MapReducer.new(builtin, collection.is_a?(Array)))

      if collection.is_a?(Array)
        collection.each_with_index do |v, i|
          slot = map_reducer.slot(i)
          with_scoped_reducers(slot.reducers) do
            result = @ctx.send(:__jrf_with_current_input, v) { block.call(v) }
            slot.template ||= result
          end
        end
      else
        collection.each do |k, v|
          slot = map_reducer.slot(k)
          with_scoped_reducers(slot.reducers) do
            result = @ctx.send(:__jrf_with_current_input, v) { invoke_block(builtin, block, k, v) }
            slot.template ||= result
          end
        end
      end

      # Detect transformation: no reducers were allocated in any slot
      if @mode.nil? && map_reducer.slots.values.all? { |s| s.reducers.empty? }
        @map_transforms[idx] = true
        @reducers[idx] = nil
        return transformed_slots(builtin, map_reducer)
      end

      ReducerToken.new(idx)
    end

    def allocate_group_by(key, &block)
      idx = @cursor
      map_reducer = (@reducers[idx] ||= MapReducer.new(:group_by, false))

      row = @ctx._
      slot = map_reducer.slot(key)
      with_scoped_reducers(slot.reducers) do
        result = @ctx.send(:__jrf_with_current_input, row) { block.call(row) }
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

    def invoke_block(builtin, block, key, value)
      case builtin
      when :map then block.call([key, value])
      when :map_values then block.call(value)
      else raise ArgumentError, "unexpected builtin: #{builtin}"
      end
    end

    def transform_collection(builtin, collection, &block)
      if collection.is_a?(Array)
        collection.each_with_object([]) do |value, result|
          mapped = @ctx.send(:__jrf_with_current_input, value) { block.call(value) }
          append_result(result, mapped, builtin)
        end
      else
        case builtin
        when :map
          collection.each_with_object([]) do |(key, value), result|
            mapped = @ctx.send(:__jrf_with_current_input, value) { invoke_block(builtin, block, key, value) }
            append_result(result, mapped, builtin)
          end
        when :map_values
          collection.each_with_object({}) do |(key, value), result|
            mapped = @ctx.send(:__jrf_with_current_input, value) { invoke_block(builtin, block, key, value) }
            next if mapped.equal?(Control::DROPPED)
            raise TypeError, "flat is not supported inside map_values" if mapped.is_a?(Control::Flat)

            result[key] = mapped
          end
        else
          raise ArgumentError, "unexpected builtin: #{builtin}"
        end
      end
    end

    def transformed_slots(builtin, map_reducer)
      if map_reducer.array_input?
        map_reducer.slots
          .sort_by { |k, _| k }
          .each_with_object([]) do |(_, slot), result|
            append_result(result, slot.template, builtin)
          end
      else
        case builtin
        when :map
          map_reducer.slots.each_with_object([]) do |(_key, slot), result|
            append_result(result, slot.template, builtin)
          end
        when :map_values
          map_reducer.slots.each_with_object({}) do |(key, slot), result|
            next if slot.template.equal?(Control::DROPPED)
            raise TypeError, "flat is not supported inside map_values" if slot.template.is_a?(Control::Flat)

            result[key] = slot.template
          end
        else
          raise ArgumentError, "unexpected builtin: #{builtin}"
        end
      end
    end

    def append_result(result, mapped, builtin)
      return if mapped.equal?(Control::DROPPED)

      if mapped.is_a?(Control::Flat)
        case builtin
        when :map
          unless mapped.value.is_a?(Array)
            raise TypeError, "flat expects Array, got #{mapped.value.class}"
          end
          result.concat(mapped.value)
        when :map_values
          raise TypeError, "flat is not supported inside map_values"
        else
          raise ArgumentError, "unexpected builtin: #{builtin}"
        end
      else
        result << mapped
      end
    end

    class MapReducer
      attr_reader :slots

      def initialize(builtin, array_input)
        @builtin = builtin
        @array_input = array_input
        @slots = {}
      end

      def array_input?
        @array_input
      end

      def slot(key)
        @slots[key] ||= SlotState.new
      end

      def finish
        if @array_input
          keys = @slots.keys.sort
          [keys.map { |k| Stage.resolve_template(@slots[k].template, @slots[k].reducers) }]
        else
          case @builtin
          when :map
            [@slots.map { |_k, s| Stage.resolve_template(s.template, s.reducers) }]
          when :map_values, :group_by
            result = {}
            @slots.each { |k, s| result[k] = Stage.resolve_template(s.template, s.reducers) }
            [result]
          else
            raise ArgumentError, "unexpected builtin: #{@builtin}"
          end
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
