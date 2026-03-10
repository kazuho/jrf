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
      input_type = detect_input_type(collection, builtin)

      # Transformation mode (detected on first call)
      if @map_transforms[idx]
        return transform_collection(builtin, input_type, collection, &block)
      end

      map_reducer = (@reducers[idx] ||= MapReducer.new(builtin, input_type))

      case input_type
      when :array
        collection.each_with_index do |v, i|
          slot = map_reducer.slot(i)
          with_scoped_reducers(slot.reducers) do
            result = @ctx.send(:__jrf_with_current_input, v) { block.call(v) }
            slot.template ||= result
          end
        end
      when :hash
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
        return transformed_slots(builtin, input_type, map_reducer)
      end

      ReducerToken.new(idx)
    end

    def allocate_group_by(key, &block)
      idx = @cursor
      map_reducer = (@reducers[idx] ||= MapReducer.new(:group_by, :hash))

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
      if builtin == :map
        block.call([key, value])
      else
        block.call(value)
      end
    end

    def detect_input_type(collection, builtin)
      return :array if collection.is_a?(Array)
      return :hash if collection.is_a?(Hash)

      if builtin == :map_values
        raise TypeError, "map_values expects Hash, got #{collection.class}"
      else
        raise TypeError, "map expects Array or Hash, got #{collection.class}"
      end
    end

    def transform_collection(builtin, input_type, collection, &block)
      case input_type
      when :array
        collection.each_with_object([]) do |value, result|
          mapped = @ctx.send(:__jrf_with_current_input, value) { block.call(value) }
          append_result(result, mapped, builtin)
        end
      when :hash
        if builtin == :map
          collection.each_with_object([]) do |(key, value), result|
            mapped = @ctx.send(:__jrf_with_current_input, value) { invoke_block(builtin, block, key, value) }
            append_result(result, mapped, builtin)
          end
        else
          collection.each_with_object({}) do |(key, value), result|
            mapped = @ctx.send(:__jrf_with_current_input, value) { invoke_block(builtin, block, key, value) }
            next if mapped.equal?(Control::DROPPED)
            raise TypeError, "flat is not supported inside map_values" if mapped.is_a?(Control::Flat)

            result[key] = mapped
          end
        end
      end
    end

    def transformed_slots(builtin, input_type, map_reducer)
      case input_type
      when :array
        map_reducer.slots
          .sort_by { |k, _| k }
          .each_with_object([]) do |(_, slot), result|
            append_result(result, slot.template, builtin)
          end
      when :hash
        if builtin == :map
          map_reducer.slots.each_with_object([]) do |(_key, slot), result|
            append_result(result, slot.template, builtin)
          end
        else
          map_reducer.slots.each_with_object({}) do |(key, slot), result|
            next if slot.template.equal?(Control::DROPPED)
            raise TypeError, "flat is not supported inside map_values" if slot.template.is_a?(Control::Flat)

            result[key] = slot.template
          end
        end
      end
    end

    def append_result(result, mapped, builtin)
      return if mapped.equal?(Control::DROPPED)

      if mapped.is_a?(Control::Flat)
        raise TypeError, "flat is not supported inside #{builtin}" unless builtin == :map
        unless mapped.value.is_a?(Array)
          raise TypeError, "flat expects Array, got #{mapped.value.class}"
        end

        result.concat(mapped.value)
      else
        result << mapped
      end
    end

    class MapReducer
      attr_reader :slots

      def initialize(builtin, input_type)
        @builtin = builtin
        @input_type = input_type
        @slots = {}
      end

      def slot(key)
        @slots[key] ||= SlotState.new
      end

      def finish
        case @input_type
        when :array
          keys = @slots.keys.sort
          [keys.map { |k| Stage.resolve_template(@slots[k].template, @slots[k].reducers) }]
        when :hash
          if @builtin == :map
            [@slots.map { |_k, s| Stage.resolve_template(s.template, s.reducers) }]
          else
            result = {}
            @slots.each { |k, s| result[k] = Stage.resolve_template(s.template, s.reducers) }
            [result]
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
