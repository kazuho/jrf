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

    def allocate_map(collection, hash_args:, method_name: "map", &block)
      idx = @cursor
      @cursor += 1
      type = map_collection_type(collection, method_name)

      # Transformation mode (detected on first call)
      if @map_transforms[idx]
        return transform_collection(type, collection, hash_args: hash_args, method_name: method_name, &block)
      end

      map_reducer = (@reducers[idx] ||= MapReducer.new(type, map_result_type(type, method_name)))

      case type
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
            result = @ctx.send(:__jrf_with_current_input, v) { invoke_hash_map_block(block, k, v, hash_args) }
            slot.template ||= result
          end
        end
      end

      # Detect transformation: no reducers were allocated in any slot
      if @mode.nil? && map_reducer.slots.values.all? { |s| s.reducers.empty? }
        @map_transforms[idx] = true
        @reducers[idx] = nil
        return transformed_slots(type, map_reducer, method_name: method_name)
      end

      ReducerToken.new(idx)
    end

    def allocate_group_by(key, &block)
      idx = @cursor
      map_reducer = (@reducers[idx] ||= MapReducer.new(:hash, :hash))

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

    def transform_collection(type, collection, hash_args:, method_name:, &block)
      case type
      when :array
        collection.each_with_object([]) do |value, result|
          mapped = @ctx.send(:__jrf_with_current_input, value) { block.call(value) }
          append_map_result(result, mapped)
        end
      when :hash
        if method_name == "map"
          collection.each_with_object([]) do |(key, value), result|
            mapped = @ctx.send(:__jrf_with_current_input, value) { invoke_hash_map_block(block, key, value, hash_args) }
            append_hash_map_result(result, mapped, method_name)
          end
        else
          collection.each_with_object({}) do |(key, value), result|
            mapped = @ctx.send(:__jrf_with_current_input, value) { invoke_hash_map_block(block, key, value, hash_args) }
            next if mapped.equal?(Control::DROPPED)
            raise TypeError, "flat is not supported inside #{method_name}" if mapped.is_a?(Control::Flat)

            result[key] = mapped
          end
        end
      end
    end

    def transformed_slots(type, map_reducer, method_name:)
      case type
      when :array
        map_reducer.slots
          .sort_by { |k, _| k }
          .each_with_object([]) do |(_, slot), result|
            append_map_result(result, slot.template)
          end
      when :hash
        if method_name == "map"
          map_reducer.slots.each_with_object([]) do |(_key, slot), result|
            append_hash_map_result(result, slot.template, method_name)
          end
        else
          map_reducer.slots.each_with_object({}) do |(key, slot), result|
            next if slot.template.equal?(Control::DROPPED)
            raise TypeError, "flat is not supported inside #{method_name}" if slot.template.is_a?(Control::Flat)

            result[key] = slot.template
          end
        end
      end
    end

    def map_collection_type(collection, method_name)
      return :array if collection.is_a?(Array)
      return :hash if collection.is_a?(Hash)

      expected = method_name == "map_values" ? "Hash" : "Array or Hash"
      raise TypeError, "#{method_name} expects #{expected}, got #{collection.class}"
    end

    def invoke_hash_map_block(block, key, value, hash_args)
      case hash_args
      when :pair
        block.call(key, value)
      when :value
        block.call(value)
      else
        raise ArgumentError, "unsupported hash map args mode: #{hash_args.inspect}"
      end
    end

    def map_result_type(type, method_name)
      return :array if type == :array

      method_name == "map" ? :array : :hash
    end

    def append_map_result(result, mapped)
      return if mapped.equal?(Control::DROPPED)

      if mapped.is_a?(Control::Flat)
        unless mapped.value.is_a?(Array)
          raise TypeError, "flat expects Array, got #{mapped.value.class}"
        end

        result.concat(mapped.value)
      else
        result << mapped
      end
    end

    def append_hash_map_result(result, mapped, method_name)
      return if mapped.equal?(Control::DROPPED)
      raise TypeError, "flat is not supported inside #{method_name}" if mapped.is_a?(Control::Flat)

      result << mapped
    end

    class MapReducer
      attr_reader :slots

      def initialize(type, result_type)
        @type = type
        @result_type = result_type
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
          case @result_type
          when :array
            [@slots.map { |_k, s| Stage.resolve_template(s.template, s.reducers) }]
          when :hash
            result = {}
            @slots.each { |k, s| result[k] = Stage.resolve_template(s.template, s.reducers) }
            [result]
          else
            raise ArgumentError, "unsupported map result type: #{@result_type.inspect}"
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
