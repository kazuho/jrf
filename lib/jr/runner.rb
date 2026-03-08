# frozen_string_literal: true

require "json"
require_relative "control"
require_relative "pipeline_parser"
require_relative "reducers"
require_relative "row_context"

module Jr
  class Runner
    class ProbeValue
      def [](key)
        self
      end

      def method_missing(name, *args, &block)
        self
      end

      def respond_to_missing?(name, include_private = false)
        true
      end
    end

    PROBE_VALUE = ProbeValue.new

    def initialize(input: ARGF, out: $stdout, err: $stderr)
      @input = input
      @out = out
      @err = err
    end

    def run(expression, dump_stages: false)
      parsed = PipelineParser.new(expression).parse
      stages = parsed[:stages]
      dump_stages(stages) if dump_stages

      ctx = RowContext.new
      compiled = compile_stages(stages, ctx)
      initialize_reducers(compiled, ctx)

      @input.each_line do |line|
        line = line.strip
        next if line.empty?

        process_value(JSON.parse(line), compiled, ctx)
      end

      flush_reducers(compiled, ctx)
    end

    private

    def process_value(input, stages, ctx)
      current_values = [input]

      stages.each do |stage|
        next_values = []

        current_values.each do |value|
          out = apply_stage(stage, value, ctx)
          if out.equal?(Control::DROPPED)
            next
          elsif flat_event?(out)
            unless out.value.is_a?(Array)
              raise TypeError, "flat expects Array, got #{out.value.class}"
            end
            next_values.concat(out.value)
          else
            next_values << out
          end
        end

        return if next_values.empty?
        current_values = next_values
      end

      current_values.each { |value| @out.puts JSON.generate(value) }
    end

    def apply_stage(stage, input, ctx)
      value = eval_stage(stage, input, ctx)
      if value.equal?(Control::DROPPED)
        Control::DROPPED
      elsif reducer_event?(value)
        stage[:reducer] ||= value.factory.call
        stage[:reducer_factory] ||= value.factory
        stage[:reducer].step(value.value)
        Control::DROPPED
      else
        value
      end
    end

    def eval_stage(stage, input, ctx)
      ctx.reset(input)
      ctx.public_send(stage[:method_name])
    end

    def reducer_event?(value)
      value.is_a?(Reducers::Event)
    end

    def flat_event?(value)
      value.is_a?(Control::Flat)
    end

    def flush_reducers(stages, ctx)
      tail = stages
      loop do
        tail = tail.drop_while { |stage| !reducer_stage?(stage) }
        break if tail.empty?

        stage = tail.first
        stage[:reducer] ||= stage[:reducer_factory]&.call
        break unless stage[:reducer]

        process_value(stage[:reducer].finish, tail.drop(1), ctx)
        tail = tail.drop(1)
      end
    end

    def compile_stages(stages, ctx)
      mod = Module.new
      compiled = []

      stages.each_with_index do |stage, i|
        method_name = :"__jr_stage_#{i}"
        mod.module_eval("def #{method_name}; #{stage[:src]}; end", "(jr stage #{i})", 1)
        compiled << stage.merge(method_name: method_name)
      end

      ctx.extend(mod)
      compiled
    end

    def dump_stages(stages)
      stages.each_with_index do |stage, i|
        @err.puts "stage[#{i}] kind=#{stage[:kind]}"
        @err.puts "  original: #{stage[:original]}"
        @err.puts "  ruby: #{stage[:src]}"
      end
    end

    def initialize_reducers(stages, ctx)
      stages.each_with_index do |stage, i|
        begin
          value = eval_stage(stage, PROBE_VALUE, ctx)
          if reducer_event?(value)
            stage[:reducer_factory] = value.factory
            stage[:reducer] = value.factory.call
          end
        rescue StandardError
          # Ignore probe-time errors; reducer will be created on first runtime event.
        end
      end
    end

    def reducer_stage?(stage)
      stage[:reducer] || stage[:reducer_factory]
    end
  end
end
