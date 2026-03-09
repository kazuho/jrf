# frozen_string_literal: true

require "json"
require_relative "control"
require_relative "pipeline_parser"
require_relative "reducers"
require_relative "row_context"
require_relative "stage"

module Jrf
  class Runner
    RS_CHAR = "\x1e"

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

    def initialize(input: ARGF, out: $stdout, err: $stderr, lax: false)
      @input = input
      @out = out
      @err = err
      @lax = lax
    end

    def run(expression, verbose: false)
      parsed = PipelineParser.new(expression).parse
      stages = parsed[:stages]
      dump_stages(stages) if verbose

      ctx = RowContext.new
      compiled = compile_stages(stages, ctx)
      compiled.each { |stage| stage.call(PROBE_VALUE, probing: true) rescue nil }
      error = nil

      begin
        each_input_value do |value|
          process_value(value, compiled)
        end
      rescue StandardError => e
        error = e
      ensure
        flush_reducers(compiled)
      end

      raise error if error
    end

    private

    def process_value(input, stages)
      current_values = [input]

      stages.each do |stage|
        next_values = []

        current_values.each do |value|
          out = stage.call(value)
          if out.equal?(Control::DROPPED)
            next
          elsif out.is_a?(Control::Flat)
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

    def each_input_value
      return each_input_value_lax { |value| yield value } if @lax

      each_input_value_ndjson { |value| yield value }
    end

    def each_input_value_ndjson
      @input.each_line do |raw_line|
        line = raw_line.strip
        next if line.empty?

        yield JSON.parse(line)
      end
    end

    def each_input_value_lax
      require "oj"
      source = @input.read.to_s
      source = source.include?(RS_CHAR) ? source.tr(RS_CHAR, "\n") : source
      handler = Class.new(Oj::ScHandler) do
        def initialize(&emit)
          @emit = emit
        end

        def hash_start = {}
        def hash_key(key) = key
        def hash_set(hash, key, value) = hash[key] = value
        def array_start = []
        def array_append(array, value) = array << value
        def add_value(value) = @emit.call(value)
      end.new { |value| yield value }
      Oj.sc_parse(handler, source)
    rescue LoadError
      raise "oj is required for --lax mode (gem install oj)"
    rescue Oj::ParseError => e
      raise JSON::ParserError, e.message
    end

    def compile_stages(stages, ctx)
      mod = Module.new

      stages.each_with_index.map do |stage, i|
        method_name = :"__jrf_stage_#{i}"
        mod.module_eval("def #{method_name}; #{stage[:src]}; end", "(jrf stage #{i})", 1)
        Stage.new(ctx, method_name, src: stage[:src])
      end.tap { ctx.extend(mod) }
    end

    def dump_stages(stages)
      stages.each_with_index do |stage, i|
        @err.puts "stage[#{i}]: #{stage[:src]}"
      end
    end

    def flush_reducers(stages)
      tail = stages
      loop do
        idx = tail.index(&:reducer?)
        break unless idx

        rows = tail[idx].finish
        rest = tail.drop(idx + 1)
        rows.each { |value| process_value(value, rest) }
        tail = rest
      end
    end
  end
end
