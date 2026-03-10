# frozen_string_literal: true

require "json"
require_relative "../pipeline"
require_relative "../pipeline_parser"

module Jrf
  class CLI
    class Runner
      RS_CHAR = "\x1e"
      DEFAULT_OUTPUT_BUFFER_LIMIT = 4096

      def initialize(input: ARGF, out: $stdout, err: $stderr, lax: false, pretty: false, atomic_write_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT)
        @input = input
        @out = out
        @err = err
        @lax = lax
        @pretty = pretty
        @atomic_write_bytes = atomic_write_bytes
        @output_buffer = +""
      end

      def run(expression, verbose: false)
        parsed = PipelineParser.new(expression).parse
        stages = parsed[:stages]
        dump_stages(stages) if verbose

        blocks = stages.map { |stage|
          eval("proc { #{stage[:src]} }", nil, "(jrf stage)", 1) # rubocop:disable Security/Eval
        }
        pipeline = Pipeline.new(*blocks)

        input_enum = Enumerator.new { |y| each_input_value { |v| y << v } }
        pipeline.call(input_enum) do |value|
          emit_output(value)
        end
      ensure
        write_output(@output_buffer)
      end

      private

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

      def dump_stages(stages)
        stages.each_with_index do |stage, i|
          @err.puts "stage[#{i}]: #{stage[:src]}"
        end
      end

      def emit_output(value)
        record = (@pretty ? JSON.pretty_generate(value) : JSON.generate(value)) << "\n"
        if @output_buffer.bytesize + record.bytesize <= @atomic_write_bytes
          @output_buffer << record
        else
          write_output(@output_buffer)
          @output_buffer = record
        end
      end

      def write_output(str)
        @out.syswrite(str)
      end
    end
  end
end
