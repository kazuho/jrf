# frozen_string_literal: true

require "json"
require_relative "pipeline_parser"
require_relative "row_context"

module Jr
  class Runner
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

      @input.each_line do |line|
        line = line.strip
        next if line.empty?

        current = JSON.parse(line)
        dropped = false

        stages.each do |stage|
          ctx.reset(current)
          case stage[:kind]
          when :select
            unless ctx.instance_eval(stage[:src])
              dropped = true
              break
            end
          when :extract
            current = ctx.instance_eval(stage[:src])
          else
            raise "internal error: unknown stage kind #{stage[:kind].inspect}"
          end
        end

        next if dropped

        @out.puts JSON.generate(current)
      end
    end

    private

    def dump_stages(stages)
      stages.each_with_index do |stage, i|
        @err.puts "stage[#{i}] kind=#{stage[:kind]}"
        @err.puts "  original: #{stage[:original]}"
        @err.puts "  ruby: #{stage[:src]}"
      end
    end
  end
end
