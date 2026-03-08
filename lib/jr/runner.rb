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
      compiled = compile_stages(stages, ctx)

      @input.each_line do |line|
        line = line.strip
        next if line.empty?

        current = JSON.parse(line)
        dropped = false

        compiled.each do |stage|
          ctx.reset(current)
          case stage[:kind]
          when :select
            unless ctx.public_send(stage[:method_name])
              dropped = true
              break
            end
          when :extract
            current = ctx.public_send(stage[:method_name])
          else
            raise "internal error: unknown stage kind #{stage[:kind].inspect}"
          end
        end

        next if dropped

        @out.puts JSON.generate(current)
      end
    end

    private

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
  end
end
