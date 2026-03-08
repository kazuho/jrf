# frozen_string_literal: true

require_relative "runner"

module Jr
  class CLI
    def self.run(argv = ARGV, input: ARGF, out: $stdout, err: $stderr)
      dump = false

      while argv.first&.start_with?("--")
        case argv.first
        when "--dump-stages"
          dump = true
          argv.shift
        else
          err.puts "unknown option: #{argv.first}"
          err.puts "usage: jr [--dump-stages] 'EXPR'"
          return 1
        end
      end

      if argv.empty?
        err.puts "usage: jr [--dump-stages] 'EXPR'"
        return 1
      end

      expression = argv.shift
      Runner.new(input: input, out: out, err: err).run(expression, dump_stages: dump)
      0
    end
  end
end
