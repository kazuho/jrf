# frozen_string_literal: true

require_relative "runner"

module Jr
  class CLI
    def self.run(argv = ARGV, input: ARGF, out: $stdout, err: $stderr)
      verbose = false

      while argv.first&.start_with?("-")
        case argv.first
        when "-v"
          verbose = true
          argv.shift
        else
          err.puts "unknown option: #{argv.first}"
          err.puts "usage: jrf [-v] 'EXPR'"
          return 1
        end
      end

      if argv.empty?
        err.puts "usage: jrf [-v] 'EXPR'"
        return 1
      end

      expression = argv.shift
      Runner.new(input: input, out: out, err: err).run(expression, verbose: verbose)
      0
    end
  end
end
