# frozen_string_literal: true

require_relative "runner"

module Jrf
  class CLI
    USAGE = "usage: jrf [-v] [--help] 'EXPR'"

    HELP_TEXT = <<~'TEXT'
      usage: jrf [-v] [--help] 'EXPR'

      NDJSON transformer using Ruby expressions.

      Options:
        -v, --verbose  print compiled stage Ruby expressions
        -h, --help     show this help and exit

      Pipeline:
        Connect stages with top-level >>.
        Current value in each stage is available as _.

      Examples:
        jrf '_["foo"]'
        jrf 'select(_["x"] > 10) >> _["foo"]'
        jrf '_["items"] >> flat'
        jrf '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'

      See README.md for full reference.
    TEXT

    def self.run(argv = ARGV, input: ARGF, out: $stdout, err: $stderr)
      verbose = false

      while argv.first&.start_with?("-")
        case argv.first
        when "-v", "--verbose"
          verbose = true
          argv.shift
        when "-h", "--help"
          out.puts HELP_TEXT
          return 0
        else
          err.puts "unknown option: #{argv.first}"
          err.puts USAGE
          return 1
        end
      end

      if argv.empty?
        err.puts USAGE
        return 1
      end

      expression = argv.shift
      Runner.new(input: input, out: out, err: err).run(expression, verbose: verbose)
      0
    end
  end
end
