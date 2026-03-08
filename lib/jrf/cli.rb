# frozen_string_literal: true

require_relative "runner"

module Jrf
  class CLI
    USAGE = "usage: jrf [-v] [--help] 'STAGE >> STAGE >> ...'"

    HELP_TEXT = <<~'TEXT'
      usage: jrf [-v] [--help] 'STAGE >> STAGE >> ...'

      JSON filter with the power and speed of Ruby.

      Options:
        -v, --verbose  print parsed stage expressions
        -h, --help     show this help and exit

      Pipeline:
        Connect stages with top-level >>.
        The current value in each stage is available as _.

      Examples:
        jrf '_["foo"]'
        jrf 'select(_["x"] > 10) >> _["foo"]'
        jrf '_["items"] >> flat'
        jrf 'sort(_["at"]) >> _["id"]'
        jrf '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'

      See Also:
        README.md
        man jrf
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
