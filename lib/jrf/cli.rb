# frozen_string_literal: true

require_relative "cli/runner"

module Jrf
  class CLI
    USAGE = "usage: jrf [-v] [--lax] [--pretty] [--atomic-write-bytes N] [--help] 'STAGE >> STAGE >> ...'"

    HELP_TEXT = <<~'TEXT'
      usage: jrf [-v] [--lax] [--pretty] [--atomic-write-bytes N] [--help] 'STAGE >> STAGE >> ...'

      JSON filter with the power and speed of Ruby.

      Options:
        -v, --verbose  print parsed stage expressions
        --lax          allow multiline JSON texts; split inputs by whitespace (also detects JSON-SEQ RS 0x1e)
        -p, --pretty   pretty-print JSON output instead of compact NDJSON
        --atomic-write-bytes N
                       group short outputs into atomic writes of up to N bytes
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
        https://github.com/kazuho/jrf#readme
    TEXT

    def self.run(argv = ARGV, input: ARGF, out: $stdout, err: $stderr)
      verbose = false
      lax = false
      pretty = false
      atomic_write_bytes = Runner::DEFAULT_OUTPUT_BUFFER_LIMIT

      while argv.first&.start_with?("-")
        case argv.first
        when "-v", "--verbose"
          verbose = true
          argv.shift
        when "--lax"
          lax = true
          argv.shift
        when "-p", "--pretty"
          pretty = true
          argv.shift
        when /\A--atomic-write-bytes=(.+)\z/
          atomic_write_bytes = parse_atomic_write_bytes(Regexp.last_match(1), err)
          return 1 unless atomic_write_bytes
          argv.shift
        when "--atomic-write-bytes"
          argv.shift
          atomic_write_bytes = parse_atomic_write_bytes(argv.shift, err)
          return 1 unless atomic_write_bytes
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
      Runner.new(
        input: input,
        out: out,
        err: err,
        lax: lax,
        pretty: pretty,
        atomic_write_bytes: atomic_write_bytes
      ).run(expression, verbose: verbose)
      0
    end

    def self.parse_atomic_write_bytes(value, err)
      bytes = Integer(value, exception: false)
      return bytes if bytes && bytes.positive?

      err.puts "--atomic-write-bytes requires a positive integer"
      nil
    end
  end
end
