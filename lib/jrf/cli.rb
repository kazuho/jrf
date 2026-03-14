# frozen_string_literal: true

require "optparse"

require_relative "cli/runner"
require_relative "version"

module Jrf
  class CLI
    USAGE = "usage: jrf [options] 'STAGE >> STAGE >> ...'"
    HELP_TEXT = <<~'TEXT'
      usage: jrf [options] 'STAGE >> STAGE >> ...'

      JSON filter with the power and speed of Ruby.

      Options:
        -v, --verbose  print parsed stage expressions
        --lax          allow multiline JSON texts; split inputs by whitespace (also detects JSON-SEQ RS 0x1e)
        -o, --output FORMAT
                       output format: json (default), pretty, tsv
        -P N           opportunistically parallelize the map-prefix across N workers
        -r, --require LIBRARY
                       require LIBRARY before evaluating stages
        --no-jit       do not enable YJIT, even when supported by the Ruby runtime
        --atomic-write-bytes N
                       group short outputs into atomic writes of up to N bytes
        -V, --version  show version and exit
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
      output_format = :json
      parallel = nil
      jit = true
      required_libraries = []
      atomic_write_bytes = Runner::DEFAULT_OUTPUT_BUFFER_LIMIT
      begin
        parser = OptionParser.new do |opts|
          opts.banner = USAGE
          opts.on("-v", "--verbose", "print parsed stage expressions") { verbose = true }
          opts.on("--lax", "allow multiline JSON texts; split inputs by whitespace (also detects JSON-SEQ RS 0x1e)") { lax = true }
          opts.on("-o", "--output FORMAT", %w[json pretty tsv], "output format: json, pretty, tsv") { |fmt| output_format = fmt.to_sym }
          opts.on("-P N", Integer, "opportunistically parallelize the map-prefix across N workers") { |n| parallel = n }
          opts.on("-r", "--require LIBRARY", "require LIBRARY before evaluating stages") { |library| required_libraries << library }
          opts.on("--no-jit", "do not enable YJIT, even when supported by the Ruby runtime") { jit = false }
          opts.on("--atomic-write-bytes N", Integer, "group short outputs into atomic writes of up to N bytes") do |value|
            if value.positive?
              atomic_write_bytes = value
            else
              raise OptionParser::InvalidArgument, "--atomic-write-bytes requires a positive integer"
            end
          end
          opts.on("-V", "--version", "show version and exit") do
            out.puts Jrf::VERSION
            exit
          end
          opts.on("-h", "--help", "show this help and exit") do
            out.puts HELP_TEXT
            exit
          end
        end

        parser.order!(argv)
      rescue OptionParser::ParseError => e
        err.puts e.message
        err.puts USAGE
        exit 1
      end

      if argv.empty?
        err.puts USAGE
        exit 1
      end

      expression = argv.shift
      enable_yjit if jit
      required_libraries.each { |library| require library }

      file_paths = argv.dup

      runner = Runner.new(
        input: file_paths.empty? ? input : file_paths,
        out: out,
        err: err,
        lax: lax,
        output_format: output_format,
        atomic_write_bytes: atomic_write_bytes
      )

      runner.run(expression, parallel: parallel, verbose: verbose)

      exit 1 if runner.input_errors?
    end

    def self.enable_yjit
      return unless defined?(RubyVM::YJIT) && RubyVM::YJIT.respond_to?(:enable)

      RubyVM::YJIT.enable
    end
  end
end
