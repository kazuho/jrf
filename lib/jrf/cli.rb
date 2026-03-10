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
        -p, --pretty   pretty-print JSON output instead of compact NDJSON
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
      pretty = false
      jit = true
      atomic_write_bytes = Runner::DEFAULT_OUTPUT_BUFFER_LIMIT
      parser = OptionParser.new do |opts|
        opts.banner = USAGE
        opts.on("-v", "--verbose", "print parsed stage expressions") { verbose = true }
        opts.on("--lax", "allow multiline JSON texts; split inputs by whitespace (also detects JSON-SEQ RS 0x1e)") { lax = true }
        opts.on("-p", "--pretty", "pretty-print JSON output instead of compact NDJSON") { pretty = true }
        opts.on("--no-jit", "do not enable YJIT, even when supported by the Ruby runtime") { jit = false }
        opts.on("--atomic-write-bytes N", Integer, "group short outputs into atomic writes of up to N bytes") do |value|
          atomic_write_bytes = parse_atomic_write_bytes(value)
        end
        opts.on("-V", "--version", "show version and exit") do
          out.puts Jrf::VERSION
          throw :jrf_cli_exit, 0
        end
        opts.on("-h", "--help", "show this help and exit") do
          out.puts HELP_TEXT
          throw :jrf_cli_exit, 0
        end
      end

      result = catch(:jrf_cli_exit) do
        begin
          parser.order!(argv)
        rescue OptionParser::ParseError => e
          err.puts e.message
          err.puts USAGE
          return 1
        end
        nil
      end
      return result unless result.nil?

      if argv.empty?
        err.puts USAGE
        return 1
      end

      expression = argv.shift
      enable_yjit if jit

      inputs = Enumerator.new do |y|
        if argv.empty?
          y << input
        else
          argv.each do |path|
            if path == "-"
              y << input
            elsif path.end_with?(".gz")
              require "zlib"
              Zlib::GzipReader.open(path) do |source|
                y << source
              end
            else
              File.open(path, "rb") do |source|
                y << source
              end
            end
          end
        end
      end
      Runner.new(
        inputs: inputs,
        out: out,
        err: err,
        lax: lax,
        pretty: pretty,
        atomic_write_bytes: atomic_write_bytes
      ).run(expression, verbose: verbose)
      0
    end

    def self.parse_atomic_write_bytes(value)
      bytes = Integer(value, exception: false)
      return bytes if bytes && bytes.positive?

      raise OptionParser::InvalidArgument, "--atomic-write-bytes requires a positive integer"
    end

    def self.enable_yjit
      return unless defined?(RubyVM::YJIT) && RubyVM::YJIT.respond_to?(:enable)

      RubyVM::YJIT.enable
    end
  end
end
