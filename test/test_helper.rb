# frozen_string_literal: true

begin
  require "bundler/setup"
rescue LoadError
  # Allow running tests in plain Ruby environments with globally installed gems.
end

require "json"
require "minitest/autorun"
require "open3"
require "stringio"
require "tmpdir"
require "zlib"
require_relative "../lib/jrf"
require_relative "../lib/jrf/cli/runner"

class RecordingRunner < Jrf::CLI::Runner
  attr_reader :writes

  def initialize(**kwargs)
    super
    @writes = []
  end

  private

  def write_output(str)
    return if str.empty?

    @writes << str
  end
end

class JrfTestCase < Minitest::Test
  def setup
    File.chmod(0o755, "./exe/jrf")
  end

  def run_jrf(expr, input, *opts)
    Open3.capture3("./exe/jrf", *opts, expr, stdin_data: input)
  end

  def assert_success(status, stderr, msg = nil)
    return if status.success?

    flunk("expected success#{msg ? " (#{msg})" : ""}, got failure\nstderr: #{stderr}")
  end

  def assert_failure(status, msg = nil)
    return unless status.success?

    flunk("expected failure#{msg ? " (#{msg})" : ""}, got success")
  end

  def assert_float_close(expected, actual, epsilon = 1e-9, msg = nil)
    assert_in_delta(expected, actual, epsilon, msg)
  end

  def lines(str)
    str.lines.map(&:strip).reject(&:empty?)
  end

  def json_stream_to_ndjson(text)
    JSON.parse("[#{text}]").map { |value| "#{JSON.generate(value)}\n" }.join
  end

  def extract_readme_examples(path, section:)
    content = File.read(path)
    section_match = content.match(/^## #{Regexp.escape(section)}\n(.*?)(?=^## |\z)/m)
    raise "section not found: #{section}" unless section_match

    examples = []
    section_text = section_match[1]
    section_text.scan(/```sh\n(.*?)```/m) do |block_match|
      block = block_match.first
      lines = block.lines.map(&:chomp)
      index = 0
      while index < lines.length
        line = lines[index]
        if (command_match = line.match(/\Ajrf '(.*)'\z/))
          comment = lines[index + 1]
          if comment && (example_match = comment.match(/\A# (.+) → (.+)\z/))
            examples << {
              expr: command_match[1],
              input: json_stream_to_ndjson(example_match[1]),
              output: lines(json_stream_to_ndjson(example_match[2]))
            }
          end
        end
        index += 1
      end
    end
    examples
  end
end

class ChunkedSource
  def initialize(str, chunk_size: 5)
    @str = str
    @chunk_size = chunk_size
    @offset = 0
  end

  def read(length = nil, outbuf = nil)
    raise "expected chunked reads" if length.nil?

    chunk = @str.byteslice(@offset, [length, @chunk_size].min)
    return nil unless chunk

    @offset += chunk.bytesize
    if outbuf
      outbuf.replace(chunk)
    else
      chunk
    end
  end
end
