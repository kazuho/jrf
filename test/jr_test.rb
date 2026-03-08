# frozen_string_literal: true

require "open3"
require "json"
require "tmpdir"

def run_jr(expr, input, *opts)
  Open3.capture3("./exe/jr", *opts, expr, stdin_data: input)
end

def assert_equal(expected, actual, msg = nil)
  return if expected == actual

  raise "assert_equal failed#{msg ? " (#{msg})" : ""}\nexpected: #{expected.inspect}\nactual: #{actual.inspect}"
end

def assert_includes(text, fragment, msg = nil)
  return if text.include?(fragment)

  raise "assert_includes failed#{msg ? " (#{msg})" : ""}\ntext: #{text.inspect}\nfragment: #{fragment.inspect}"
end

def assert_success(status, stderr, msg = nil)
  return if status.success?

  raise "expected success#{msg ? " (#{msg})" : ""}, got failure\nstderr: #{stderr}"
end

def assert_failure(status, msg = nil)
  return unless status.success?

  raise "expected failure#{msg ? " (#{msg})" : ""}, got success"
end

def lines(str)
  str.lines.map(&:strip).reject(&:empty?)
end

File.chmod(0o755, "./exe/jr")

input = <<~NDJSON
  {"foo":1,"x":5}
  {"foo":2,"x":11}
  {"foo":{"bar":"ok"},"x":50}
  {"x":70}
NDJSON

stdout, stderr, status = run_jr('_["foo"]', input)
assert_success(status, stderr, "simple extract")
assert_equal(%w[1 2 {"bar":"ok"} null], lines(stdout), "extract output")

input_nested = <<~NDJSON
  {"foo":{"bar":"a"}}
  {"foo":{"bar":"b"}}
NDJSON

stdout, stderr, status = run_jr('_["foo"]["bar"]', input_nested)
assert_success(status, stderr, "nested extract")
assert_equal(%w["a" "b"], lines(stdout), "nested output")

stdout, stderr, status = run_jr('select(_["x"] > 10) >> _["foo"]', input)
assert_success(status, stderr, "select + extract")
assert_equal(%w[2 {"bar":"ok"} null], lines(stdout), "filtered output")

stdout, stderr, status = run_jr('select(_["x"] > 10)', input)
assert_success(status, stderr, "select only")
assert_equal(
  ['{"foo":2,"x":11}', '{"foo":{"bar":"ok"},"x":50}', '{"x":70}'],
  lines(stdout),
  "select-only output"
)

input_hello = <<~NDJSON
  {"hello":123}
  {"hello":456}
NDJSON

stdout, stderr, status = run_jr('select(_["hello"] == 123)', input_hello)
assert_success(status, stderr, "select-only hello")
assert_equal(['{"hello":123}'], lines(stdout), "select-only hello output")

stdout, stderr, status = run_jr('select(_["hello"] == 123) >> _["hello"]', input_hello, "--dump-stages")
assert_success(status, stderr, "dump stages")
assert_equal(%w[123], lines(stdout), "dump stages output")
assert_includes(stderr, "stage[0] kind=select")
assert_includes(stderr, 'original: select(_["hello"] == 123)')
assert_includes(stderr, 'ruby: _["hello"] == 123')
assert_includes(stderr, "stage[1] kind=extract")
assert_includes(stderr, 'original: _["hello"]')
assert_includes(stderr, 'ruby: _["hello"]')

input_regex = <<~NDJSON
  {"foo":{"bar":"ok"},"x":50}
  {"foo":{"bar":"ng"},"x":70}
NDJSON

stdout, stderr, status = run_jr('select(/ok/.match(_["foo"]["bar"])) >> _["x"]', input_regex)
assert_success(status, stderr, "regex in select")
assert_equal(%w[50], lines(stdout), "regex filter output")

stdout, stderr, status = run_jr('_["foo"] >> flat', input)
assert_failure(status, "flat unsupported")
assert_includes(stderr, "flat is not supported yet")

stdout, stderr, status = run_jr('sum(_["foo"])', input)
assert_failure(status, "sum unsupported")
assert_includes(stderr, "sum(...) is not supported yet")

input_chain = <<~NDJSON
  {"foo":{"bar":{"z":1},"keep":true}}
  {"foo":{"bar":{"z":2},"keep":false}}
  {"foo":{"bar":{"z":3},"keep":true}}
NDJSON

stdout, stderr, status = run_jr('_["foo"] >> select(_["keep"]) >> _["bar"] >> select(_["z"] > 1) >> _["z"]', input_chain)
assert_success(status, stderr, "select/extract chain")
assert_equal(%w[3], lines(stdout), "chain output")

puts "ok"
