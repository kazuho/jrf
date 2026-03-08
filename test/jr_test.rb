# frozen_string_literal: true

require "open3"

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
assert_includes(stderr, 'ruby: (_["hello"] == 123) ? _ : ::Jr::Control::DROPPED')
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

input_flat = <<~NDJSON
  {"items":[1,2]}
  {"items":[3]}
  {"items":[]}
NDJSON

stdout, stderr, status = run_jr('_["items"] >> flat', input_flat)
assert_success(status, stderr, "flat basic")
assert_equal(%w[1 2 3], lines(stdout), "flat basic output")

input_flat_hash = <<~NDJSON
  {"items":[{"x":1},{"x":2}]}
NDJSON

stdout, stderr, status = run_jr('_["items"] >> flat >> _["x"]', input_flat_hash)
assert_success(status, stderr, "flat then extract")
assert_equal(%w[1 2], lines(stdout), "flat then extract output")

stdout, stderr, status = run_jr('_["items"] >> flat >> sum(_)', input_flat)
assert_success(status, stderr, "flat then sum")
assert_equal(%w[6], lines(stdout), "flat then sum output")

stdout, stderr, status = run_jr('_["foo"] >> flat', input)
assert_failure(status, "flat requires array")
assert_includes(stderr, "flat expects Array")

input_sum = <<~NDJSON
  {"foo":1,"x":5}
  {"foo":2,"x":11}
  {"foo":3,"x":50}
  {"foo":4,"x":70}
NDJSON

stdout, stderr, status = run_jr('sum(_["foo"])', input_sum)
assert_success(status, stderr, "sum only")
assert_equal(%w[10], lines(stdout), "sum output")

stdout, stderr, status = run_jr('min(_["foo"])', input_sum)
assert_success(status, stderr, "min only")
assert_equal(%w[1], lines(stdout), "min output")

stdout, stderr, status = run_jr('max(_["foo"])', input_sum)
assert_success(status, stderr, "max only")
assert_equal(%w[4], lines(stdout), "max output")

stdout, stderr, status = run_jr('select(_["x"] > 10) >> sum(_["foo"])', input_sum)
assert_success(status, stderr, "select + sum")
assert_equal(%w[9], lines(stdout), "select + sum output")

stdout, stderr, status = run_jr('_["foo"] >> sum(_ * 2)', input_sum)
assert_success(status, stderr, "extract + sum")
assert_equal(%w[20], lines(stdout), "extract + sum output")

stdout, stderr, status = run_jr('select(_["x"] > 1000) >> sum(_["foo"])', input_sum)
assert_success(status, stderr, "sum no matches")
assert_equal(%w[0], lines(stdout), "sum no matches output")

stdout, stderr, status = run_jr('select(_["x"] > 1000) >> min(_["foo"])', input_sum)
assert_success(status, stderr, "min no matches")
assert_equal(%w[null], lines(stdout), "min no matches output")

stdout, stderr, status = run_jr('select(_["x"] > 1000) >> max(_["foo"])', input_sum)
assert_success(status, stderr, "max no matches")
assert_equal(%w[null], lines(stdout), "max no matches output")

stdout, stderr, status = run_jr('sum(_["foo"]) >> _ + 1', input_sum)
assert_success(status, stderr, "reduce in middle")
assert_equal(%w[11], lines(stdout), "reduce in middle output")

stdout, stderr, status = run_jr('select(_["x"] > 10) >> _["foo"] >> sum(_ * 2) >> select(_ > 10) >> _ + 1', input_sum)
assert_success(status, stderr, "reduce mixed with select/extract")
assert_equal(%w[19], lines(stdout), "reduce mixed output")

stdout, stderr, status = run_jr('_["foo"] >> sum(_) >> _ * 10 >> sum(_)', input_sum)
assert_success(status, stderr, "multiple reducers")
assert_equal(%w[100], lines(stdout), "multiple reducers output")

stdout, stderr, status = run_jr('_["foo"] >> min(_) >> _ * 10 >> max(_)', input_sum)
assert_success(status, stderr, "min/max mixed reducers")
assert_equal(%w[10], lines(stdout), "min/max mixed reducers output")

stdout, stderr, status = run_jr('sum(_["foo"]) >> select(_ > 100)', input_sum)
assert_success(status, stderr, "post-reduce select drop")
assert_equal([], lines(stdout), "post-reduce select drop output")

stdout, stderr, status = run_jr('select(_["x"] > ) >> _["foo"]', "")
assert_failure(status, "syntax error should fail before row loop")
assert_includes(stderr, "syntax error")

input_chain = <<~NDJSON
  {"foo":{"bar":{"z":1},"keep":true}}
  {"foo":{"bar":{"z":2},"keep":false}}
  {"foo":{"bar":{"z":3},"keep":true}}
NDJSON

stdout, stderr, status = run_jr('_["foo"] >> select(_["keep"]) >> _["bar"] >> select(_["z"] > 1) >> _["z"]', input_chain)
assert_success(status, stderr, "select/extract chain")
assert_equal(%w[3], lines(stdout), "chain output")

puts "ok"
