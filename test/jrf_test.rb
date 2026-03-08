# frozen_string_literal: true

require "open3"

def run_jrf(expr, input, *opts)
  Open3.capture3("./exe/jrf", *opts, expr, stdin_data: input)
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

def assert_float_close(expected, actual, epsilon = 1e-9, msg = nil)
  return if (expected - actual).abs <= epsilon

  raise "assert_float_close failed#{msg ? " (#{msg})" : ""}\nexpected: #{expected}\nactual: #{actual}\nepsilon: #{epsilon}"
end

def lines(str)
  str.lines.map(&:strip).reject(&:empty?)
end

File.chmod(0o755, "./exe/jrf")

input = <<~NDJSON
  {"foo":1,"x":5}
  {"foo":2,"x":11}
  {"foo":{"bar":"ok"},"x":50}
  {"x":70}
NDJSON

stdout, stderr, status = run_jrf('_["foo"]', input)
assert_success(status, stderr, "simple extract")
assert_equal(%w[1 2 {"bar":"ok"} null], lines(stdout), "extract output")

input_nested = <<~NDJSON
  {"foo":{"bar":"a"}}
  {"foo":{"bar":"b"}}
NDJSON

stdout, stderr, status = run_jrf('_["foo"]["bar"]', input_nested)
assert_success(status, stderr, "nested extract")
assert_equal(%w["a" "b"], lines(stdout), "nested output")

stdout, stderr, status = run_jrf('select(_["x"] > 10) >> _["foo"]', input)
assert_success(status, stderr, "select + extract")
assert_equal(%w[2 {"bar":"ok"} null], lines(stdout), "filtered output")

stdout, stderr, status = run_jrf('select(_["x"] > 10)', input)
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

stdout, stderr, status = run_jrf('select(_["hello"] == 123)', input_hello)
assert_success(status, stderr, "select-only hello")
assert_equal(['{"hello":123}'], lines(stdout), "select-only hello output")

stdout, stderr, status = run_jrf('select(_["hello"] == 123) >> _["hello"]', input_hello, "-v")
assert_success(status, stderr, "dump stages")
assert_equal(%w[123], lines(stdout), "dump stages output")
assert_includes(stderr, 'stage[0]: select(_["hello"] == 123)')
assert_includes(stderr, 'stage[1]: _["hello"]')

stdout, stderr, status = Open3.capture3("./exe/jrf", "--help")
assert_success(status, stderr, "help option")
assert_includes(stdout, "usage: jrf [-v] [--help] 'STAGE >> STAGE >> ...'")
assert_includes(stdout, "JSON filter with the power and speed of Ruby.")
assert_includes(stdout, "Pipeline:")
assert_includes(stdout, "Connect stages with top-level >>.")
assert_includes(stdout, "The current value in each stage is available as _.")
assert_includes(stdout, "See Also:")
assert_includes(stdout, "README.md")
assert_includes(stdout, "man jrf")
assert_equal([], lines(stderr), "help stderr output")

stdout, stderr, status = run_jrf('select(_["hello"] == 123) >> _["hello"]', input_hello, "--verbose")
assert_success(status, stderr, "dump stages verbose alias")
assert_equal(%w[123], lines(stdout), "dump stages verbose alias output")
assert_includes(stderr, 'stage[0]: select(_["hello"] == 123)')

input_regex = <<~NDJSON
  {"foo":{"bar":"ok"},"x":50}
  {"foo":{"bar":"ng"},"x":70}
NDJSON

stdout, stderr, status = run_jrf('select(/ok/.match(_["foo"]["bar"])) >> _["x"]', input_regex)
assert_success(status, stderr, "regex in select")
assert_equal(%w[50], lines(stdout), "regex filter output")

input_split = <<~NDJSON
  {"x":1}
NDJSON

stdout, stderr, status = run_jrf('[1 >> 2] >> _', input_split)
assert_success(status, stderr, "no split inside []")
assert_equal(['[0]'], lines(stdout), "no split inside [] output")

stdout, stderr, status = run_jrf('{a: 1 >> 2} >> _[:a]', input_split)
assert_success(status, stderr, "no split inside {}")
assert_equal(%w[0], lines(stdout), "no split inside {} output")

stdout, stderr, status = run_jrf('(-> { 1 >> 2 }).call >> _ + 1', input_split)
assert_success(status, stderr, "no split inside block")
assert_equal(%w[1], lines(stdout), "no split inside block output")

input_flat = <<~NDJSON
  {"items":[1,2]}
  {"items":[3]}
  {"items":[]}
NDJSON

stdout, stderr, status = run_jrf('_["items"] >> flat', input_flat)
assert_success(status, stderr, "flat basic")
assert_equal(%w[1 2 3], lines(stdout), "flat basic output")

input_flat_hash = <<~NDJSON
  {"items":[{"x":1},{"x":2}]}
NDJSON

stdout, stderr, status = run_jrf('_["items"] >> flat >> _["x"]', input_flat_hash)
assert_success(status, stderr, "flat then extract")
assert_equal(%w[1 2], lines(stdout), "flat then extract output")

stdout, stderr, status = run_jrf('_["items"] >> flat >> sum(_)', input_flat)
assert_success(status, stderr, "flat then sum")
assert_equal(%w[6], lines(stdout), "flat then sum output")

stdout, stderr, status = run_jrf('_["items"] >> flat >> group', input_flat)
assert_success(status, stderr, "flat then group")
assert_equal(['[1,2,3]'], lines(stdout), "flat then group output")

stdout, stderr, status = run_jrf('_["foo"] >> flat', input)
assert_failure(status, "flat requires array")
assert_includes(stderr, "flat expects Array")

input_sum = <<~NDJSON
  {"foo":1,"x":5}
  {"foo":2,"x":11}
  {"foo":3,"x":50}
  {"foo":4,"x":70}
NDJSON

stdout, stderr, status = run_jrf('sum(_["foo"])', input_sum)
assert_success(status, stderr, "sum only")
assert_equal(%w[10], lines(stdout), "sum output")

stdout, stderr, status = run_jrf('min(_["foo"])', input_sum)
assert_success(status, stderr, "min only")
assert_equal(%w[1], lines(stdout), "min output")

stdout, stderr, status = run_jrf('max(_["foo"])', input_sum)
assert_success(status, stderr, "max only")
assert_equal(%w[4], lines(stdout), "max output")

stdout, stderr, status = run_jrf('select(_["x"] > 10) >> sum(_["foo"])', input_sum)
assert_success(status, stderr, "select + sum")
assert_equal(%w[9], lines(stdout), "select + sum output")

stdout, stderr, status = run_jrf('average(_["foo"])', input_sum)
assert_success(status, stderr, "average")
assert_float_close(2.5, lines(stdout).first.to_f, 1e-12, "average output")

stdout, stderr, status = run_jrf('stdev(_["foo"])', input_sum)
assert_success(status, stderr, "stdev")
assert_float_close(1.118033988749895, lines(stdout).first.to_f, 1e-12, "stdev output")

stdout, stderr, status = run_jrf('_["foo"] >> sum(_ * 2)', input_sum)
assert_success(status, stderr, "extract + sum")
assert_equal(%w[20], lines(stdout), "extract + sum output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> sum(_["foo"])', input_sum)
assert_success(status, stderr, "sum no matches")
assert_equal(%w[0], lines(stdout), "sum no matches output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> average(_["foo"])', input_sum)
assert_success(status, stderr, "average no matches")
assert_equal(%w[null], lines(stdout), "average no matches output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> stdev(_["foo"])', input_sum)
assert_success(status, stderr, "stdev no matches")
assert_equal(%w[null], lines(stdout), "stdev no matches output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> min(_["foo"])', input_sum)
assert_success(status, stderr, "min no matches")
assert_equal(%w[null], lines(stdout), "min no matches output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> max(_["foo"])', input_sum)
assert_success(status, stderr, "max no matches")
assert_equal(%w[null], lines(stdout), "max no matches output")

stdout, stderr, status = run_jrf('sum(_["foo"]) >> _ + 1', input_sum)
assert_success(status, stderr, "reduce in middle")
assert_equal(%w[11], lines(stdout), "reduce in middle output")

stdout, stderr, status = run_jrf('select(_["x"] > 10) >> _["foo"] >> sum(_ * 2) >> select(_ > 10) >> _ + 1', input_sum)
assert_success(status, stderr, "reduce mixed with select/extract")
assert_equal(%w[19], lines(stdout), "reduce mixed output")

stdout, stderr, status = run_jrf('_["foo"] >> sum(_) >> _ * 10 >> sum(_)', input_sum)
assert_success(status, stderr, "multiple reducers")
assert_equal(%w[100], lines(stdout), "multiple reducers output")

stdout, stderr, status = run_jrf('_["foo"] >> min(_) >> _ * 10 >> max(_)', input_sum)
assert_success(status, stderr, "min/max mixed reducers")
assert_equal(%w[10], lines(stdout), "min/max mixed reducers output")

input_sort_rows = <<~NDJSON
  {"foo":"b","at":2}
  {"foo":"c","at":3}
  {"foo":"a","at":1}
NDJSON

stdout, stderr, status = run_jrf('sort(_["at"]) >> _["foo"]', input_sort_rows)
assert_success(status, stderr, "sort rows by field")
assert_equal(%w["a" "b" "c"], lines(stdout), "sort rows by field output")

stdout, stderr, status = run_jrf('sort { |a, b| b["at"] <=> a["at"] } >> _["foo"]', input_sort_rows)
assert_success(status, stderr, "sort rows by comparator")
assert_equal(%w["c" "b" "a"], lines(stdout), "sort rows by comparator output")

stdout, stderr, status = run_jrf('sort(_["at"]) >> _["foo"] >> group', input_sort_rows)
assert_success(status, stderr, "sort then group")
assert_equal(['["a","b","c"]'], lines(stdout), "sort then group output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> sort(_["x"]) >> _["foo"]', input_sum)
assert_success(status, stderr, "sort no matches")
assert_equal([], lines(stdout), "sort no matches output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> _["foo"] >> group', input_sum)
assert_success(status, stderr, "group no matches")
assert_equal(['[]'], lines(stdout), "group no matches output")

input_group_multi = <<~NDJSON
  {"x":1,"y":"a"}
  {"x":2,"y":"b"}
  {"x":3,"y":"c"}
NDJSON

stdout, stderr, status = run_jrf('{a: group(_["x"]), b: group(_["y"])}', input_group_multi)
assert_success(status, stderr, "group in hash")
assert_equal(['{"a":[1,2,3],"b":["a","b","c"]}'], lines(stdout), "group in hash output")

stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> {a: group(_["x"]), b: group(_["y"])}', input_group_multi)
assert_success(status, stderr, "group in hash no matches")
assert_equal(['{"a":[],"b":[]}'], lines(stdout), "group in hash no-match output")

stdout, stderr, status = run_jrf('percentile(_["foo"], 0.50)', input_sum)
assert_success(status, stderr, "single percentile")
assert_equal(%w[2], lines(stdout), "single percentile output")

stdout, stderr, status = run_jrf('percentile(_["foo"], [0.25, 0.50, 1.0])', input_sum)
assert_success(status, stderr, "array percentile")
assert_equal(
  ['{"percentile":0.25,"value":1}', '{"percentile":0.5,"value":2}', '{"percentile":1.0,"value":4}'],
  lines(stdout),
  "array percentile output"
)

input_reduce = <<~NDJSON
  {"s":"hello"}
  {"s":"world"}
  {"s":"jrf"}
NDJSON

stdout, stderr, status = run_jrf('_["s"] >> reduce("") { |acc, v| acc.empty? ? v : "#{acc} #{v}" }', input_reduce)
assert_success(status, stderr, "reduce with implicit value")
assert_equal(['"hello world jrf"'], lines(stdout), "reduce implicit value output")

stdout, stderr, status = run_jrf('_["s"] >> reduce("") { |acc, v| acc.empty? ? v : "#{acc} #{v}" }', input_reduce)
assert_success(status, stderr, "reduce in two-stage form")
assert_equal(['"hello world jrf"'], lines(stdout), "reduce in two-stage form output")

stdout, stderr, status = run_jrf('sum(_["foo"]) >> select(_ > 100)', input_sum)
assert_success(status, stderr, "post-reduce select drop")
assert_equal([], lines(stdout), "post-reduce select drop output")

stdout, stderr, status = run_jrf('select(_["x"] > ) >> _["foo"]', "")
assert_failure(status, "syntax error should fail before row loop")
assert_includes(stderr, "syntax error")

stdout, stderr, status = run_jrf('([)] >> _', "")
assert_failure(status, "mismatched delimiter should fail")
assert_includes(stderr, "mismatched delimiter")

stdout, stderr, status = run_jrf('(_["x"] >> _["y"]', "")
assert_failure(status, "unclosed delimiter should fail")
assert_includes(stderr, "unclosed delimiter")

input_broken_tail = <<~NDJSON
  {"foo":1}
  {"foo":2}
  {"foo":
NDJSON

stdout, stderr, status = run_jrf('sum(_["foo"])', input_broken_tail)
assert_failure(status, "broken input should fail")
assert_equal(%w[3], lines(stdout), "reducers flush before parse error")
assert_includes(stderr, "JSON::ParserError")

input_chain = <<~NDJSON
  {"foo":{"bar":{"z":1},"keep":true}}
  {"foo":{"bar":{"z":2},"keep":false}}
  {"foo":{"bar":{"z":3},"keep":true}}
NDJSON

stdout, stderr, status = run_jrf('_["foo"] >> select(_["keep"]) >> _["bar"] >> select(_["z"] > 1) >> _["z"]', input_chain)
assert_success(status, stderr, "select/extract chain")
assert_equal(%w[3], lines(stdout), "chain output")

puts "ok"
