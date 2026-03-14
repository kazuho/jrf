# frozen_string_literal: true

require_relative "test_helper"

class CliRunnerTest < JrfTestCase
  def test_extract_and_select
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
  end

  def test_help_version_and_atomic_write_options
    input_hello = <<~NDJSON
      {"hello":123}
      {"hello":456}
    NDJSON

    stdout, stderr, status = Open3.capture3("./exe/jrf", "--help")
    assert_success(status, stderr, "help option")
    assert_includes(stdout, "usage: jrf [options] 'STAGE >> STAGE >> ...'")
    assert_includes(stdout, "JSON filter with the power and speed of Ruby.")
    assert_includes(stdout, "--lax")
    assert_includes(stdout, "--output")
    assert_includes(stdout, "--require LIBRARY")
    assert_includes(stdout, "--no-jit")
    assert_includes(stdout, "-V")
    assert_includes(stdout, "--version")
    assert_includes(stdout, "--atomic-write-bytes N")
    assert_includes(stdout, "Pipeline:")
    assert_includes(stdout, "Connect stages with top-level >>.")
    assert_includes(stdout, "The current value in each stage is available as _.")
    assert_includes(stdout, "See Also:")
    assert_includes(stdout, "https://github.com/kazuho/jrf#readme")
    assert_equal([], lines(stderr), "help stderr output")

    stdout, stderr, status = Open3.capture3("./exe/jrf", "--version")
    assert_success(status, stderr, "version long option")
    assert_equal([Jrf::VERSION], lines(stdout), "version long option output")
    assert_equal([], lines(stderr), "version long option stderr")

    stdout, stderr, status = Open3.capture3("./exe/jrf", "-V")
    assert_success(status, stderr, "version short option")
    assert_equal([Jrf::VERSION], lines(stdout), "version short option output")
    assert_equal([], lines(stderr), "version short option stderr")

    stdout, stderr, status = run_jrf('select(_["hello"] == 123) >> _["hello"]', input_hello, "--verbose")
    assert_success(status, stderr, "dump stages verbose alias")
    assert_equal(%w[123], lines(stdout), "dump stages verbose alias output")
    assert_includes(stderr, 'stage[0]: select(_["hello"] == 123)')

    stdout, stderr, status = run_jrf('_["hello"]', input_hello, "--atomic-write-bytes", "512")
    assert_success(status, stderr, "atomic write bytes option")
    assert_equal(%w[123 456], lines(stdout), "atomic write bytes option output")

    stdout, stderr, status = run_jrf('_["hello"]', input_hello, "--atomic-write-bytes=512")
    assert_success(status, stderr, "atomic write bytes equals form")
    assert_equal(%w[123 456], lines(stdout), "atomic write bytes equals form output")

    stdout, stderr, status = Open3.capture3("./exe/jrf", "--atomic-write-bytes", "0", '_["hello"]', stdin_data: input_hello)
    assert_failure(status, "atomic write bytes rejects zero")
    assert_includes(stderr, "--atomic-write-bytes requires a positive integer")
  end

  def test_runner_buffering_and_require_option
    threshold_input = StringIO.new((1..4).map { |i| "{\"foo\":\"#{'x' * 1020}\",\"i\":#{i}}\n" }.join)
    buffered_runner = RecordingRunner.new(input: threshold_input, out: StringIO.new, err: StringIO.new)
    buffered_runner.run('_')
    expected_line = JSON.generate({"foo" => "x" * 1020, "i" => 1}) + "\n"
    assert_equal(2, buffered_runner.writes.length, "default atomic write limit buffers records until the configured threshold")
    assert_equal(expected_line.bytesize * 3, buffered_runner.writes.first.bytesize, "default atomic write limit flushes before the next record would exceed the threshold")
    assert_equal(expected_line.bytesize, buffered_runner.writes.last.bytesize, "final buffer flush emits the remaining record")

    small_limit_runner = RecordingRunner.new(input: StringIO.new("{\"foo\":1}\n{\"foo\":2}\n"), out: StringIO.new, err: StringIO.new, atomic_write_bytes: 1)
    small_limit_runner.run('_["foo"]')
    assert_equal(["1\n", "2\n"], small_limit_runner.writes, "small atomic write limit emits oversized records directly")

    err_io = StringIO.new
    error_runner = RecordingRunner.new(input: StringIO.new("{\"foo\":1}\n{\"foo\":"), out: StringIO.new, err: err_io)
    error_runner.run('_["foo"]')
    assert_equal(["1\n"], error_runner.writes, "buffer flushes pending output before parse errors")
    assert_includes(err_io.string, "JSON::ParserError", "parse error reported to stderr")
    assert(error_runner.input_errors?, "input_errors? is true after parse error")

    input_hello = <<~NDJSON
      {"hello":123}
      {"hello":456}
    NDJSON

    Dir.mktmpdir do |dir|
      helper = File.join(dir, "helpers.rb")
      File.write(helper, <<~RUBY)
        def double(value)
          value * 2
        end
      RUBY

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-r", helper, 'double(_["hello"])', stdin_data: input_hello)
      assert_success(status, stderr, "require helper option")
      assert_equal(%w[246 912], lines(stdout), "require helper option output")
    end
  end

  def test_yjit_option
    if defined?(RubyVM::YJIT) && RubyVM::YJIT.respond_to?(:enabled?)
      yjit_probe = "{\"probe\":1}\n"

      stdout, stderr, status = run_jrf('RubyVM::YJIT.enabled?', yjit_probe)
      assert_success(status, stderr, "default jit enablement")
      assert_equal(%w[true], lines(stdout), "default jit enablement output")

      stdout, stderr, status = run_jrf('RubyVM::YJIT.enabled?', yjit_probe, "--no-jit")
      assert_success(status, stderr, "no-jit option")
      assert_equal(%w[false], lines(stdout), "no-jit option output")
    end
  end

  def test_compressed_inputs
    Dir.mktmpdir do |dir|
      gz_path = File.join(dir, "input.ndjson.gz")
      Zlib::GzipWriter.open(gz_path) do |io|
        io.write("{\"foo\":10}\n{\"foo\":20}\n")
      end

      stdout, stderr, status = Open3.capture3("./exe/jrf", '_["foo"]', gz_path)
      assert_success(status, stderr, "compressed input by suffix")
      assert_equal(%w[10 20], lines(stdout), "compressed input output")

      lax_gz_path = File.join(dir, "input-lax.json.gz")
      Zlib::GzipWriter.open(lax_gz_path) do |io|
        io.write("{\"foo\":30}\n\x1e{\"foo\":40}\n")
      end

      stdout, stderr, status = Open3.capture3("./exe/jrf", "--lax", '_["foo"]', lax_gz_path)
      assert_success(status, stderr, "compressed lax input by suffix")
      assert_equal(%w[30 40], lines(stdout), "compressed lax input output")

      second_gz_path = File.join(dir, "input2.ndjson.gz")
      Zlib::GzipWriter.open(second_gz_path) do |io|
        io.write("{\"foo\":50}\n")
      end

      stdout, stderr, status = Open3.capture3("./exe/jrf", '_["foo"]', gz_path, second_gz_path)
      assert_success(status, stderr, "multiple compressed inputs by suffix")
      assert_equal(%w[10 20 50], lines(stdout), "multiple compressed input output")
    end
  end

  def test_output_formats
    input_hello = <<~NDJSON
      {"hello":123}
      {"hello":456}
    NDJSON

    stdout, stderr, status = run_jrf('_', input_hello, "-o", "pretty")
    assert_success(status, stderr, "pretty output")
    assert_equal(
      [
        "{",
        "\"hello\": 123",
        "}",
        "{",
        "\"hello\": 456",
        "}"
      ],
      lines(stdout),
      "pretty output lines"
    )

    input_table_hash = '{"a":[1,2],"b":[3,4]}'
    stdout, stderr, status = run_jrf('_', input_table_hash, "-o", "tsv")
    assert_success(status, stderr, "tsv output hash of arrays")
    assert_equal(["a\t1\t2", "b\t3\t4"], lines(stdout), "tsv output hash of arrays")

    input_table_array = '[[1,"hello",true],[2,"world",false]]'
    stdout, stderr, status = run_jrf('_', input_table_array, "-o", "tsv")
    assert_success(status, stderr, "tsv output array of arrays")
    assert_equal(["1\thello\ttrue", "2\tworld\tfalse"], lines(stdout), "tsv output array of arrays")

    input_table_scalar = '{"foo":"bar","baz":42}'
    stdout, stderr, status = run_jrf('_', input_table_scalar, "-o", "tsv")
    assert_success(status, stderr, "tsv output hash of scalars")
    assert_equal(["foo\tbar", "baz\t42"], lines(stdout), "tsv output hash of scalars")

    input_table_nested = '{"a":[[1,2],[3,4]],"b":[[5,6],[7,8]]}'
    stdout, stderr, status = run_jrf('_', input_table_nested, "-o", "tsv")
    assert_success(status, stderr, "tsv output nested arrays as JSON")
    assert_equal(["a\t[1,2]\t[3,4]", "b\t[5,6]\t[7,8]"], lines(stdout), "tsv output nested arrays as JSON")
  end

  def test_regex_and_parser_boundaries
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
  end

  def test_flat
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

    stdout, stderr, status = run_jrf('map { |x| flat }', "[[1,2],[3],[4,5,6]]\n")
    assert_success(status, stderr, "flat inside map")
    assert_equal(['[1,2,3,4,5,6]'], lines(stdout), "flat inside map output")

    stdout, stderr, status = run_jrf('map_values { |v| flat }', "{\"a\":[1,2],\"b\":[3]}\n")
    assert_failure(status, "flat inside map_values")
    assert_includes(stderr, "flat is not supported inside map_values")

    stdout, stderr, status = run_jrf('_["foo"] >> flat', "{\"foo\":1}\n")
    assert_failure(status, "flat requires array")
    assert_includes(stderr, "flat expects Array")
  end

  def test_reducers
    input = <<~NDJSON
      {"foo":1,"x":5}
      {"foo":2,"x":11}
      {"foo":3,"x":50}
      {"foo":4,"x":70}
    NDJSON

    stdout, stderr, status = run_jrf('sum(_["foo"])', input)
    assert_success(status, stderr, "sum only")
    assert_equal(%w[10], lines(stdout), "sum output")

    stdout, stderr, status = run_jrf('count()', input)
    assert_success(status, stderr, "count only")
    assert_equal(%w[4], lines(stdout), "count output")

    stdout, stderr, status = run_jrf('count(_["foo"])', input)
    assert_success(status, stderr, "count(expr) only")
    assert_equal(%w[4], lines(stdout), "count(expr) output")

    stdout, stderr, status = run_jrf('min(_["foo"])', input)
    assert_success(status, stderr, "min only")
    assert_equal(%w[1], lines(stdout), "min output")

    stdout, stderr, status = run_jrf('max(_["foo"])', input)
    assert_success(status, stderr, "max only")
    assert_equal(%w[4], lines(stdout), "max output")

    stdout, stderr, status = run_jrf('select(_["x"] > 10) >> sum(_["foo"])', input)
    assert_success(status, stderr, "select + sum")
    assert_equal(%w[9], lines(stdout), "select + sum output")

    stdout, stderr, status = run_jrf('{total: sum(_["foo"]), n: count()}', input)
    assert_success(status, stderr, "structured reducer result")
    assert_equal(['{"total":10,"n":4}'], lines(stdout), "structured reducer result output")

    stdout, stderr, status = run_jrf('average(_["foo"])', input)
    assert_success(status, stderr, "average")
    assert_float_close(2.5, lines(stdout).first.to_f, 1e-12, "average output")

    stdout, stderr, status = run_jrf('stdev(_["foo"])', input)
    assert_success(status, stderr, "stdev")
    assert_float_close(1.118033988749895, lines(stdout).first.to_f, 1e-12, "stdev output")

    stdout, stderr, status = run_jrf('_["foo"] >> sum(_ * 2)', input)
    assert_success(status, stderr, "extract + sum")
    assert_equal(%w[20], lines(stdout), "extract + sum output")

    stdout, stderr, status = run_jrf('sum(2 * _["foo"])', input)
    assert_success(status, stderr, "sum with literal on left")
    assert_equal(%w[20], lines(stdout), "sum with literal on left output")

    stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> sum(_["foo"])', input)
    assert_success(status, stderr, "sum no matches")
    assert_equal([], lines(stdout), "sum no matches output")

    stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> count()', input)
    assert_success(status, stderr, "count no matches")
    assert_equal([], lines(stdout), "count no matches output")

    stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> count(_["foo"])', input)
    assert_success(status, stderr, "count(expr) no matches")
    assert_equal([], lines(stdout), "count(expr) no matches output")

    stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> average(_["foo"])', input)
    assert_success(status, stderr, "average no matches")
    assert_equal([], lines(stdout), "average no matches output")

    stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> stdev(_["foo"])', input)
    assert_success(status, stderr, "stdev no matches")
    assert_equal([], lines(stdout), "stdev no matches output")

    stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> min(_["foo"])', input)
    assert_success(status, stderr, "min no matches")
    assert_equal([], lines(stdout), "min no matches output")

    stdout, stderr, status = run_jrf('select(_["x"] > 1000) >> max(_["foo"])', input)
    assert_success(status, stderr, "max no matches")
    assert_equal([], lines(stdout), "max no matches output")

    stdout, stderr, status = run_jrf('sum(_["foo"]) >> _ + 1', input)
    assert_success(status, stderr, "reduce in middle")
    assert_equal(%w[11], lines(stdout), "reduce in middle output")

    stdout, stderr, status = run_jrf('select(_["x"] > 10) >> _["foo"] >> sum(_ * 2) >> select(_ > 10) >> _ + 1', input)
    assert_success(status, stderr, "reduce mixed with select/extract")
    assert_equal(%w[19], lines(stdout), "reduce mixed output")

    stdout, stderr, status = run_jrf('_["foo"] >> sum(_) >> _ * 10 >> sum(_)', input)
    assert_success(status, stderr, "multiple reducers")
    assert_equal(%w[100], lines(stdout), "multiple reducers output")

    stdout, stderr, status = run_jrf('_["foo"] >> min(_) >> _ * 10 >> max(_)', input)
    assert_success(status, stderr, "min/max mixed reducers")
    assert_equal(%w[10], lines(stdout), "min/max mixed reducers output")
  end

  def test_sort
    input_sum = <<~NDJSON
      {"foo":1,"x":5}
      {"foo":2,"x":11}
      {"foo":3,"x":50}
      {"foo":4,"x":70}
    NDJSON

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
    assert_equal([], lines(stdout), "group no matches output")
  end

  def test_group
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
    assert_equal([], lines(stdout), "group in hash no-match output")
  end

  def test_percentile
    input_sum = <<~NDJSON
      {"foo":1,"x":5}
      {"foo":2,"x":11}
      {"foo":3,"x":50}
      {"foo":4,"x":70}
    NDJSON

    stdout, stderr, status = run_jrf('percentile(_["foo"], 0.50)', input_sum)
    assert_success(status, stderr, "single percentile")
    assert_equal(%w[2], lines(stdout), "single percentile output")

    stdout, stderr, status = run_jrf('percentile(_["foo"], [0.25, 0.50, 1.0])', input_sum)
    assert_success(status, stderr, "array percentile")
    assert_equal(['[1,2,4]'], lines(stdout), "array percentile output")

    stdout, stderr, status = run_jrf('percentile(_["foo"], 0.25.step(1.0, 0.25))', input_sum)
    assert_success(status, stderr, "enumerable percentile")
    assert_equal(['[1,2,3,4]'], lines(stdout), "enumerable percentile output")
  end

  def test_nil_handling_for_aggregates
    input_with_nil = <<~NDJSON
      {"foo":1}
      {"foo":null}
      {"bar":999}
      {"foo":3}
    NDJSON

    stdout, stderr, status = run_jrf('sum(_["foo"])', input_with_nil)
    assert_success(status, stderr, "sum ignores nil")
    assert_equal(%w[4], lines(stdout), "sum ignores nil output")

    stdout, stderr, status = run_jrf('min(_["foo"])', input_with_nil)
    assert_success(status, stderr, "min ignores nil")
    assert_equal(%w[1], lines(stdout), "min ignores nil output")

    stdout, stderr, status = run_jrf('max(_["foo"])', input_with_nil)
    assert_success(status, stderr, "max ignores nil")
    assert_equal(%w[3], lines(stdout), "max ignores nil output")

    stdout, stderr, status = run_jrf('average(_["foo"])', input_with_nil)
    assert_success(status, stderr, "average ignores nil")
    assert_float_close(2.0, lines(stdout).first.to_f, 1e-12, "average ignores nil output")

    stdout, stderr, status = run_jrf('stdev(_["foo"])', input_with_nil)
    assert_success(status, stderr, "stdev ignores nil")
    assert_float_close(1.0, lines(stdout).first.to_f, 1e-12, "stdev ignores nil output")

    stdout, stderr, status = run_jrf('percentile(_["foo"], [0.5, 1.0])', input_with_nil)
    assert_success(status, stderr, "percentile ignores nil")
    assert_equal(['[1,3]'], lines(stdout), "percentile ignores nil output")

    stdout, stderr, status = run_jrf('count()', input_with_nil)
    assert_success(status, stderr, "count with nil rows")
    assert_equal(%w[4], lines(stdout), "count with nil rows output")

    stdout, stderr, status = run_jrf('count(_["foo"])', input_with_nil)
    assert_success(status, stderr, "count(expr) ignores nil")
    assert_equal(%w[2], lines(stdout), "count(expr) ignores nil output")

    input_count_if = <<~NDJSON
      {"x":1}
      {"x":-2}
      {"x":3}
      {"x":-4}
      {"x":5}
    NDJSON

    stdout, stderr, status = run_jrf('count_if(_["x"] > 0)', input_count_if)
    assert_success(status, stderr, "count_if")
    assert_equal(%w[3], lines(stdout), "count_if output")

    stdout, stderr, status = run_jrf('[count_if(_["x"] > 0), count_if(_["x"] < 0)]', input_count_if)
    assert_success(status, stderr, "count_if multiple")
    assert_equal(["[3,2]"], lines(stdout), "count_if multiple output")

    input_all_nil = <<~NDJSON
      {"foo":null}
      {"bar":1}
    NDJSON

    stdout, stderr, status = run_jrf('sum(_["foo"])', input_all_nil)
    assert_success(status, stderr, "sum all nil")
    assert_equal(%w[0], lines(stdout), "sum all nil output")

    stdout, stderr, status = run_jrf('min(_["foo"])', input_all_nil)
    assert_success(status, stderr, "min all nil")
    assert_equal(%w[null], lines(stdout), "min all nil output")

    stdout, stderr, status = run_jrf('max(_["foo"])', input_all_nil)
    assert_success(status, stderr, "max all nil")
    assert_equal(%w[null], lines(stdout), "max all nil output")

    stdout, stderr, status = run_jrf('average(_["foo"])', input_all_nil)
    assert_success(status, stderr, "average all nil")
    assert_equal(%w[null], lines(stdout), "average all nil output")

    stdout, stderr, status = run_jrf('stdev(_["foo"])', input_all_nil)
    assert_success(status, stderr, "stdev all nil")
    assert_equal(%w[null], lines(stdout), "stdev all nil output")

    stdout, stderr, status = run_jrf('percentile(_["foo"], 0.5)', input_all_nil)
    assert_success(status, stderr, "percentile all nil")
    assert_equal(%w[null], lines(stdout), "percentile all nil output")

    stdout, stderr, status = run_jrf('count(_["foo"])', input_all_nil)
    assert_success(status, stderr, "count(expr) all nil")
    assert_equal(%w[0], lines(stdout), "count(expr) all nil output")
  end

  def test_reduce
    input_multi_cols = <<~NDJSON
      {"a":1,"b":10}
      {"a":2,"b":20}
      {"a":3,"b":30}
      {"a":4,"b":40}
    NDJSON

    stdout, stderr, status = run_jrf('{a: percentile(_["a"], [0.25, 0.50, 1.0]), b: percentile(_["b"], [0.25, 0.50, 1.0])}', input_multi_cols)
    assert_success(status, stderr, "nested array percentile for multiple columns")
    assert_equal(['{"a":[1,2,4],"b":[10,20,40]}'], lines(stdout), "nested array percentile output")

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

    input_sum = <<~NDJSON
      {"foo":1,"x":5}
      {"foo":2,"x":11}
      {"foo":3,"x":50}
      {"foo":4,"x":70}
    NDJSON

    stdout, stderr, status = run_jrf('sum(_["foo"]) >> select(_ > 100)', input_sum)
    assert_success(status, stderr, "post-reduce select drop")
    assert_equal([], lines(stdout), "post-reduce select drop output")
  end

  def test_lax_input_mode
    input_whitespace_stream = "{\"foo\":1} {\"foo\":2}\n\t{\"foo\":3}\n"
    stdout, stderr, status = run_jrf('_["foo"]', input_whitespace_stream)
    assert_failure(status, "default NDJSON should reject same-line multi-values")
    assert_includes(stderr, "JSON::ParserError")

    stdout, stderr, status = run_jrf('_["foo"]', input_whitespace_stream, "--lax")
    assert_success(status, stderr, "whitespace-separated JSON stream with --lax")
    assert_equal(%w[1 2 3], lines(stdout), "whitespace-separated stream output")

    input_json_seq = "\x1e{\"foo\":10}\n\x1e{\"foo\":20}\n"
    stdout, stderr, status = run_jrf('_["foo"]', input_json_seq)
    assert_failure(status, "RS framing requires --lax")
    assert_includes(stderr, "JSON::ParserError")

    stdout, stderr, status = run_jrf('_["foo"]', input_json_seq, "--lax")
    assert_success(status, stderr, "json-seq style RS framing with --lax")
    assert_equal(%w[10 20], lines(stdout), "json-seq style output")

    input_lax_multiline = <<~JSONS
      {
        "foo": 101,
        "bar": {"x": 1}
      }
      {
        "foo": 202,
        "bar": {"x": 2}
      }
    JSONS
    stdout, stderr, status = run_jrf('_["foo"]', input_lax_multiline)
    assert_failure(status, "default NDJSON rejects multiline objects")
    assert_includes(stderr, "JSON::ParserError")

    stdout, stderr, status = run_jrf('_["bar"]["x"]', input_lax_multiline, "--lax")
    assert_success(status, stderr, "lax accepts multiline objects")
    assert_equal(%w[1 2], lines(stdout), "lax multiline object output")

    input_lax_mixed_separators = "{\"foo\":1}\n\x1e{\"foo\":2}\t{\"foo\":3}\n"
    stdout, stderr, status = run_jrf('_["foo"]', input_lax_mixed_separators, "--lax")
    assert_success(status, stderr, "lax accepts mixed whitespace and RS separators")
    assert_equal(%w[1 2 3], lines(stdout), "lax mixed separators output")

    input_lax_with_escaped_newline = "{\"s\":\"line1\\nline2\"}\n{\"s\":\"ok\"}\n"
    stdout, stderr, status = run_jrf('_["s"]', input_lax_with_escaped_newline, "--lax")
    assert_success(status, stderr, "lax handles escaped newlines in strings")
    assert_equal(['"line1\nline2"', '"ok"'], lines(stdout), "lax escaped newline string output")

    input_lax_trailing_rs = "\x1e{\"foo\":9}\n\x1e"
    stdout, stderr, status = run_jrf('_["foo"]', input_lax_trailing_rs, "--lax")
    assert_success(status, stderr, "lax ignores trailing separator")
    assert_equal(%w[9], lines(stdout), "lax trailing separator output")

    chunked_lax_out = RecordingRunner.new(
      input: ChunkedSource.new("{\"foo\":1}\n\x1e{\"foo\":2}\n\t{\"foo\":3}\n"),
      out: StringIO.new,
      err: StringIO.new,
      lax: true
    )
    chunked_lax_out.run('_["foo"]')
    assert_equal(%w[1 2 3], lines(chunked_lax_out.writes.join), "lax mode streams chunked input without whole-input reads")

    Dir.mktmpdir do |dir|
      one = File.join(dir, "one.json")
      two = File.join(dir, "two.json")
      File.write(one, "1")
      File.write(two, "2")

      stdout, stderr, status = Open3.capture3("./exe/jrf", "--lax", "_", one, two)
      assert_success(status, stderr, "lax keeps file boundaries")
      assert_equal(%w[1 2], lines(stdout), "lax does not merge JSON across file boundaries")
    end
  end

  def test_parse_errors
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
    refute_includes(stderr, "from ", "no stacktrace for parse errors")
  end

  def test_map
    input_chain = <<~NDJSON
      {"foo":{"bar":{"z":1},"keep":true}}
      {"foo":{"bar":{"z":2},"keep":false}}
      {"foo":{"bar":{"z":3},"keep":true}}
    NDJSON

    stdout, stderr, status = run_jrf('_["foo"] >> select(_["keep"]) >> _["bar"] >> select(_["z"] > 1) >> _["z"]', input_chain)
    assert_success(status, stderr, "select/extract chain")
    assert_equal(%w[3], lines(stdout), "chain output")

    input_map = <<~NDJSON
      {"values":[1,10,100]}
      {"values":[2,20,200]}
      {"values":[3,30,300]}
    NDJSON

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| sum(x) }', input_map)
    assert_success(status, stderr, "map with sum")
    assert_equal(['[6,60,600]'], lines(stdout), "map with sum output")

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| min(x) }', input_map)
    assert_success(status, stderr, "map with min")
    assert_equal(['[1,10,100]'], lines(stdout), "map with min output")

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| max(x) }', input_map)
    assert_success(status, stderr, "map with max")
    assert_equal(['[3,30,300]'], lines(stdout), "map with max output")

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| sum(_[0] + x) }', input_map)
    assert_success(status, stderr, "map keeps ambient _")
    assert_equal(['[12,66,606]'], lines(stdout), "map ambient _ output")

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| reduce(0) { |acc, v| acc + v } }', input_map)
    assert_success(status, stderr, "map with reduce")
    assert_equal(['[6,60,600]'], lines(stdout), "map with reduce output")

    input_map_varying = <<~NDJSON
      [1,10]
      [2,20,200]
      [3]
    NDJSON

    stdout, stderr, status = run_jrf('map { |x| sum(x) }', input_map_varying)
    assert_success(status, stderr, "map varying lengths")
    assert_equal(['[6,30,200]'], lines(stdout), "map varying lengths output")

    input_map_unsorted = <<~NDJSON
      {"values":[3,30]}
      {"values":[1,10]}
      {"values":[2,20]}
    NDJSON

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| group }', input_map)
    assert_success(status, stderr, "map with group")
    assert_equal(['[[1,2,3],[10,20,30],[100,200,300]]'], lines(stdout), "map with group output")

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| sort }', input_map_unsorted)
    assert_success(status, stderr, "map with sort default key")
    assert_equal(['[[1,2,3],[10,20,30]]'], lines(stdout), "map with sort default key output")

    stdout, stderr, status = run_jrf('select(false) >> map { |x| sum(x) }', input_map)
    assert_success(status, stderr, "map no matches")
    assert_equal([], lines(stdout), "map no matches output")

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| x + 1 }', input_map)
    assert_success(status, stderr, "map transform")
    assert_equal(['[2,11,101]', '[3,21,201]', '[4,31,301]'], lines(stdout), "map transform output")

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| select(x >= 20) }', input_map)
    assert_success(status, stderr, "map transform with select")
    assert_equal(['[100]', '[20,200]', '[30,300]'], lines(stdout), "map transform with select output")
  end

  def test_map_values
    input_map_values = <<~NDJSON
      {"a":1,"b":10}
      {"a":2,"b":20}
      {"a":3,"b":30}
    NDJSON

    stdout, stderr, status = run_jrf('map_values { |v| sum(v) }', input_map_values)
    assert_success(status, stderr, "map_values with sum")
    assert_equal(['{"a":6,"b":60}'], lines(stdout), "map_values with sum output")

    stdout, stderr, status = run_jrf('map_values { |v| min(v) }', input_map_values)
    assert_success(status, stderr, "map_values with min")
    assert_equal(['{"a":1,"b":10}'], lines(stdout), "map_values with min output")

    input_map_values_varying = <<~NDJSON
      {"a":1}
      {"a":2,"b":20}
      {"a":3,"b":30}
    NDJSON

    stdout, stderr, status = run_jrf('map_values { |v| sum(v) }', input_map_values_varying)
    assert_success(status, stderr, "map_values varying keys")
    assert_equal(['{"a":6,"b":50}'], lines(stdout), "map_values varying keys output")

    stdout, stderr, status = run_jrf('map_values { |v| count(v) }', input_map_values)
    assert_success(status, stderr, "map_values with count")
    assert_equal(['{"a":3,"b":3}'], lines(stdout), "map_values with count output")

    stdout, stderr, status = run_jrf('map_values { |v| group }', input_map_values)
    assert_success(status, stderr, "map_values with group")
    assert_equal(['{"a":[1,2,3],"b":[10,20,30]}'], lines(stdout), "map_values with group output")

    stdout, stderr, status = run_jrf('map_values { |v| sum(_["a"] + v) }', input_map_values)
    assert_success(status, stderr, "map_values keeps ambient _")
    assert_equal(['{"a":12,"b":66}'], lines(stdout), "map_values ambient _ output")

    stdout, stderr, status = run_jrf('map_values { |v| reduce(0) { |acc, x| acc + x } }', input_map_values)
    assert_success(status, stderr, "map_values with reduce")
    assert_equal(['{"a":6,"b":60}'], lines(stdout), "map_values with reduce output")

    stdout, stderr, status = run_jrf('map { |k, v| "#{k}:#{v}" }', input_map_values)
    assert_success(status, stderr, "map over hash transform")
    assert_equal(['["a:1","b:10"]', '["a:2","b:20"]', '["a:3","b:30"]'], lines(stdout), "map over hash transform output")

    stdout, stderr, status = run_jrf('map { |pair| pair }', input_map_values)
    assert_success(status, stderr, "map over hash single block arg")
    assert_equal(['[["a",1],["b",10]]', '[["a",2],["b",20]]', '[["a",3],["b",30]]'], lines(stdout), "map over hash single block arg output")

    stdout, stderr, status = run_jrf('map { |k, v| select(v >= 10 && k != "a") }', input_map_values)
    assert_success(status, stderr, "map over hash transform with select")
    assert_equal(['[10]', '[20]', '[30]'], lines(stdout), "map over hash transform with select output")

    stdout, stderr, status = run_jrf('map { |k, v| sum(v + k.length) }', input_map_values)
    assert_success(status, stderr, "map over hash with sum")
    assert_equal(['[9,63]'], lines(stdout), "map over hash with sum output")

    stdout, stderr, status = run_jrf('map { |k, v| sum(_["a"] + v + k.length) }', input_map_values)
    assert_success(status, stderr, "map over hash keeps ambient _")
    assert_equal(['[15,69]'], lines(stdout), "map over hash ambient _ output")

    stdout, stderr, status = run_jrf('select(false) >> map_values { |v| sum(v) }', input_map_values)
    assert_success(status, stderr, "map_values no matches")
    assert_equal([], lines(stdout), "map_values no matches output")

    stdout, stderr, status = run_jrf('map_values { |v| sum(v) } >> map_values { |v| v * 10 }', input_map_values)
    assert_success(status, stderr, "map_values piped to map_values passthrough")
    assert_equal(['{"a":60,"b":600}'], lines(stdout), "map_values piped output")

    stdout, stderr, status = run_jrf('map_values { |v| v * 2 }', input_map_values)
    assert_success(status, stderr, "map_values transform")
    assert_equal(['{"a":2,"b":20}', '{"a":4,"b":40}', '{"a":6,"b":60}'], lines(stdout), "map_values transform output")

    stdout, stderr, status = run_jrf('map_values { |v| select(v >= 10) }', input_map_values)
    assert_success(status, stderr, "map_values transform with select")
    assert_equal(['{"b":10}', '{"b":20}', '{"b":30}'], lines(stdout), "map_values transform with select output")
  end

  def test_apply
    input_map = <<~NDJSON
      {"values":[1,10,100]}
      {"values":[2,20,200]}
      {"values":[3,30,300]}
    NDJSON

    stdout, stderr, status = run_jrf('_["values"] >> map { |x| x + 1 } >> map { |x| x * 10 }', input_map)
    assert_success(status, stderr, "chained map transforms")
    assert_equal(['[20,110,1010]', '[30,210,2010]', '[40,310,3010]'], lines(stdout), "chained map transforms output")

    stdout, stderr, status = run_jrf('map { map { |y| [ sum(y[0]), sum(y[1]) ] } }', "[[[1,2]]]\n[[[3,4]]]\n")
    assert_success(status, stderr, "nested map reducer binds to current target")
    assert_equal(['[[[4,6]]]'], lines(stdout), "nested map reducer output")

    stdout, stderr, status = run_jrf('map_values { |obj| map_values { |v| sum(v) } }', "{\"a\":{\"x\":1,\"y\":2},\"b\":{\"x\":10,\"y\":20}}\n{\"a\":{\"x\":3,\"y\":4},\"b\":{\"x\":30,\"y\":40}}\n")
    assert_success(status, stderr, "nested map_values reducer binds to current target")
    assert_equal(['{"a":{"x":4,"y":6},"b":{"x":40,"y":60}}'], lines(stdout), "nested map_values reducer output")

    stdout, stderr, status = run_jrf('[apply { |x| sum(x["foo"]) }, _.length]', '[{"foo":1},{"foo":2}]' + "\n" + '[{"foo":10}]' + "\n")
    assert_success(status, stderr, "apply with sum")
    assert_equal(["[3,2]", "[10,1]"], lines(stdout), "apply with sum output")

    stdout, stderr, status = run_jrf('apply { |x| x["foo"] }', '[{"foo":1},{"foo":2}]' + "\n")
    assert_success(status, stderr, "apply passthrough")
    assert_equal(["[1,2]"], lines(stdout), "apply passthrough output")

    stdout, stderr, status = run_jrf('apply { |x| percentile(x, 0.5) }', '[10,20,30]' + "\n")
    assert_success(status, stderr, "apply with percentile")
    assert_equal(["20"], lines(stdout), "apply with percentile output")

    stdout, stderr, status = run_jrf('map { |o| [apply(o["vals"]) { |x| sum(x) }, o["name"]] }', '[{"name":"a","vals":[1,2]},{"name":"b","vals":[10,20]}]' + "\n")
    assert_success(status, stderr, "apply with explicit collection")
    assert_equal(['[[3,"a"],[30,"b"]]'], lines(stdout), "apply with explicit collection output")

    stdout, stderr, status = run_jrf('map(_["items"]) { |x| x * 2 }', '{"items":[1,2,3]}' + "\n")
    assert_success(status, stderr, "map with explicit collection")
    assert_equal(["[2,4,6]"], lines(stdout), "map with explicit collection output")

    stdout, stderr, status = run_jrf('map_values(_["data"]) { |v| v * 10 }', '{"data":{"a":1,"b":2}}' + "\n")
    assert_success(status, stderr, "map_values with explicit collection")
    assert_equal(['{"a":10,"b":20}'], lines(stdout), "map_values with explicit collection output")
  end

  def test_group_by
    input_gb = <<~NDJSON
      {"status":200,"path":"/a","latency":10}
      {"status":404,"path":"/b","latency":50}
      {"status":200,"path":"/c","latency":30}
      {"status":200,"path":"/d","latency":20}
    NDJSON

    stdout, stderr, status = run_jrf('group_by(_["status"]) { count() }', input_gb)
    assert_success(status, stderr, "group_by with count")
    assert_equal(['{"200":3,"404":1}'], lines(stdout), "group_by with count output")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { |row| sum(row["latency"]) }', input_gb)
    assert_success(status, stderr, "group_by with sum")
    assert_equal(['{"200":60,"404":50}'], lines(stdout), "group_by with sum output")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { |row| average(row["latency"]) }', input_gb)
    assert_success(status, stderr, "group_by with average")
    result = JSON.parse(lines(stdout).first)
    assert_float_close(20.0, result["200"], 1e-12, "group_by average 200")
    assert_float_close(50.0, result["404"], 1e-12, "group_by average 404")

    stdout, stderr, status = run_jrf('group_by(_["status"])', input_gb)
    assert_success(status, stderr, "group_by default (collect rows)")
    result = JSON.parse(lines(stdout).first)
    assert_equal(3, result["200"].length, "group_by default 200 count")
    assert_equal(1, result["404"].length, "group_by default 404 count")
    assert_equal("/a", result["200"][0]["path"], "group_by default first row")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { |row| group(row["path"]) }', input_gb)
    assert_success(status, stderr, "group_by with group(expr)")
    assert_equal(['{"200":["/a","/c","/d"],"404":["/b"]}'], lines(stdout), "group_by with group(expr) output")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { group }', input_gb)
    assert_success(status, stderr, "group_by with implicit group")
    result = JSON.parse(lines(stdout).first)
    assert_equal(3, result["200"].length, "group_by implicit group 200 count")
    assert_equal("/a", result["200"][0]["path"], "group_by implicit group first row")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { |row| min(row["latency"]) }', input_gb)
    assert_success(status, stderr, "group_by with min")
    assert_equal(['{"200":10,"404":50}'], lines(stdout), "group_by with min output")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { |row| {total: sum(row["latency"]), n: count()} }', input_gb)
    assert_success(status, stderr, "group_by with multi-reducer")
    assert_equal(['{"200":{"total":60,"n":3},"404":{"total":50,"n":1}}'], lines(stdout), "group_by multi-reducer output")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { reduce(0) { |acc, row| acc + row["latency"] } }', input_gb)
    assert_success(status, stderr, "group_by with reduce")
    assert_equal(['{"200":60,"404":50}'], lines(stdout), "group_by with reduce output")

    stdout, stderr, status = run_jrf('select(false) >> group_by(_["status"]) { count() }', input_gb)
    assert_success(status, stderr, "group_by no matches")
    assert_equal([], lines(stdout), "group_by no matches output")

    stdout, stderr, status = run_jrf('group_by(_["status"]) { count() } >> _[200]', input_gb)
    assert_success(status, stderr, "group_by then extract")
    assert_equal(%w[3], lines(stdout), "group_by then extract output")
  end
end
