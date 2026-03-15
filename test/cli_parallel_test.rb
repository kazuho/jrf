# frozen_string_literal: true

require_relative "test_helper"

class CliParallelTest < JrfTestCase
  def test_parallel_map_only
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}, {"x" => 4}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", '_["x"]', *ndjson_files(dir))
      assert_success(status, stderr, "parallel map only")
      assert_equal([1, 2, 3, 4], lines(stdout).map(&:to_i).sort, "parallel map only output")
      assert_includes(stderr, "parallel: enabled workers=2 files=2 split=1/1", "parallel verbose summary")
    end
  end

  def test_parallel_map_only_pretty_output
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}])
      write_ndjson(dir, "b.ndjson", [{"x" => 2}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", "-o", "pretty", '_["x"]', *ndjson_files(dir))
      assert_success(status, stderr, "parallel pretty map only")
      assert_equal(["1", "2"], stdout.lines.map(&:strip).reject(&:empty?).sort, "parallel pretty map only output")
    end
  end

  def test_parallel_map_only_tsv_output
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"a" => 1, "b" => 2}])
      write_ndjson(dir, "b.ndjson", [{"a" => 3, "b" => 4}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", "-o", "tsv", "_", *ndjson_files(dir))
      assert_success(status, stderr, "parallel tsv map only")
      assert_equal(["a\t1", "a\t3", "b\t2", "b\t4"], lines(stdout).sort, "parallel tsv map only output")
    end
  end

  def test_parallel_map_reduce
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}, {"x" => 4}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", 'sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel map reduce")
      assert_equal(%w[10], lines(stdout), "parallel sum output")
    end
  end

  def test_parallel_split_map_and_reduce
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 10}, {"x" => 20}])
      write_ndjson(dir, "b.ndjson", [{"x" => 30}, {"x" => 40}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'select(_["x"] > 10) >> sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel split map+reduce")
      assert_includes(stderr, "decompose=2/2", "select+sum decomposed")
      assert_equal(%w[90], lines(stdout), "parallel split map+reduce output")
    end
  end

  def test_parallel_group_by
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"k" => "a", "v" => 1}, {"k" => "b", "v" => 2}])
      write_ndjson(dir, "b.ndjson", [{"k" => "a", "v" => 3}, {"k" => "b", "v" => 4}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", 'group_by(_["k"]) { |r| sum(r["v"]) }', *ndjson_files(dir))
      assert_success(status, stderr, "parallel group_by")
      result = JSON.parse(lines(stdout).first)
      assert_equal(4, result["a"], "parallel group_by a")
      assert_equal(6, result["b"], "parallel group_by b")
    end
  end

  def test_parallel_decomposable_reducer
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel decomposable reducer")
      assert_equal(%w[6], lines(stdout), "parallel decomposable reducer output")
      assert_includes(stderr, "parallel: enabled", "parallel enabled for decomposable reducer")
      assert_includes(stderr, "decompose=", "decompose mode indicated")
    end
  end

  def test_parallel_with_gz_files
    Dir.mktmpdir do |dir|
      gz_path_a = File.join(dir, "a.ndjson.gz")
      Zlib::GzipWriter.open(gz_path_a) { |io| io.write("{\"x\":10}\n{\"x\":20}\n") }
      gz_path_b = File.join(dir, "b.ndjson.gz")
      Zlib::GzipWriter.open(gz_path_b) { |io| io.write("{\"x\":30}\n") }

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", 'sum(_["x"])', gz_path_a, gz_path_b)
      assert_success(status, stderr, "parallel with gz")
      assert_equal(%w[60], lines(stdout), "parallel with gz output")
    end
  end

  def test_parallel_matches_serial_output
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", (1..50).map { |i| {"v" => i, "g" => i % 3} })
      write_ndjson(dir, "b.ndjson", (51..100).map { |i| {"v" => i, "g" => i % 3} })

      files = ndjson_files(dir)
      expr = 'group_by(_["g"]) { |r| sum(r["v"]) }'

      serial_stdout, serial_stderr, serial_status = Open3.capture3("./exe/jrf", expr, *files)
      assert_success(serial_status, serial_stderr, "serial baseline")

      parallel_stdout, parallel_stderr, parallel_status = Open3.capture3("./exe/jrf", "-P", "2", expr, *files)
      assert_success(parallel_status, parallel_stderr, "parallel run")

      assert_equal(JSON.parse(serial_stdout), JSON.parse(parallel_stdout), "parallel matches serial")
    end
  end

  def test_parallel_worker_error_handling
    Dir.mktmpdir do |dir|
      good_path = File.join(dir, "a.ndjson")
      File.write(good_path, "{\"x\":1}\n{\"x\":2}\n")

      # Create a truncated gz file (valid header, truncated body)
      bad_gz_path = File.join(dir, "b.ndjson.gz")
      full_gz = StringIO.new
      Zlib::GzipWriter.wrap(full_gz) { |io| io.write("{\"x\":10}\n" * 100) }
      # Write only the first half to simulate truncation
      File.binwrite(bad_gz_path, full_gz.string[0, full_gz.string.bytesize / 2])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", '_["x"]', good_path, bad_gz_path)
      assert_failure(status, "worker error causes non-zero exit")
      assert_includes(stderr, bad_gz_path, "error message includes filename")
      # Good file data should still be present
      output_values = lines(stdout).map(&:to_i)
      assert_includes(output_values, 1, "good file data preserved")
      assert_includes(output_values, 2, "good file data preserved")
    end
  end

  def test_parallel_requires_multiple_files
    # With single file and -P, should still work (falls back to serial)
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", 'sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "single file with -P")
      assert_equal(%w[3], lines(stdout), "single file with -P output")
    end
  end

  def test_parallel_select_then_sum
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 20}, {"x" => 3}])
      write_ndjson(dir, "b.ndjson", [{"x" => 40}, {"x" => 5}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'select(_["x"] > 10) >> sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel select then sum")
      assert_includes(stderr, "decompose=2/2", "select+sum fully decomposed in workers")
      assert_equal(%w[60], lines(stdout), "parallel select then sum output")
    end
  end

  def test_parallel_decomposable_multi_reducer
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}, {"x" => 4}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", '{s: sum(_["x"]), n: count(), mn: min(_["x"]), mx: max(_["x"])}', *ndjson_files(dir))
      assert_success(status, stderr, "parallel multi reducer")
      assert_includes(stderr, "decompose=", "multi reducer decomposed")
      result = JSON.parse(lines(stdout).first)
      assert_equal(10, result["s"], "sum")
      assert_equal(4, result["n"], "count")
      assert_equal(1, result["mn"], "min")
      assert_equal(4, result["mx"], "max")
    end
  end

  def test_parallel_decomposable_average
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 10}, {"x" => 20}])
      write_ndjson(dir, "b.ndjson", [{"x" => 30}, {"x" => 40}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'average(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel average")
      assert_includes(stderr, "decompose=", "average decomposed")
      assert_equal(["25.0"], lines(stdout), "parallel average output")
    end
  end

  def test_parallel_decomposable_group
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'group(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel group")
      assert_includes(stderr, "decompose=", "group decomposed")
      result = JSON.parse(lines(stdout).first)
      assert_equal([1, 2, 3], result.sort, "parallel group output")
    end
  end

  def test_parallel_decomposable_sum_with_initial
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'sum(_["x"], initial: 100)', *ndjson_files(dir))
      assert_success(status, stderr, "sum with numeric initial")
      assert_includes(stderr, "decompose=", "numeric initial decomposes")
      assert_equal(%w[106], lines(stdout), "sum with initial output")
    end
  end

  def test_parallel_sum_with_non_numeric_initial_falls_back
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => "a"}, {"x" => "b"}])
      write_ndjson(dir, "b.ndjson", [{"x" => "c"}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'sum(_["x"], initial: "")', *ndjson_files(dir))
      assert_success(status, stderr, "sum with string initial")
      assert_includes(stderr, "parallel: disabled", "non-numeric initial falls back to serial")
      assert_equal(['"abc"'], lines(stdout), "sum with string initial output")
    end
  end

  def test_sum_with_string_initial
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => "hello "}, {"x" => "world"}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", 'sum(_["x"], initial: "")', *ndjson_files(dir))
      assert_success(status, stderr, "sum with string initial")
      assert_equal(['"hello world"'], lines(stdout), "sum with string initial output")
    end
  end

  def test_parallel_decomposable_reducer_then_passthrough
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}, {"x" => 4}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'sum(_["x"]) >> _ * 2', *ndjson_files(dir))
      assert_success(status, stderr, "parallel decomposable then passthrough")
      assert_includes(stderr, "decompose=", "reducer then passthrough decomposed")
      assert_equal(%w[20], lines(stdout), "parallel decomposable then passthrough output")
    end
  end

  def test_parallel_mixed_decomposable_reducers
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 10}, {"x" => 20}])
      write_ndjson(dir, "b.ndjson", [{"x" => 30}, {"x" => 40}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", '[sum(_["x"]), average(_["x"]), min(_["x"]), max(_["x"]), count()]', *ndjson_files(dir))
      assert_success(status, stderr, "mixed decomposable")
      assert_includes(stderr, "decompose=", "mixed decomposable used decompose")
      result = JSON.parse(lines(stdout).first)
      assert_equal([100, 25.0, 10, 40, 4], result, "mixed decomposable output")
    end
  end

  def test_parallel_mixed_decomposable_and_non_decomposable_falls_back
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 10}, {"x" => 20}])
      write_ndjson(dir, "b.ndjson", [{"x" => 30}, {"x" => 40}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", '[sum(_["x"]), percentile(_["x"], 0.5)]', *ndjson_files(dir))
      assert_success(status, stderr, "mixed with non-decomposable")
      assert_includes(stderr, "parallel: disabled", "mixed with non-decomposable falls back to serial")
      result = JSON.parse(lines(stdout).first)
      assert_equal([100, 20], result, "mixed with non-decomposable output")
    end
  end

  def test_parallel_select_sum_passthrough_decomposes
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 20}])
      write_ndjson(dir, "b.ndjson", [{"x" => 40}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'select(_["x"] > 10) >> sum(_["x"]) >> _ * 2', *ndjson_files(dir))
      assert_success(status, stderr, "select+sum+passthrough")
      assert_includes(stderr, "decompose=2/3", "select+sum decomposed, passthrough in parent")
      assert_equal(%w[120], lines(stdout), "select+sum+passthrough output")
    end
  end

  def test_parallel_select_non_decomposable_uses_split
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 3}, {"x" => 1}])
      write_ndjson(dir, "b.ndjson", [{"x" => 2}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'select(_["x"] > 0) >> sort(_["x"]) >> _["x"]', *ndjson_files(dir))
      assert_success(status, stderr, "select+sort uses split")
      assert_includes(stderr, "split=1/3", "non-decomposable sort uses map-prefix split")
      assert_equal([1, 2, 3], lines(stdout).map { |l| JSON.parse(l) }, "select+sort output")
    end
  end

  def test_parallel_decomposable_with_empty_file
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      File.write(File.join(dir, "b.ndjson"), "")

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", '{s: sum(_["x"]), n: count(), mn: min(_["x"])}', *ndjson_files(dir))
      assert_success(status, stderr, "decomposable with empty file")
      assert_includes(stderr, "decompose=", "decomposable with empty file used decompose")
      result = JSON.parse(lines(stdout).first)
      assert_equal(3, result["s"], "sum ignores empty file")
      assert_equal(2, result["n"], "count ignores empty file")
      assert_equal(1, result["mn"], "min ignores empty file")
    end
  end

  def test_parallel_decomposable_all_files_empty
    Dir.mktmpdir do |dir|
      File.write(File.join(dir, "a.ndjson"), "")
      File.write(File.join(dir, "b.ndjson"), "")

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "all files empty")
      # All files empty means first_value is nil, so classify returns nil → serial fallback
      assert_includes(stderr, "parallel: disabled", "all files empty falls back to serial")
      assert_equal([], lines(stdout), "no output for empty input")
    end
  end

  def test_parallel_non_decomposable_falls_back_to_serial
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-v", "-P", "2", 'sort(_["x"]) >> _["x"]', *ndjson_files(dir))
      assert_success(status, stderr, "non-decomposable serial fallback")
      assert_equal([1, 2, 3], lines(stdout).map { |l| JSON.parse(l) }, "sort output")
      assert_includes(stderr, "parallel: disabled", "non-decomposable falls back to serial")
    end
  end

  def test_parallel_decomposable_matches_serial
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", (1..50).map { |i| {"v" => i} })
      write_ndjson(dir, "b.ndjson", (51..100).map { |i| {"v" => i} })

      files = ndjson_files(dir)
      expr = '{s: sum(_["v"]), n: count(), mn: min(_["v"]), mx: max(_["v"]), avg: average(_["v"])}'

      serial_stdout, serial_stderr, serial_status = Open3.capture3("./exe/jrf", expr, *files)
      assert_success(serial_status, serial_stderr, "serial baseline")

      parallel_stdout, parallel_stderr, parallel_status = Open3.capture3("./exe/jrf", "-v", "-P", "2", expr, *files)
      assert_success(parallel_status, parallel_stderr, "parallel run")
      assert_includes(parallel_stderr, "decompose=", "decomposable matches serial used decompose")

      assert_equal(JSON.parse(serial_stdout), JSON.parse(parallel_stdout), "parallel decomposable matches serial")
    end
  end

  def test_serial_error_includes_filename
    Dir.mktmpdir do |dir|
      good_path = File.join(dir, "a.ndjson")
      File.write(good_path, "{\"x\":1}\n{\"x\":2}\n")

      bad_gz_path = File.join(dir, "b.ndjson.gz")
      full_gz = StringIO.new
      Zlib::GzipWriter.wrap(full_gz) { |io| io.write("{\"x\":10}\n" * 100) }
      File.binwrite(bad_gz_path, full_gz.string[0, full_gz.string.bytesize / 2])

      good_path2 = File.join(dir, "c.ndjson")
      File.write(good_path2, "{\"x\":3}\n")

      stdout, stderr, status = Open3.capture3("./exe/jrf", '_["x"]', good_path, bad_gz_path, good_path2)
      assert_failure(status, "serial error causes non-zero exit")
      assert_includes(stderr, bad_gz_path, "serial error message includes filename")
      refute_includes(stderr, "from ", "serial error does not include stacktrace")
      # Data from good files should still be present
      output_values = lines(stdout).map(&:to_i)
      assert_includes(output_values, 1, "data before bad file preserved")
      assert_includes(output_values, 3, "data after bad file preserved")
    end
  end

  private

  def write_ndjson(dir, name, rows)
    File.write(File.join(dir, name), rows.map { |r| JSON.generate(r) + "\n" }.join)
  end

  def ndjson_files(dir)
    Dir.glob(File.join(dir, "*.ndjson")).sort
  end
end
