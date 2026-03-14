# frozen_string_literal: true

require_relative "test_helper"

class CliParallelTest < JrfTestCase
  def test_parallel_map_only
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}, {"x" => 4}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", '_["x"]', *ndjson_files(dir))
      assert_success(status, stderr, "parallel map only")
      assert_equal([1, 2, 3, 4], lines(stdout).map(&:to_i).sort, "parallel map only output")
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

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", 'select(_["x"] > 10) >> sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel split map+reduce")
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

  def test_parallel_all_reducers_falls_back_to_serial
    Dir.mktmpdir do |dir|
      write_ndjson(dir, "a.ndjson", [{"x" => 1}, {"x" => 2}])
      write_ndjson(dir, "b.ndjson", [{"x" => 3}])

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", 'sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "all-reducer serial fallback")
      assert_equal(%w[6], lines(stdout), "all-reducer serial fallback output")
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
      refute_empty(stderr, "worker error reported to stderr")
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

      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", 'select(_["x"] > 10) >> sum(_["x"])', *ndjson_files(dir))
      assert_success(status, stderr, "parallel select then sum")
      assert_equal(%w[60], lines(stdout), "parallel select then sum output")
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
