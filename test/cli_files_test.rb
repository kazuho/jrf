# frozen_string_literal: true

require_relative "test_helper"

class CliFilesTest < JrfTestCase
  def test_files_lists_path_arguments
    Dir.mktmpdir do |dir|
      a = File.join(dir, "a.ndjson")
      b = File.join(dir, "b.ndjson")
      File.write(a, %({"x":1}\n))
      File.write(b, %({"x":2}\n))

      # The expression emits Jrf::CLI.files once per input row; the list is the
      # same for every row, so both rows print the full argv path list.
      stdout, stderr, status = Open3.capture3("./exe/jrf", "Jrf::CLI.files", a, b)
      assert_success(status, stderr, "Jrf::CLI.files with file arguments")
      assert_equal([[a, b], [a, b]], lines(stdout).map { |l| JSON.parse(l) }, "files output")
    end
  end

  def test_files_is_empty_for_stdin
    stdout, stderr, status = run_jrf("Jrf::CLI.files", %({"x":1}\n))
    assert_success(status, stderr, "Jrf::CLI.files reading stdin")
    assert_equal([[]], lines(stdout).map { |l| JSON.parse(l) }, "files empty on stdin")
  end

  def test_streaming_hash_join_against_own_files
    Dir.mktmpdir do |dir|
      log = File.join(dir, "log.ndjson")
      File.write(log, <<~NDJSON)
        {"type":"conn_stats","conn":1,"late":0}
        {"type":"conn_stats","conn":2,"late":3}
        {"type":"event","conn":1,"v":"keep"}
        {"type":"event","conn":2,"v":"drop"}
      NDJSON

      expr = <<~RUBY
        $lookup ||= Jrf.new(
          proc { select(_["type"] == "conn_stats") },
          proc { reduce({}) { |a, v| a[v["conn"]] = v["late"]; a } }
        ).read(*Jrf::CLI.files).first
        select(_["type"] == "event" && $lookup[_["conn"]] == 0) >> _["v"]
      RUBY

      stdout, stderr, status = Open3.capture3("./exe/jrf", expr, log)
      assert_success(status, stderr, "streaming hash-join")
      assert_equal(%w["keep"], lines(stdout), "hash-join filters to conn with late==0")
    end
  end

  def test_streaming_hash_join_under_parallel
    Dir.mktmpdir do |dir|
      a = File.join(dir, "a.ndjson")
      b = File.join(dir, "b.ndjson")
      File.write(a, <<~NDJSON)
        {"type":"conn_stats","conn":1,"late":0}
        {"type":"event","conn":1,"v":"keep1"}
      NDJSON
      File.write(b, <<~NDJSON)
        {"type":"conn_stats","conn":2,"late":5}
        {"type":"event","conn":2,"v":"drop2"}
      NDJSON

      expr = <<~RUBY
        $lookup ||= Jrf.new(
          proc { select(_["type"] == "conn_stats") },
          proc { reduce({}) { |a, v| a[v["conn"]] = v["late"]; a } }
        ).read(*Jrf::CLI.files).first
        select(_["type"] == "event" && $lookup[_["conn"]] == 0) >> _["v"]
      RUBY

      # -P forks per file; each worker sees the full file list via the
      # copy-on-write-inherited Jrf::CLI.files, so the global lookup spans both.
      stdout, stderr, status = Open3.capture3("./exe/jrf", "-P", "2", expr, a, b)
      assert_success(status, stderr, "parallel streaming hash-join")
      assert_equal(%w["keep1"], lines(stdout), "parallel hash-join filters across files")
    end
  end
end
