# frozen_string_literal: true

require_relative "test_helper"

class LibraryApiTest < JrfTestCase
  def test_basic_pipeline_api
    j = Jrf.new(proc { _ })
    assert_equal([{"a" => 1}, {"a" => 2}], j.call([{"a" => 1}, {"a" => 2}]), "library passthrough")

    j = Jrf.new(proc { _["a"] })
    assert_equal([1, 2], j.call([{"a" => 1}, {"a" => 2}]), "library extract")

    j = Jrf.new(
      proc { select(_["a"] > 1) },
      proc { _["a"] }
    )
    assert_equal([2, 3], j.call([{"a" => 1}, {"a" => 2}, {"a" => 3}]), "library select + extract")

    j = Jrf.new(proc { sum(_["a"]) })
    assert_equal([6], j.call([{"a" => 1}, {"a" => 2}, {"a" => 3}]), "library sum")

    j = Jrf.new(proc { sum(2 * _["a"]) })
    assert_equal([12], j.call([{"a" => 1}, {"a" => 2}, {"a" => 3}]), "library sum literal on left")

    j = Jrf.new(proc { {total: sum(_["a"]), n: count()} })
    assert_equal([{total: 6, n: 3}], j.call([{"a" => 1}, {"a" => 2}, {"a" => 3}]), "library structured reducers")
  end

  def test_map_and_map_values_api
    j = Jrf.new(proc { map { |x| x + 1 } })
    assert_equal([[2, 3], [4, 5]], j.call([[1, 2], [3, 4]]), "library map transform")

    j = Jrf.new(proc { map { |x| sum(x) } })
    assert_equal([[4, 6]], j.call([[1, 2], [3, 4]]), "library map reduce")

    j = Jrf.new(proc { map { map { |y| [sum(y[0]), sum(y[1])] } } })
    assert_equal([[[[4, 6]]]], j.call([[[[1, 2]]], [[[3, 4]]]]), "library nested map reduce")

    j = Jrf.new(proc { map_values { |v| v * 10 } })
    assert_equal([{"a" => 10, "b" => 20}], j.call([{"a" => 1, "b" => 2}]), "library map_values transform")

    j = Jrf.new(proc { map_values { |obj| map_values { |v| sum(v) } } })
    assert_equal([{"a" => {"x" => 4, "y" => 6}, "b" => {"x" => 40, "y" => 60}}], j.call([{"a" => {"x" => 1, "y" => 2}, "b" => {"x" => 10, "y" => 20}}, {"a" => {"x" => 3, "y" => 4}, "b" => {"x" => 30, "y" => 40}}]), "library nested map_values reduce")

    j = Jrf.new(proc { map { |k, v| "#{k}=#{v}" } })
    assert_equal([["a=1", "b=2"]], j.call([{"a" => 1, "b" => 2}]), "library map hash transform")

    j = Jrf.new(proc { map { |pair| pair } })
    assert_equal([[["a", 1], ["b", 2]]], j.call([{"a" => 1, "b" => 2}]), "library map hash single block arg")

    j = Jrf.new(proc { map { |k, v| sum(v + k.length) } })
    assert_equal([[5, 7]], j.call([{"a" => 1, "b" => 2}, {"a" => 2, "b" => 3}]), "library map hash reduce")
  end

  def test_apply_and_group_by_api
    j = Jrf.new(proc { [apply { |x| sum(x["foo"]) }, _.length] })
    assert_equal([[3, 2], [10, 1]], j.call([[{"foo" => 1}, {"foo" => 2}], [{"foo" => 10}]]), "library apply reducer")

    j = Jrf.new(proc { apply { |x| x["foo"] } })
    assert_equal([[1, 2]], j.call([[{"foo" => 1}, {"foo" => 2}]]), "library apply passthrough")

    j = Jrf.new(proc { apply { |x| percentile(x, 0.5) } })
    assert_equal([20], j.call([[10, 20, 30]]), "library apply percentile")

    j = Jrf.new(proc { map { |o| [apply(o["vals"]) { |x| sum(x) }, o["name"]] } })
    assert_equal([[[3, "a"], [30, "b"]]], j.call([[{"name" => "a", "vals" => [1, 2]}, {"name" => "b", "vals" => [10, 20]}]]), "library apply explicit collection")

    j = Jrf.new(proc { map(_["items"]) { |x| x * 2 } })
    assert_equal([[2, 4, 6]], j.call([{"items" => [1, 2, 3]}]), "library map explicit collection")

    j = Jrf.new(proc { map_values(_["data"]) { |v| v * 10 } })
    assert_equal([{"a" => 10, "b" => 20}], j.call([{"data" => {"a" => 1, "b" => 2}}]), "library map_values explicit collection")

    j = Jrf.new(proc { group_by(_["k"]) { count() } })
    assert_equal([{"x" => 2, "y" => 1}], j.call([{"k" => "x"}, {"k" => "x"}, {"k" => "y"}]), "library group_by")
  end

  def test_percentile_and_control_flow_api
    j = Jrf.new(proc { percentile(_["a"], _["p"]) })
    assert_equal([2], j.call([{"a" => 1, "p" => 0.5}, {"a" => 2, "p" => [0.5, 1.0]}, {"a" => 3, "p" => [0.5, 1.0]}]), "library percentile configuration fixed by first row")

    counting_percentiles = Class.new do
      include Enumerable

      attr_reader :each_calls

      def initialize(values)
        @values = values
        @each_calls = 0
      end

      def each(&block)
        @each_calls += 1
        @values.each(&block)
      end
    end.new([0.25, 0.5, 1.0])

    j = Jrf.new(proc { percentile(_["a"], counting_percentiles) })
    assert_equal([[1, 2, 3]], j.call([{"a" => 1}, {"a" => 2}, {"a" => 3}]), "library percentile enumerable values")
    assert_equal(1, counting_percentiles.each_calls, "library percentile materializes enumerable once")

    j = Jrf.new(
      proc { sum(_["a"]) },
      proc { _ + 1 }
    )
    assert_equal([7], j.call([{"a" => 1}, {"a" => 2}, {"a" => 3}]), "library reducer then passthrough")

    threshold = 2
    j = Jrf.new(proc { select(_["a"] > threshold) })
    assert_equal([{"a" => 3}], j.call([{"a" => 1}, {"a" => 2}, {"a" => 3}]), "library closure")

    j = Jrf.new(proc { sum(_) })
    assert_equal([], j.call([]), "library empty input")
  end

  def test_stage_reduce_control_tokens
    ctx = Jrf::RowContext.new
    stage = Jrf::Stage.new(ctx, proc { })
    first_token = stage.step_reduce(1, initial: 0) { |acc, v| acc + v }
    assert_equal(0, first_token.index, "step_reduce returns token while classifying reducer stage")
    stage.instance_variable_set(:@mode, :reducer)
    stage.instance_variable_set(:@cursor, 0)
    second_token = stage.step_reduce(2, initial: 0) { |acc, v| acc + v }
    assert_same(Jrf::Control::DROPPED, second_token, "expected DROPPED for established reducer slot")
  end
end
