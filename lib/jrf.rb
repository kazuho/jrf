# frozen_string_literal: true

require_relative "jrf/version"
require_relative "jrf/cli"
require_relative "jrf/pipeline"

module Jrf
  # Create a pipeline from one or more stage blocks.
  #
  # Each block is evaluated in a context where +_+ is the current value.
  # All jrf built-in functions (+select+, +sum+, +map+, +group_by+, etc.)
  # are available inside blocks. See README.md for the full list.
  #
  # @param blocks [Array<Proc>] one or more stage procs
  # @return [Pipeline] a callable pipeline
  # @example
  #   j = Jrf.new(proc { select(_["x"] > 10) }, proc { sum(_["x"]) })
  #   j.call([{"x" => 20}, {"x" => 30}])  # => [50]
  def self.new(*blocks)
    Pipeline.new(*blocks)
  end
end
