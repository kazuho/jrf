# frozen_string_literal: true

require_relative "control"
require_relative "input_reader"
require_relative "row_context"
require_relative "stage"

module Jrf
  class Pipeline
    def initialize(*blocks)
      raise ArgumentError, "at least one stage block is required" if blocks.empty?

      @stages = blocks.map { |block| Stage.new(block, src: nil) }
    end

    # Run the pipeline on one or more files, mirroring how the CLI reads its
    # file arguments: each path is opened (with .gz auto-decompression) and
    # parsed as NDJSON. Pass +lax: true+ for multiline JSON / JSON-SEQ input.
    #
    # Without a block, returns an Array of output values; with a block, streams
    # each output value to the block.
    #
    # @param paths [Array<String>] one or more file paths
    # @param lax [Boolean] enable lax (multiline / whitespace-delimited) parsing
    # @yieldparam value output value
    # @return [Array, nil] output values (without block), or nil (with block)
    # @example Build a lookup hash from one file, use it to filter another
    #   lookup = Jrf.new(
    #     proc { reduce({}) { |a, v| a[[v["tid"], v["conn"]]] = v["late_acked"]; a } }
    #   ).read("conn_stats.ndjson").first
    def read(*paths, lax: false, &on_output)
      raise ArgumentError, "at least one path is required" if paths.empty?

      input = Enumerator.new do |y|
        paths.each do |path|
          InputReader.open_path(path) do |stream|
            InputReader.each_value(stream, lax: lax) { |value| y << value }
          end
        end
      end
      call(input, &on_output)
    end

    # Run the pipeline on an enumerable of input values.
    #
    # Without a block, returns an Array of output values.
    # With a block, streams each output value to the block.
    #
    # @param input [Enumerable] input values to process
    # @yieldparam value output value
    # @return [Array, nil] output values (without block), or nil (with block)
    def call(input, &on_output)
      if on_output.nil?
        results = []
        on_output = proc { |value| results << value }
      end

      begin
        input.each { |value| process_value(value, @stages, &on_output) }
      ensure
        flush_reducers(@stages, &on_output)
      end

      results unless results.nil?
    end

    private

    def process_value(value, stages, idx = 0, &on_output)
      while idx < stages.length
        value = stages[idx].call(value)

        if value.equal?(Control::DROPPED)
          return
        elsif value.is_a?(Control::Flat)
          value = value.value
          unless value.is_a?(Array)
            raise TypeError, "flat expects Array, got #{value.class}"
          end
          value.each do |child|
            process_value(child, stages, idx + 1, &on_output)
          end
          return
        end

        idx += 1
      end

      on_output.call(value)
    end

    def flush_reducers(stages, &on_output)
      stages.each_with_index do |stage, idx|
        rows = stage.finish
        next if rows.empty?

        rest = stages.drop(idx + 1)
        rows.each { |value| process_value(value, rest, &on_output) }
      end
    end
  end
end
