# frozen_string_literal: true

require_relative "control"
require_relative "row_context"
require_relative "stage"

module Jrf
  class Pipeline
    def initialize(*blocks)
      raise ArgumentError, "at least one stage block is required" if blocks.empty?

      @ctx = RowContext.new
      @stages = blocks.map { |block| Stage.new(@ctx, block, src: nil) }
    end

    def call(input, &on_output)
      if on_output
        call_streaming(input, &on_output)
      else
        results = []
        call_streaming(input) { |v| results << v }
        results
      end
    end

    private

    def call_streaming(input, &on_output)
      error = nil
      begin
        input.each { |value| process_value(value, @stages, &on_output) }
      rescue StandardError => e
        error = e
      ensure
        flush_reducers(@stages, &on_output)
      end
      raise error if error
    end

    def process_value(input, stages, &on_output)
      current_values = [input]

      stages.each do |stage|
        next_values = []

        current_values.each do |value|
          out = stage.call(value)
          if out.equal?(Control::DROPPED)
            next
          elsif out.is_a?(Control::Flat)
            unless out.value.is_a?(Array)
              raise TypeError, "flat expects Array, got #{out.value.class}"
            end
            next_values.concat(out.value)
          else
            next_values << out
          end
        end

        return if next_values.empty?
        current_values = next_values
      end

      current_values.each(&on_output)
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
