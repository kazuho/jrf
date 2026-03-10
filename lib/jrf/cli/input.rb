# frozen_string_literal: true

require "zlib"

module Jrf
  class CLI
    class Input
      def initialize(paths, stdin:)
        @paths = paths.dup
        @stdin = stdin
      end

      def each_source
        if @paths.empty?
          yield @stdin
          return
        end

        @paths.each do |path|
          if path == "-"
            yield @stdin
          elsif path.end_with?(".gz")
            Zlib::GzipReader.open(path) do |io|
              yield io
            end
          else
            File.open(path, "rb") do |io|
              yield io
            end
          end
        end
      end
    end
  end
end
