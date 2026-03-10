# frozen_string_literal: true

require "zlib"

module Jrf
  class CLI
    class Input
      def initialize(paths, stdin:, auto_decompress: false)
        @paths = paths
        @stdin = stdin
        @auto_decompress = auto_decompress
      end

      def each_line(&block)
        each_source do |source|
          source.each_line(&block)
        end
      end

      def read
        chunks = +""
        each_source do |source|
          chunks << source.read.to_s
        end
        chunks
      end

      private

      def each_source
        if @paths.empty?
          yield @stdin
          return
        end

        @paths.each do |path|
          if path == "-"
            yield @stdin
          elsif @auto_decompress && path.end_with?(".gz")
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
