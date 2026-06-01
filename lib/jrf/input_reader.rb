# frozen_string_literal: true

require "json"
require "zlib"

module Jrf
  # File and stream input reading for jrf pipelines.
  #
  # Used by both the CLI runner and Pipeline#read to share gzip auto-detection,
  # strict NDJSON parsing, and (lazily loaded) --lax multiline parsing.
  module InputReader
    RS_CHAR = "\x1e"

    module_function

    def open_path(path, &block)
      if path.end_with?(".gz")
        Zlib::GzipReader.open(path, &block)
      else
        File.open(path, "rb", &block)
      end
    end

    def each_value(stream, lax: false, &block)
      if lax
        each_value_lax(stream, &block)
      else
        stream.each_line do |line|
          line.strip!
          next if line.empty?
          block.call(JSON.parse(line))
        end
      end
    end

    def each_value_lax(stream, &block)
      require "oj"
      Oj.sc_parse(streaming_handler_class.new(&block), RsNormalizer.new(stream))
    rescue LoadError
      raise "oj is required for --lax mode (gem install oj)"
    rescue Oj::ParseError => e
      raise JSON::ParserError, e.message
    end

    def streaming_handler_class
      @streaming_handler_class ||= Class.new(Oj::ScHandler) do
        def initialize(&emit)
          @emit = emit
        end

        def hash_start = {}
        def hash_key(key) = key
        def hash_set(hash, key, value) = hash[key] = value
        def array_start = []
        def array_append(array, value) = array << value
        def add_value(value) = @emit.call(value)
      end
    end

    # Translates JSON-SEQ record separators (RS, 0x1e) to newlines so the
    # underlying Oj scanner sees a stream of whitespace-delimited values.
    class RsNormalizer
      def initialize(input)
        @input = input
      end

      def read(length = nil, outbuf = nil)
        chunk = @input.read(length)
        return nil if chunk.nil?

        chunk.tr!(RS_CHAR, "\n")
        if outbuf
          outbuf.replace(chunk)
        else
          chunk
        end
      end
    end
  end
end
