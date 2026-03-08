# frozen_string_literal: true

module Jrf
  class PipelineParser
    def initialize(source)
      @source = source.to_s
    end

    def parse
      stages = split_top_level_pipeline(@source).map(&:strip).reject(&:empty?)
      raise ArgumentError, "empty expression" if stages.empty?
      { stages: stages.map { |stage| { src: stage } } }
    end

    private

    def split_top_level_pipeline(source)
      parts = []
      start_idx = 0
      i = 0
      stack = []
      quote = nil
      escaped = false
      regex = false
      regex_class = false

      while i < source.length
        ch = source[i]

        if quote
          escaped = !escaped && ch == "\\" if quote != "'"
          if quote == "'" && ch == "'" && !escaped
            quote = nil
          elsif quote != "'" && ch == quote && !escaped
            quote = nil
          end
          escaped = false if ch != "\\" && quote != "'"
          i += 1
          next
        end

        if regex
          if escaped
            escaped = false
          elsif regex_class
            regex_class = false if ch == "]"
          else
            case ch
            when "\\"
              escaped = true
            when "["
              regex_class = true
            when "/"
              regex = false
            end
          end
          i += 1
          next
        end

        case ch
        when "'", '"'
          quote = ch
        when "("
          stack << [")", i]
        when "["
          stack << ["]", i]
        when "{"
          stack << ["}", i]
        when ")", "]", "}"
          expected, open_idx = stack.pop
          unless expected == ch
            raise ArgumentError, "mismatched delimiter #{ch.inspect} at offset #{i}"
          end
        when "/"
          regex = looks_like_regex_start?(source, i)
        when ">"
          if stack.empty? && source[i, 2] == ">>"
            parts << source[start_idx...i]
            i += 2
            start_idx = i
            next
          end
        end

        i += 1
      end

      parts << source[start_idx..]
      unless stack.empty?
        expected, open_idx = stack.last
        raise ArgumentError, "unclosed delimiter #{expected.inspect} at offset #{open_idx}"
      end

      parts
    end

    def looks_like_regex_start?(source, slash_idx)
      j = slash_idx - 1
      j -= 1 while j >= 0 && source[j] =~ /\s/
      return true if j < 0

      prev = source[j]
      !(/[[:alnum:]_\]\)]/.match?(prev))
    end
  end
end
