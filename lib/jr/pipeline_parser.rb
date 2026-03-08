# frozen_string_literal: true

module Jr
  class PipelineParser
    def initialize(source)
      @source = source.to_s
    end

    def parse
      stages = split_top_level_pipeline(@source).map(&:strip).reject(&:empty?)
      raise ArgumentError, "empty expression" if stages.empty?

      { stages: stages.map { |stage| parse_stage!(stage) } }
    end

    private

    def parse_stage!(stage)
      reject_unsupported_stage!(stage)
      if select_stage?(stage)
        {
          kind: :select,
          original: stage,
          src: parse_select!(stage)
        }
      else
        {
          kind: :extract,
          original: stage,
          src: validate_extract!(stage)
        }
      end
    end

    def validate_extract!(stage)
      reject_unsupported_stage!(stage)
      stage
    end

    def parse_select!(stage)
      reject_unsupported_stage!(stage)
      match = /\Aselect\s*\((.*)\)\s*\z/m.match(stage)
      raise ArgumentError, "first stage must be select(...)" unless match

      inner = match[1].strip
      raise ArgumentError, "select(...) must contain an expression" if inner.empty?

      inner
    end

    def select_stage?(stage)
      /\Aselect\s*\(/.match?(stage)
    end

    def reject_unsupported_stage!(stage)
      raise ArgumentError, "flat is not supported yet" if stage == "flat"
      raise ArgumentError, "sum(...) is not supported yet" if /\Asum\s*\(/.match?(stage)
    end

    def split_top_level_pipeline(source)
      parts = []
      start_idx = 0
      i = 0
      depth = 0
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
          depth += 1
        when ")"
          depth -= 1 if depth > 0
        when "/"
          regex = looks_like_regex_start?(source, i)
        when ">"
          if depth.zero? && source[i, 2] == ">>"
            parts << source[start_idx...i]
            i += 2
            start_idx = i
            next
          end
        end

        i += 1
      end

      parts << source[start_idx..]
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
