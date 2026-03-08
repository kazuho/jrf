# frozen_string_literal: true

module Jr
  class RowContext
    def initialize(obj = nil)
      @obj = obj
    end

    def reset(obj)
      @obj = obj
      self
    end

    def [](key)
      @obj[key]
    end

    def _
      @obj
    end

    def __row__
      @obj
    end
  end
end
