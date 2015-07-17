class Tag

    def self.create value
        Tag.new value
    end

    def initialize value
        @value = value.to_s
    end

    def to_s
        @value
    end

end
