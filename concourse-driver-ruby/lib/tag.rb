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

    def ==(other)
        if other.is_a? Tag
            return other.to_s == to_s
        else
            return false
        end
    end

end
