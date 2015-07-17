class Link

    def self.to record
        Link.new record
    end

    def initialize record
        @record = record
    end

    def to_s
        "@#{@record}@"
    end

    def record
        @record
    end

    def ==(other)
        if other.is_a? Link
            return other.record == record
        else
            return false
        end
    end

end
