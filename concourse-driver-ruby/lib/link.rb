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
        record
    end

end
