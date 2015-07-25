class TestUtils

    def self.random_string(length=8)
        o = [('a'..'z'), ('A'..'Z')].map { |i| i.to_a }.flatten
        return (0...length).map { o[rand(o.length)] }.join
    end

end
