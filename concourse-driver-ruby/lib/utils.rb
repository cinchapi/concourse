require_relative 'thrift_api/shared_types'
require_relative 'thrift_api/data_types'
require_relative 'link'
require_relative 'tag'

module Utils

    CONCOURSE_MAX_INT = 2147483647
    CONCOURSE_MIN_INT = -2147483648
    BIG_ENDIAN = [1].pack('l') == [1].pack('N')

    class Args
        @@kwarg_aliases = {
            :criteria => [:ccl, :where, :query],
            :timestamp => [:time, :ts],
            :username => [:user, :uname],
            :password => [:pass, :pword],
            :prefs => [:file, :filename, :config, :path],
            :expected => [:value, :current, :old],
            :replacement => [:new, :other, :value2]
        }

        def self.require(arg)
            func = caller[0]
            raise "#{func} requires the #{arg} keyword argument(s)"
        end

        def self.find_in_kwargs_by_alias(key, **kwargs)
            if key.is_a? String
                key = key.to_sym
            end
            if kwargs[key].nil?
                for x in @@kwarg_aliases[key]
                    value = kwargs[x]
                    if !value.nil?
                        return value
                    end
                end
                return nil
            else
                return kwargs[key]
            end
        end
    end

    class Convert

        def self.thrift_to_ruby(tobject)
            case tobject.type
            when Type::BOOLEAN
                rb = tobject.data.unpack('C')[0]
                rb = rb == 1 ? true : false
            when Type::INTEGER
                rb = tobject.data.unpack('l>')[0]
            when Type::LONG
                rb = tobject.data.unpack('q>')[0]
            when Type::DOUBLE
                rb = tobject.data.unpack('G')[0]
            when Type::FLOAT
                rb = tobject.data.unpack('G')[0]
            when Type::LINK
                rb = tobject.data.unpack('q>')[0]
                rb = Link.to rb
            when Type::TAG
                rb = tobject.data.encode('UTF-8')
                rb = Tag.create rb
            when Type::NULL
                rb = nil
            else
                rb = tobject.data.encode('UTF-8')
            end
            return rb
        end

        def self.ruby_to_thrift(value)
            if value == true
                data = [1].pack('c')
                type = Type::BOOLEAN
            elsif value == false
                data = [0].pack('c')
                type = Type::BOOLEAN
            elsif value.is_a? Integer
                if value > CONCOURSE_MAX_INT or value < CONCOURSE_MIN_INT
                    data = [value].pack('q>')
                    type = Type::LONG
                else
                    data = [value].pack('l>')
                    type = Type::INTEGER
                end
            elsif value.is_a? Float
                data = [value].pack('G')
                type = Type::FLOAT
            elsif value.is_a? Link
                data = [value.record].pack('q>')
                type = Type::LINK
            elsif value.is_a? Tag
                data = value.to_s.encode('UTF-8')
                type = Type::TAG
            else
                data = value.encode('UTF-8')
                type = Type::STRING
            end
            tobject = TObject.new
            tobject.data = data
            tobject.type = type
            return tobject
        end
    end
end
