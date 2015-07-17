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
                data = [value].pack('q>')
                type = Type::LINK
            elsif value.is_a? Tag
                data = value.encode('UTF-8')
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
