module Utils
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
end
