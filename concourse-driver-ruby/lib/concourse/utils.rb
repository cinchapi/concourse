# Copyright (c) 2013-2017 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'concourse/thrift/shared_types'
require 'concourse/thrift/data_types'

module Concourse
    # @!visibility private
    module Utils

        # Monkey patch the Object class to handle common conversions.
        Object.class_eval do

            # Convert the object to its Thrift representation
            # @return [Concourse::Thrift::TObject] The Thrift representation
            def to_thrift
                return Utils::Convert::ruby_to_thrift self
            end

            # Recursively convert a collection to its ruby representation.
            # @return [Object] The same collection, where each element is a ruby Object
            def rubyify
                return Utils::Convert::rubyify self
            end

            # Recursively convert elements in collection to their Thrift
            # representations.
            # @return [Object] The same collection, where each element is a TObject
            def thriftify
                return Utils::Convert::thriftify self
            end

        end

        # Monkey patch the TObject class to handle common conversions.
        Concourse::Thrift::TObject.class_eval do

            # Convert the TObject to its ruby representation.
            # @return [Object] The ruby representation
            def to_ruby
                return Utils::Convert::thrift_to_ruby self
            end

        end

        # Corresponds to java.lang.Integer#MAX_VALUE
        CONCOURSE_MAX_INT = 2147483647

        # Corresponds to java.lang.Integer#MIN_VALUE
        CONCOURSE_MIN_INT = -2147483648

        # A flag that indicates whether the underlying platform's byte order
        # is BIG_ENDIAN or not.
        BIG_ENDIAN = [1].pack('l') == [1].pack('N')

        # Utilities for dealing with method arguments.
        # @!visibility private
        class Args

            # A mapping from the canonical kwarg to a list of acceptable
            # aliases.
            @@kwarg_aliases = {
                :criteria => [:ccl, :where, :query],
                :timestamp => [:time, :ts],
                :username => [:user, :uname],
                :password => [:pass, :pword],
                :prefs => [:file, :filename, :config, :path],
                :expected => [:value, :current, :old],
                :replacement => [:new, :other, :value2],
                :json => [:data],
                :record => [:id]
            }

            # Util function to raise a RuntimeError that indicates that the
            # calling function requires a particular argument.
            def self.require(arg)
                func = caller[0]
                raise "#{func} requires the #{arg} keyword argument(s)"
            end

            # Given a hash of kwargs, look for a certain key or any of the
            # acceptable aliases for that key.
            # @param [String] key The canonical key to search for
            # @param [Hash] kwargs The kwargs that were passed into the function
            # @return the value from kwargs
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

        # A collection of functions to convert data between ruby and thrift
        # representations.
        # @!visibility private
        class Convert

            include Concourse::Thrift
            include Concourse::Utils
            include Concourse

            # Convert a thrift object to its ruby counterpart.
            # @param [TObject] tobject The thrift object
            # @return the analogous ruby object
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

            # Convert a ruby object to its thrift counterpart.
            # @param value The ruby object
            # @return [TObject] the analogous thrift object
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

            # Given a complex collection of thrift data, convert each item
            # therewithin to the ruby counterpart.
            # @param data the thrift collection
            # @return an analogous collection of ruby objects
            def self.rubyify(data)
                if data.is_a? Hash
                    result = {}
                    data.each_pair { |key, value|
                        k = key.is_a?(TObject) ? Convert::thrift_to_ruby(key) : Convert::rubyify(key)
                        v = value.is_a?(TObject) ? Convert::thrift_to_ruby(value) : Convert::rubyify(value)
                        result.store(k.is_a?(String) ? k.to_sym : k, v)
                    }
                    return result
                elsif data.is_a? Array or data.is_a? Set
                    result = []
                    data.each { |x| result.push Convert::rubyify(x) }
                    return result
                elsif data.is_a? TObject
                    return Convert::thrift_to_ruby(data)
                else
                    return data
                end
            end

            # Given a complex collection of ruby data, convert each item
            # therewithin to the trift counterpart.
            # @param data the ruby collection
            # @return an analogous collection of thrift objects
            def self.thriftify(data)
                if data.is_a? Hash
                    result = {}
                    data.each_pair { |key, value|
                    k = (!key.is_a?(Array) or !key.is_a?(Hash)) ? Convert::ruby_to_thrift(key) : Convert::thriftify(key)
                    v = (!value.is_a?(Array) or !value.is_a?(Hash)) ? Convert::ruby_to_thrift(value) : Convert::thriftify(value)
                    result.store(k.is_a?(String) ? k.to_sym : k, v)
                    }
                    return result
                elsif data.is_a? Array
                    result = []
                    data.each { |x| result.push Convert::thriftify(x) }
                    return result
                elsif !data.is_a? TObject
                    return Convert::ruby_to_thrift(data)
                else
                    return data
                end
            end

        end
    end
end
