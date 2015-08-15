# Copyright (c) 2015 Cinchapi Inc.
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

require 'concourse/utils'

module Concourse

    # Return the name of the thrift method to call and the args

    # document me
    # @!visibility private
    class Dispatch

        # A Hash mapping ruby methods to dispatchable information about applicable thrift methods.
        @@methods = {}

        # ROUTINE: Here we need to get a mapping from every possible kwarg aliases to the canoncial kwarg.
        @@aliases = {}
        Concourse::Utils::Args.class_variable_get(:@@kwarg_aliases).each do |key, value|
            value.each do |item|
                @@aliases[item] = key
            end
        end

        # A Method struct to encapsulate the name of a tmethod that can be sent
        # to the Thrift client and the names of the variables to extract from
        # the collection of paramters and send along with the the tmethod to the
        # thrift client.
        # Usage: Struct::Method::new("selectKeyCclTime", ["key", "criteria", "timestamp"])
        Struct.new("Method", :tmethod, :variables) do
            def to_s
                return "(#{tmethod}:#{variables})"
            end

            def inspect
                return to_s
            end
        end

        # Given some kwargs, remove any aliases and replace them with the
        # canonical kwarg.
        # @param [Hash] kwargs The kwargs
        # @return [Hash] The same kwargs without any aliases
        def self.resolve_kwarg_aliases(**kwargs)
            nkwargs = {}
            kwargs.each do |key, value|
                k = @@aliases.fetch(key, key)
                if k == "key".to_sym
                    k = "keys".to_sym
                elsif k == "record".to_sym
                    k = "records"
                end
                nkwargs[k] = value
            end
            return nkwargs
        end

        # Given the name of a ruby _method_ and the combination or _args_ and _kwargs_ supplied by the caller, dynamically return dispatch information for the appropriate thrift method to call.
        # @param [String] method The ruby method name
        # @param [Array] args The positional arguments
        # @param [Hash] kwargs The keyword arguments
        # @param [Array] An array of arguments to pass to the #send method on the thrift client object to perform a dynamic dispatch
        def self.dynamic(method, *args, **kwargs)
            method = method.to_s
            kwargs = self.resolve_kwarg_aliases(**kwargs)
            signature = []
            variables = []
            comboargs = args + kwargs.values
            comboargs.each do |item|
                signature << item.class
                variables << item
            end
            tocall = [] + @@methods[method][signature]
            if tocall.length > 1
                resolvetocall = []
                tocall.each do |item|
                    inc = true
                    kwargs.keys.each do |key|
                        index = item.variables.index{|x| key.to_sym == x.to_sym}
                        if index.nil? or index < args.length
                            inc = false
                            break
                        end
                    end
                    resolvetocall << item if inc
                end
                tocall = resolvetocall
            end
            if tocall.length != 1
                raise "Cannot deterministically dispatch the #{method} method with positional arguments (#{args}) and keyword arguments (#{kwargs}). The possible solutions are #{tocall}."
            else
                return [tocall.shift.tmethod, *comboargs]
            end
        end

        # Given the name of a ruby method, dynamically get all the dispatchable
        # thrift methods and place them in the _@@methods__ collection.
        # @param [String] rmethod The ruby method name
        # @param [Void]
        def self.enable_dynamic_dispatch(rmethod)
            @@methods[rmethod] = Hash.new do |h,k|
                h[k] = []
            end
            Thrift::ConcourseService::Client.instance_methods.grep(/(?=^#{rmethod})(^((?!Criteria).)*$)/).each do |tmethod|
                # Get the arguments that the tmethod is expecting
                tmethod = tmethod.to_s unless tmethod.is_a? String
                args = tmethod.split(/(?=[A-Z])/)
                # We need to shift the appropriate number of elements in the name of the tmethod before the expected args are listed (i.e. for findOrAddKeyValue we need to shift 3 elements that correspond to "find", "or" and "add")
                if tmethod.start_with? "findOrAdd"
                    shift = 3
                else
                    shift = 1
                end
                (0...shift).each do
                    args.shift
                end
                # For each of the args, we need to, if necessary, map its tname to that used within the ruby code (i.e. time and timestr are known as timestamp) and we must figure out the expected class of the arg
                signature = []
                args.each do |arg|
                    arg.downcase!
                    if arg == 'timestr'
                        arg.gsub! "timestr", "timestamp"
                        signature << String
                    elsif arg == "time"
                        arg.gsub! "time", "timestamp"
                        signature << Fixnum
                    elsif arg == "key"
                        arg << "s"
                        signature << String
                    elsif arg == "record"
                        arg << "s"
                        signature << Fixnum
                    elsif arg == "ccl"
                        arg.gsub! "ccl", "criteria"
                        signature << String
                    elsif arg.end_with? "s"
                        signature << Array
                    else
                        raise "Cannot handle #{arg} argument in method #{tmethod}"
                    end
                end
                @@methods[rmethod][signature] << Struct::Method::new(tmethod, args)
             end
        end

        # Enable dynamic dispatch
        self.enable_dynamic_dispatch "select"
    end
end
