# Copyright (c) 2015 Cinchapi, Inc.
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

module Concourse

    # A Link is a wrapper around an Integer that represents the primary key of
    # a record in graph contexts.
    class Link

        attr_reader :record

        def self.to record
            Link.new record
        end

        def initialize record
            @record = record
        end

        def to_s
            "@#{@record}@"
        end
        
        def ==(other)
            if other.is_a? Link
                return other.record == record
            else
                return false
            end
        end

    end
end
