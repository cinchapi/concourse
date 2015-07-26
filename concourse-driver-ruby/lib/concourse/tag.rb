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
end
