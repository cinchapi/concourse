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

    # A Tag a Concourse data type for a string that does not get indexed for
    # full text search.
    #
    # Each Tag is equivalent to its String counterpart. Tags merely exist for
    # the client to instruct Concourse not to perform full text indexing on the
    # data. Within Concourse, Tags are stored on disk as strings. So, any value
    # that is written as a Tag is always returned as a String when read from
    # Concourse.
    class Tag < String

        def self.create value
            Tag.new value
        end

        def ==(other)
            if other.is_a? Tag
                return other.to_s == to_s
            else
                super(other)
            end
        end

    end
end
