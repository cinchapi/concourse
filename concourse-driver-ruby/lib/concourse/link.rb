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

module Concourse

    # A Link is a pointer to a record.
    #
    # Links should never be written directly. They can be created using the
    # `Concourse.link` method in the `Concourse` API.
    #
    # Links may be returned when reading data using the `Concourse.select()`,
    # `Concourse.get()` and `Concourse.browse()` methods. When handling Link
    # objects, you can retrieve the underlying record id by accessing the
    # `Link.record` property.
    #
    # When performing a bulk insert (using the `Concourse.insert()` method) you
    # can use this class to create Link objects that are added to the data/json
    # blob. Links inserted in this manner will be written in the same way they
    # would have been if they were written using the `Concourse.link()` method.
    #
    # To create a static link to a single record, use `Link.to()`.
    #
    # To create static link to each of the records that match a criteria, use
    # the `Link.to_where` method.
    class Link

        # Allow read access to the _record_ field.
        attr_reader :record

        # Alias for the constructor.
        # @param [Integer] record The target of the link
        # @return [Link] the Link
        def self.to record
            Link.new record
        end

        # Return a string that instructs Concourse to create links that point to
        # each of the records that match the _ccl_ string.
        #
        # NOTE: This method does not return a _Link_ object, so it should only
        # be used when adding a resolvable link value to a data/json blob that
        # will be passed to the `Concourse.insert()` method.
        #
        # @param [String] ccl a CCL string that describes the records to which a
        #   Link should point
        # @return [String] a resolvable link instruction
        def self.to_where ccl
            "@#{ccl}@"
        end

        # Return a _Link_ that points to _record_.
        #
        # @param [Integer] record The record id
        # @return [Link] a _Link_ that points to _record_
        def initialize record
            @record = record
        end

        # Overriden
        def to_s
            "@#{@record}"
        end

        # Overriden
        def ==(other)
            if other.is_a? Link
                return other.record == record
            else
                return false
            end
        end

    end
end
