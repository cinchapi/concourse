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

require_relative 'base'

class RubyClientDriverTest < IntegrationBaseTest

    def test_abort
        @client.stage
        key = TestUtils.random_string
        value = "some value"
        record = 1
        @client.add key, value, record
        @client.abort
        assert_equal(nil, @client.get(key:key, record:record))
    end

    def test_add_key_value
        key = TestUtils.random_string
        value = "some value"
        record = @client.add key, value
        assert_not_nil record
        stored = @client.get key, record
        assert_equal(value, stored)
    end

    def test_add_key_value_record
        key = "foo"
        value = "static value"
        record = 17
        assert @client.add key, value, record
        stored = @client.get key, record
        assert_equal(value, stored)
    end

    def test_add_key_value_records
        key = TestUtils.random_string
        value = "static value"
        records = [1, 2, 3]
        result = @client.add key, value, records
        assert result.is_a? Hash
        assert result[1]
        assert result[2]
        assert result[3]
    end

    def test_audit_key_record
        key = TestUtils.random_string
        values = ["one", "two", "three"]
        record = 1000
        for value in values do
            @client.set key, value, record
        end
        audit = @client.audit key, record
        assert_equal(5, audit.length)
        expected = "ADD"
        audit.each do |k, v|
            assert(v.start_with? expected)
            expected = expected == "ADD" ? "REMOVE" : "ADD"
        end
    end

end
