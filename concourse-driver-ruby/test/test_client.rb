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

    def test_audit_key_record_start
        key = TestUtils.random_string
        values = ["one", "two", "three"]
        record = 1000
        for value in values do
            @client.set key, value, record
        end
        start = @client.time
        values = [4, 5, 6]
        for value in values do
            @client.set key, value, record
        end
        audit = @client.audit key, record, start:start
        assert_equal 6, audit.length
    end

    def test_audit_key_record_start_end
        key = TestUtils.random_string
        values = ["one", "two", "three"]
        record = 1000
        for value in values do
            @client.set key, value, record
        end
        start = @client.time
        values = [4, 5, 6]
        for value in values do
            @client.set key, value, record
        end
        tend = @client.time
        values = [true, false]
        for value in values do
            @client.set key, value, record
        end
        audit = @client.audit key, record, start:start, end:tend
        assert_equal 6, audit.length
    end

    def test_audit_key_record_startstr
        key = TestUtils.random_string
        values = ["one", "two", "three"]
        record = 1000
        for value in values do
            @client.set key, value, record
        end
        anchor = get_time_anchor
        values = [4, 5, 6]
        for value in values do
            @client.set key, value, record
        end
        start = get_elapsed_millis_string anchor
        audit = @client.audit key, record, start:start
        assert_equal 6, audit.length
    end

    def test_adit_key_record_startstr_endstr
        key = TestUtils.random_string
        values = ["one", "two", "three"]
        record = 1000
        for value in values do
            @client.set key, value, record
        end
        sanchor = get_time_anchor
        values = [4, 5, 6]
        for value in values do
            @client.set key, value, record
        end
        eanchor = get_time_anchor
        values = [true, false]
        for value in values do
            @client.set key, value, record
        end
        start = get_elapsed_millis_string sanchor
        tend = get_elapsed_millis_string eanchor
        audit = @client.audit key, record, start:start, end:tend
        assert_equal 6, audit.length
    end

    def test_audit_record
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        key3 = TestUtils.random_string
        value = "foo"
        record = 1002
        @client.add key1, value, record
        @client.add key2, value, record
        @client.add key3, value, record
        audit = @client.audit record
        assert_equal 3, audit.length
    end

    def test_audit_record_start
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        key3 = TestUtils.random_string
        value = "bar"
        record = 344
        @client.add key1, value, record
        @client.add key2, value, record
        @client.add key3, value, record
        start = @client.time
        @client.remove key1, value, record
        @client.remove key2, value, record
        @client.remove key3, value, record
        audit = @client.audit record, start:start
        assert_equal 3, audit.length
    end

end
