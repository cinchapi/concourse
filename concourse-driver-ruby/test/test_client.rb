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

require_relative 'base'
require 'json'

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
        stored = @client.get key, record:record
        assert_equal(value, stored)
    end

    def test_add_key_value_record
        key = "foo"
        value = "static value"
        record = 17
        assert @client.add key, value, record
        stored = @client.get key, record:record
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

    def test_audit_record_start_end
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
        tend = @client.time
        @client.add key1, value, record
        @client.add key2, value, record
        @client.add key3, value, record
        audit = @client.audit record, start:start, end:tend
        assert_equal 3, audit.length
    end

    def test_audit_record_startstr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        key3 = TestUtils.random_string
        value = "bar"
        record = 344
        @client.add key1, value, record
        @client.add key2, value, record
        @client.add key3, value, record
        anchor = get_time_anchor
        @client.remove key1, value, record
        @client.remove key2, value, record
        @client.remove key3, value, record
        start = get_elapsed_millis_string anchor
        audit = @client.audit record, start:start
        assert_equal 3, audit.length
    end

    def test_audit_record_startstr_endstr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        key3 = TestUtils.random_string
        value = "bar"
        record = 344
        @client.add key1, value, record
        @client.add key2, value, record
        @client.add key3, value, record
        sanchor = get_time_anchor
        @client.remove key1, value, record
        @client.remove key2, value, record
        @client.remove key3, value, record
        eanchor = get_time_anchor
        @client.add key1, value, record
        @client.add key2, value, record
        @client.add key3, value, record
        start = get_elapsed_millis_string sanchor
        tend = get_elapsed_millis_string eanchor
        audit = @client.audit record, start:start, end:tend
        assert_equal 3, audit.length
    end

    def test_browse_key
        key = TestUtils.random_string
        value = 10
        @client.add key, value, [1, 2, 3]
        value = TestUtils.random_string
        @client.add key, value, [10, 20, 30]
        data = @client.browse key
        assert_equal [1, 2, 3].sort!, data[10].sort!
        assert_equal [10, 20, 30].sort!, data[value.to_sym].sort!
    end

    def test_browse_key_time
        key = TestUtils.random_string
        value = 10
        @client.add key, value, [1, 2, 3]
        value = TestUtils.random_string
        @client.add key, value, [10, 20, 30]
        time = @client.time
        @client.add key, value, [100, 200, 300]
        data = @client.browse key, time:time
        assert_equal [10, 20, 30].sort!, data[value.to_sym].sort!
    end

    def test_browse_key_timestr
        key = TestUtils.random_string
        value = 10
        @client.add key, value, [1, 2, 3]
        value = TestUtils.random_string
        @client.add key, value, [10, 20, 30]
        anchor = get_time_anchor
        @client.add key, value, [100, 200, 300]
        time = get_elapsed_millis_string anchor
        data = @client.browse key, time:time
        assert_equal [10, 20, 30].sort!, data[value.to_sym].sort!
    end

    def test_browse_keys
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        key3 = TestUtils.random_string
        value1 = "A"
        value2 = "B"
        value3 = "C"
        record1 = 1
        record2 = 2
        record3 = 3
        @client.add key1, value1, record1
        @client.add key2, value2, record2
        @client.add key3, value3, record3
        data = @client.browse [key1, key2, key3]
        assert_equal({value1.to_sym => [record1]}, data[key1.to_sym])
        assert_equal({value2.to_sym => [record2]}, data[key2.to_sym])
        assert_equal({value3.to_sym => [record3]}, data[key3.to_sym])
    end

    def test_browse_keys_time
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        key3 = TestUtils.random_string
        value1 = "A"
        value2 = "B"
        value3 = "C"
        record1 = 1
        record2 = 2
        record3 = 3
        @client.add key1, value1, record1
        @client.add key2, value2, record2
        @client.add key3, value3, record3
        time = @client.time
        @client.add key1, "Foo"
        @client.add key2, "Foo"
        @client.add key3, "Foo"
        data = @client.browse [key1, key2, key3], time:time
        assert_equal({value1.to_sym => [record1]}, data[key1.to_sym])
        assert_equal({value2.to_sym => [record2]}, data[key2.to_sym])
        assert_equal({value3.to_sym => [record3]}, data[key3.to_sym])
    end

    def test_browse_keys_timestr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        key3 = TestUtils.random_string
        value1 = "A"
        value2 = "B"
        value3 = "C"
        record1 = 1
        record2 = 2
        record3 = 3
        @client.add key1, value1, record1
        @client.add key2, value2, record2
        @client.add key3, value3, record3
        anchor = get_time_anchor
        @client.add key1, "Foo"
        @client.add key2, "Foo"
        @client.add key3, "Foo"
        time = get_elapsed_millis_string anchor
        data = @client.browse [key1, key2, key3], time:time
        assert_equal({value1.to_sym => [record1]}, data[key1.to_sym])
        assert_equal({value2.to_sym => [record2]}, data[key2.to_sym])
        assert_equal({value3.to_sym => [record3]}, data[key3.to_sym])
    end

    def test_chronologize_key_record
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        @client.add key, 2, record
        @client.add key, 3, record
        @client.remove key, 1, record
        @client.remove key, 2, record
        @client.remove key, 3, record
        data = @client.chronologize key:key, record:record
        assert_equal [[1], [1, 2], [1, 2, 3], [2, 3], [3]], data.values
    end

    def test_chronologize_key_record_start
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        @client.add key, 2, record
        @client.add key, 3, record
        start = @client.time
        @client.remove key, 1, record
        @client.remove key, 2, record
        @client.remove key, 3, record
        data = @client.chronologize key:key, record:record, start:start
        assert_equal [[2, 3], [3]], data.values
    end

    def test_chronologize_key_record_startstr
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        @client.add key, 2, record
        @client.add key, 3, record
        anchor = get_time_anchor
        @client.remove key, 1, record
        @client.remove key, 2, record
        @client.remove key, 3, record
        start = get_elapsed_millis_string anchor
        data = @client.chronologize key:key, record:record, start:start
        assert_equal [[2, 3], [3]], data.values
    end

    def test_chronologize_key_record_start_end
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        @client.add key, 2, record
        @client.add key, 3, record
        start = @client.time
        @client.remove key, 1, record
        tend = @client.time
        @client.remove key, 2, record
        @client.remove key, 3, record
        data = @client.chronologize key:key, record:record, time:start, end:tend
        assert_equal [[2, 3]], data.values
    end

    def test_chronologize_key_record_startstr_endstr
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        @client.add key, 2, record
        @client.add key, 3, record
        sanchor = get_time_anchor
        @client.remove key, 1, record
        eanchor = get_time_anchor
        @client.remove key, 2, record
        @client.remove key, 3, record
        start = get_elapsed_millis_string sanchor
        tend = get_elapsed_millis_string eanchor
        data = @client.chronologize key:key, record:record, time:start, end:tend
        assert_equal [[2, 3]], data.values
    end

    def test_clear_key_record
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        @client.add key, 2, record
        @client.add key, 3, record
        @client.clear key, record
        data = @client.select key:key, record:record
        assert_equal([], data)
    end

    def test_clear_key_records
        key = TestUtils.random_string
        records = [1, 2, 3]
        @client.add key, 1, records
        @client.add key, 2, records
        @client.add key, 3, records
        @client.clear key, records
        data = @client.select key:key, record:records
        assert_equal({}, data)
    end

    def test_clear_keys_record
        key1 = TestUtils.random_string 6
        key2 = TestUtils.random_string 7
        key3 = TestUtils.random_string 8
        record = TestUtils.random_integer
        @client.add key1, 1, record
        @client.add key2, 2, record
        @client.add key3, 3, record
        @client.clear [key1, key2, key3], record
        data = @client.select keys:[key1, key2, key3], record:record
        assert_equal({}, data)
    end

    def test_clear_keys_records
        data = {"a" => "A", "b" => "B", "c" => ["C", true], "d" => "D"}
        records = [1, 2, 3]
        @client.insert(data, records)
        @client.clear ['a', 'b', 'c'], records
        data = @client.get key:"d", records: records
        assert_equal({1 => "D", 2 => "D", 3 => "D"}, data)
    end

    def test_clear_record
        data = {
            "a" => "A",
            "B" => "B",
            "C" => ["C", true]
        }
        record = @client.insert(data)[0]
        @client.clear record
        data = @client.select record:record
        assert_equal({}, data)
    end

    def test_clear_records
        data = {
            'a'=> 'A',
            'b'=> 'B',
            'c'=> ['C', true],
            'd'=> 'D'
        }
        records = [1, 2, 3]
        @client.insert data, records
        @client.clear records
        data = @client.select records
        assert_equal({1=> {}, 2 => {}, 3 => {}}, data)
    end

    def test_commit
        @client.stage
        record = @client.add "name", "jeff nelson"
        @client.commit
        assert_equal(["name"], @client.describe(record))
    end

    def test_describe_record
        @client.set "name", "tom brady", 1
        @client.set "age", 100, 1
        @client.set "team", "new england patriots", 1
        keys = @client.describe 1
        assert_equal ["name", "age", "team"].sort!, keys.sort!
    end

    def test_describe_record_time
        @client.set "name", "tom brady", 1
        @client.set "age", 100, 1
        @client.set "team", "new england patriots", 1
        time = @client.time
        @client.clear "name", 1
        keys = @client.describe 1, time
        assert_equal ["name", "age", "team"].sort!, keys.sort!
    end

    def test_describe_record_timestr
        @client.set "name", "tom brady", 1
        @client.set "age", 100, 1
        @client.set "team", "new england patriots", 1
        anchor = get_time_anchor
        @client.clear "name", 1
        time = get_elapsed_millis_string anchor
        keys = @client.describe 1, time
        assert_equal ["name", "age", "team"].sort!, keys.sort!
    end

    def test_describe_records
        records = [1, 2, 3]
        @client.set "name", "tom brady", records
        @client.set "age", 100, records
        @client.set "team", "new england patriots", records
        keys = @client.describe records
        assert_equal ["name", "age", "team"].sort!, keys[1].sort!
        assert_equal ["name", "age", "team"].sort!, keys[2].sort!
        assert_equal ["name", "age", "team"].sort!, keys[3].sort!
    end

    def test_describe_records_time
        records = [1, 2, 3]
        @client.set "name", "tom brady", records
        @client.set "age", 100, records
        @client.set "team", "new england patriots", records
        time = @client.time
        @client.clear records
        keys = @client.describe records, time
        assert_equal ["name", "age", "team"].sort!, keys[1].sort!
        assert_equal ["name", "age", "team"].sort!, keys[2].sort!
        assert_equal ["name", "age", "team"].sort!, keys[3].sort!
    end

    def test_describe_records_timestr
        records = [1, 2, 3]
        @client.set "name", "tom brady", records
        @client.set "age", 100, records
        @client.set "team", "new england patriots", records
        anchor = get_time_anchor
        @client.clear records
        time = get_elapsed_millis_string anchor
        keys = @client.describe records, time
        assert_equal ["name", "age", "team"].sort!, keys[1].sort!
        assert_equal ["name", "age", "team"].sort!, keys[2].sort!
        assert_equal ["name", "age", "team"].sort!, keys[3].sort!
    end

    def test_diff_key_record_start
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        start = @client.time
        @client.add key, 2, record
        @client.remove key, 1, record
        diff = @client.diff key, record, start
        assert_equal [2], diff[Diff::ADDED]
        assert_equal [1], diff[Diff::REMOVED]
    end

    def test_diff_key_record_startstr
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        anchor = get_time_anchor
        @client.add key, 2, record
        @client.remove key, 1, record
        start = get_elapsed_millis_string anchor
        diff = @client.diff key, record, start
        assert_equal [2], diff[Diff::ADDED]
        assert_equal [1], diff[Diff::REMOVED]
    end

    def test_diff_key_record_start_end
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        start = @client.time
        @client.add key, 2, record
        @client.remove key, 1, record
        tend = @client.time
        @client.set key, 3, record
        diff = @client.diff key, record, start, tend
        assert_equal [2], diff[Diff::ADDED]
        assert_equal [1], diff[Diff::REMOVED]
    end

    def test_diff_key_record_startstr_endstr
        key = TestUtils.random_string
        record = TestUtils.random_integer
        @client.add key, 1, record
        sanchor = get_time_anchor
        @client.add key, 2, record
        @client.remove key, 1, record
        eanchor = get_time_anchor
        @client.set key, 3, record
        start = get_elapsed_millis_string sanchor
        tend = get_elapsed_millis_string eanchor
        diff = @client.diff key, record, start, tend
        assert_equal [2], diff[Diff::ADDED]
        assert_equal [1], diff[Diff::REMOVED]
    end

    def test_diff_key_start
        key = TestUtils.random_string
        @client.add key, 1, 1
        start = @client.time
        @client.add key, 2, 1
        @client.add key, 1, 2
        @client.add key, 3, 3
        @client.remove key, 1, 2
        diff = @client.diff key:key, start:start
        assert_equal 2, diff.length
        diff2 = diff[2]
        diff3 = diff[3]
        assert_equal [1], diff2[Diff::ADDED]
        assert_equal [3], diff3[Diff::ADDED]
        assert_equal nil, diff2[Diff::REMOVED]
        assert_equal nil, diff3[Diff::REMOVED]
    end

    def test_diff_key_startstr
        key = TestUtils.random_string
        @client.add key, 1, 1
        anchor = get_time_anchor
        @client.add key, 2, 1
        @client.add key, 1, 2
        @client.add key, 3, 3
        @client.remove key, 1, 2
        start = get_elapsed_millis_string anchor
        diff = @client.diff key:key, start:start
        assert_equal 2, diff.length
        diff2 = diff[2]
        diff3 = diff[3]
        assert_equal [1], diff2[Diff::ADDED]
        assert_equal [3], diff3[Diff::ADDED]
        assert_equal nil, diff2[Diff::REMOVED]
        assert_equal nil, diff3[Diff::REMOVED]
    end

    def test_diff_key_start_end
        key = TestUtils.random_string
        @client.add key, 1, 1
        start = @client.time
        @client.add key, 2, 1
        @client.add key, 1, 2
        @client.add key, 3, 3
        @client.remove key, 1, 2
        tend = @client.time
        @client.add key, 4, 1
        diff = @client.diff key:key, start:start, end:tend
        assert_equal 2, diff.length
        diff2 = diff[2]
        diff3 = diff[3]
        assert_equal [1], diff2[Diff::ADDED]
        assert_equal [3], diff3[Diff::ADDED]
        assert_equal nil, diff2[Diff::REMOVED]
        assert_equal nil, diff3[Diff::REMOVED]
    end

    def test_diff_key_startstr_endstr
        key = TestUtils.random_string
        @client.add key, 1, 1
        sanchor = get_time_anchor
        @client.add key, 2, 1
        @client.add key, 1, 2
        @client.add key, 3, 3
        @client.remove key, 1, 2
        eanchor = get_time_anchor
        @client.add key, 4, 1
        start = get_elapsed_millis_string sanchor
        tend = get_elapsed_millis_string eanchor
        diff = @client.diff key:key, start:start, end:tend
        assert_equal 2, diff.length
        diff2 = diff[2]
        diff3 = diff[3]
        assert_equal [1], diff2[Diff::ADDED]
        assert_equal [3], diff3[Diff::ADDED]
        assert_equal nil, diff2[Diff::REMOVED]
        assert_equal nil, diff3[Diff::REMOVED]
    end

    def test_diff_record_start
        @client.add "foo", 1, 1
        start = @client.time
        @client.set "foo", 2, 1
        @client.add "bar", true, 1
        diff = @client.diff record:1, time:start
        assert_equal [1], diff[:foo][Diff::REMOVED]
        assert_equal [2], diff[:foo][Diff::ADDED]
        assert_equal [true], diff[:bar][Diff::ADDED]
    end

    def test_diff_record_startstr
        @client.add "foo", 1, 1
        anchor = get_time_anchor
        @client.set "foo", 2, 1
        @client.add "bar", true, 1
        start = get_elapsed_millis_string anchor
        diff = @client.diff record:1, time:start
        assert_equal [1], diff[:foo][Diff::REMOVED]
        assert_equal [2], diff[:foo][Diff::ADDED]
        assert_equal [true], diff[:bar][Diff::ADDED]
    end

    def test_diff_record_start_end
        @client.add "foo", 1, 1
        start = @client.time
        @client.set "foo", 2, 1
        @client.add "bar", true, 1
        tend = @client.time
        @client.set "car", 100, 1
        diff = @client.diff record:1, time:start, end:tend
        assert_equal [1], diff[:foo][Diff::REMOVED]
        assert_equal [2], diff[:foo][Diff::ADDED]
        assert_equal [true], diff[:bar][Diff::ADDED]
    end

    def test_diff_record_startstr_endstr
        @client.add "foo", 1, 1
        sanchor = get_time_anchor
        @client.set "foo", 2, 1
        @client.add "bar", true, 1
        eanchor = get_time_anchor
        @client.set "car", 100, 1
        start = get_elapsed_millis_string sanchor
        tend = get_elapsed_millis_string eanchor
        diff = @client.diff record:1, time:start, end:tend
        assert_equal [1], diff[:foo][Diff::REMOVED]
        assert_equal [2], diff[:foo][Diff::ADDED]
        assert_equal [true], diff[:bar][Diff::ADDED]
    end

    def test_find_ccl
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        records = @client.find "#{key} > 3"
        assert_equal (4..10).to_a, records
    end

    def test_find_ccl_handle_parse_exception
        assert_raise Concourse::ParseException do
            @client.find("throw parse exception")
        end
    end

    def test_find_key_operator_value
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        records = @client.find key:key, operator:Operator::EQUALS, value:5
        assert_equal [5], records
    end

    def test_find_key_operator_values
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        records = @client.find key:key, operator:Operator::BETWEEN, values:[3,6]
        assert_equal [3,4,5], records
    end

    def test_find_key_operator_values_time
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        time = @client.time
        (1..10).step(1) do |x|
            @client.add key, x, x+1
        end
        records = @client.find key:key, operator:Operator::BETWEEN, values:[3,6], timestamp:time
        assert_equal [3,4,5], records
    end

    def test_find_key_operator_values_timestr
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        anchor = self.get_time_anchor
        (1..10).step(1) do |x|
            @client.add key, x, x+1
        end
        time = self.get_elapsed_millis_string anchor
        records = @client.find key:key, operator:Operator::BETWEEN, values:[3,6], timestamp:time
        assert_equal [3,4,5], records
    end

    def test_find_key_operatorstr_values_time
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        time = @client.time
        (1..10).step(1) do |x|
            @client.add key, x, x+1
        end
        records = @client.find key:key, operator:"bw", values:[3,6], timestamp:time
        assert_equal [3,4,5], records
    end

    def test_find_key_operatorstr_values_timestr
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        anchor = self.get_time_anchor
        (1..10).step(1) do |x|
            @client.add key, x, x+1
        end
        time = self.get_elapsed_millis_string anchor
        records = @client.find key:key, operator:"bw", values:[3,6], timestamp:time
        assert_equal [3,4,5], records
    end

    def test_find_key_operatorstr_value
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        records = @client.find key:key, operator:">", value:5
        assert_equal [6, 7, 8, 9, 10], records
    end

    def test_find_key_operatorstr_values
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        records = @client.find key:key, operator:"bw", values:[3,6]
        assert_equal [3,4,5], records
    end

    def test_find_key_operator_value_time
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        time = @client.time
        (1..10).step(1) do |x|
            @client.add key, 5, x
        end
        records = @client.find key:key, operator:Operator::EQUALS, value:5, timestamp:time
        assert_equal [5], records
    end

    def test_find_key_operatorstr_value_time
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        time = @client.time
        (1..10).step(1) do |x|
            @client.add key, 5, x
        end
        records = @client.find key:key, operator:"=", value:5, timestamp:time
        assert_equal [5], records
    end

    def test_find_key_operator_value_timestr
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        anchor = self.get_time_anchor
        (1..10).step(1) do |x|
            @client.add key, 5, x
        end
        time = self.get_elapsed_millis_string anchor
        records = @client.find key:key, operator:Operator::EQUALS, value:5, timestamp:time
        assert_equal [5], records
    end

    def test_find_key_operatorstr_value_timestr
        key = TestUtils.random_string
        (1..10).step(1) do |x|
            @client.add key, x, x
        end
        anchor = self.get_time_anchor
        (1..10).step(1) do |x|
            @client.add key, 5, x
        end
        time = self.get_elapsed_millis_string anchor
        records = @client.find key:key, operator:"=", value:5, timestamp:time
        assert_equal [5], records
    end

    def test_get_ccl
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        ccl = "#{key2} = 10"
        data = @client.get ccl:ccl
        expected = {key1.to_sym => 3, key2.to_sym => 10}
        assert_equal expected, data.fetch(record1)
        assert_equal expected, data.fetch(record2)
    end

    def test_get_ccl_time
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        time = @client.time
        @client.add key2, 11, [record1, record2]
        ccl = "#{key2} = 10"
        data = @client.get ccl:ccl, time:time
        expected = {key1.to_sym => 3, key2.to_sym => 10}
        assert_equal expected, data.fetch(record1)
        assert_equal expected, data.fetch(record2)
    end

    def test_get_ccl_timestr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        anchor = self.get_time_anchor
        @client.add key2, 11, [record1, record2]
        ccl = "#{key2} = 10"
        time = self.get_elapsed_millis_string anchor
        data = @client.get ccl:ccl, time:time
        expected = {key1.to_sym => 3, key2.to_sym => 10}
        assert_equal expected, data.fetch(record1)
        assert_equal expected, data.fetch(record2)
    end

    def test_get_key_ccl
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        ccl = "#{key2} = 10"
        data = @client.get key:key1, ccl:ccl
        expected = {record1 => 3, record2 => 4}
        assert_equal expected, data
    end

    def test_get_key_ccl_time
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        time = @client.time
        @client.set key1, 100, [record1, record2]
        ccl = "#{key2} = 10"
        data = @client.get ccl:ccl, key:key1, time:time
        expected = {record1 => 3, record2 => 4}
        assert_equal expected, data
    end

    def test_get_key_ccl_timestr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        anchor = self.get_time_anchor
        @client.set key1, 100, [record1, record2]
        ccl = "#{key2} = 10"
        time = self.get_elapsed_millis_string anchor
        data = @client.get ccl:ccl, key:key1, time:time
        expected = {record1 => 3, record2 => 4}
        assert_equal expected, data
    end

    def test_get_keys_ccl
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        ccl = "#{key2} = 10"
        data = @client.get ccl:ccl, key:[key1, key2]
        expected = {record1 => {key1.to_sym => 3, key2.to_sym => 10}, record2 => {key1.to_sym => 4, key2.to_sym => 10}}
        assert_equal expected, data
    end

    def test_get_keys_ccl_time
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        time = @client.time
        @client.set key1, 100, [record1, record2]
        ccl = "#{key2} = 10"
        data = @client.get ccl:ccl, key:[key1, key2], time:time
        expected = {record1 => {key1.to_sym => 3, key2.to_sym => 10}, record2 => {key1.to_sym => 4, key2.to_sym => 10}}
        assert_equal expected, data
    end

    def test_get_keys_ccl_timestr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        anchor = self.get_time_anchor
        @client.set key1, 100, [record1, record2]
        ccl = "#{key2} = 10"
        time = self.get_elapsed_millis_string anchor
        data = @client.get ccl:ccl, key:[key1, key2], time:time
        expected = {record1 => {key1.to_sym => 3, key2.to_sym => 10}, record2 => {key1.to_sym => 4, key2.to_sym => 10}}
        assert_equal expected, data
    end

    def test_get_key_record
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "foo", 3, 1
        assert_equal(3, @client.get("foo", record:1))
    end

    def test_get_key_record_time
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "foo", 3, 1
        ts = @client.time
        @client.add "foo", 4, 1
        assert_equal(3, @client.get("foo", 1, time:ts))
    end

    def test_get_key_record_timestr
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "foo", 3, 1
        anchor = get_time_anchor
        @client.add "foo", 4, 1
        ts = get_elapsed_millis_string anchor
        assert_equal(3, @client.get("foo", 1, time:ts))
    end

    def test_get_key_records
        @client.add "foo", 1, [1, 2, 3]
        @client.add "foo", 2, [1, 2, 3]
        @client.add "foo", 3, [1, 2, 3]
        assert_equal({1 => 3, 2 => 3, 3 =>3}, @client.get(key:"foo", records:[1, 2, 3]))
    end

    def test_get_key_records_time
        @client.add "foo", 1, [1, 2, 3]
        @client.add "foo", 2, [1, 2, 3]
        @client.add "foo", 3, [1, 2, 3]
        ts = @client.time
        @client.add "foo", 4, [1, 2, 3]
        assert_equal({1 => 3, 2 => 3, 3 =>3}, @client.get(key:"foo", records:[1, 2, 3], time:ts))
    end

    def test_get_key_records_timestr
        @client.add "foo", 1, [1, 2, 3]
        @client.add "foo", 2, [1, 2, 3]
        @client.add "foo", 3, [1, 2, 3]
        anchor = get_time_anchor
        @client.add "foo", 4, [1, 2, 3]
        ts = get_elapsed_millis_string anchor
        assert_equal({1 => 3, 2 => 3, 3 =>3}, @client.get(key:"foo", records:[1, 2, 3], time:ts))
    end

    def test_get_keys_record
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "bar", 1, 1
        @client.add "bar", 2, 1
        data = @client.get(keys:["foo", "bar"], record:1)
        expected = {:foo => 2, :bar => 2}
        assert_equal data, expected
    end

    def test_get_keys_record_time
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "bar", 1, 1
        @client.add "bar", 2, 1
        time = @client.time
        @client.add "foo", 3, 1
        @client.add "bar", 3, 1
        data = @client.get(keys:["foo", "bar"], record:1, time:time)
        expected = {:foo => 2, :bar => 2}
        assert_equal data, expected
    end

    def test_get_keys_record_timestr
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "bar", 1, 1
        @client.add "bar", 2, 1
        anchor = get_time_anchor
        @client.add "foo", 3, 1
        @client.add "bar", 3, 1
        time = get_elapsed_millis_string anchor
        data = @client.get(keys:["foo", "bar"], record:1, time:time)
        expected = {:foo => 2, :bar => 2}
        assert_equal data, expected
    end

    def test_get_keys_records
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        data = @client.get keys: ["foo", "bar"], records: [1, 2]
        expected = {:foo => 2, :bar => 2}
        assert_equal({1 => expected, 2 => expected}, data)
    end

    def test_get_keys_records_time
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        time = @client.time
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        data = @client.get keys: ["foo", "bar"], records: [1, 2], time:time
        expected = {:foo => 2, :bar => 2}
        assert_equal({1 => expected, 2 => expected}, data)
    end

    def test_get_keys_records_timestr
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        anchor = get_time_anchor
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        time = get_elapsed_millis_string anchor
        data = @client.get keys: ["foo", "bar"], records: [1, 2], time:time
        expected = {:foo => 2, :bar => 2}
        assert_equal({1 => expected, 2 => expected}, data)
    end

    def test_insert_hash
        data = {
            :string => "a",
            :int => 1,
            :double => 3.14,
            :bool => true,
            :multi => ["a", 1, 3.14, true]
        }
        record = @client.insert(data:data)[0]
        assert_equal "a", @client.get("string", record:record)
        assert_equal 1, @client.get("int", record:record)
        assert_equal true, @client.get("bool", record:record)
        assert_equal ["a", 1, 3.14, true], @client.select(key:"multi", record:record)
    end

    def test_insert_json
        data = {
            :string => "a",
            :int => 1,
            :double => 3.14,
            :bool => true,
            :multi => ["a", 1, 3.14, true]
        }
        data = data.to_json
        record = @client.insert(data:data)[0]
        assert_equal "a", @client.get("string", record:record)
        assert_equal 1, @client.get("int", record:record)
        assert_equal true, @client.get("bool", record:record)
        assert_equal ["a", 1, 3.14, true], @client.select(key:"multi", record:record)
    end

    def test_insert_hashes
        data = [
            {:foo => 1},
            {:foo => 2},
            {:foo => 3}
        ]
        records = @client.insert(data:data)
        assert_equal data.length, records.length
    end

    def test_insert_json_list
        data = [
            {:foo => 1},
            {:foo => 2},
            {:foo => 3}
        ]
        count = data.length
        data = data.to_json
        records = @client.insert(data:data)
        assert_equal count, records.length
    end

    def test_insert_hash_record
        data = {
            :string => "a",
            :int => 1,
            :double => 3.14,
            :bool => true,
            :multi => ["a", 1, 3.14, true]
        }
        record = TestUtils.random_integer
        @client.insert(data:data, record:record)
        assert_equal "a", @client.get("string", record:record)
        assert_equal 1, @client.get("int", record:record)
        assert_equal true, @client.get("bool", record:record)
        assert_equal ["a", 1, 3.14, true], @client.select(key:"multi", record:record)
    end

    def test_insert_json_record
        data = {
            :string => "a",
            :int => 1,
            :double => 3.14,
            :bool => true,
            :multi => ["a", 1, 3.14, true]
        }
        data = data.to_json
        record = TestUtils.random_integer
        assert @client.insert(data:data, record:record)
        assert_equal "a", @client.get("string", record:record)
        assert_equal 1, @client.get("int", record:record)
        assert_equal true, @client.get("bool", record:record)
        assert_equal ["a", 1, 3.14, true], @client.select(key:"multi", record:record)
    end

    def test_insert_hash_records
        data = {
            :string => "a",
            :int => 1,
            :double => 3.14,
            :bool => true,
            :multi => ["a", 1, 3.14, true]
        }
        records = [TestUtils.random_integer, TestUtils.random_integer, TestUtils.random_integer]
        result = @client.insert(data:data, record:records)
        assert result[records[0]]
        assert result[records[1]]
        assert result[records[2]]
    end

    def test_insert_json_records
        data = {
            :string => "a",
            :int => 1,
            :double => 3.14,
            :bool => true,
            :multi => ["a", 1, 3.14, true]
        }
        data = data.to_json
        records = [TestUtils.random_integer, TestUtils.random_integer, TestUtils.random_integer]
        result = @client.insert(data:data, record:records)
        assert result[records[0]]
        assert result[records[1]]
        assert result[records[2]]
    end

    def test_inventory
        records = [1, 2, 3, 4, 5, 6, 7]
        @client.add "favorite_number", 17, records
        assert_equal records, @client.inventory
    end

    def test_jsonify_records
        record1 = 1
        record2 = 2
        data = {
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        }
        @client.insert data:data, records:[record1, record2]
        dump = @client.jsonify records:[record1, record2]
        expected = {
            "int" => [1],
            "multi" => [1, 2, 3, 4]
        }
        assert_equal [expected, expected], JSON.parse(dump)
    end

    def test_jsonify_records_identifier
        record1 = 1
        record2 = 2
        data = {
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        }
        @client.insert data:data, records:[record1, record2]
        dump = @client.jsonify records:[record1, record2], include_id:true
        expected1 = {
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse::Thrift::JSON_RESERVED_IDENTIFIER_NAME => 1
        }
        expected2 = {
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse::Thrift::JSON_RESERVED_IDENTIFIER_NAME => 2
        }
        assert_equal [expected1, expected2], JSON.parse(dump)
    end

    def test_jsonify_records_time
        record1 = 1
        record2 = 2
        data = {
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        }
        @client.insert data:data, records:[record1, record2]
        time = @client.time
        @client.add 'foo', 10, [record1, record2]
        dump = @client.jsonify records:[record1, record2], time:time
        expected = {
            "int" => [1],
            "multi" => [1, 2, 3, 4]
        }
        assert_equal [expected, expected], JSON.parse(dump)
    end

    def test_jsonify_records_timestr
        record1 = 1
        record2 = 2
        data = {
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        }
        @client.insert data:data, records:[record1, record2]
        anchor = get_time_anchor
        @client.add 'foo', 10, [record1, record2]
        time = get_elapsed_millis_string anchor
        dump = @client.jsonify records:[record1, record2], time:time
        expected = {
            "int" => [1],
            "multi" => [1, 2, 3, 4]
        }
        assert_equal [expected, expected], JSON.parse(dump)
    end

    def test_jsonify_records_identifier_time
        record1 = 1
        record2 = 2
        data = {
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        }
        @client.insert data:data, records:[record1, record2]
        time = @client.time
        @client.add "foo", 17, [record1, record2]
        dump = @client.jsonify records:[record1, record2], include_id:true, time:time
        expected1 = {
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse::Thrift::JSON_RESERVED_IDENTIFIER_NAME => 1
        }
        expected2 = {
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse::Thrift::JSON_RESERVED_IDENTIFIER_NAME => 2
        }
        assert_equal [expected1, expected2], JSON.parse(dump)
    end

    def test_jsonify_records_identifier_timestr
        record1 = 1
        record2 = 2
        data = {
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        }
        @client.insert data:data, records:[record1, record2]
        anchor = get_time_anchor
        @client.add "foo", 17, [record1, record2]
        time = get_elapsed_millis_string anchor
        dump = @client.jsonify records:[record1, record2], include_id:true, time:time
        expected1 = {
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse::Thrift::JSON_RESERVED_IDENTIFIER_NAME => 1
        }
        expected2 = {
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse::Thrift::JSON_RESERVED_IDENTIFIER_NAME => 2
        }
        assert_equal [expected1, expected2], JSON.parse(dump)
    end

    def test_ping_record
        record = 1
        assert !@client.ping(record)
        @client.add "foo", 1, record
        assert @client.ping record
        @client.clear "foo", record
        assert !@client.ping(record)
    end

    def test_ping_records
        @client.add "foo", 1, [1, 2]
        data = @client.ping [1, 2, 3]
        expected = {
            1 => true,
            2 => true,
            3 => false
        }
        assert_equal expected, data
    end

    def test_remove_key_value_record
        key = "foo"
        value = 1
        record = 1
        assert !@client.remove(key, value, record)
        @client.add key, value, record
        assert @client.remove key, value, record
    end

    def test_remove_key_value_records
        key = "foo"
        value = 1
        @client.add key, value, [1, 2]
        data = @client.remove key, value, [1, 2, 3]
        assert_equal({1=> true, 2=> true, 3=> false}, data)
    end

    def test_revert_key_records_time
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        time = @client.time
        @client.insert data:data2, records:[1,2,3]
        @client.revert key:"one", record:[1,2,3], time:time
        data = @client.select key:"one", records:[1,2,3]
        assert_equal({
            1 => [1],
            2 => [1],
            3 => [1]
        }, data)
    end

    def test_revert_key_records_timestr
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        anchor = get_time_anchor
        @client.insert data:data2, records:[1,2,3]
        time = get_elapsed_millis_string anchor
        @client.revert key:"one", record:[1,2,3], time:time
        data = @client.select key:"one", records:[1,2,3]
        assert_equal({
            1 => [1],
            2 => [1],
            3 => [1]
        }, data)
    end

    def test_revert_keys_records_time
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        time = @client.time
        @client.insert data:data2, records:[1,2,3]
        @client.revert keys:["one", "two", "three"], record:[1,2,3], time:time
        data = @client.select keys:["one", "two", "three"], records:[1,2,3]
        data3 = {
            :one => [1],
            :two => [2],
            :three => [3]
        }
        assert_equal({
            1 => data3,
            2 => data3,
            3 => data3
        }, data)
    end

    def test_revert_keys_records_timestr
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        anchor = get_time_anchor
        @client.insert data:data2, records:[1,2,3]
        time = get_elapsed_millis_string anchor
        @client.revert keys:["one", "two", "three"], record:[1,2,3], time:time
        data = @client.select keys:["one", "two", "three"], records:[1,2,3]
        data3 = {
            :one => [1],
            :two => [2],
            :three => [3]
        }
        assert_equal({
            1 => data3,
            2 => data3,
            3 => data3
        }, data)
    end

    def test_revert_keys_record_time
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        time = @client.time
        @client.insert data:data2, records:[1,2,3]
        @client.revert key:["one", "two", "three"], record:1, time:time
        data = @client.select key:["one", "two", "three"], records:1
        assert_equal({
            :one => [1],
            :two => [2],
            :three => [3]
        }, data)
    end

    def test_revert_keys_record_timestr
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        anchor = get_time_anchor
        @client.insert data:data2, records:[1,2,3]
        time = get_elapsed_millis_string anchor
        @client.revert key:["one", "two", "three"], record:1, time:time
        data = @client.select key:["one", "two", "three"], records:1
        assert_equal({
            :one => [1],
            :two => [2],
            :three => [3]
        }, data)
    end

    def test_revert_key_record_time
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        time = @client.time
        @client.insert data:data2, records:[1,2,3]
        @client.revert key:"one", record:1, time:time
        data = @client.select key:"one", records:1
        assert_equal([1], data)
    end

    def test_revert_key_record_timestr
        data1 = {
            'one'=> 1,
            'two'=> 2,
            'three'=> 3
        }
        data2 = {
            'one'=> true,
            'two'=> true,
            'three'=> true
        }
        @client.insert data:data1, records:[1,2,3]
        anchor = get_time_anchor
        @client.insert data:data2, records:[1,2,3]
        time = get_elapsed_millis_string anchor
        @client.revert key:"one", record:1, time:time
        data = @client.select key:"one", records:1
        assert_equal([1], data)
    end

    def test_search
        @client.add "name", "jeff", 1
        @client.add "name", "jeffery", 2
        @client.add "name", "jeremy", 3
        @client.add "name", "ben jefferson", 4
        records = @client.search "name", "jef"
        assert_equal([1, 2, 4], records)
    end

    def test_select_ccl
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        ccl = "#{key2} = 10"
        data = @client.select ccl:ccl
        expected = {
            key1.to_sym => [1, 2, 3],
            key2.to_sym => [10]
        }
        assert_equal expected, data[record1]
        assert_equal expected, data[record2]
    end

    def test_select_ccl_time
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        time = @client.time
        @client.set key2, 11, [record1, record2]
        ccl = "#{key2} > 10"
        data = @client.select ccl:ccl, time:time # the timestamp does not affect the criteria. The criteria queries the present state, but the data selection happends relative to the timestamp
        expected = {
            key1.to_sym => [1, 2, 3],
            key2.to_sym => [10]
        }
        assert_equal expected, data[record1]
        assert_equal expected, data[record2]
    end

    def test_select_ccl_timestr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        anchor = get_time_anchor
        @client.set key2, 11, [record1, record2]
        ccl = "#{key2} > 10"
        time = get_elapsed_millis_string anchor
        data = @client.select ccl:ccl, time:time # the timestamp does not affect the criteria. The criteria queries the present state, but the data selection happends relative to the timestamp
        expected = {
            key1.to_sym => [1, 2, 3],
            key2.to_sym => [10]
        }
        assert_equal expected, data[record1]
        assert_equal expected, data[record2]
    end

    def test_select_key_ccl
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        ccl = "#{key2} = 10"
        data = @client.select key:key1, ccl:ccl
        expected = {
            record1 => [1, 2, 3],
            record2 => [1, 2, 3, 4]
        }
        assert_equal expected, data
    end

    def test_select_keys_ccl
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        ccl = "#{key2} = 10"
        data = @client.select key:[key1, key2], ccl:ccl
        expected = {
            record1 => {key1.to_sym => [1, 2, 3], key2.to_sym => [10]},
            record2 => {key1.to_sym => [1, 2, 3, 4], key2.to_sym => [10]}
        }
        assert_equal expected, data
    end

    def test_select_key_ccl_time
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        time = @client.time
        ccl = "#{key2} = 10"
        @client.set key1, 100, [record2, record1]
        data = @client.select key:key1, ccl:ccl, time:time
        expected = {
            record1 => [1, 2, 3],
            record2 => [1, 2, 3, 4]
        }
        assert_equal expected, data
    end

    def test_select_keys_ccl_time
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        time = @client.time
        ccl = "#{key2} = 10"
        @client.set key1, 100, [record2, record1]
        data = @client.select key:[key1, key2], ccl:ccl, time:time
        expected = {
            record1 => {key1.to_sym => [1, 2, 3], key2.to_sym => [10]},
            record2 => {key1.to_sym => [1, 2, 3, 4], key2.to_sym => [10]}
        }
        assert_equal expected, data
    end

    def test_select_key_ccl_timestr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        anchor = get_time_anchor
        ccl = "#{key2} = 10"
        @client.set key1, 100, [record2, record1]
        time = get_elapsed_millis_string anchor
        data = @client.select key:key1, ccl:ccl, time:time
        expected = {
            record1 => [1, 2, 3],
            record2 => [1, 2, 3, 4]
        }
        assert_equal expected, data
    end

    def test_select_keys_ccl_timestr
        key1 = TestUtils.random_string
        key2 = TestUtils.random_string
        record1 = TestUtils.random_integer
        record2 = TestUtils.random_integer
        @client.add key1, 1, [record1, record2]
        @client.add key1, 2, [record1, record2]
        @client.add key1, 3, [record1, record2]
        @client.add key2, 10, [record1, record2]
        @client.add key1, 4, record2
        anchor = get_time_anchor
        ccl = "#{key2} = 10"
        @client.set key1, 100, [record2, record1]
        time = get_elapsed_millis_string anchor
        data = @client.select key:[key1, key2], ccl:ccl, time:time
        expected = {
            record1 => {key1.to_sym => [1, 2, 3], key2.to_sym => [10]},
            record2 => {key1.to_sym => [1, 2, 3, 4], key2.to_sym => [10]}
        }
        assert_equal expected, data
    end

    def test_select_key_record
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "foo", 3, 1
        assert_equal([1, 2, 3], @client.select(key:"foo", record:1))
    end

    def test_select_key_record_time
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "foo", 3, 1
        time = @client.time
        @client.add "foo", 4, 1
        assert_equal([1, 2, 3], @client.select(key:"foo", record:1, time:time))
    end

    def test_select_key_record_timestr
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "foo", 3, 1
        anchor = get_time_anchor
        @client.add "foo", 4, 1
        time = get_elapsed_millis_string anchor
        assert_equal([1, 2, 3], @client.select(key:"foo", record:1, time:time))
    end

    def test_select_key_records
        @client.add "foo", 1, [1, 2, 3]
        @client.add "foo", 2, [1, 2, 3]
        @client.add "foo", 3, [1, 2, 3]
        assert_equal({
            1 => [1, 2, 3],
            2 => [1, 2, 3],
            3 => [1, 2, 3]
        }, @client.select(key:"foo", record:[1, 2, 3]))
    end

    def test_selecT_key_records_time
        @client.add "foo", 1, [1, 2, 3]
        @client.add "foo", 2, [1, 2, 3]
        @client.add "foo", 3, [1, 2, 3]
        time = @client.time
        @client.add "foo", 4, [1, 2, 3]
        assert_equal({
            1 => [1, 2, 3],
            2 => [1, 2, 3],
            3 => [1, 2, 3]
        }, @client.select(key:"foo", record:[1, 2, 3], time:time))
    end

    def test_select_key_records_timestr
        @client.add "foo", 1, [1, 2, 3]
        @client.add "foo", 2, [1, 2, 3]
        @client.add "foo", 3, [1, 2, 3]
        anchor = get_time_anchor
        @client.add "foo", 4, [1, 2, 3]
        time = get_elapsed_millis_string anchor
        assert_equal({
            1 => [1, 2, 3],
            2 => [1, 2, 3],
            3 => [1, 2, 3]
        }, @client.select(key:"foo", record:[1, 2, 3], time:time))
    end

    def test_select_keys_record
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "bar", 1, 1
        @client.add "bar", 2, 1
        data = @client.select(keys:["foo", "bar"], record:1)
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal expected, data
    end

    def test_select_keys_record_time
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "bar", 1, 1
        @client.add "bar", 2, 1
        time = @client.time
        @client.add "foo", 3, 1
        @client.add "bar", 3, 1
        data = @client.select(keys:["foo", "bar"], record:1, time:time)
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal expected, data
    end

    def test_select_keys_record_timestr
        @client.add "foo", 1, 1
        @client.add "foo", 2, 1
        @client.add "bar", 1, 1
        @client.add "bar", 2, 1
        anchor = get_time_anchor
        @client.add "foo", 3, 1
        @client.add "bar", 3, 1
        time = get_elapsed_millis_string anchor
        data = @client.select(keys:["foo", "bar"], record:1, time:time)
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal expected, data
    end

    def test_select_keys_records
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        data = @client.select keys:["foo", "bar"], records:[1, 2]
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal({
            1 => expected,
            2 => expected
        }, data)
    end

    def test_select_keys_records_time
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        time = @client.time
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        data = @client.select keys:["foo", "bar"], records:[1, 2], time:time
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal({
            1 => expected,
            2 => expected
        }, data)
    end

    def test_select_keys_records_timestr
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        anchor = get_time_anchor
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        time = get_elapsed_millis_string anchor
        data = @client.select keys:["foo", "bar"], records:[1, 2], time:time
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal({
            1 => expected,
            2 => expected
        }, data)
    end

    def test_select_record
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        data = @client.select record:1
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal expected, data
    end

    def test_select_record_time
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        time = @client.time
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        data = @client.select record:1, time:time
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal expected, data
    end

    def test_select_record_timestr
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        anchor = get_time_anchor
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        time = get_elapsed_millis_string anchor
        data = @client.select record:1, time:time
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal expected, data
    end

    def test_select_records
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        data = @client.select record:[1,2]
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal({
            1 => expected,
            2 => expected
        }, data)
    end

    def test_select_records_time
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        time = @client.time
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        data = @client.select record:[1, 2], time:time
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal({
            1 => expected,
            2 => expected
        }, data)
    end

    def test_select_records_timestr
        @client.add "foo", 1, [1, 2]
        @client.add "foo", 2, [1, 2]
        @client.add "bar", 1, [1, 2]
        @client.add "bar", 2, [1, 2]
        anchor = get_time_anchor
        @client.add "foo", 3, [1, 2]
        @client.add "bar", 3, [1, 2]
        time = get_elapsed_millis_string anchor
        data = @client.select record:[1, 2], time:time
        expected = {
            :foo => [1, 2],
            :bar => [1, 2]
        }
        assert_equal({
            1 => expected,
            2 => expected
        }, data)
    end

    def test_set_key_value
        key = "foo"
        value = 1
        record = @client.set key, value
        data = @client.select record:record
        assert_equal({:foo => [1]}, data)
    end

    def test_set_key_value_record
        @client.add "foo", 2, 1
        @client.add "foo", 3, 1
        @client.set "foo", 1, 1
        data = @client.select record:1
        assert_equal({:foo => [1]}, data)
    end

    def test_set_key_value_records
        @client.add "foo", 2, [1, 2, 3]
        @client.add "foo", 3, [1, 2, 3]
        @client.set "foo", 1, [1, 2, 3]
        data = @client.select record: [1, 2, 3]
        expected = {:foo => [1]}
        assert_equal({1 => expected, 2 => expected, 3 => expected}, data)
    end

    def test_stage
        begin
            token =  @client.instance_variable_get("@transaction")
            assert token.nil?
            @client.stage
            token =  @client.instance_variable_get("@transaction")
            assert !token.nil?
        ensure
            @client.abort
        end
    end

    def test_stage_block
        @client.stage do
            @client.add "name", "jeff", 17
        end
        assert_equal("jeff", @client.get(key:"name", record:17))
    end

    def test_stage_block_transaction_exception
        assert_raise Concourse::TransactionException do
            @client.stage do
                @client.find(ccl:"throw transaction exception")
            end
        end
    end

    def test_stage_block_embedded
        @client.stage do
            @client.stage
        end
        token =  @client.instance_variable_get("@transaction")
        assert token.nil?
    end

    def test_time
        assert @client.time.is_a? Integer
    end

    def test_time_phrase
        assert @client.time("3 seconds ago").is_a? Integer
    end

    def test_verify_and_swap
        @client.add "foo", 2, 2
        assert !@client.verify_and_swap("foo", 1, 2, 3)
        assert @client.verify_and_swap("foo", 2, 2, 3)
        assert_equal 3, @client.get(key:"foo", record:2)
    end

    def test_verify_or_set
        @client.add "foo", 2, 2
        @client.verify_or_set "foo", 3, 2
        assert_equal 3, @client.get(key:"foo", record:2)
    end

    def test_verify_key_value_record
        @client.add "name", "jeff", 1
        @client.add "name", "jeffery", 1
        @client.add "name", "bob", 1
        assert @client.verify "name", "jeff", 1
        @client.remove "name", "jeff", 1
        assert(!@client.verify("name", "jeff", 1))
    end

    def test_verify_key_value_record_time
        @client.add "name", "jeff", 1
        @client.add "name", "jeffery", 1
        @client.add "name", "bob", 1
        time = @client.time
        @client.remove "name", "jeff", 1
        assert @client.verify "name", "jeff", 1, time:time
    end

    def test_verify_key_value_record_timestr
        @client.add "name", "jeff", 1
        @client.add "name", "jeffery", 1
        @client.add "name", "bob", 1
        anchor = get_time_anchor
        @client.remove "name", "jeff", 1
        time = get_elapsed_millis_string anchor
        assert @client.verify "name", "jeff", 1, time:time
    end

    def test_link_key_source_destination
        assert @client.link "friends", 1, 2
        assert_equal Concourse::Link.to(2), @client.get(key:"friends", record:1)
    end

    def test_link_key_source_destinations
        @client.link "friends", 1, 5
        assert_equal({
            1 => true,
            2 => true,
            3 => true,
            4 => true,
            5 => false}, @client.link("friends", 1, [1, 2, 3, 4, 5]))
    end

    def test_unlink_key_source_destination
        @client.link "friends", 1, 2
        assert @client.unlink "friends", 1, 2
    end

    def test_unlink_key_source_destinations
        @client.link "friends", 1, 2
        assert_equal({
            2 => true,
            3 => false
            }, @client.unlink("friends", 1, [2, 3]))
    end

    def test_find_or_add_key_value
        record = @client.find_or_add "age", 23
        assert_equal 23, @client.get("age", record:record)
    end

    def test_find_or_insert_ccl_json
        data = {
            :name => "Jeff Nelson"
        }
        data = data.to_json
        record = @client.find_or_insert criteria:"age > 10", data:data
        assert_equal "Jeff Nelson", @client.get(key:"name", record:record)
    end

    def test_find_or_insert_ccl_hash
        data = {
            :name => "Jeff Nelson"
        }
        record = @client.find_or_insert criteria:"age > 10", data:data
        assert_equal "Jeff Nelson", @client.get(key:"name", record:record)
    end

    def test_reconcile_empty_values
        @client.reconcile key:"foo", record:17, values:[]
        assert_equal 0, @client.select(key:"foo", record:17).length
    end

    def test_reconcile
        record = 1
        key = "testKey"
        @client.add(key, "A", record)
        @client.add(key, "C", record)
        @client.add(key, "D", record)
        @client.add(key, "E", record)
        @client.add(key, "F", record)
        values = ['A', 'B', 'D', 'G']
        @client.reconcile(key:key, record:record, values:values)
        stored = @client.select(key:key, record:record)
        assert_equal(values.sort!, stored.sort!)
    end

end
