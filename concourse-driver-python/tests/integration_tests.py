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

from nose.tools import *

import os
import time
from subprocess import *
import signal
from . import test_data
from concourse import Concourse, Tag, Link, Diff, Operator, constants
from concourse.thriftapi.shared.ttypes import Type
from concourse.thriftapi.complex.ttypes import ComplexTObject
from concourse.utils import python_to_thrift
import ujson
from tests import ignore
import socket


class IntegrationBaseTest(object):
    """
    Base class for unit tests that use Mockcourse.
    """

    port = None
    process = None
    client = None
    expected_network_latency = 0.05

    @classmethod
    def setup_class(cls):
        """ Fixture method to start Mockcourse and connect before the tests start to run.
        """
        port = IntegrationBaseTest.get_open_port()
        dir = os.path.dirname(os.path.realpath(__file__)) + '/../../mockcourse'
        script = dir + '/mockcourse '+str(port)
        cls.process = Popen(script, shell=True, preexec_fn=os.setsid)
        cls.client = None
        tries = 5
        while tries > 0 and cls.client is None:
            tries -= 1
            time.sleep(1)  # Wait for Mockcourse to start
            try:
                cls.client = Concourse.connect(port=port)
            except RuntimeError as e:
                if tries == 0:
                    raise e
                else:
                    continue

    @classmethod
    def teardown_class(cls):
        """ Fixture method to kill Mockcourse after all the tests have fun.
        """
        os.killpg(cls.process.pid, signal.SIGTERM)

    def tearDown(self):
        """" Logout" and clear all the data that the client stored in Mockcourse after each test. This ensures that the
        environment for each test is clean and predicatable.
        """
        self.client.logout()  # Mockcourse logout simply clears the content of the datastore

    def get_time_anchor(self):
        """ Return a time anchor and sleep for long enough to account for network latency
        """
        anchor = test_data.current_time_millis()
        time.sleep(self.expected_network_latency)
        return anchor

    @staticmethod
    def get_open_port():
        """Return an open port that is chosen by the OS
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("localhost", 0))
        port = sock.getsockname()[1]
        sock.close()
        return port


class TestPythonClientDriver(IntegrationBaseTest):
    """
    Implementations for standard unit tests that verify the Python client driver
    conforms to the Concourse standard
    """

    def __do_test_value_round_trip(self, value, ttype):
        """
        Do the round_trip test logic for the specified value of the specified type
        :param value:
        """
        key = test_data.random_string()
        record = self.client.add(key=key, value=value)
        stored = self.client.get(key=key, record=record)
        assert_equal(value, stored)
        assert_equal(python_to_thrift(stored).type, ttype)

    def test_string_round_trip(self):
        self.__do_test_value_round_trip(test_data.random_string(), Type.STRING)

    def test_bool_round_trip(self):
        self.__do_test_value_round_trip(test_data.random_bool(), Type.BOOLEAN)

    def test_tag_round_trip(self):
        self.__do_test_value_round_trip(Tag.create(test_data.random_string()), Type.TAG)

    def test_link_round_trip(self):
        self.__do_test_value_round_trip(Link.to(test_data.random_int()), Type.LINK)

    def test_int_round_trip(self):
        self.__do_test_value_round_trip(test_data.random_int(), Type.INTEGER)
        self.__do_test_value_round_trip(2147483647, Type.INTEGER)
        self.__do_test_value_round_trip(-2147483648, Type.INTEGER)

    def test_long_round_trip(self):
        self.__do_test_value_round_trip(2147483648, Type.LONG)
        self.__do_test_value_round_trip(-2147483649, Type.LONG)
        self.__do_test_value_round_trip(test_data.random_long(), Type.LONG)

    def test_float_round_trip(self):
        self.__do_test_value_round_trip(3.4028235E38, Type.DOUBLE)
        self.__do_test_value_round_trip(-1.4E-45, Type.DOUBLE)

    def test_abort(self):
        self.client.stage()
        key = test_data.random_string()
        value = "some value"
        record = 1
        self.client.add(key=key, value=value, record=record)
        self.client.abort()
        assert_is_none(self.client.get(key=key, record=record))

    def test_add_key_value(self):
        key = test_data.random_string()
        value = "static value"
        record = self.client.add(key=key, value=value)
        assert_is_not_none(record)
        stored = self.client.get(key=key, record=record)
        assert_equal(stored, value)

    def test_add_key_value_record(self):
        key = test_data.random_string()
        value = "static value"
        record = 17
        assert_true(self.client.add(key=key, value=value, record=record))
        stored = self.client.get(key=key, record=record)
        assert_equal(stored, value)

    def test_add_key_value_records(self):
        key = test_data.random_string()
        value = "static value"
        records = [1, 2, 3]
        result = self.client.add(key=key, value=value, records=records)
        assert_true(isinstance(result, dict))
        assert_true(result.get(1))
        assert_true(result.get(2))
        assert_true(result.get(3))

    def test_audit_key_record(self):
        key = test_data.random_string()
        values = ["one", "two", "three"]
        record = 1000
        for value in values:
            self.client.set(key, value, record)
        audit = self.client.audit(key, record)
        assert_equal(5, len(audit))
        expected = 'ADD'
        for k, v in audit.items():
            assert_true(v.startswith(expected))
            expected = 'REMOVE' if expected == 'ADD' else 'ADD'

    def test_audit_key_record_start(self):
        key = test_data.random_string()
        values = ["one", "two", "three"]
        record = 1001
        for value in values:
            self.client.set(key, value, record)
        start = self.client.time()
        values = [4, 5, 6]
        for value in values:
            self.client.set(key, value, record)
        audit = self.client.audit(key, record, start=start)
        assert_equal(6, len(audit))

    def test_audit_key_record_start_end(self):
        key = test_data.random_string()
        values = ["one", "two", "three"]
        record = 1002
        for value in values:
            self.client.set(key, value, record)
        start = self.client.time()
        values = [4, 5, 6]
        for value in values:
            self.client.set(key, value, record)
        end = self.client.time()
        values = [True, False]
        for value in values:
            self.client.set(key, value, record)
        audit = self.client.audit(key, record, start=start, end=end)
        assert_equal(6, len(audit))

    def test_audit_key_record_startstr(self):
        key = test_data.random_string()
        values = ["one", "two", "three"]
        record = 1001
        for value in values:
            self.client.set(key, value, record)
        anchor = self.get_time_anchor()
        values = [4, 5, 6]
        for value in values:
            self.client.set(key, value, record)
        start = test_data.get_elapsed_millis_string(anchor)
        audit = self.client.audit(key, record, start=start)
        assert_equal(6, len(audit))

    def test_audit_key_record_startstr_endstr(self):
        key = test_data.random_string()
        values = ["one", "two", "three"]
        record = 1002
        for value in values:
            self.client.set(key, value, record)
        start_anchor = self.get_time_anchor()
        values = [4, 5, 6]
        for value in values:
            self.client.set(key, value, record)
        end_anchor = self.get_time_anchor()
        values = [True, False]
        for value in values:
            self.client.set(key, value, record)
        start = test_data.get_elapsed_millis_string(start_anchor)
        end = test_data.get_elapsed_millis_string(end_anchor)
        audit = self.client.audit(key, record, start=start, end=end)
        assert_equal(6, len(audit))

    def test_audit_record(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value = "foo"
        record = 1002
        self.client.add(key1, value, record)
        self.client.add(key2, value, record)
        self.client.add(key3, value, record)
        audit = self.client.audit(record)
        assert_equal(3, len(audit))

    def test_audit_record_start(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value = "bar"
        record = 344
        self.client.add(key1, value, record)
        self.client.add(key2, value, record)
        self.client.add(key3, value, record)
        start = self.client.time()
        self.client.remove(key1, value, record)
        self.client.remove(key2, value, record)
        self.client.remove(key3, value, record)
        audit = self.client.audit(record, start=start)
        assert_equal(3, len(audit))

    def test_audit_record_start_end(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value = "bar"
        record = 344
        self.client.add(key1, value, record)
        self.client.add(key2, value, record)
        self.client.add(key3, value, record)
        start = self.client.time()
        self.client.remove(key1, value, record)
        self.client.remove(key2, value, record)
        self.client.remove(key3, value, record)
        end = self.client.time()
        self.client.add(key1, value, record)
        self.client.add(key2, value, record)
        self.client.add(key3, value, record)
        audit = self.client.audit(record, start=start, end=end)
        assert_equal(3, len(audit))

    def test_audit_record_startstr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value = "bar"
        record = 344
        self.client.add(key1, value, record)
        self.client.add(key2, value, record)
        self.client.add(key3, value, record)
        anchor = self.get_time_anchor()
        self.client.remove(key1, value, record)
        self.client.remove(key2, value, record)
        self.client.remove(key3, value, record)
        start = test_data.get_elapsed_millis_string(anchor)
        audit = self.client.audit(record, start=start)
        assert_equal(3, len(audit))

    def test_audit_record_startstr_endstr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value = "bar"
        record = 344
        self.client.add(key1, value, record)
        self.client.add(key2, value, record)
        self.client.add(key3, value, record)
        start_anchor = self.get_time_anchor()
        self.client.remove(key1, value, record)
        self.client.remove(key2, value, record)
        self.client.remove(key3, value, record)
        end_anchor = self.get_time_anchor()
        self.client.add(key1, value, record)
        self.client.add(key2, value, record)
        self.client.add(key3, value, record)
        start = test_data.get_elapsed_millis_string(start_anchor)
        end = test_data.get_elapsed_millis_string(end_anchor)
        audit = self.client.audit(record, start=start, end=end)
        assert_equal(3, len(audit))

    def test_browse_key(self):
        key = test_data.random_string()
        value = 10
        self.client.add(key, value, [1, 2, 3])
        value = test_data.random_string()
        self.client.add(key, value, [10, 20, 30])
        data = self.client.browse(key)
        assert_equal([1, 2, 3], data.get(10))
        assert_equal([20, 10, 30], data.get(value))

    def test_browse_keys(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value1 = "A"
        value2 = "B"
        value3 = "C"
        record1 = 1
        record2 = 2
        record3 = 3
        self.client.add(key1, value1, record1)
        self.client.add(key2, value2, record2)
        self.client.add(key3, value3, record3)
        data = self.client.browse([key1, key2, key3])
        assert_equal({value1: [record1]}, data.get(key1))
        assert_equal({value2: [record2]}, data.get(key2))
        assert_equal({value3: [record3]}, data.get(key3))

    def test_browse_keys_time(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value1 = "A"
        value2 = "B"
        value3 = "C"
        record1 = 1
        record2 = 2
        record3 = 3
        self.client.add(key1, value1, record1)
        self.client.add(key2, value2, record2)
        self.client.add(key3, value3, record3)
        time = self.client.time()
        self.client.add(key1, "Foo")
        self.client.add(key2, "Foo")
        self.client.add(key3, "Foo")
        data = self.client.browse([key1, key2, key3], time=time)
        assert_equal({value1: [record1]}, data.get(key1))
        assert_equal({value2: [record2]}, data.get(key2))
        assert_equal({value3: [record3]}, data.get(key3))

    def test_browse_key_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value1 = "A"
        value2 = "B"
        value3 = "C"
        record1 = 1
        record2 = 2
        record3 = 3
        self.client.add(key1, value1, record1)
        self.client.add(key2, value2, record2)
        self.client.add(key3, value3, record3)
        ts = test_data.get_elapsed_millis_string(self.get_time_anchor())
        data = self.client.browse([key1, key2, key3], time=ts)
        assert_equal({value1: [record1]}, data.get(key1))
        assert_equal({value2: [record2]}, data.get(key2))
        assert_equal({value3: [record3]}, data.get(key3))

    @ignore
    def test_browse_keys_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        key3 = test_data.random_string()
        value1 = "A"
        value2 = "B"
        value3 = "C"
        record1 = 1
        record2 = 2
        record3 = 3
        self.client.add(key1, value1, record1)
        self.client.add(key2, value2, record2)
        self.client.add(key3, value3, record3)
        anchor = self.get_time_anchor()
        self.client.add(key1, "D", record1)
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.browse([key1, key2, key3], time=ts)
        assert_equal({value1: [record1]}, data.get(key1))
        assert_equal({value2: [record2]}, data.get(key2))
        assert_equal({value3: [record3]}, data.get(key3))

    def test_browse_key_time(self):
        key = test_data.random_string()
        value = 10
        self.client.add(key, value, [1, 2, 3])
        value = test_data.random_string()
        self.client.add(key, value, [10, 20, 30])
        timestamp = self.client.time()
        self.client.add(key=key, value=True)
        data = self.client.browse(key, timestamp)
        assert_equal([1, 2, 3], data.get(10))
        assert_equal([20, 10, 30], data.get(value))

    def test_chronologize_key_record(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        self.client.add(key, 2, record)
        self.client.add(key, 3, record)
        self.client.remove(key, 1, record)
        self.client.remove(key, 2, record)
        self.client.remove(key, 3, record)
        data = self.client.chronologize(key, record)
        assert_equal([[1], [1, 2], [1, 2, 3], [2, 3], [3]], list(data.values()))

    def test_chronologize_key_record_start(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        self.client.add(key, 2, record)
        self.client.add(key, 3, record)
        start = self.client.time()
        self.client.remove(key, 1, record)
        self.client.remove(key, 2, record)
        self.client.remove(key, 3, record)
        data = self.client.chronologize(key, record, time=start)
        assert_equal([[2, 3], [3]], list(data.values()))

    def test_chronologize_key_record_start_end(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        self.client.add(key, 2, record)
        self.client.add(key, 3, record)
        start = self.client.time()
        self.client.remove(key, 1, record)
        end = self.client.time()
        self.client.remove(key, 2, record)
        self.client.remove(key, 3, record)
        data = self.client.chronologize(key, record, timestamp=start, end=end)
        assert_equal([[2, 3]], list(data.values()))

    def test_chronologize_key_record_startstr(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        self.client.add(key, 2, record)
        self.client.add(key, 3, record)
        anchor = self.get_time_anchor()
        self.client.remove(key, 1, record)
        self.client.remove(key, 2, record)
        self.client.remove(key, 3, record)
        start = test_data.get_elapsed_millis_string(anchor)
        data = self.client.chronologize(key, record, time=start)
        assert_equal([[2, 3], [3]], list(data.values()))

    def test_chronologize_key_record_startstr_endstr(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        self.client.add(key, 2, record)
        self.client.add(key, 3, record)
        start_anchor = self.get_time_anchor()
        self.client.remove(key, 1, record)
        end_anchor = self.get_time_anchor()
        self.client.remove(key, 2, record)
        self.client.remove(key, 3, record)
        start = test_data.get_elapsed_millis_string(start_anchor)
        end = test_data.get_elapsed_millis_string(end_anchor)
        data = self.client.chronologize(key, record, timestamp=start, end=end)
        assert_equal([[2, 3]], list(data.values()))

    def test_clear_key_record(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        self.client.add(key, 2, record)
        self.client.add(key, 3, record)
        self.client.clear(key=key, record=record)
        data = self.client.select(key=key, record=record)
        assert_equal([], data)

    def test_clear_key_records(self):
        key = test_data.random_string()
        records = [1, 2, 3]
        self.client.add(key, 1, records)
        self.client.add(key, 2, records)
        self.client.add(key, 3, records)
        self.client.clear(key=key, records=records)
        data = self.client.select(key=key, records=records)
        assert_equal({}, data)

    def test_clear_keys_record(self):
        key1 = test_data.random_string(6)
        key2 = test_data.random_string(7)
        key3 = test_data.random_string(8)
        record = test_data.random_long()
        self.client.add(key1, 1, record)
        self.client.add(key2, 2, record)
        self.client.add(key3, 3, record)
        self.client.clear(keys=[key1, key2, key3], record=record)
        data = self.client.select(keys=[key1, key2, key3], record=record)
        assert_equal({}, data)

    def test_clear_keys_records(self):
        data = {
            'a': 'A',
            'b': 'B',
            'c': ['C', True],
            'd': 'D'
        }
        records = [1, 2, 3]
        self.client.insert(data=data, records=records)
        self.client.clear(keys=['a', 'b', 'c'], records=records)
        data = self.client.get(key='d', records=records)
        assert_equal({
            1: 'D',
            2: 'D',
            3: 'D'
        }, data)

    def test_clear_record(self):
        data = {
            'a': 'A',
            'b': 'B',
            'c': ['C', True]
        }
        record = next(iter(self.client.insert(data)))
        self.client.clear(record=record)
        data = self.client.select(record=record)
        assert_equal({}, data)

    def test_clear_records(self):
        data = {
            'a': 'A',
            'b': 'B',
            'c': ['C', True],
            'd': 'D'
        }
        records = [1, 2, 3]
        self.client.insert(data=data, records=records)
        self.client.clear(records=records)
        data = self.client.select(records=records)
        assert_equal({1: {}, 2: {}, 3: {}}, data)

    def test_commit(self):
        self.client.stage()
        record = self.client.add("name", "jeff nelson")
        self.client.commit()
        assert_equal(['name'], list(self.client.describe(record)))

    def test_describe_record(self):
        self.client.set('name', 'tom brady', 1)
        self.client.set('age', 100, 1)
        self.client.set('team', 'new england patriots', 1)
        keys = self.client.describe(1)
        assert_equals(['age', 'name', 'team'], keys)

    def test_describe_record_time(self):
        self.client.set('name', 'tom brady', 1)
        self.client.set('age', 100, 1)
        self.client.set('team', 'new england patriots', 1)
        timestamp = self.client.time()
        self.client.clear('name', 1)
        keys = self.client.describe(1, time=timestamp)
        assert_equals(['age', 'name', 'team'], keys)

    def test_describe_record_timestr(self):
        self.client.set('name', 'tom brady', 1)
        self.client.set('age', 100, 1)
        self.client.set('team', 'new england patriots', 1)
        anchor = self.get_time_anchor()
        self.client.clear('name', 1)
        timestamp = test_data.get_elapsed_millis_string(anchor)
        keys = self.client.describe(1, time=timestamp)
        assert_equals(['age', 'name', 'team'], keys)

    def test_describe_records(self):
        records = [1, 2, 3]
        self.client.set('name', 'tom brady', records)
        self.client.set('age', 100, records)
        self.client.set('team', 'new england patriots', records)
        keys = self.client.describe(records)
        assert_equals(['age', 'name', 'team'], keys[1])
        assert_equals(['age', 'name', 'team'], keys[2])
        assert_equals(['age', 'name', 'team'], keys[3])

    def test_describe_records_time(self):
        records = [1, 2, 3]
        self.client.set('name', 'tom brady', records)
        self.client.set('age', 100, records)
        self.client.set('team', 'new england patriots', records)
        timestamp = self.client.time()
        self.client.clear(records=records)
        keys = self.client.describe(records, timestamp=timestamp)
        assert_equals(['age', 'name', 'team'], keys[1])
        assert_equals(['age', 'name', 'team'], keys[2])
        assert_equals(['age', 'name', 'team'], keys[3])

    def test_describe_records_timestr(self):
        records = [1, 2, 3]
        self.client.set('name', 'tom brady', records)
        self.client.set('age', 100, records)
        self.client.set('team', 'new england patriots', records)
        anchor = self.get_time_anchor()
        self.client.clear(records=records)
        timestamp = test_data.get_elapsed_millis_string(anchor)
        keys = self.client.describe(records, timestamp=timestamp)
        assert_equals(['age', 'name', 'team'], keys[1])
        assert_equals(['age', 'name', 'team'], keys[2])
        assert_equals(['age', 'name', 'team'], keys[3])

    def test_diff_key_record_start(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        start = self.client.time()
        self.client.add(key, 2, record)
        self.client.remove(key, 1, record)
        diff = self.client.diff(key, record, start)
        assert_equal([2], diff.get(Diff.ADDED))
        assert_equal([1], diff.get(Diff.REMOVED))

    def test_diff_key_record_startstr(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        anchor = self.get_time_anchor()
        self.client.add(key, 2, record)
        self.client.remove(key, 1, record)
        start = test_data.get_elapsed_millis_string(anchor)
        diff = self.client.diff(key, record, start)
        assert_equal([2], diff.get(Diff.ADDED))
        assert_equal([1], diff.get(Diff.REMOVED))

    def test_diff_key_record_start_end(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        start = self.client.time()
        self.client.add(key, 2, record)
        self.client.remove(key, 1, record)
        end = self.client.time()
        self.client.set(key, 3, record)
        diff = self.client.diff(key, record, start, end)
        assert_equal([2], diff.get(Diff.ADDED))
        assert_equal([1], diff.get(Diff.REMOVED))

    def test_diff_key_record_startstr_endstr(self):
        key = test_data.random_string()
        record = test_data.random_long()
        self.client.add(key, 1, record)
        start_anchor = self.get_time_anchor()
        self.client.add(key, 2, record)
        self.client.remove(key, 1, record)
        end_anchor = self.get_time_anchor()
        self.client.set(key, 3, record)
        start = test_data.get_elapsed_millis_string(start_anchor)
        end = test_data.get_elapsed_millis_string(end_anchor)
        diff = self.client.diff(key, record, start, end)
        assert_equal([2], diff.get(Diff.ADDED))
        assert_equal([1], diff.get(Diff.REMOVED))

    def test_diff_key_start(self):
        key = test_data.random_string()
        self.client.add(key=key, value=1, record=1)
        start = self.client.time()
        self.client.add(key=key, value=2, record=1)
        self.client.add(key=key, value=1, record=2)
        self.client.add(key=key, value=3, record=3)
        self.client.remove(key=key, value=1, record=2)
        diff = self.client.diff(key=key, start=start)
        assert_equal(2, len(diff.keys()))
        diff2 = diff.get(2)
        diff3 = diff.get(3)
        assert_equal([1], diff2.get(Diff.ADDED))
        assert_equal([3], diff3.get(Diff.ADDED))
        assert_is_none(diff2.get(Diff.REMOVED))
        assert_is_none(diff3.get(Diff.REMOVED))

    def test_diff_key_startstr(self):
        key = test_data.random_string()
        self.client.add(key=key, value=1, record=1)
        anchor = self.get_time_anchor()
        self.client.add(key=key, value=2, record=1)
        self.client.add(key=key, value=1, record=2)
        self.client.add(key=key, value=3, record=3)
        self.client.remove(key=key, value=1, record=2)
        start = test_data.get_elapsed_millis_string(anchor)
        diff = self.client.diff(key=key, start=start)
        assert_equal(2, len(diff.keys()))
        diff2 = diff.get(2)
        diff3 = diff.get(3)
        assert_equal([1], diff2.get(Diff.ADDED))
        assert_equal([3], diff3.get(Diff.ADDED))
        assert_is_none(diff2.get(Diff.REMOVED))
        assert_is_none(diff3.get(Diff.REMOVED))

    def test_diff_key_start_end(self):
        key = test_data.random_string()
        self.client.add(key=key, value=1, record=1)
        start = self.client.time()
        self.client.add(key=key, value=2, record=1)
        self.client.add(key=key, value=1, record=2)
        self.client.add(key=key, value=3, record=3)
        self.client.remove(key=key, value=1, record=2)
        end = self.client.time()
        self.client.add(key=key, value=4, record=1)
        diff = self.client.diff(key=key, start=start, end=end)
        assert_equal(2, len(diff.keys()))
        diff2 = diff.get(2)
        diff3 = diff.get(3)
        assert_equal([1], diff2.get(Diff.ADDED))
        assert_equal([3], diff3.get(Diff.ADDED))
        assert_is_none(diff2.get(Diff.REMOVED))
        assert_is_none(diff3.get(Diff.REMOVED))

    def test_diff_key_startstr_endstr(self):
        key = test_data.random_string()
        self.client.add(key=key, value=1, record=1)
        start_anchor = self.get_time_anchor()
        self.client.add(key=key, value=2, record=1)
        self.client.add(key=key, value=1, record=2)
        self.client.add(key=key, value=3, record=3)
        self.client.remove(key=key, value=1, record=2)
        end_anchor = self.get_time_anchor()
        self.client.add(key=key, value=4, record=1)
        start = test_data.get_elapsed_millis_string(start_anchor)
        end = test_data.get_elapsed_millis_string(end_anchor)
        diff = self.client.diff(key=key, start=start, end=end)
        assert_equal(2, len(diff.keys()))
        diff2 = diff.get(2)
        diff3 = diff.get(3)
        assert_equal([1], diff2.get(Diff.ADDED))
        assert_equal([3], diff3.get(Diff.ADDED))
        assert_is_none(diff2.get(Diff.REMOVED))
        assert_is_none(diff3.get(Diff.REMOVED))

    def test_diff_record_start(self):
        self.client.add(key="foo", value=1, record=1)
        start = self.client.time()
        self.client.set(key="foo", value=2, record=1)
        self.client.add(key="bar", value=True, record=1)
        diff = self.client.diff(record=1, time=start)
        assert_equal([1], diff.get('foo').get(Diff.REMOVED))
        assert_equal([2], diff.get('foo').get(Diff.ADDED))
        assert_equal([True], diff.get('bar').get(Diff.ADDED))

    def test_diff_record_startstr(self):
        self.client.add(key="foo", value=1, record=1)
        anchor = self.get_time_anchor()
        self.client.set(key="foo", value=2, record=1)
        self.client.add(key="bar", value=True, record=1)
        start = test_data.get_elapsed_millis_string(anchor)
        diff = self.client.diff(record=1, time=start)
        assert_equal([1], diff.get('foo').get(Diff.REMOVED))
        assert_equal([2], diff.get('foo').get(Diff.ADDED))
        assert_equal([True], diff.get('bar').get(Diff.ADDED))

    def test_diff_record_start_end(self):
        self.client.add(key="foo", value=1, record=1)
        start = self.client.time()
        self.client.set(key="foo", value=2, record=1)
        self.client.add(key="bar", value=True, record=1)
        end = self.client.time()
        self.client.set(key="car", value=100, record=1)
        diff = self.client.diff(record=1, time=start, end=end)
        assert_equal([1], diff.get('foo').get(Diff.REMOVED))
        assert_equal([2], diff.get('foo').get(Diff.ADDED))
        assert_equal([True], diff.get('bar').get(Diff.ADDED))

    def test_diff_record_startstr_endstr(self):
        self.client.add(key="foo", value=1, record=1)
        start_anchor = self.get_time_anchor()
        self.client.set(key="foo", value=2, record=1)
        self.client.add(key="bar", value=True, record=1)
        end_anchor = self.get_time_anchor()
        self.client.set(key="car", value=100, record=1)
        start = test_data.get_elapsed_millis_string(start_anchor)
        end = test_data.get_elapsed_millis_string(end_anchor)
        diff = self.client.diff(record=1, time=start, end=end)
        assert_equal([1], diff.get('foo').get(Diff.REMOVED))
        assert_equal([2], diff.get('foo').get(Diff.ADDED))
        assert_equal([True], diff.get('bar').get(Diff.ADDED))

    def test_find_ccl(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        records = list(self.client.find(key+' > 3'))
        assert_equal(list(range(4, 10)), records)

    @raises(Exception)
    def test_find_ccl_handle_parse_exception(self):
        self.client.find(ccl="throw parse exception")

    def test_find_key_operator_value(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        records = list(self.client.find(key=key, operator=Operator.GREATER_THAN, value=3))
        assert_equal(list(range(4, 10)), records)

    def test_find_key_operator_values(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        records = list(self.client.find(key=key, operator=Operator.BETWEEN, values=[3, 6]))
        assert_equal([3, 4, 5], records)

    def test_find_key_operator_value_time(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        ts = self.client.time()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        records = list(self.client.find(key=key, operator=Operator.GREATER_THAN, value=3, time=ts))
        assert_equal(list(range(4, 10)), records)

    def test_find_key_operator_value_timestr(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        anchor = self.get_time_anchor()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        ts = test_data.get_elapsed_millis_string(anchor)
        records = list(self.client.find(key=key, operator=Operator.GREATER_THAN, value=3, time=ts))
        assert_equal(list(range(4, 10)), records)

    def test_find_key_operator_values_time(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        ts = self.client.time()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        records = list(self.client.find(key=key, operator=Operator.BETWEEN, values=[3, 6], time=ts))
        assert_equal([3, 4, 5], records)

    def test_find_key_operator_values_timestr(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        anchor = self.get_time_anchor()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        ts = test_data.get_elapsed_millis_string(anchor)
        records = list(self.client.find(key=key, operator=Operator.BETWEEN, values=[3, 6], time=ts))
        assert_equal([3, 4, 5], records)

    def test_find_key_operatorstr_values_time(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        ts = self.client.time()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        records = list(self.client.find(key=key, operator="bw", values=[3, 6], time=ts))
        assert_equal([3, 4, 5], records)

    def test_find_key_operatorstr_values(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        records = list(self.client.find(key=key, operator="bw", values=[3, 6]))
        assert_equal([3, 4, 5], records)

    def test_find_key_operatorstr_values_timestr(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        anchor = self.get_time_anchor()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        ts = test_data.get_elapsed_millis_string(anchor)
        records = list(self.client.find(key=key, operator="bw", values=[3, 6], time=ts))
        assert_equal([3, 4, 5], records)

    def test_find_key_operatorstr_value(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        records = list(self.client.find(key=key, operator="gt", value=3))
        assert_equal(list(range(4, 10)), records)

    def test_find_key_operatorstr_value_time(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        ts = self.client.time()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        records = list(self.client.find(key=key, operator="gt", value=3, time=ts))
        assert_equal(list(range(4, 10)), records)

    def test_find_key_operatorstr_value_timestr(self):
        key = test_data.random_string()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n)
        anchor = self.get_time_anchor()
        for n in range(0, 10):
            self.client.add(key=key, value=n, record=n+1)
        ts = test_data.get_elapsed_millis_string(anchor)
        records = list(self.client.find(key=key, operator="gt", value=3, time=ts))
        assert_equal(list(range(4, 10)), records)

    def test_get_ccl(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        ccl = key2 + ' = 10'
        data = self.client.get(ccl=ccl)
        expected = {
            key1: 3,
            key2: 10
        }
        assert_equal(data.get(record1), expected)
        assert_equal(data.get(record2), expected)

    def test_get_ccl_time(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        ts = self.client.time()
        self.client.set(key=key2, value=11, records=[record1, record2])
        ccl = key2 + ' > 10'
        data = self.client.get(ccl=ccl, time=ts)
        expected = {
            key1: 3,
            key2: 10
        }
        assert_equal(data.get(record1), expected)
        assert_equal(data.get(record2), expected)

    def test_get_ccl_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        anchor = self.get_time_anchor()
        self.client.set(key=key2, value=11, records=[record1, record2])
        ccl = key2 + ' > 10'
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.get(ccl=ccl, time=ts)
        expected = {
            key1: 3,
            key2: 10
        }
        assert_equal(data.get(record1), expected)
        assert_equal(data.get(record2), expected)

    def test_get_key_ccl(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ccl = key2 + ' = 10'
        data = self.client.get(key=key1, ccl=ccl)
        expected = {
            record1: 3,
            record2: 4
        }
        assert_equal(expected, data)

    def test_get_keys_ccl(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ccl = key2 + ' = 10'
        data = self.client.get(keys=[key1, key2], ccl=ccl)
        expected = {
            record1: {key1: 3, key2: 10},
            record2: {key1: 4, key2: 10},
        }
        assert_equal(expected, data)

    def test_get_key_ccl_time(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ts = self.client.time()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        data = self.client.get(key=key1, ccl=ccl, time=ts)
        expected = {
            record1: 3,
            record2: 4
        }
        assert_equal(expected, data)

    def test_get_keys_ccl_time(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ts = self.client.time()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        data = self.client.get(key=[key1, key2], ccl=ccl, time=ts)
        expected = {
            record1: {key1: 3, key2: 10},
            record2: {key1: 4, key2: 10},
        }
        assert_equal(expected, data)

    def test_get_key_ccl_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        anchor = self.get_time_anchor()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.get(key=key1, ccl=ccl, time=ts)
        expected = {
            record1: 3,
            record2: 4
        }
        assert_equal(expected, data)

    def test_get_keys_ccl_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        anchor = self.get_time_anchor()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.get(key=[key1, key2], ccl=ccl, time=ts)
        expected = {
            record1: {key1: 3, key2: 10},
            record2: {key1: 4, key2: 10},
        }
        assert_equal(expected, data)

    def test_get_key_record(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('foo', 3, 1)
        assert_equal(3, self.client.get(key='foo', record=1))

    def test_get_key_record_time(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('foo', 3, 1)
        ts = self.client.time()
        self.client.add('foo', 4, 1)
        assert_equal(3, self.client.get(key='foo', record=1, time=ts))

    def test_get_key_record_timestr(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('foo', 3, 1)
        anchor = self.get_time_anchor()
        self.client.add('foo', 4, 1)
        ts = test_data.get_elapsed_millis_string(anchor)
        assert_equal(3, self.client.get(key='foo', record=1, time=ts))

    def test_get_key_records(self):
        self.client.add('foo', 1, [1, 2, 3])
        self.client.add('foo', 2, [1, 2, 3])
        self.client.add('foo', 3, [1, 2, 3])
        assert_equal({
            1: 3,
            2: 3,
            3: 3
        }, self.client.get(key='foo', record=[1, 2, 3]))

    def test_get_key_records_time(self):
        self.client.add('foo', 1, [1, 2, 3])
        self.client.add('foo', 2, [1, 2, 3])
        self.client.add('foo', 3, [1, 2, 3])
        ts = self.client.time()
        self.client.add('foo', 4, [1, 2, 3])
        assert_equal({
            1: 3,
            2: 3,
            3: 3
        }, self.client.get(key='foo', record=[1, 2, 3], time=ts))

    def test_get_key_records_timestr(self):
        self.client.add('foo', 1, [1, 2, 3])
        self.client.add('foo', 2, [1, 2, 3])
        self.client.add('foo', 3, [1, 2, 3])
        anchor = self.get_time_anchor()
        self.client.add('foo', 4, [1, 2, 3])
        ts = test_data.get_elapsed_millis_string(anchor)
        assert_equal({
            1: 3,
            2: 3,
            3: 3
        }, self.client.get(key='foo', record=[1, 2, 3], time=ts))

    def test_get_keys_record(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('bar', 1, 1)
        self.client.add('bar', 2, 1)
        data = self.client.get(keys=['foo', 'bar'], record=1)
        expected = {
            'foo': 2,
            'bar': 2
        }
        assert_equal(expected, data)

    def test_get_keys_record_time(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('bar', 1, 1)
        self.client.add('bar', 2, 1)
        ts = self.client.time()
        self.client.add('foo', 3, 1)
        self.client.add('bar', 3, 1)
        data = self.client.get(keys=['foo', 'bar'], record=1, time=ts)
        expected = {
            'foo': 2,
            'bar': 2
        }
        assert_equal(expected, data)

    def test_get_keys_record_timestr(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('bar', 1, 1)
        self.client.add('bar', 2, 1)
        anchor = self.get_time_anchor()
        self.client.add('foo', 3, 1)
        self.client.add('bar', 3, 1)
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.get(keys=['foo', 'bar'], record=1, time=ts)
        expected = {
            'foo': 2,
            'bar': 2
        }
        assert_equal(expected, data)

    def test_get_keys_records_time(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        ts = self.client.time()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        data = self.client.get(keys=['foo', 'bar'], records=[1, 2], time=ts)
        expected = {
            'foo': 2,
            'bar': 2
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_get_keys_records_timestr(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        anchor = self.get_time_anchor()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.get(keys=['foo', 'bar'], records=[1, 2], time=ts)
        expected = {
            'foo': 2,
            'bar': 2
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_get_keys_records(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        data = self.client.get(keys=['foo', 'bar'], records=[1, 2])
        expected = {
            'foo': 2,
            'bar': 2
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_insert_dict(self):
        data = {
            'string': 'a',
            'int': 1,
            'double': 3.14,
            'bool': True,
            'multi': ['a', 1, 3.14, True]
        }
        record = self.client.insert(data=data)[0]
        assert_equal('a', self.client.get(key='string', record=record))
        assert_equal(1, self.client.get(key='int', record=record))
        assert_equal(3.14, self.client.get(key='double', record=record))
        assert_equal(True, self.client.get(key='bool', record=record))
        assert_equal(['a', 1, 3.14, True], self.client.select(key='multi', record=record))

    def test_insert_dicts(self):
        data = [
            {
                'foo': 1
            },
            {
                'foo': 2
            },
            {
                'foo': 3
            }
        ]
        records = self.client.insert(data=data)
        assert_equal(len(data), len(records))

    def test_insert_json(self):
        data = {
            'string': 'a',
            'int': 1,
            'double': 3.14,
            'bool': True,
            'multi': ['a', 1, 3.14, True]
        }
        data = ujson.dumps(data)
        record = self.client.insert(data=data)[0]
        assert_equal('a', self.client.get(key='string', record=record))
        assert_equal(1, self.client.get(key='int', record=record))
        assert_equal(3.14, self.client.get(key='double', record=record))
        assert_equal(True, self.client.get(key='bool', record=record))
        assert_equal(['a', 1, 3.14, True], self.client.select(key='multi', record=record))

    def test_insert_json_list(self):
        data = [
            {
                'foo': 1
            },
            {
                'foo': 2
            },
            {
                'foo': 3
            }
        ]
        count = len(data)
        data = ujson.dumps(data)
        records = self.client.insert(data=data)
        assert_equal(count, len(records))

    def test_insert_dict_record(self):
        record = test_data.random_long()
        data = {
            'string': 'a',
            'int': 1,
            'double': 3.14,
            'bool': True,
            'multi': ['a', 1, 3.14, True]
        }
        result = self.client.insert(data=data, record=record)
        assert_true(result)
        assert_equal('a', self.client.get(key='string', record=record))
        assert_equal(1, self.client.get(key='int', record=record))
        assert_equal(3.14, self.client.get(key='double', record=record))
        assert_equal(True, self.client.get(key='bool', record=record))
        assert_equal(['a', 1, 3.14, True], self.client.select(key='multi', record=record))

    def test_insert_json_record(self):
        record = test_data.random_long()
        data = {
            'string': 'a',
            'int': 1,
            'double': 3.14,
            'bool': True,
            'multi': ['a', 1, 3.14, True]
        }
        data = ujson.dumps(data)
        result = self.client.insert(data=data, record=record)
        assert_true(result)
        assert_equal('a', self.client.get(key='string', record=record))
        assert_equal(1, self.client.get(key='int', record=record))
        assert_equal(3.14, self.client.get(key='double', record=record))
        assert_equal(True, self.client.get(key='bool', record=record))
        assert_equal(['a', 1, 3.14, True], self.client.select(key='multi', record=record))

    def test_insert_dict_records(self):
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        record3 = test_data.random_long()
        data = {
            'string': 'a',
            'int': 1,
            'double': 3.14,
            'bool': True,
            'multi': ['a', 1, 3.14, True]
        }
        result = self.client.insert(data=data, records=[record1, record2, record3])
        assert_true({
            record1: True,
            record2: True,
            record3: True
        }, result)

    def test_insert_json_records(self):
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        record3 = test_data.random_long()
        data = {
            'string': 'a',
            'int': 1,
            'double': 3.14,
            'bool': True,
            'multi': ['a', 1, 3.14, True]
        }
        data = ujson.dumps(data)
        result = self.client.insert(data=data, records=[record1, record2, record3])
        assert_true({
            record1: True,
            record2: True,
            record3: True
        }, result)

    def test_inventory(self):
        records = [1, 2, 3, 4, 5, 6, 7]
        self.client.add(key='foo', value=17, records=records)
        assert_equal(records, self.client.inventory())

    def test_jsonify_records(self):
        record1 = 1
        record2 = 2
        data = {
            'int': 1,
            'multi': [1, 2, 3, 4]
        }
        self.client.insert(data=data, records=[record1, record2])
        dump = self.client.jsonify(records=[record1, record2])
        data = {
            'int': [1],
            'multi': [1, 2, 3, 4]
        }
        assert_equal([data, data], ujson.loads(dump))

    def test_jsonify_records_identifier(self):
        record1 = 1
        record2 = 2
        data = {
            'int': 1,
            'multi': [1, 2, 3, 4]
        }
        self.client.insert(data=data, records=[record1, record2])
        dump = self.client.jsonify(records=[record1, record2], id=True)
        data1 = {
            'int': [1],
            'multi': [1, 2, 3, 4],
            constants.JSON_RESERVED_IDENTIFIER_NAME: 1
        }
        data2 = {
            'int': [1],
            'multi': [1, 2, 3, 4],
            constants.JSON_RESERVED_IDENTIFIER_NAME: 2
        }
        assert_equal([data1, data2], ujson.loads(dump))

    def test_jsonify_records_time(self):
        record1 = 1
        record2 = 2
        data = {
            'int': 1,
            'multi': [1, 2, 3, 4]
        }
        self.client.insert(data=data, records=[record1, record2])
        ts = self.client.time()
        self.client.add('foo', 10, [record1, record2])
        dump = self.client.jsonify(records=[record1, record2], time=ts)
        data = {
            'int': [1],
            'multi': [1, 2, 3, 4]
        }
        assert_equal([data, data], ujson.loads(dump))

    @ignore
    def test_jsonify_records_timestr(self):
        record1 = 1
        record2 = 2
        data = {
            'int': 1,
            'multi': [1, 2, 3, 4]
        }
        self.client.insert(data=data, records=[record1, record2])
        anchor = self.get_time_anchor()
        self.client.add('foo', 10, [record1, record2])
        ts = test_data.get_elapsed_millis_string(anchor)
        dump = self.client.jsonify(records=[record1, record2], time=ts)
        data = {
            'int': [1],
            'multi': [1, 2, 3, 4]
        }
        assert_equal([data, data], ujson.loads(dump))

    def test_jsonify_records_identifier_time(self):
        record1 = 1
        record2 = 2
        data = {
            'int': 1,
            'multi': [1, 2, 3, 4]
        }
        self.client.insert(data=data, records=[record1, record2])
        ts = self.client.time()
        self.client.add(key='foo', value=True, records=[record1, record2])
        dump = self.client.jsonify(records=[record1, record2], id=True, time=ts)
        data1 = {
            'int': [1],
            'multi': [1, 2, 3, 4],
            constants.JSON_RESERVED_IDENTIFIER_NAME: 1
        }
        data2 = {
            'int': [1],
            'multi': [1, 2, 3, 4],
            constants.JSON_RESERVED_IDENTIFIER_NAME: 2
        }
        assert_equal([data1, data2], ujson.loads(dump))

    def test_jsonify_records_identifier_timestr(self):
        record1 = 1
        record2 = 2
        data = {
            'int': 1,
            'multi': [1, 2, 3, 4]
        }
        self.client.insert(data=data, records=[record1, record2])
        anchor = self.get_time_anchor()
        self.client.add(key='foo', value=True, records=[record1, record2])
        ts = test_data.get_elapsed_millis_string(anchor)
        dump = self.client.jsonify(records=[record1, record2], id=True, time=ts)
        data1 = {
            'int': [1],
            'multi': [1, 2, 3, 4],
            constants.JSON_RESERVED_IDENTIFIER_NAME: 1
        }
        data2 = {
            'int': [1],
            'multi': [1, 2, 3, 4],
            constants.JSON_RESERVED_IDENTIFIER_NAME: 2
        }
        assert_equal([data1, data2], ujson.loads(dump))

    def test_ping_record(self):
        record = 1
        assert_false(self.client.ping(record))
        self.client.add(key='foo', value=1, record=record)
        assert_true(self.client.ping(record))
        self.client.clear(key='foo', record=record)
        assert_false(self.client.ping(record))

    def test_ping_records(self):
        self.client.add(key='foo', value=1, records=[1, 2])
        data = self.client.ping([1, 2, 3])
        assert_equal({
            1: True,
            2: True,
            3: False
        }, data)

    def test_remove_key_value_record(self):
        key = 'foo'
        value = 1
        record = 1
        assert_false(self.client.remove(key, value, record))
        self.client.add(key, value, record)
        assert_true(self.client.remove(key=key, record=record, value=value))

    def test_remove_key_value_records(self):
        key = 'foo'
        value = 1
        self.client.add(key, value, records=[1, 2])
        data = self.client.remove(key, value, records=[1, 2, 3])
        assert_equal({
            1: True,
            2: True,
            3: False
        }, data)

    def test_revert_key_records_time(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        ts = self.client.time()
        self.client.insert(data=data2, records=[1, 2, 3])
        self.client.revert(key='one', records=[1, 2, 3], time=ts)
        data = self.client.select(key='one', record=[1, 2, 3])
        assert_equal({
            1: [1],
            2: [1],
            3: [1]
        }, data)

    def test_revert_key_records_timestr(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        anchor = self.get_time_anchor()
        self.client.insert(data=data2, records=[1, 2, 3])
        ts = test_data.get_elapsed_millis_string(anchor)
        self.client.revert(key='one', records=[1, 2, 3], time=ts)
        data = self.client.select(key='one', record=[1, 2, 3])
        assert_equal({
            1: [1],
            2: [1],
            3: [1]
        }, data)

    def test_revert_keys_records_time(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        ts = self.client.time()
        self.client.insert(data=data2, records=[1, 2, 3])
        self.client.revert(keys=['one', 'two', 'three'], records=[1, 2, 3], time=ts)
        data = self.client.select(key=['one', 'two', 'three'], record=[1, 2, 3])
        data3 = {
            'one': [1],
            'two': [2],
            'three': [3]
        }
        assert_equal({
            1: data3,
            2: data3,
            3: data3
        }, data)

    def test_revert_keys_records_timestr(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        anchor = self.get_time_anchor()
        self.client.insert(data=data2, records=[1, 2, 3])
        ts = test_data.get_elapsed_millis_string(anchor)
        self.client.revert(keys=['one', 'two', 'three'], records=[1, 2, 3], time=ts)
        data = self.client.select(key=['one', 'two', 'three'], record=[1, 2, 3])
        data3 = {
            'one': [1],
            'two': [2],
            'three': [3]
        }
        assert_equal({
            1: data3,
            2: data3,
            3: data3
        }, data)

    def test_revert_keys_record_time(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        ts = self.client.time()
        self.client.insert(data=data2, records=[1, 2, 3])
        self.client.revert(key=['one', 'two', 'three'], records=1, time=ts)
        data = self.client.select(key=['one', 'two', 'three'], record=1)
        assert_equal({
            'one': [1],
            'two': [2],
            'three': [3]
        }, data)

    def test_revert_keys_record_timestr(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        anchor = self.get_time_anchor()
        self.client.insert(data=data2, records=[1, 2, 3])
        ts = test_data.get_elapsed_millis_string(anchor)
        self.client.revert(key=['one', 'two', 'three'], records=1, time=ts)
        data = self.client.select(key=['one', 'two', 'three'], record=1)
        assert_equal({
            'one': [1],
            'two': [2],
            'three': [3]
        }, data)

    def test_revert_key_record_time(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        ts = self.client.time()
        self.client.insert(data=data2, records=[1, 2, 3])
        self.client.revert(key='one', records=1, time=ts)
        data = self.client.select(key='one', record=1)
        assert_equal([1], data)

    def test_revert_key_record_timestr(self):
        data1 = {
            'one': 1,
            'two': 2,
            'three': 3
        }
        data2 = {
            'one': True,
            'two': True,
            'three': True
        }
        self.client.insert(data=data1, records=[1, 2, 3])
        anchor = self.get_time_anchor()
        self.client.insert(data=data2, records=[1, 2, 3])
        ts = test_data.get_elapsed_millis_string(anchor)
        self.client.revert(key='one', records=1, time=ts)
        data = self.client.select(key='one', record=1)
        assert_equal([1], data)

    def test_search(self):
        self.client.add(key="name", value="jeff", record=1)
        self.client.add(key="name", value="jeffery", record=2)
        self.client.add(key="name", value="jeremy", record=3)
        self.client.add(key="name", value="ben jefferson", record=4)
        records = self.client.search(key="name", query="jef")
        assert_equal([1, 2, 4], records)

    def test_select_ccl(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        ccl = key2 + ' = 10'
        data = self.client.select(ccl=ccl)
        expected = {
            key1: [1, 2, 3],
            key2: [10]
        }
        assert_equal(data.get(record1), expected)
        assert_equal(data.get(record2), expected)

    def test_select_ccl_time(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        ts = self.client.time()
        self.client.set(key=key2, value=11, records=[record1, record2])
        ccl = key2 + ' > 10'
        data = self.client.select(ccl=ccl, time=ts)
        expected = {
            key1: [1, 2, 3],
            key2: [10]
        }
        assert_equal(data.get(record1), expected)
        assert_equal(data.get(record2), expected)

    def test_select_ccl_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        anchor = self.get_time_anchor()
        self.client.set(key=key2, value=11, records=[record1, record2])
        ccl = key2 + ' > 10'
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.select(ccl=ccl, time=ts)
        expected = {
            key1: [1, 2, 3],
            key2: [10]
        }
        assert_equal(data.get(record1), expected)
        assert_equal(data.get(record2), expected)

    def test_select_key_ccl(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ccl = key2 + ' = 10'
        data = self.client.select(key=key1, ccl=ccl)
        expected = {
            record1: [1, 2, 3],
            record2: [1, 2, 3, 4]
        }
        assert_equal(expected, data)

    def test_select_keys_ccl(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ccl = key2 + ' = 10'
        data = self.client.select(keys=[key1, key2], ccl=ccl)
        expected = {
            record1: {key1: [1, 2, 3], key2: [10]},
            record2: {key1: [1, 2, 3, 4], key2: [10]},
        }
        assert_equal(expected, data)

    def test_select_key_ccl_time(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ts = self.client.time()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        data = self.client.select(key=key1, ccl=ccl, time=ts)
        expected = {
            record1: [1, 2, 3],
            record2: [1, 2, 3, 4]
        }
        assert_equal(expected, data)

    def test_select_keys_ccl_time(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        ts = self.client.time()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        data = self.client.select(key=[key1, key2], ccl=ccl, time=ts)
        expected = {
            record1: {key1: [1, 2, 3], key2: [10]},
            record2: {key1: [1, 2, 3, 4], key2: [10]},
        }
        assert_equal(expected, data)

    def test_select_key_ccl_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        anchor = self.get_time_anchor()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.select(key=key1, ccl=ccl, time=ts)
        expected = {
            record1: [1, 2, 3],
            record2: [1, 2, 3, 4]
        }
        assert_equal(expected, data)

    def test_select_keys_ccl_timestr(self):
        key1 = test_data.random_string()
        key2 = test_data.random_string()
        record1 = test_data.random_long()
        record2 = test_data.random_long()
        self.client.add(key=key1, value=1, records=[record1, record2])
        self.client.add(key=key1, value=2, records=[record1, record2])
        self.client.add(key=key1, value=3, records=[record1, record2])
        self.client.add(key=key2, value=10, records=[record1, record2])
        self.client.add(key=key1, value=4, record=record2)
        anchor = self.get_time_anchor()
        ccl = key2 + ' = 10'
        self.client.set(key=key1, value=100, record=[record2, record1])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.select(key=[key1, key2], ccl=ccl, time=ts)
        expected = {
            record1: {key1: [1, 2, 3], key2: [10]},
            record2: {key1: [1, 2, 3, 4], key2: [10]},
        }
        assert_equal(expected, data)

    def test_select_key_record(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('foo', 3, 1)
        assert_equal([1, 2, 3], self.client.select(key='foo', record=1))

    def test_select_key_record_time(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('foo', 3, 1)
        ts = self.client.time()
        self.client.add('foo', 4, 1)
        assert_equal([1, 2, 3], self.client.select(key='foo', record=1, time=ts))

    def test_select_key_record_timestr(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('foo', 3, 1)
        anchor = self.get_time_anchor()
        self.client.add('foo', 4, 1)
        ts = test_data.get_elapsed_millis_string(anchor)
        assert_equal([1, 2, 3], self.client.select(key='foo', record=1, time=ts))

    def test_select_key_records(self):
        self.client.add('foo', 1, [1, 2, 3])
        self.client.add('foo', 2, [1, 2, 3])
        self.client.add('foo', 3, [1, 2, 3])
        assert_equal({
            1: [1, 2, 3],
            2: [1, 2, 3],
            3: [1, 2, 3]
        }, self.client.select(key='foo', record=[1, 2, 3]))

    def test_select_key_records_time(self):
        self.client.add('foo', 1, [1, 2, 3])
        self.client.add('foo', 2, [1, 2, 3])
        self.client.add('foo', 3, [1, 2, 3])
        ts = self.client.time()
        self.client.add('foo', 4, [1, 2, 3])
        assert_equal({
            1: [1, 2, 3],
            2: [1, 2, 3],
            3: [1, 2, 3]
        }, self.client.select(key='foo', record=[1, 2, 3], time=ts))

    def test_select_key_records_timestr(self):
        self.client.add('foo', 1, [1, 2, 3])
        self.client.add('foo', 2, [1, 2, 3])
        self.client.add('foo', 3, [1, 2, 3])
        anchor = self.get_time_anchor()
        self.client.add('foo', 4, [1, 2, 3])
        ts = test_data.get_elapsed_millis_string(anchor)
        assert_equal({
            1: [1, 2, 3],
            2: [1, 2, 3],
            3: [1, 2, 3]
        }, self.client.select(key='foo', record=[1, 2, 3], time=ts))

    def test_select_keys_record(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('bar', 1, 1)
        self.client.add('bar', 2, 1)
        data = self.client.select(keys=['foo', 'bar'], record=1)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal(expected, data)

    def test_select_keys_record_time(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('bar', 1, 1)
        self.client.add('bar', 2, 1)
        ts = self.client.time()
        self.client.add('foo', 3, 1)
        self.client.add('bar', 3, 1)
        data = self.client.select(keys=['foo', 'bar'], record=1, time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal(expected, data)

    def test_select_keys_record_timestr(self):
        self.client.add('foo', 1, 1)
        self.client.add('foo', 2, 1)
        self.client.add('bar', 1, 1)
        self.client.add('bar', 2, 1)
        anchor = self.get_time_anchor()
        self.client.add('foo', 3, 1)
        self.client.add('bar', 3, 1)
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.select(keys=['foo', 'bar'], record=1, time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal(expected, data)

    def test_select_keys_records_time(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        ts = self.client.time()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        data = self.client.select(keys=['foo', 'bar'], records=[1, 2], time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_select_keys_records_timestr(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        anchor = self.get_time_anchor()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.select(keys=['foo', 'bar'], records=[1, 2], time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_select_keys_records(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        data = self.client.select(keys=['foo', 'bar'], records=[1, 2])
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_select_record(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        data = self.client.select(record=1)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal(expected, data)

    def test_select_record_time(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        ts = self.client.time()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        data = self.client.select(record=2, time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal(expected, data)

    def test_select_record_timestr(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        anchor = self.get_time_anchor()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.select(record=2, time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal(expected, data)

    def test_select_records(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        data = self.client.select(records=[1, 2])
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_select_records_time(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        ts = self.client.time()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        data = self.client.select( records=[1, 2], time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_select_records_timestr(self):
        self.client.add('foo', 1, [1, 2])
        self.client.add('foo', 2, [1, 2])
        self.client.add('bar', 1, [1, 2])
        self.client.add('bar', 2, [1, 2])
        anchor = self.get_time_anchor()
        self.client.add('foo', 3, [1, 2])
        self.client.add('bar', 3, [1, 2])
        ts = test_data.get_elapsed_millis_string(anchor)
        data = self.client.select( records=[1, 2], time=ts)
        expected = {
            'foo': [1, 2],
            'bar': [1, 2]
        }
        assert_equal({
            1: expected,
            2: expected
        }, data)

    def test_set_key_value(self):
        key = "foo"
        value = 1
        record = self.client.set(key=key, value=value)
        data = self.client.select(record=record)
        assert_equal({
            'foo': [1]
        }, data)

    def test_set_key_value_record(self):
        key = "foo"
        value = 1
        record = 1
        self.client.add(key=key, value=2, record=record)
        self.client.add(key=key, value=2, record=record)
        self.client.set(key=key, value=value, record=record)
        data = self.client.select(record=record)
        assert_equal({
            'foo': [1]
        }, data)

    def test_set_key_value_records(self):
        key = "foo"
        value = 1
        records = [1, 2, 3]
        self.client.add(key=key, value=2, record=records)
        self.client.add(key=key, value=2, record=records)
        self.client.set(key=key, value=value, record=records)
        data = self.client.select(record=records)
        expected = {
            'foo': [1]
        }
        assert_equal({
            1: expected,
            2: expected,
            3: expected
        }, data)

    def test_stage(self):
        assert_is_none(self.client.transaction)
        self.client.stage()
        assert_is_not_none(self.client.transaction)
        self.client.abort()

    def test_time(self):
        assert_true(isinstance(self.client.time(), int))

    def test_time_phrase(self):
        assert_true(isinstance(self.client.time("3 seconds ago"), int))

    def test_verify_and_swap(self):
        self.client.add("foo", 2, 2)
        assert_false(self.client.verify_and_swap(key='foo', expected=1, record=2, replacement=3))
        assert_true(self.client.verify_and_swap(key='foo', expected=2, record=2, replacement=3))
        assert_equal(3, self.client.get(key='foo', record=2))

    def test_verify_or_set(self):
        self.client.add("foo", 2, 2)
        self.client.verify_or_set(key='foo', value=3, record=2)
        assert_equal(3, self.client.get(key='foo', record=2))

    def test_verify_key_value_record(self):
        self.client.add('name', 'jeff', 1)
        self.client.add('name', 'jeffery', 1)
        self.client.add('name', 'bob', 1)
        assert_true(self.client.verify('name', 'jeff', 1))
        self.client.remove('name', 'jeff', 1)
        assert_false(self.client.verify('name', 'jeff', 1))

    def test_verify_key_value_record_time(self):
        self.client.add('name', 'jeff', 1)
        self.client.add('name', 'jeffery', 1)
        self.client.add('name', 'bob', 1)
        ts = self.client.time()
        self.client.remove('name', 'jeff', 1)
        assert_true(self.client.verify('name', 'jeff', 1, time=ts))

    def test_verify_key_value_record_timestr(self):
        self.client.add('name', 'jeff', 1)
        self.client.add('name', 'jeffery', 1)
        self.client.add('name', 'bob', 1)
        anchor = self.get_time_anchor()
        self.client.remove('name', 'jeff', 1)
        ts = test_data.get_elapsed_millis_string(anchor)
        assert_true(self.client.verify('name', 'jeff', 1, time=ts))

    def test_link_key_source_destination(self):
        assert_true(self.client.link(key='friends', source=1, destination=2))
        assert_equal(Link.to(2), self.client.get('friends', record=1))

    def test_link_key_source_destinations(self):
        assert_equal({
            2: True,
            3: True,
            4: True
        }, self.client.link(key='friends', source=1, destination=[2, 3, 4]))

    def test_unlink_key_source_destination(self):
        assert_true(self.client.link(key='friends', source=1, destination=2))
        assert_true(self.client.unlink(key='friends', source=1, destination=2))

    def test_unlink_key_source_destinations(self):
        assert_true(self.client.link(key='friends', source=1, destination=2))
        assert_equal({
            2: True,
            3: False
        }, self.client.unlink(key='friends', source=1, destination=[2, 3]))

    def test_find_or_add_key_value(self):
        record = self.client.find_or_add("age", 23)
        assert_equal(23, self.client.get("age", record))

    def test_find_or_insert_ccl_json(self):
        data = {
            'name': 'jeff nelson'
        }
        data = ujson.dumps(data)
        record = self.client.find_or_insert(criteria="age > 10", data=data)
        assert_equal('jeff nelson', self.client.get("name", record))

    def test_find_or_insert_ccl_dict(self):
        data = {
            'name': 'jeff nelson'
        }
        record = self.client.find_or_insert(criteria="age > 10", data=data)
        assert_equal('jeff nelson', self.client.get("name", record))

    def test_insert_dict_with_link(self):
        data = {
            'foo': Link.to(1)
        }
        record = self.client.insert(data=data)[0]
        assert_equal(Link.to(1), self.client.get(key='foo', record=record))

    def test_insert_dict_with_resolvable_link(self):
        record1 = self.client.add('foo', 1)
        record2 = self.client.insert(data={
            'foo': Link.to_where('foo = 1')
        })[0]
        assert_equal(Link.to(record1), self.client.get(key='foo', record=record2))

    def test_reconcile_empty_values(self):
        self.client.reconcile(key="foo", record=17, values=[])
        assert_equal(0, len(self.client.select(key="foo", record=17)))

    def test_reconcile(self):
        record = 1
        key = "testKey"
        self.client.add(key, "A", record)
        self.client.add(key, "C", record)
        self.client.add(key, "D", record)
        self.client.add(key, "E", record)
        self.client.add(key, "F", record)
        values = ['A', 'B', 'D', 'G']
        self.client.reconcile(key=key, record=record, values=values)
        stored = self.client.select(key=key, record=record)
        assert_equal(set(values), set(stored))

    def test_complex_tobject_serialize_string(self):
        expected = test_data.random_string()
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_int(self):
        expected = test_data.random_int()
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_bool(self):
        expected = test_data.random_bool()
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_long(self):
        expected = test_data.random_long()
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_float(self):
        expected = test_data.random_float()
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_list_basic(self):
        expected = [1, 2, 3, 4, 5, 6, 7, 8, "9"]
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_list(self):
        count = test_data.scale_count()
        expected = []
        for n in range(0, count):
            expected.append(test_data.random_object())
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_set(self):
        count = test_data.scale_count()
        expected = set()
        for n in range(0, count):
            expected.add(test_data.random_object())
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_list_of_lists(self):
        expected = ["1", True, 1, [1, 2, 3, "4"], [1, 2], set(["1", True])]
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)

    def test_complex_tobject_serialize_dict(self):
        expected = {}
        count = test_data.scale_count()
        for n in range(0, count):
            key = test_data.random_object()
            value = test_data.random_object()
            expected[key] = value
        actual = ComplexTObject.from_python_object(expected).get_python_object()
        assert_equal(expected, actual)
