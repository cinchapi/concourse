from nose.tools import *

import os
import time
from subprocess import *
import signal
import test_data
from concourse import Concourse, Tag, Link, Diff
from concourse.thriftapi.shared.ttypes import Type
from concourse.utils import python_to_thrift


class IntegrationBaseTest(object):

    process = None
    client = None

    @classmethod
    def setup_class(cls):
        dir = os.path.dirname(os.path.realpath(__file__)) + '/../../mockcourse'
        script = dir + '/mockcourse'
        cls.process = Popen(script, shell=True, preexec_fn=os.setsid)
        cls.client = None
        tries = 5
        while tries > 0 and cls.client is None:
            tries -= 1
            time.sleep(1)  # Wait for Mockcourse to start
            try:
                cls.client = Concourse.connect(port=1818)
            except RuntimeError as e:
                if tries == 0:
                    raise e
                else:
                    continue

    @classmethod
    def teardown_class(cls):
        os.killpg(cls.process.pid, signal.SIGTERM)

    def tearDown(self):
        self.client.logout()  # Mockcourse logout simply clears the content of the datastore


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
        record = test_data.random_long()
        start = test_data.random_string()
        audit = self.client.audit(key=key, record=record, start=start)
        assert_equal(start, audit.get(1))

    def test_audit_key_record_startstr_endstr(self):
        key = test_data.random_string()
        record = test_data.random_long()
        start = test_data.random_string()
        end = test_data.random_string()
        audit = self.client.audit(key=key, record=record, start=start, end=end)
        assert_equal(start, audit.get(1))
        assert_equal(end, audit.get(2))

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
        record = test_data.random_long()
        start = test_data.random_string()
        audit = self.client.audit(record=record, start=start)
        assert_equal(start, audit.get(1))

    def test_audit_record_startstr_endstr(self):
        record = test_data.random_long()
        start = test_data.random_string()
        end = test_data.random_string()
        audit = self.client.audit(record=record, start=start, end=end)
        assert_equal(start, audit.get(1))
        assert_equal(end, audit.get(2))

    def test_browse_key(self):
        key = test_data.random_string()
        value = 10
        self.client.add(key, value, [1, 2, 3])
        value = test_data.random_string()
        self.client.add(key, value, [10, 20, 30])
        data = self.client.browse(key)
        assert_equal([1, 2, 3], data.get(10))
        assert_equal([10, 20, 30], data.get(value))

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
        time = test_data.random_string()
        data = self.client.browse(test_data.random_string(), timestamp=time)
        assert_equal(0, len(data))

    def test_browse_keys_timestr(self):
        time = test_data.random_string()
        data = self.client.browse(["A", "B"], timestamp=time)
        assert_equal(0, len(data))

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
        assert_equal([10, 20, 30], data.get(value))

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
        assert_equal([[1], [1, 2], [1, 2, 3], [2, 3], [3]], data.values())

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
        assert_equal([[2, 3], [3]], data.values())

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
        assert_equal([[2, 3]], data.values())

    def test_chronologize_key_record_startstr(self):
        data = self.client.chronologize(key="foo", record=1, start="two years ago")
        assert_equal(0, len(data))

    def test_chronologize_key_record_startstr_endstr(self):
        data = self.client.chronologize(key="foo", record=1, start="two years ago", end="today")
        assert_equal(0, len(data))

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
        assert_equals(set(['name', 'age', 'team']), keys)

    def test_describe_record_time(self):
        self.client.set('name', 'tom brady', 1)
        self.client.set('age', 100, 1)
        self.client.set('team', 'new england patriots', 1)
        timestamp = self.client.time()
        self.client.remove('name', 1)
        keys = self.client.describe(1, time=timestamp)
        assert_equals(set(['name', 'age', 'team']), keys)

    def test_describe_record_timestr(self):
        timestamp = test_data.random_string()
        keys = self.client.describe(record=1, timestamp=timestamp)
        assert_equals(set([timestamp]), keys)

    def test_describe_records(self):
        records = [1, 2, 3]
        self.client.set('name', 'tom brady', records)
        self.client.set('age', 100, records)
        self.client.set('team', 'new england patriots', records)
        keys = self.client.describe(records)
        assert_equals(set(['name', 'age', 'team']), keys[1])
        assert_equals(set(['name', 'age', 'team']), keys[2])
        assert_equals(set(['name', 'age', 'team']), keys[3])

    def test_describe_records_time(self):
        records = [1, 2, 3]
        self.client.set('name', 'tom brady', records)
        self.client.set('age', 100, records)
        self.client.set('team', 'new england patriots', records)
        timestamp = self.client.time()
        self.client.clear(records=records)
        keys = self.client.describe(records, timestamp=timestamp)
        assert_equals(set(['name', 'age', 'team']), keys[1])
        assert_equals(set(['name', 'age', 'team']), keys[2])
        assert_equals(set(['name', 'age', 'team']), keys[3])

    def test_describe_records_timestr(self):
        timestamp = test_data.random_string()
        keys = self.client.describe(records=[1, 2], timestamp=timestamp)
        assert_equals({1: set([timestamp])}, keys)

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
        assert_equal({}, self.client.diff(key, record, start="last week"))

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
        assert_equal({}, self.client.diff(key, record, start="last week", end="now"))