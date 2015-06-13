from nose.tools import *

import os
import time
from subprocess import *
import signal
import test_data
from concourse import Concourse, Tag, Link
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
