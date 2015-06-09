from nose.tools import *

import os
import time
from subprocess import *
import signal
import testdata
from concourse.concourse import Concourse


class IntegrationBaseTest(object):

    process = None
    client = None

    @classmethod
    def setup_class(cls):
        dir = os.path.dirname(os.path.realpath(__file__)) + '/../../mockcourse'
        script = dir + '/mockcourse'
        cls.process = Popen(script, shell=True, preexec_fn=os.setsid)
        time.sleep(2)  # Wait for Mockcourse to start
        cls.client = Concourse.connect()

    @classmethod
    def teardown_class(cls):
        os.killpg(cls.process.pid, signal.SIGTERM)


class TestDriverCore(IntegrationBaseTest):

    def test_add_string(self):
        key = testdata.random_string()
        value = testdata.random_string()
        record = self.client.add(key=key, value=value)
        assert_equal(value, self.client.get(key=key, record=record))

    def test_add_bool(self):
        key = testdata.random_string()
        value = testdata.random_bool()
        record = self.client.add(key=key, value=value)
        assert_equal(value, self.client.get(key=key, record=record))

    # TODO: look into test generator so I can use the same function logic but just give different values
