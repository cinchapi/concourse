from nose.tools import *
import string
import random
from concourse.utils import *

class TestUtils(object):

    @staticmethod
    def generate_random_string(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def test_convert_string_roundtrip(self):
        orig = TestUtils.generate_random_string()
        assert_equals(orig, thrift_to_python(python_to_thrift(orig)))

    def test_convert_tag_roundtrip(self):
        orig = Tag.create(TestUtils.generate_random_string())
        assert_equals(orig, thrift_to_python(python_to_thrift(orig)))

    def test_convert_int_roundtrip(self):
        orig = 100
        assert_equals(orig, thrift_to_python(python_to_thrift(orig)))

    def test_convert_long_roundtrip(self):
        orig = 2147483648
        assert_equals(orig, thrift_to_python(python_to_thrift(orig)))

    def test_convert_link_roundtrip(self):
        orig = Link.to(2147483648)
        assert_equals(orig, thrift_to_python(python_to_thrift(orig)))

    def test_convert_boolean_roundtrip(self):
        orig = False
        assert_equals(orig, thrift_to_python(python_to_thrift(orig)))

    def test_convert_float_roundtrip(self):
        orig = 3.14353
        assert_equals(orig, thrift_to_python(python_to_thrift(orig)))