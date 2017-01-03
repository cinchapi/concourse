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

    def test_find_in_kwargs_bad_key(self):
        value = find_in_kwargs_by_alias('foo', {})
        assert_is_none(value)

    def test_find_in_kwargs_criteria(self):
        kwargs = {
            'ccl': 'foo'
        }
        value = find_in_kwargs_by_alias('criteria', kwargs)
        assert_equals('foo', value)
        kwargs = {
            'query': 'foo'
        }
        value = find_in_kwargs_by_alias('criteria', kwargs)
        assert_equals('foo', value)
        kwargs = {
            'where': 'foo'
        }
        value = find_in_kwargs_by_alias('criteria', kwargs)
        assert_equals('foo', value)
        kwargs = {
            'foo': 'foo'
        }
        value = find_in_kwargs_by_alias('criteria', kwargs)
        assert_is_none(value)
