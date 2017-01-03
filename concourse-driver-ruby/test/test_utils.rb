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

require 'test/unit'
require 'concourse'

class TestUtils < Test::Unit::TestCase

    def test_convert_string_round_trip
        orig = "foo"
        assert_equal(orig, Concourse::Utils::Convert::thrift_to_ruby(Concourse::Utils::Convert::ruby_to_thrift(orig)))
    end

    def test_convert_tag_round_trip
        orig = Concourse::Tag.create "foo"
        assert_equal(orig, Concourse::Utils::Convert::thrift_to_ruby(Concourse::Utils::Convert::ruby_to_thrift(orig)))
    end

    def test_convert_link_round_trip
        orig = Concourse::Link.to 2147483648
        assert_equal(orig, Concourse::Utils::Convert::thrift_to_ruby(Concourse::Utils::Convert::ruby_to_thrift(orig)))
    end

    def test_convert_int_round_trip
        orig = 10
        assert_equal(orig, Concourse::Utils::Convert::thrift_to_ruby(Concourse::Utils::Convert::ruby_to_thrift(orig)))
    end

    def test_convert_long_round_trip
        orig = 2147483649
        assert_equal(orig, Concourse::Utils::Convert::thrift_to_ruby(Concourse::Utils::Convert::ruby_to_thrift(orig)))
    end

    def test_convert_boolean_round_trip
        orig = false
        assert_equal(orig, Concourse::Utils::Convert::thrift_to_ruby(Concourse::Utils::Convert::ruby_to_thrift(orig)))
    end

    def test_convert_float_round_trip
        orig = 3.14353
        assert_equal(orig, Concourse::Utils::Convert::thrift_to_ruby(Concourse::Utils::Convert::ruby_to_thrift(orig)))
    end

end
