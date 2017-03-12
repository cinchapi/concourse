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

class TestTag < Test::Unit::TestCase

    def test_tag__tag_equality
        tag1 = Concourse::Tag.new "foo string"
        tag2 = Concourse::Tag.new "foo string"
        assert(tag1 == tag2)
    end

    def test_tag_tag_inequality
        tag1 = Concourse::Tag.new "foo string"
        tag2 = Concourse::Tag.new "Foo String"
        assert(tag1 != tag2)
    end

    def test_tag_string_equality
        tag1 = Concourse::Tag.new "hello world"
        assert(tag1 == "hello world")
    end

    def test_tag_string_inequality
        tag1 = Concourse::Tag.new "hello world"
        assert(tag1 != "foo bar")
    end

    def test_string_to_tag
        string = "whats this"
        assert(string.to_tag == string)
        assert(string.to_tag == Concourse::Tag.new(string))
    end
end
