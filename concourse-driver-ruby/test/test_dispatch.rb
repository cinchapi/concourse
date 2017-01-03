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
require 'concourse/dispatch'

class DispatchTest < Test::Unit::TestCase

    def test_dynamic_dispatch_select_string_string
        assert_raise RuntimeError do
            Concourse::Dispatch.dynamic "select", "name = jeff", "last week"
        end
    end

    def test_dynamic_dispatch_select_string_timestr
        assert_equal(["selectCclTimestr", "name = jeff", "last week"], Concourse::Dispatch.dynamic("select", "name = jeff", time:"last week"))
    end

    def test_dynamic_dispatch_select_key_ccl
        assert_equal(["selectKeyCcl", "name", "name = jeff"], Concourse::Dispatch.dynamic("select", "name", ccl:"name = jeff"))
    end

    def test_dynamic_dispatch_select_string_string_string
        assert_equal(["selectKeyCclTimestr", "name", "name = jeff", "last week"], Concourse::Dispatch.dynamic("select", "name", "name = jeff", "last week"))
    end

end
