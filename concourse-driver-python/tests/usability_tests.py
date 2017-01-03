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
from tests.integration_tests import IntegrationBaseTest


class TestPythonClientDriverUsability(IntegrationBaseTest):
    """A collection of tests that are designed to show that the client driver meets usability standards
    """

    def test_commit_always_returns_false_if_no_transaction(self):
        assert_false(self.client.commit())