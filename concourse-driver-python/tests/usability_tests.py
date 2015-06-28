from nose.tools import *
from tests.integration_tests import IntegrationBaseTest


class TestPythonClientDriverUsability(IntegrationBaseTest):
    """A collection of tests that are designed to show that the client driver meets usability standards
    """

    def test_commit_always_returns_false_if_no_transaction(self):
        assert_false(self.client.commit())