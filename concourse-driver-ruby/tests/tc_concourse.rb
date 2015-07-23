require 'test/unit'
require 'socket'
require_relative '../lib/concourse'
require_relative 'testutils'
require_relative 'tc_base'

class RubyClientDriverTest < IntegrationBaseTest

    def test_abort
        @client.stage
        key = TestUtils.random_string
        value = "some value"
        record = 1
        @client.add key, value, record
        @client.abort
        assert_equal(nil, @client.get(key:key, record:record))
    end

    def test_add_key_value
        key = TestUtils.random_string
        value = "some value"
        record = @client.add key, value
        assert_not_nil record
        stored = @client.get key, record
        assert_equal(value, stored)
    end

    def test_add_key_value_record
        key = "foo"
        value = "static value"
        record = 17
        assert @client.add key, value, record
        stored = @client.get key, record
        assert_equal(value, stored)
    end

end
