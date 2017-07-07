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

class InsertTest < IntegrationBaseTest

    def test_insert_hash_with_link
        data = {
            :foo => Link.to(1)
        }
        record = @client.insert(data:data)[0]
        assert_equal Link.to(1), @client.get(key:"foo", record:record)
    end

    def test_insert_hash_with_resolvable_link()
        record1 = @client.add "foo", 1
        record2 = @client.insert({"foo" => Link.to_where("foo = 1")})[0]
        assert_equal Link.to(record1), @client.get(key:"foo", record:record2)
    end

end
