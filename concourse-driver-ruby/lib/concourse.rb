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

require 'thrift'
require 'java-properties'
require 'concourse/client'
require 'concourse/pool'
require 'concourse/link'
require 'concourse/tag'
require 'concourse/utils'
require 'concourse/thrift/shared_types'
require 'concourse/thrift/concourse_constants'
require 'concourse/exceptions_types'

include Concourse
include Concourse::Thrift

# Intercept the constructor for TransactionException to supply the
# approrpaite message
TransactionException.class_eval do

    def initialize
        @message = "Another client has made changes to data used within the current transaction, so it cannot continue. Please abort the transaction and try again."
    end
end
