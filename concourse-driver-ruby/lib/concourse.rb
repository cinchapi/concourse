# Copyright (c) 2015 Cinchapi Inc.
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

include Concourse::Thrift

# Intercept the constructor for TTransactionException to supply the
# approrpaite message
TTransactionException.class_eval do

    def initialize
        @message = "Another client has made changes to data used within the current transaction, so it cannot continue. Please abort the transaction and try again."
    end
end

module Concourse

    # The base class for all exceptions that happen during (staged) operations
    # in a transaction.
    TransactionException = TTransactionException

    # An exception that is thrown when attempting to conditionally add or insert
    # data based on a condition that should be unique, but is not.
    DuplicateEntryException = TDuplicateEntryException

    # An exception that is thrown when Concourse Server cannot properly parse a
    # string.
    ParseException = TParseException
end
