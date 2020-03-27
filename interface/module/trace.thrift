# Copyright (c) 2013-2020 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Interface definition for the Concourse Server Trace API.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

include "../data.thrift"
include "../shared.thrift"
include "../exceptions.thrift"
include "../complex.thrift"

# To generate java source code run:
# utils/compile-thrift-java.sh
namespace java com.cinchapi.concourse.thrift

# To generate python source code run:
# utils/compile—thrift-python.sh
namespace py concourse.thriftapi

# To generate PHP source code run:
# utils/compile-thrift-php.sh
namespace php concourse.thrift

# To generate Ruby source code run:
# utils/compile—thrift-ruby.sh
namespace rb concourse.thrift

service ConcourseTraceService {

  map<string, set<i64>> traceRecord(
      1: i64 record,
      2: shared.AccessToken creds,
      3: shared.TransactionToken transaction,
      4: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

    map<i64, map<string, set<i64>>> traceRecords(
      1: list<i64> records,
      2: shared.AccessToken creds,
      3: shared.TransactionToken transaction,
      4: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

}