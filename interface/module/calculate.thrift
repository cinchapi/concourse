# Copyright (c) 2013-2024 Cinchapi Inc.
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
# Interface definition for the Concourse Server Aggregation API.
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

service ConcourseCalculateService {

    data.TObject sumKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject sumKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject sumKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyTimestr(
    1: string key,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject sumKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject sumKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject sumKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject sumKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject sumKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject averageKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject averageKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject averageKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyTimestr(
    1: string key,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject averageKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject averageKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject averageKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject averageKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject averageKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  i64 countKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  i64 countKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  i64 countKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

   i64 countKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  i64 countKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  i64 countKeyTimestr(
    1: string key,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  i64 countKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  i64 countKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject maxKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject maxKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject maxKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject maxKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject maxKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

   data.TObject maxKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject maxKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject maxKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject maxKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject maxKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject maxKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject maxKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject maxKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject maxKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject maxKeyTimestr(
    1: string key,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject minKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject minKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject minKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject minKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject minKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

   data.TObject minKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject minKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject minKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );

  data.TObject minKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject minKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject minKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject minKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  )

    data.TObject minKeyTime(
      1: string key,
      2: i64 timestamp,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3,
      4: exceptions.InvalidOperationException ex4
    );

  data.TObject minKeyTimestr(
    1: string key,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4,
    5: exceptions.InvalidOperationException ex5
  );

  data.TObject minKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3,
    4: exceptions.InvalidOperationException ex4
  );
}