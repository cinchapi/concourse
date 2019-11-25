# Copyright (c) 2013-2019 Cinchapi Inc.
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
# Interface definition for the Concourse Server Navigate API.
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

service ConcourseNavigateService {

  map<i64, set<data.TObject>> navigateKeyRecord(
      1: string key,
      2: i64 record,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

    map<i64, set<data.TObject>> navigateKeyRecordTime(
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
      3: exceptions.PermissionException ex3
    );

    map<i64, set<data.TObject>> navigateKeyRecordTimestr(
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
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysRecord(
      1: list<string> keys,
      2: i64 record,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysRecordTime(
      1: list<string> keys,
      2: i64 record,
      3: i64 timestamp,
      4: shared.AccessToken creds,
      5: shared.TransactionToken transaction,
      6: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysRecordTimestr(
      1: list<string> keys,
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
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysRecords(
      1: list<string> keys,
      2: list<i64> records,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

    map<i64, set<data.TObject>> navigateKeyRecords(
      1: string key,
      2: list<i64> records,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

    map<i64, set<data.TObject>> navigateKeyRecordsTime(
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
      3: exceptions.PermissionException ex3
    );

    map<i64, set<data.TObject>> navigateKeyRecordsTimestr(
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
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysRecordsTime(
      1: list<string> keys,
      2: list<i64> records,
      3: i64 timestamp,
      4: shared.AccessToken creds,
      5: shared.TransactionToken transaction,
      6: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.PermissionException ex3
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysRecordsTimestr(
      1: list<string> keys,
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
      4: exceptions.PermissionException ex4
    );

    map<i64, set<data.TObject>> navigateKeyCcl(
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
      4: exceptions.PermissionException ex4
    );

    map<i64, set<data.TObject>> navigateKeyCclTime(
      1: string key,
      2: string ccl,
      3: i64 timestamp
      4: shared.AccessToken creds,
      5: shared.TransactionToken transaction,
      6: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.ParseException ex3,
      4: exceptions.PermissionException ex4
    );

    map<i64, set<data.TObject>> navigateKeyCclTimestr(
      1: string key,
      2: string ccl,
      3: string timestamp
      4: shared.AccessToken creds,
      5: shared.TransactionToken transaction,
      6: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.ParseException ex3,
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysCcl(
      1: list<string> keys,
      2: string ccl,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.ParseException ex3,
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysCclTime(
      1: list<string> keys,
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
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysCclTimestr(
      1: list<string> keys,
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
      4: exceptions.PermissionException ex4
    );

    map<i64, set<data.TObject>> navigateKeyCriteria(
      1: string key,
      2: data.TCriteria criteria,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.ParseException ex3,
      4: exceptions.PermissionException ex4
    );

    map<i64, set<data.TObject>> navigateKeyCriteriaTime(
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
      4: exceptions.PermissionException ex4
    );

    map<i64, set<data.TObject>> navigateKeyCriteriaTimestr(
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
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysCriteria(
      1: list<string> keys,
      2: data.TCriteria criteria,
      3: shared.AccessToken creds,
      4: shared.TransactionToken transaction,
      5: string environment
    )
    throws (
      1: exceptions.SecurityException ex,
      2: exceptions.TransactionException ex2,
      3: exceptions.ParseException ex3,
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysCriteriaTime(
      1: list<string> keys,
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
      4: exceptions.PermissionException ex4
    );

    map<i64, map<string, set<data.TObject>>> navigateKeysCriteriaTimestr(
      1: list<string> keys,
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
      4: exceptions.PermissionException ex4
    );

  map<string, set<data.TObject>> grabKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> grabKeyRecordTime(
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
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> grabKeyRecordTimestr(
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
    4: exceptions.PermissionException ex4
  );

  map<string, set<data.TObject>> grabKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> grabKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> grabKeysRecordTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>>  grabKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsOrder(
    1: list<string> keys,
    2: list<i64> records,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsOrderPage(
    1: list<string> keys,
    2: list<i64> records,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsPage(
    1: list<string> keys,
    2: list<i64> records,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsOrder(
    1: string key,
    2: list<i64> records,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsOrderPage(
    1: string key,
    2: list<i64> records,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsPage(
    1: string key,
    2: list<i64> records,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTime(
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
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTimeOrder(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTimeOrderPage(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTimePage(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTimestr(
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTimestrOrder(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTimestrOrderPage(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyRecordsTimestrPage(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTimeOrder(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTimeOrderPage(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTimePage(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTimestrOrder(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTimestrOrderPage(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysRecordsTimestrPage(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaOrder(
    1: string key,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaOrderPage(
    1: string key,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaPage(
    1: string key,
    2: data.TCriteria criteria,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteria(
    1: data.TCriteria criteria,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaOrder(
    1: data.TCriteria criteria,
    2: data.TOrder order,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaOrderPage(
    1: data.TCriteria criteria,
    2: data.TOrder order,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaPage(
    1: data.TCriteria criteria,
    2: data.TPage page,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCcl(
    1: string ccl,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclOrder(
    1: string ccl,
    2: data.TOrder order,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclOrderPage(
    1: string ccl,
    2: data.TOrder order,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclPage(
    1: string ccl,
    2: data.TPage page,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTime(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTimeOrder(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTimeOrderPage(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTimePage(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTimestr(
    1: data.TCriteria criteria,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTimestrOrder(
    1: data.TCriteria criteria,
    2: string timestamp
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTimestrOrderPage(
    1: data.TCriteria criteria,
    2: string timestamp
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCriteriaTimestrPage(
    1: data.TCriteria criteria,
    2: string timestamp
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTime(
    1: string ccl,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTimeOrder(
    1: string ccl,
    2: i64 timestamp
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTimeOrderPage(
    1: string ccl,
    2: i64 timestamp
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTimePage(
    1: string ccl,
    2: i64 timestamp
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTimestr(
    1: string ccl,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTimestrOrder(
    1: string ccl,
    2: string timestamp
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTimestrOrderPage(
    1: string ccl,
    2: string timestamp
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabCclTimestrPage(
    1: string ccl,
    2: string timestamp
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCcl(
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclOrder(
    1: string key,
    2: string ccl,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclOrderPage(
    1: string key,
    2: string ccl,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclPage(
    1: string key,
    2: string ccl,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTimeOrder(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTimeOrderPage(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTimePage(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTimestrOrder(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTimestrOrderPage(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCriteriaTimestrPage(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTimeOrder(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTimeOrderPage(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTimePage(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTimestrOrder(
    1: string key,
    2: string ccl,
    3: string timestamp
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTimestrOrderPage(
    1: string key,
    2: string ccl,
    3: string timestamp
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeyCclTimestrPage(
    1: string key,
    2: string ccl,
    3: string timestamp
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteria(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaOrder(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaOrderPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCcl(
    1: list<string> keys,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclOrder(
    1: list<string> keys,
    2: string ccl,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclOrderPage(
    1: list<string> keys,
    2: string ccl,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclPage(
    1: list<string> keys,
    2: string ccl,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTime(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTimeOrder(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTimeOrderPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTimePage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTimestrOrder(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTimestrOrderPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCriteriaTimestrPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTime(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTimeOrder(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTimeOrderPage(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTimePage(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTimestrOrder(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTimestrOrderPage(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> grabKeysCclTimestrPage(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<string, set<data.TObject>> gatherRecord(
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

  map<i64, map<string, set<data.TObject>>> gatherRecords(
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

  map<i64, map<string, set<data.TObject>>> gatherRecordsOrder(
    1: list<i64> records,
    2: data.TOrder order,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsOrderPage(
    1: list<i64> records,
    2: data.TOrder order,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsPage(
    1: list<i64> records,
    2: data.TPage page,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> gatherRecordTime(
    1: i64 record,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> gatherRecordTimestr(
    1: i64 record,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTime(
    1: list<i64> records,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTimeOrder(
    1: list<i64> records,
    2: i64 timestamp,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTimeOrderPage(
    1: list<i64> records,
    2: i64 timestamp,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTimePage(
    1: list<i64> records,
    2: i64 timestamp,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTimestr(
    1: list<i64> records,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTimestrOrder(
    1: list<i64> records,
    2: string timestamp,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTimestrOrderPage(
    1: list<i64> records,
    2: string timestamp,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherRecordsTimestrPage(
    1: list<i64> records,
    2: string timestamp,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );


  map<string, set<data.TObject>> gatherKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> gatherKeyRecordTime(
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
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> gatherKeyRecordTimestr(
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
    4: exceptions.PermissionException ex4
  );

  map<string, set<data.TObject>> gatherKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> gatherKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<string, set<data.TObject>> gatherKeysRecordTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsOrder(
    1: list<string> keys,
    2: list<i64> records,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsOrderPage(
    1: list<string> keys,
    2: list<i64> records,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsPage(
    1: list<string> keys,
    2: list<i64> records,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsOrder(
    1: string key,
    2: list<i64> records,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsOrderPage(
    1: string key,
    2: list<i64> records,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsPage(
    1: string key,
    2: list<i64> records,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTime(
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
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTimeOrder(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTimeOrderPage(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTimePage(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTimestr(
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTimestrOrder(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTimestrOrderPage(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyRecordsTimestrPage(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTimeOrder(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTimeOrderPage(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTimePage(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTimestrOrder(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTimestrOrderPage(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysRecordsTimestrPage(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteria(
    1: data.TCriteria criteria,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaOrder(
    1: data.TCriteria criteria,
    2: data.TOrder order,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaOrderPage(
    1: data.TCriteria criteria,
    2: data.TOrder order,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaPage(
    1: data.TCriteria criteria,
    2: data.TPage page,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCcl(
    1: string ccl,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclOrder(
    1: string ccl,
    2: data.TOrder order,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclOrderPage(
    1: string ccl,
    2: data.TOrder order,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclPage(
    1: string ccl,
    2: data.TPage page,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTime(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTimeOrder(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTimeOrderPage(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTimePage(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTimestr(
    1: data.TCriteria criteria,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTimestrOrder(
    1: data.TCriteria criteria,
    2: string timestamp,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTimestrOrderPage(
    1: data.TCriteria criteria,
    2: string timestamp,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCriteriaTimestrPage(
    1: data.TCriteria criteria,
    2: string timestamp,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTime(
    1: string ccl,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTimeOrder(
    1: string ccl,
    2: i64 timestamp
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTimeOrderPage(
    1: string ccl,
    2: i64 timestamp
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTimePage(
    1: string ccl,
    2: i64 timestamp
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTimestr(
    1: string ccl,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTimestrOrder(
    1: string ccl,
    2: string timestamp
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTimestrOrderPage(
    1: string ccl,
    2: string timestamp
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherCclTimestrPage(
    1: string ccl,
    2: string timestamp
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaOrder(
    1: string key,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaOrderPage(
    1: string key,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaPage(
    1: string key,
    2: data.TCriteria criteria,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCcl(
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclOrder(
    1: string key,
    2: string ccl,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclOrderPage(
    1: string key,
    2: string ccl,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclPage(
    1: string key,
    2: string ccl,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTimeOrder(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTimeOrderPage(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTimePage(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTimestrOrder(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTimestrOrderPage(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCriteriaTimestrPage(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTimeOrder(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTimeOrderPage(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTimePage(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTimestrOrder(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTimestrOrderPage(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeyCclTimestrPage(
    1: string key,
    2: string ccl,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteria(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaOrder(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaOrderPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCcl(
    1: list<string> keys,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclOrder(
    1: list<string> keys,
    2: string ccl,
    3: data.TOrder order,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclOrderPage(
    1: list<string> keys,
    2: string ccl,
    3: data.TOrder order,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclPage(
    1: list<string> keys,
    2: string ccl,
    3: data.TPage page,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTime(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTimeOrder(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTimeOrderPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTimePage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.PermissionException ex3
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTimestrOrder(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTimestrOrderPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCriteriaTimestrPage(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTime(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTimeOrder(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTimeOrderPage(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTimePage(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTimestr(
    1: list<string> keys,
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
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTimestrOrder(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: data.TOrder order,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTimestrOrderPage(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: data.TOrder order,
    5: data.TPage page,
    6: shared.AccessToken creds,
    7: shared.TransactionToken transaction,
    8: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );

  map<i64, map<string, set<data.TObject>>> gatherKeysCclTimestrPage(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: data.TPage page,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment
  )
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.PermissionException ex4
  );
}
