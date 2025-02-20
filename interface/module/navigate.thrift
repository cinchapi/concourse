# Copyright (c) 2013-2025 Cinchapi Inc.
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
}