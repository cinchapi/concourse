# Copyright (c) 2013-2015 Cinchapi, Inc.
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

#
# Interface definition for the Concourse Server API.
#

include "data.thrift"
include "shared.thrift"

# To generate java source code run:
# utils/thrift-compile-java.sh
namespace java org.cinchapi.concourse.thrift

# To generate python source code run:
# utils/thrift-compile-python.sh
namespace py concourse.rpc

# To generate PHP source code run:
# utils/thrift-compile-php.sh
namespace php thrift

# The API/Product version is maintained under the Semantic Versioning
# guidelines such that versions are formatted <major>.<minor>.<patch>
#
# - Major: Incremented for backward incompatible changes. An example would be
#          changes to the number or position of method args.
# - Minor: Incremented for backward compatible changes. An example would be
#          the addition of a new method.
# - Patch: Incremented for bug fixes.
#
# As much as possible, try to preserve backward compatibility so that
# Concourse Server can always talk to older drivers. But it is not necessary
# to ensure that drivers can talk to older Concourse Server instances.
const string VERSION = "0.5.0"

# This value is passed over the wire to represent a null value, usually
# for get/select methods where a key/record has no data.
const data.TObject NULL = {'type': shared.Type.NULL}

/**
 * The interface definition for the Concourse Server API. This API defines
 * the methods that are necessary to perform CRUD operations on data in
 * accordance with the Concourse data model.
 */
service ConcourseService {

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Authentication ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /**
   * Login to the service and receive an AccessToken, which is required for
   * all CRUD operations. The AccessToken has an undocumented TTL, so clients
   * must be prepared to handle token expiration for active clients.
   *
   * @param username
   * @param password
   * @param environment
   * @return AccessToken
   * @throws TSecurityException
   */
  shared.AccessToken login(
    1: binary username,
    2: binary password,
    3: string environment)
  throws (1: shared.TSecurityException ex);

  /**
   * Logout of the service and immediately expire the access token. For
   * optimal security, the client should also discard the token after
   * invoking this method.
   *
   * @param token
   * @param environment
   * @throws TSecurityException
   */
  void logout(
    1: shared.AccessToken token,
    2: string environment)
  throws (1: shared.TSecurityException ex);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Transactions ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /**
   * Start a new transaction.
   *
   * This method will turn on STAGING mode so that all subsequent changes are
   * collected in an isolated buffer before possibly being committed to the
   * database. Staged operations are guaranteed to be reliable, all or nothing
   * units of work that allow correct  recovery from failures and provide
   * isolation between clients so the database is always in a consistent state.
   *
   * After this method returns, all subsequent operations will be done in
   * {@code staging} mode until either #abort(shared.AccessToken) or
   * #commit(shared.AccessToken) is invoked.
   *
   *
   * @param token
   * @param environment
   * @return TransactionToken
   * @throws TSecurityException
   */
  shared.TransactionToken stage(
    1: shared.AccessToken token,
    2: string environment)
  throws (1: shared.TSecurityException ex);

  /**
   * Abort the current transaction, if one exists.
   *
   * This method will discard any changes that are currently sitting in the
   * staging area. After this function returns, all subsequent operations will
   * commit to the database immediately until #stage(shared.AccessToken) is
   * called.
   *
   * @param creds
   * @param transaction
   * @param environment
   * @throws TSecurityException
   */
  void abort(
    1: shared.AccessToken creds,
    2: shared.TransactionToken transaction,
    3: string environment)
  throws (1: shared.TSecurityException ex);

  /**
   * Commit the current transaction, if one exists.
   *
   * This method will attempt to permanently commit all the changes that are
   * currently sitting in the staging area. This function only returns TRUE
   * if all the changes can be successfully applied to the database. Otherwise,
   * this function returns FALSE and all the changes are discarded.
   *
   * After this function returns, all subsequent operations will commit to the
   * database immediately until #stage(shared.AccessToken) is invoked.
   *
   * @param creds
   * @param transaction
   * @param environment
   * @return boolean
   * @throws TSecurityException
   * @throws TTransactionException
   */
  bool commit(
    1: shared.AccessToken creds,
    2: shared.TransactionToken transaction,
     3: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Write Methods ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  bool addKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  i64 addKeyValue(
    1: string key,
    2: data.TObject value,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, bool> addKeyValueRecords(
    1: string key
    2: data.TObject value,
    3: list<i64> records,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void clearRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void clearRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void clearKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void clearKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void clearKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void clearKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<i64> insertJson(
    1: string json
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  bool insertJsonRecord(
    1: string json
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, bool> insertJsonRecords(
    1: string json
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  bool removeKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, bool> removeKeyValueRecords(
    1: string key
    2: data.TObject value,
    3: list<i64> records,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void setKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  i64 setKeyValue(
    1: string key,
    2: data.TObject value,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void setKeyValueRecords(
    1: string key
    2: data.TObject value,
    3: list<i64> records,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Read Methods ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  set<i64> find(
    1: shared.AccessToken creds,
    2: shared.TransactionToken transaction,
    3: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, set<data.TObject>> selectRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, set<data.TObject>> selectRecordTime(
    1: i64 record,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectRecordsTime(
    1: list<i64> records,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<data.TObject, set<i64>> browseKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, map<data.TObject, set<i64>>> browseKeys(
    1: list<string> keys,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<data.TObject, set<i64>> browseKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, map<data.TObject, set<i64>>> browseKeysTime(
    1: list<string> keys,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<string> describeRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<string> describeRecordTime(
    1: i64 record,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<string>> describeRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<string>> describeRecordsTime(
    1: list<i64> records,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<data.TObject> selectKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<data.TObject> selectKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, set<data.TObject>> selectKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, set<data.TObject>> selectKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<data.TObject>> selectKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<data.TObject>> selectKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<data.TObject>> selectKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<data.TObject>> selectKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectKeysCriteria(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectKeysCriteriaTime(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  data.TObject getKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  data.TObject getKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, data.TObject> getKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<string, data.TObject> getKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, data.TObject>> getKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, data.TObject> getKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, data.TObject> getKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, data.TObject>> getKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, data.TObject> getKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, data.TObject> getKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, data.TObject>> getKeysCriteria(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, map<string, data.TObject>> getKeysCriteriaTime(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  bool verifyKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  bool verifyKeyValueRecordTime(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: i64 timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Query Methods ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  set<i64> findCriteria(
    1: data.TCriteria criteria,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<i64> findKeyOperatorValues(
    1: string key,
    2: shared.Operator operator,
    3: list<data.TObject> values
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<i64> findKeyOperatorValuesTime(
    1: string key,
    2: shared.Operator operator,
    3: list<data.TObject> values
    4: i64 timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<i64> findKeyStringOperatorValues(
    1: string key,
    2: string operator,
    3: list<data.TObject> values
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<i64> findKeyStringOperatorValuesTime(
    1: string key,
    2: string operator,
    3: list<data.TObject> values
    4: i64 timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  set<i64> search(
    1: string key,
    2: string query,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Version Control ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  map<i64, string> auditRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, string> auditRecordStart(
    1: i64 record,
    2: i64 start,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, string> auditRecordStartEnd(
    1: i64 record,
    2: i64 start,
    3: i64 tend,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, string> auditKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, string> auditKeyRecordStart(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, string> auditKeyRecordStartEnd(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: i64 tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<data.TObject>> chronologizeKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<data.TObject>> chronologizeKeyRecordStart(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  map<i64, set<data.TObject>> chronologizeKeyRecordStartEnd(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: i64 tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void revertKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void revertKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void revertKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void revertKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Status ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~

  map<i64, bool> pingRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  bool pingRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Atomic Operations ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  bool verifyAndSwap(
    1: string key,
    2: data.TObject expected,
    3: i64 record,
    4: data.TObject replacement,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  void verifyOrSet(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Metadata ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~

  string getServerEnvironment(
    1: shared.AccessToken creds,
    2: shared.TransactionToken token,
    3: string environment)
  throws (1: shared.TSecurityException ex, 2: shared.TTransactionException ex2);

  string getServerVersion() throws (
    1: shared.TSecurityException ex,
    2: shared.TTransactionException ex2);
}
