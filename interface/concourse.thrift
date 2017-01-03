# Copyright (c) 2013-2017 Cinchapi Inc.
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
# Interface definition for the Concourse Server API.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

include "data.thrift"
include "shared.thrift"
include "exceptions.thrift"
include "complex.thrift"

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
# Concourse Server can always talk to older drivers.
const string VERSION = "0.5.0"

# This value is passed over the wire to represent a null value, usually
# for get/select methods where a key/record has no data.
const data.TObject NULL = {'type': shared.Type.NULL}

# The key that is used to refer to the record id/primary key in a JSON
# dump.
const string JSON_RESERVED_IDENTIFIER_NAME = "$id$"

/**
 * The interface definition for the Concourse Server API.
 */
service ConcourseService {

  /**
   * Abort the current transaction and discard any changes that are
   * currently staged.
   * <p>
   * After returning, the driver will return to {@code autocommit} mode and
   * all subsequent changes will be committed immediately.
   * </p>
   * <p>
   * Calling this method when the driver is not in {@code staging} mode is a
   * no-op.
   * </p>
   * @param record the id of the record in which an attempt is made to add
   *                 the data
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @throws exceptions.SecurityException if the {@code creds} don't represent a
   *         valid session
   */
  void abort(
    1: shared.AccessToken creds,
    2: shared.TransactionToken transaction,
    3: string environment
  )
  throws (
    1: exceptions.SecurityException ex
  );

  /**
   * Append {@code key} as {@code value} in a new record.
   *
   * @param key the field name
   * @param value the value to add
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return the new record id
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.InvalidArgumentException if any of provided data
   *         can't be stored
   */
  i64 addKeyValue(
    1: string key,
    2: data.TObject value,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  /**
   * Append {@code key} as {@code value} in {@code record}.
   *
   * @param key the field name
   * @param value the value to add
   * @param record the record id where an attempt is made to add the data
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a bool that indicates if the data was added
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.InvalidArgumentException if any of provided data
   *         can't be stored
   */
  bool addKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  /**
   * Append {@code key} as {@code value} in each of the {@code records} where it
   * doesn't exist.
   *
   * @param key the field name
   * @param value the value to add
   * @param records a list of record ids where an attempt is made to add the
   *                  data
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a mapping from each record id to a boolean that indicates if the
   *                   data was added
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.InvalidArgumentException if any of provided data
   *         can't be stored
   */
  map<i64, bool> addKeyValueRecords(
    1: string key
    2: data.TObject value,
    3: list<i64> records,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  /**
   * List all the changes ever made to {@code record}.
   *
   * @param record the record id
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *                  revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, string> auditRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the changes made to {@code record} since {@code start}
   * (inclusive).
   *
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *                  revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, string> auditRecordStart(
    1: i64 record,
    2: i64 start,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the changes made to {@code record} since {@code start}
   * (inclusive).
   *
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *                  revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<i64, string> auditRecordStartstr(
    1: i64 record,
    2: string start,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List all the changes made to {@code record} between {@code start}
   * (inclusive) and {@code end} (non-inclusive).
   *
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param end a non-inclusive timestamp that for the most recent recent
   *              change that should possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *         revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, string> auditRecordStartEnd(
    1: i64 record,
    2: i64 start,
    3: i64 tend,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the changes made to {@code record} between {@code start}
   * (inclusive) and {@code end} (non-inclusive).
   *
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param end a non-inclusive timestamp that for the most recent recent
   *              change that should possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *         revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<i64, string> auditRecordStartstrEndstr(
    1: i64 record,
    2: string start,
    3: string tend,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List all the changes ever made to the {@code key} field in {@code record}.
   *
   * @param key the field name
   * @param record the record id
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *         revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, string> auditKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the changes made to the {@code key} field in {@code record} since
   * {@code start} (inclusive).
   *
   * @param key the field name
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *         revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, string> auditKeyRecordStart(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the changes made to the {@code key} field in {@code record} since
   * {@code start} (inclusive).
   *
   * @param key the field name
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *         revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<i64, string> auditKeyRecordStartstr(
    1: string key,
    2: i64 record,
    3: string start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List all the changes made to the {@code key} field in {@code record}
   * between {@code start} (inclusive) and {@code end} (non-inclusive).
   *
   * @param key the field name
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param end a non-inclusive timestamp that for the most recent change that
   *              should possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *         revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, string> auditKeyRecordStartEnd(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: i64 tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the changes made to the {@code key} field in {@code record}
   * between {@code start} (inclusive) and {@code end} (non-inclusive).
   *
   * @param key the field name
   * @param record the record id
   * @param start an inclusive timestamp for the oldest change that should
   *                possibly be included in the audit
   * @param end a non-inclusive timestamp that for the most recent recent
   *              change that should possibly be included in the audit
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return for each change, a mapping from timestamp to a description of the
   *         revision
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<i64, string> auditKeyRecordStartstrEndstr(
    1: string key,
    2: i64 record,
    3: string start,
    4: string tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * View the values from all records that are currently stored for {@code key}.
   *
   * @param keys the field name
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each value to the {@link Set} of records
   *         that contain that value in the {@code key} field
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<data.TObject, set<i64>> browseKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * View the values from all records that are currently stored for each of the
   * {@code keys}.
   *
   * @param keys a list of field names
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each key to a {@link Map} associating
   *         each value to the set of records that contain that value in the
   *         {@code key} field
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<string, map<data.TObject, set<i64>>> browseKeys(
    1: list<string> keys,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * View the values from all records that were stored for {@code key} at
   * {@code timestamp}.
   *
   * @param keys the field name
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each value to the {@link Set} of records
   *         that contained that value in the {@code key} field at {@code
   *         timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<data.TObject, set<i64>> browseKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * View the values from all records that were stored for {@code key} at
   * {@code timestamp}.
   *
   * @param keys the field name
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each value to the {@link Set} of records
   *         that contained that value in the {@code key} field at {@code
   *         timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<data.TObject, set<i64>> browseKeyTimestr(
    1: string key,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * View the values from all records that were stored for each of the
   * {@code keys} at {@code timestamp}.
   *
   * @param keys a list of field names
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each key to a {@link Map} associating
   *         each value to the {@link Set} of records that contained that value
   *         in the {@code key} field at {@code timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<string, map<data.TObject, set<i64>>> browseKeysTime(
    1: list<string> keys,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * View the values from all records that were stored for each of the
   * {@code keys} at {@code timestamp}.
   *
   * @param keys a list of field names
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each key to a {@link Map} associating
   *         each value to the {@link Set} of records that contained that value
   *         in the {@code key} field at {@code timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<string, map<data.TObject, set<i64>>> browseKeysTimestr(
    1: list<string> keys,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * View a time series that associates the timestamp of each modification for
   * {@code key} in {@code record} to a snapshot containing the values that
   * were stored in the field after the change.
   *
   * @param key the field name
   * @param record the record id
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each modification timestamp to the
   *         {@link Set} of values that were stored in the field after the
   *         change.
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, set<data.TObject>> chronologizeKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * View a time series between {@code start} (inclusive) and the present that
   * associates the timestamp of each modification for {@code key} in
   * {@code record} to a snapshot containing the values that
   * were stored in the field after the change.
   *
   * @param key the field name
   * @param record the record id
   * @param start the first possible {@link Timestamp} to include in the
   *            time series
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each modification timestamp to the
   *         {@link Set} of values that were stored in the field after the
   *         change.
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, set<data.TObject>> chronologizeKeyRecordStart(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * View a time series between {@code start} (inclusive) and the present that
   * associates the timestamp of each modification for {@code key} in
   * {@code record} to a snapshot containing the values that
   * were stored in the field after the change.
   *
   * @param key the field name
   * @param record the record id
   * @param start the first possible {@link Timestamp} to include in the
   *            time series
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each modification timestamp to the
   *         {@link Set} of values that were stored in the field after the
   *         change.
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<i64, set<data.TObject>> chronologizeKeyRecordStartstr(
    1: string key,
    2: i64 record,
    3: string start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * View a time series between {@code start} (inclusive) and {@code end}
   * (non-inclusive) that associates the timestamp of each modification for
   * {@code key} in {@code record} to a snapshot containing the values that
   * were stored in the field after the change.
   *
   * @param key the field name
   * @param record the record id
   * @param start the first possible {@link Timestamp} to include in the
   *            time series
   * @param end the {@link Timestamp} that should be greater than every
   *            timestamp in the time series
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each modification timestamp to the
   *         {@link Set} of values that were stored in the field after the
   *         change.
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, set<data.TObject>> chronologizeKeyRecordStartEnd(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: i64 tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * View a time series between {@code start} (inclusive) and {@code end}
   * (non-inclusive) that associates the timestamp of each modification for
   * {@code key} in {@code record} to a snapshot containing the values that
   * were stored in the field after the change.
   *
   * @param key the field name
   * @param record the record id
   * @param start the first possible {@link Timestamp} to include in the
   *            time series
   * @param end the {@link Timestamp} that should be greater than every
   *            timestamp in the time series
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each modification timestamp to the
   *         {@link Set} of values that were stored in the field after the
   *         change.
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<i64, set<data.TObject>> chronologizeKeyRecordStartstrEndstr(
    1: string key,
    2: i64 record,
    3: string start,
    4: string tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /*
   * Atomically remove all the values stored for every key in {@code record}.
   *
   * @param record the record id
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  void clearRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /*
   * Atomically remove all the values stored for every key in each of the
   * {@code records}.
   *
   * @param records a list of record ids
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  void clearRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /*
   * Atomically remove all the values stored for {@code key} in {@code record}
   *
   * @param key the field name
   * @param record the record id
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  void clearKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /*
   * Atomically remove all the values stored for each of the {@code keys} in
   * {@code record}.
   *
   * @param keys a list of field names
   * @param record the record id
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  void clearKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /*
   * Atomically remove all the values stored for {@code key} in each of the
   * {@code records}.
   *
   * @param key the field name
   * @param records a list of record ids
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  void clearKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /*
   * Atomically remove all the values stored for each of the {@code keys} in
   * each of the {@code records}.
   *
   * @param keys a list of field names
   * @param records a list of record ids.
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  void clearKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * Attempt to permanently commit any changes that are staged in a transaction
   * and return {@code true} if and only if all the changes can be applied.
   * Otherwise, returns {@code false} and all the changes are discarded.
   * <p>
   * After returning, the driver will return to {@code autocommit} mode and
   * all subsequent changes will be committed immediately.
   * </p>
   * <p>
   * This method will return {@code false} if it is called when the driver is
   * not in {@code staging} mode.
   * </p>
   *
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return {@code true} if all staged changes are committed, otherwise {@code
   *                      false}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  bool commit(
    1: shared.AccessToken creds,
    2: shared.TransactionToken transaction,
    3: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the keys in {@code record} that have at least one value.
   *
   * @param record the record id
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return the {@link Set} of keys in {@code record}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  set<string> describeRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the keys in {@code record} that had at least one value at
   * {@code timestamp}.
   *
   * @param record the record id
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return the {@link Set} of keys that were in {@code record} at
   *         {@code timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  set<string> describeRecordTime(
    1: i64 record,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List all the keys in {@code record} that have at least one value.
   *
   * @param record the record id
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return the {@link Set} of keys that were in {@code record} at
   *         {@code timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  set<string> describeRecordTimestr(
    1: i64 record,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * For each of the {@code records}, list all of the keys that have at least
   * one value.
   *
   * @param records a collection of record ids
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each record id to the {@link Set} of
   *         keys in that record
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, set<string>> describeRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * For each of the {@code records}, list all the keys that had at least one
   * value at {@code timestamp}.
   *
   * @param records a collection of record ids
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each record id to the {@link Set} of
   *         keys that were in that record at {@code timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<i64, set<string>> describeRecordsTime(
    1: list<i64> records,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * For each of the {@code records}, list all the keys that had at least one
   * value at {@code timestamp}.
   *
   * @param records a collection of record ids
   * @param timestamp the historical timestamp to use in the lookup
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} associating each record id to the {@link Set} of
   *         keys that were in that record at {@code timestamp}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<i64, set<string>> describeRecordsTimestr(
    1: list<i64> records,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List the net changes made to {@code record} since {@code start}.
   *
   * <p>
   * If you begin with the state of the {@code record} at {@code start} and
   * re-apply all the changes in the diff, you'll re-create the state of the
   * {@code record} at the present.
   * </p>
   *
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each key in the {@code record} to
   *         another {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3",
   *         "value4"]}}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<string, map<shared.Diff, set<data.TObject>>> diffRecordStart(
    1: i64 record,
    2: i64 start,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List the net changes made to {@code record} since {@code start}.
   *
   * <p>
   * If you begin with the state of the {@code record} at {@code start} and
   * re-apply all the changes in the diff, you'll re-create the state of the
   * {@code record} at the present.
   * </p>
   *
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each key in the {@code record} to
   *         another {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3",
   *         "value4"]}}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<string, map<shared.Diff, set<data.TObject>>> diffRecordStartstr(
    1: i64 record,
    2: string start,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List the net changes made to {@code record} from {@code start} to
   * {@code end}.
   *
   * <p>
   * If you begin with the state of the {@code record} at {@code start} and
   * re-apply all the changes in the diff, you'll re-create the state of the
   * {@code record} at {@code end}.
   * </p>
   *
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param end the comparison timestamp to which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each key in the {@code record} to
   *         another {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3",
   *         "value4"]}}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<string, map<shared.Diff, set<data.TObject>>> diffRecordStartEnd(
    1: i64 record,
    2: i64 start,
    3: i64 tend,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List the net changes made to {@code record} from {@code start} to
   * {@code end}.
   *
   * <p>
   * If you begin with the state of the {@code record} at {@code start} and
   * re-apply all the changes in the diff, you'll re-create the state of the
   * {@code record} at {@code end}.
   * </p>
   *
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param end the comparison timestamp to which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each key in the {@code record} to
   *         another {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3",
   *         "value4"]}}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<string, map<shared.Diff, set<data.TObject>>> diffRecordStartstrEndstr(
    1: i64 record,
    2: string start,
    3: string tend,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List the net changes made to {@code key} in {@code record} since
   * {@code start}.
   *
   * <p>
   * If you begin with the state of the field at {@code start} and re-apply
   * all the changes in the diff, you'll re-create the state of the field at
   * the present.
   * </p>
   *
   * @param key the field name
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<shared.Diff, set<data.TObject>> diffKeyRecordStart(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List the net changes made to {@code key} in {@code record} since
   * {@code start}.
   *
   * <p>
   * If you begin with the state of the field at {@code start} and re-apply
   * all the changes in the diff, you'll re-create the state of the field at
   * the present.
   * </p>
   *
   * @param key the field name
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<shared.Diff, set<data.TObject>> diffKeyRecordStartstr(
    1: string key,
    2: i64 record,
    3: string start,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List the net changes made to {@code key} in {@code record} from
   * {@code start} to {@code end}.
   *
   * <p>
   * If you begin with the state of the field at {@code start} and re-apply
   * all the changes in the diff, you'll re-create the state of the field at
   * {@code end}.
   * </p>
   *
   * @param key the field name
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param end the comparison timestamp to which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<shared.Diff, set<data.TObject>> diffKeyRecordStartEnd(
    1: string key,
    2: i64 record,
    3: i64 start,
    4: i64 tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List the net changes made to {@code key} in {@code record} from
   * {@code start} to {@code end}.
   *
   * <p>
   * If you begin with the state of the field at {@code start} and re-apply
   * all the changes in the diff, you'll re-create the state of the field at
   * {@code end}.
   * </p>
   *
   * @param key the field name
   * @param record the record id
   * @param start the base timestamp from which the diff is calculated
   * @param end the comparison timestamp to which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates a {@link Diff change
   *         description} to the {@link Set} of values that fit the
   *         description (i.e. <code>
   *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
   *         </code> )
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<shared.Diff, set<data.TObject>> diffKeyRecordStartstrEndstr(
    1: string key,
    2: i64 record,
    3: string start,
    4: string tend,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List the net changes made to the {@code key} field across all records
   * since {@code start}.
   *
   * <p>
   * If you begin with the state of an inverted index for {@code key} at
   * {@code start} and re-apply all the changes in the diff, you'll re-create
   * the state of the same index at the present.
   * </p>
   *
   * @param key the field name
   * @param start the base timestamp from which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each value stored for {@code key}
   *         across all records to another {@link Map} that associates a
   *         {@link Diff change description} to the {@link Set} of records
   *         where the description applies to that value in the {@code key}
   *         field (i.e. <code>
   *         {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
   *         </code>)
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<data.TObject, map<shared.Diff, set<i64>>> diffKeyStart(
    1: string key,
    2: i64 start,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List the net changes made to the {@code key} field across all records
   * since {@code start}.
   *
   * <p>
   * If you begin with the state of an inverted index for {@code key} at
   * {@code start} and re-apply all the changes in the diff, you'll re-create
   * the state of the same index at the present.
   * </p>
   *
   * @param key the field name
   * @param start the base timestamp from which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each value stored for {@code key}
   *         across all records to another {@link Map} that associates a
   *         {@link Diff change description} to the {@link Set} of records
   *         where the description applies to that value in the {@code key}
   *         field (i.e. <code>
   *         {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
   *         </code>)
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<data.TObject, map<shared.Diff, set<i64>>> diffKeyStartstr(
    1: string key,
    2: string start,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * List the net changes made to the {@code key} field across all records
   * from {@code start} to {@code end}.
   *
   * <p>
   * If you begin with the state of an inverted index for {@code key} at
   * {@code start} and re-apply all the changes in the diff, you'll re-create
   * the state of the same index at {@code end}.
   * </p>
   *
   * @param key the field name
   * @param start the base timestamp from which the diff is calculated
   * @param end the comparison timestamp to which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each value stored for {@code key}
   *         across all records to another {@link Map} that associates a
   *         {@link Diff change description} to the {@link Set} of records
   *         where the description applies to that value in the {@code key}
   *         field (i.e. <code>
   *         {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
   *         </code>)
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   */
  map<data.TObject, map<shared.Diff, set<i64>>> diffKeyStartEnd(
    1: string key,
    2: i64 start,
    3: i64 tend,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  /**
   * List the net changes made to the {@code key} field across all records
   * from {@code start} to {@code end}.
   *
   * <p>
   * If you begin with the state of an inverted index for {@code key} at
   * {@code start} and re-apply all the changes in the diff, you'll re-create
   * the state of the same index at {@code end}.
   * </p>
   *
   * @param key the field name
   * @param start the base timestamp from which the diff is calculated
   * @param end the comparison timestamp to which the diff is calculated
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a {@link Map} that associates each value stored for {@code key}
   *         across all records to another {@link Map} that associates a
   *         {@link Diff change description} to the {@link Set} of records
   *         where the description applies to that value in the {@code key}
   *         field (i.e. <code>
   *         {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
   *         </code>)
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.ParseException if a string cannot be properly parsed
   *         into a timestamp
   */
  map<data.TObject, map<shared.Diff, set<i64>>> diffKeyStartstrEndstr(
    1: string key,
    2: string start,
    3: string tend,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  /**
   * Invoke a Plugin method.
   *
   * <p>
   * Assuming that there is a plugin distribution that contains a class
   * named after {@code id}, and has the specified {@code method}, invoke the
   * same with {@code params} and return the result.
   * </p>
   *
   * @param id the fully qualified name of the plugin class
   * @param method the name of the method in {@code clazz} to invoke
   * @param params a list of TObjects to pass to {@code method} as args
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return the result of the method invocation
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.InvalidArgumentException if any of the arguments are
   *         invalid
   */
  complex.ComplexTObject invokePlugin(
    1: string id,
    2: string method,
    3: list<complex.ComplexTObject> params,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  /**
   * Attempt to authenticate the user identified by the {@code username} and
   * {@code password} combination to the specified {@code environment}. If
   * successful, establish a new session within the {@code environment} on
   * behalf of that user and return an {@link shared.AccessToken}, which is
   * required for all subsequent operations.
   *
   * <p>
   * The AccessToken <em>may</em> expire after a while so clients should be
   * prepared to seamlessly login again for active user sessions.
   * </p>
   *
   * @param username a binary representation of the UTF-8 encoded username
   * @param password a binary representation of the UTF-8 encoded password
   * @param environment the name of the environment into which to login
   * @return an {@link shared.AccessToken} to submit with all subsequent method
   *         calls
   * @throws exceptions.SecurityException if the login is not successful
   */
  shared.AccessToken login(
    1: binary username,
    2: binary password,
    3: string environment)
  throws (
    1: exceptions.SecurityException ex);

  /**
   * Terminate the session within {@code environment} for the user represented
   * by the {@code token}. Afterwards, all other attempts to use {@code token}
   * will result in a {@link exceptions.SecurityException} being thrown.
   *
   * @param token the {@link shared.AccessToken to expire}
   * @param environment the environment of the session represented by the
   *                    {@code token}
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   */
  void logout(
    1: shared.AccessToken token,
    2: string environment)
  throws (
    1: exceptions.SecurityException ex);

  /**
   * Start a new transaction.
   * <p>
   * This method will turn on STAGING mode so that all subsequent changes are
   * collected in an isolated buffer before possibly being committed to the
   * database. Staged operations are guaranteed to be reliable, all or nothing
   * units of work that allow correct recovery from failures and provide
   * isolation between clients so the database is always in a consistent state.
   * </p>
   * <p>
   * After this method returns, all subsequent operations will be done in
   * {@code staging} mode until either #abort(shared.AccessToken) or
   * #commit(shared.AccessToken) is called.
   * </p>
   *
   * @param token
   * @param environment
   * @return TransactionToken
   * @throws TSecurityException
   */
  shared.TransactionToken stage(
    1: shared.AccessToken token,
    2: string environment)
  throws (
    1: exceptions.SecurityException ex);


  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Write Methods ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  set<i64> insertJson(
    1: string json
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  bool insertJsonRecord(
    1: string json
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, bool> insertJsonRecords(
    1: string json
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  bool removeKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  map<i64, bool> removeKeyValueRecords(
    1: string key
    2: data.TObject value,
    3: list<i64> records,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  void setKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  i64 setKeyValue(
    1: string key,
    2: data.TObject value,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  void setKeyValueRecords(
    1: string key
    2: data.TObject value,
    3: list<i64> records,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  /**
   * The {@code value} in {@code key} of {@code record} are added
   * and removed to be set as exactly the same as the input values
   *
   * @param key the field name
   * @param record the record id where an attempt is made to add the data
   * @param values collection of values to set
   * @param creds the {@link shared.AccessToken} that is used to authenticate
   *                the user on behalf of whom the client is connected
   * @param transaction the {@link shared.TransactionToken} that the
   *                      server uses to find the current transaction for the
   *                      client (optional)
   * @param environment the environment to which the client is connected
   * @return a bool that indicates if the data was added
   * @throws exceptions.SecurityException if the {@code creds} don't
   *         represent a valid session
   * @throws exceptions.TransactionException if the client was in a
   *         transaction and an error occurred that caused the transaction
   *         to end itself
   * @throws exceptions.InvalidArgumentException if any of provided data
   *         can't be stored
   */
  void reconcileKeyRecordValues(
    1: string key,
    2: i64 record,
    3: set<data.TObject> values,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Read Methods ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  set<i64> inventory(
    1: shared.AccessToken creds,
    2: shared.TransactionToken transaction,
    3: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<string, set<data.TObject>> selectRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<string, set<data.TObject>> selectRecordTime(
    1: i64 record,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<string, set<data.TObject>> selectRecordTimestr(
    1: i64 record,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectRecordsTime(
    1: list<i64> records,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectRecordsTimestr(
    1: list<i64> records,
    2: string timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  set<data.TObject> selectKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  set<data.TObject> selectKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  set<data.TObject> selectKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<string, set<data.TObject>> selectKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<string, set<data.TObject>> selectKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<string, set<data.TObject>> selectKeysRecordTimestr(
    1: list<string> keys,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, set<data.TObject>> selectKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, set<data.TObject>> selectKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, set<data.TObject>> selectKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectKeysRecordsTimestr(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectCriteria(
    1: data.TCriteria criteria,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectCcl(
    1: string ccl,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, map<string, set<data.TObject>>> selectCriteriaTime(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectCriteriaTimestr(
    1: data.TCriteria criteria,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectCclTime(
    1: string ccl,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, map<string, set<data.TObject>>> selectCclTimestr(
    1: string ccl,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, set<data.TObject>> selectKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, set<data.TObject>> selectKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, set<data.TObject>> selectKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, set<data.TObject>> selectKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, set<data.TObject>> selectKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, set<data.TObject>> selectKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectKeysCriteria(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectKeysCcl(
    1: list<string> keys,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectKeysCriteriaTime(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, set<data.TObject>>> selectKeysCriteriaTimestr(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, set<data.TObject>>> selectKeysCclTime(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, map<string, set<data.TObject>>> selectKeysCclTimestr(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  data.TObject getKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject getKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject getKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<string, data.TObject> getKeysRecord(
    1: list<string> keys,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<string, data.TObject> getKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<string, data.TObject> getKeysRecordTimestr(
    1: list<string> keys,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, data.TObject>> getKeysRecords(
    1: list<string> keys,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, data.TObject> getKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, data.TObject> getKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, data.TObject> getKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, data.TObject>> getKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, data.TObject>> getKeysRecordsTimestr(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, data.TObject> getKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, data.TObject>> getCriteria(
    1: data.TCriteria criteria,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, data.TObject>> getCcl(
    1: string ccl,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, map<string, data.TObject>> getCriteriaTime(
    1: data.TCriteria criteria,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, data.TObject>> getCriteriaTimestr(
    1: data.TCriteria criteria,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, data.TObject>> getCclTime(
    1: string ccl,
    2: i64 timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, map<string, data.TObject>> getCclTimestr(
    1: string ccl,
    2: string timestamp
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, data.TObject> getKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, data.TObject> getKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, data.TObject> getKeyCriteriaTimestr(
    1: string key,
    2: data.TCriteria criteria,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, data.TObject> getKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, data.TObject> getKeyCclTimestr(
    1: string key,
    2: string ccl,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, data.TObject>> getKeysCriteria(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, data.TObject>> getKeysCcl(
    1: list<string> keys,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, data.TObject>> getKeysCriteriaTime(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  map<i64, map<string, data.TObject>> getKeysCriteriaTimestr(
    1: list<string> keys,
    2: data.TCriteria criteria,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  map<i64, map<string, data.TObject>> getKeysCclTime(
    1: list<string> keys,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

    map<i64, map<string, data.TObject>> getKeysCclTimestr(
    1: list<string> keys,
    2: string ccl,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  bool verifyKeyValueRecord(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  bool verifyKeyValueRecordTime(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: i64 timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  bool verifyKeyValueRecordTimestr(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: string timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  string jsonifyRecords(
    1: list<i64> records,
    2: bool identifier,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  string jsonifyRecordsTime(
    1: list<i64> records,
    2: i64 timestamp
    3: bool identifier,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  string jsonifyRecordsTimestr(
    1: list<i64> records,
    2: string timestamp
    3: bool identifier,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Query Methods ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  set<i64> findCriteria(
    1: data.TCriteria criteria,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  set<i64> findCcl(
    1: string ccl,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  set<i64> findKeyOperatorValues(
    1: string key,
    2: shared.Operator operator,
    3: list<data.TObject> values
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  set<i64> findKeyOperatorValuesTime(
    1: string key,
    2: shared.Operator operator,
    3: list<data.TObject> values
    4: i64 timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  set<i64> findKeyOperatorValuesTimestr(
    1: string key,
    2: shared.Operator operator,
    3: list<data.TObject> values
    4: string timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  set<i64> findKeyOperatorstrValues(
    1: string key,
    2: string operator,
    3: list<data.TObject> values
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  set<i64> findKeyOperatorstrValuesTime(
    1: string key,
    2: string operator,
    3: list<data.TObject> values
    4: i64 timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  set<i64> findKeyOperatorstrValuesTimestr(
    1: string key,
    2: string operator,
    3: list<data.TObject> values
    4: string timestamp,
    5: shared.AccessToken creds,
    6: shared.TransactionToken transaction,
    7: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  set<i64> search(
    1: string key,
    2: string query,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Version Control ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  void revertKeysRecordsTime(
    1: list<string> keys,
    2: list<i64> records,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  void revertKeysRecordsTimestr(
    1: list<string> keys,
    2: list<i64> records,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  void revertKeysRecordTime(
    1: list<string> keys,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  void revertKeysRecordTimestr(
    1: list<string> keys,
    2: i64 record,
    3: string timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  void revertKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  void revertKeyRecordsTimestr(
    1: string key,
    2: list<i64> records,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  void revertKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  void revertKeyRecordTimestr(
    1: string key,
    2: i64 record,
    3: string timestamp
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  # ~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Status ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~

  map<i64, bool> pingRecords(
    1: list<i64> records,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  bool pingRecord(
    1: i64 record,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

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
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  void verifyOrSet(
    1: string key,
    2: data.TObject value,
    3: i64 record,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.InvalidArgumentException ex3);

  i64 findOrAddKeyValue(
    1: string key,
    2: data.TObject value,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.DuplicateEntryException ex3,
    4: exceptions.InvalidArgumentException ex4);

  i64 findOrInsertCriteriaJson(
    1: data.TCriteria criteria,
    2: string json,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.DuplicateEntryException ex3);

  i64 findOrInsertCclJson(
    1: string ccl,
    2: string json,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3,
    4: exceptions.DuplicateEntryException ex4);
   
  data.TObject sumKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);
  
  data.TObject sumKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);
  
  data.TObject sumKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject sumKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);
  
  data.TObject sumKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject sumKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2); 
  
  data.TObject sumKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject sumKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  data.TObject sumKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject sumKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  data.TObject averageKeyRecord(
    1: string key,
    2: i64 record,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKey(
    1: string key,
    2: shared.AccessToken creds,
    3: shared.TransactionToken transaction,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKeyTime(
    1: string key,
    2: i64 timestamp,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKeyRecordTime(
    1: string key,
    2: i64 record,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKeyRecords(
    1: string key,
    2: list<i64> records,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKeyRecordsTime(
    1: string key,
    2: list<i64> records,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKeyCriteria(
    1: string key,
    2: data.TCriteria criteria,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKeyCcl(
    1: string key,
    2: string ccl,
    3: shared.AccessToken creds,
    4: shared.TransactionToken transaction,
    5: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  data.TObject averageKeyCriteriaTime(
    1: string key,
    2: data.TCriteria criteria,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  data.TObject averageKeyCclTime(
    1: string key,
    2: string ccl,
    3: i64 timestamp,
    4: shared.AccessToken creds,
    5: shared.TransactionToken transaction,
    6: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~ Metadata ~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~

  string getServerEnvironment(
    1: shared.AccessToken creds,
    2: shared.TransactionToken token,
    3: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  string getServerVersion() throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  i64 time(
    1: shared.AccessToken creds,
    2: shared.TransactionToken token,
    3: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2);

  i64 timePhrase(
    1: string phrase
    2: shared.AccessToken creds,
    3: shared.TransactionToken token,
    4: string environment)
  throws (
    1: exceptions.SecurityException ex,
    2: exceptions.TransactionException ex2,
    3: exceptions.ParseException ex3);
}
