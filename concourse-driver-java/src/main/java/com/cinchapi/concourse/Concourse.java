/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cinchapi.concourse.annotate.CompoundOperation;
import com.cinchapi.concourse.config.ConcourseClientPreferences;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.security.ClientSecurity;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ConcourseService;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Collections;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedHashMap;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * A client connection to a Concourse deployment.
 * <p>
 * Use one of the {@link Concourse#connect()} methods to instantiate.
 * </p>
 * 
 * <h2>Overview</h2>
 * <p>
 * Concourse is a self-tuning database that is designed for both ad hoc
 * analytics and high volume transactions at scale. Developers use Concourse to
 * quickly build mission critical software while also benefiting from real time
 * insight into their most important data. With Concourse, end-to-end data
 * management requires no extra infrastructure, no prior configuration and no
 * additional coding–all of which greatly reduce costs and allow developers to
 * focus on core business problems.
 * </p>
 * 
 * <h2>Using Transactions</h2>
 * <p>
 * By default, Concourse conducts every operation in {@code autocommit} mode
 * where every change is immediately written. Concourse also supports the
 * ability to stage a group of operations in transactions that are atomic,
 * consistent, isolated, and durable using the {@link #stage()},
 * {@link #commit()} and {@link #abort()} methods.
 * 
 * </p>
 * <h2>Thread Safety</h2>
 * <p>
 * You should <strong>not</strong> use the same client connection in multiple
 * threads. If you need to interact with Concourse using multiple threads, you
 * should create a separate connection for each thread or use a
 * {@link ConnectionPool}.
 * </p>
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class Concourse implements AutoCloseable {

    /**
     * Create a new connection to the Concourse deployment described in
     * {@code ./concourse_client.prefs} (or, if the file does not exist, the
     * default environment of the server at localhost:1717) and return a handle
     * to facilitate interaction.
     * 
     * @return the handle
     */
    public static Concourse connect() {
        return new Client();
    }

    /**
     * Create a new connection to the specified {@code environment} of the
     * Concourse deployment described in {@code ~/concourse_client.prefs} (or,
     * if the file does not exist, the server at localhost:1717) and return a
     * handle to facilitate interaction.
     * 
     * @param environment the environment to use
     * @return the handle
     */
    public static Concourse connect(String environment) {
        return new Client(environment);
    }

    /**
     * Create a new connection to the default environment of the specified
     * Concourse Server and return a handle to facilitate interaction.
     * 
     * @param host the server host
     * @param port the listener port for client connections
     * @param username the name of the user on behalf of whom to connect
     * @param password the password for the {@code username}
     * @return the handle
     */
    public static Concourse connect(String host, int port, String username,
            String password) {
        return new Client(host, port, username, password);
    }

    /**
     * Create a new connection to the specified {@code environment} of the
     * specified Concourse Server and return a handle to facilitate interaction.
     * 
     * @param host the server host
     * @param port the listener port for client connections
     * @param username the name of the user on behalf of whom to connect
     * @param password the password for the {@code username}
     * @param environment the name of the environment to use for the
     *            connection
     * @return the handle
     */
    public static Concourse connect(String host, int port, String username,
            String password, String environment) {
        return new Client(host, port, username, password, environment);
    }

    /**
     * Create a new connection using the information specified in the prefs
     * {@code file}.
     * 
     * @param file the absolute path to the prefs file that contains the
     *            information for the Concourse deployment (relative paths will
     *            resolve to the user's home directory)
     * @return the handle
     */
    public static Concourse connectWithPrefs(String file) {
        ConcourseClientPreferences prefs = ConcourseClientPreferences
                .open(file);
        return connect(prefs.getHost(), prefs.getPort(), prefs.getUsername(),
                String.valueOf(prefs.getPassword()), prefs.getEnvironment());
    }

    /**
     * Abort the current transaction and discard any changes that are currently
     * staged.
     * <p>
     * After returning, the driver will return to {@code autocommit} mode and
     * all subsequent changes will be committed immediately.
     * </p>
     * <p>
     * Calling this method when the driver is not in {@code staging} mode is a
     * no-op.
     * </p>
     */
    public abstract void abort();

    /**
     * Append {@code key} as {@code value} in a new record.
     * 
     * @param key the field name
     * @param value the value to add
     * @return the new record id
     */
    public abstract <T> long add(String key, T value);

    /**
     * Append {@code key} as {@code value} in each of the {@code records} where
     * it doesn't exist.
     * 
     * @param key the field name
     * @param value the value to add
     * @param records a collection of record ids where an attempt is made to
     *            add the data
     * @return a mapping from each record id to a boolean that indicates if the
     *         data was added
     */
    @CompoundOperation
    public abstract <T> Map<Long, Boolean> add(String key, T value,
            Collection<Long> records);

    /**
     * Append {@code key} as {@code value} in {@code record} if and only if it
     * doesn't exist.
     * 
     * @param key the field name
     * @param value the value to add
     * @param record the record id where an attempt is made to add the data
     * @return a boolean that indicates if the data was added
     */
    public abstract <T> boolean add(String key, T value, long record);

    /**
     * List all the changes ever made to {@code record}.
     * 
     * @param record the record id
     * @return for each change, a mapping from timestamp to a description of the
     *         revision
     */
    public abstract Map<Timestamp, String> audit(long record);

    /**
     * List all the changes made to {@code record} since {@code start}
     * (inclusive).
     *
     * @param record the record id
     * @param start an inclusive {@link Timestamp} of the oldest change that
     *            should possibly be included in the audit
     * @return for each change, a mapping from timestamp to a description of the
     *         revision
     */
    public abstract Map<Timestamp, String> audit(long record, Timestamp start);

    /**
     * List all the changes made to {@code record} between {@code start}
     * (inclusive) and {@code end} (non-inclusive).
     *
     * @param record the record id
     * @param start an inclusive {@link Timestamp} for the oldest change that
     *            should possibly be included in the audit
     * @param end a non-inclusive {@link Timestamp} for the most recent change
     *            that should possibly be included in the audit
     * @return for each change, a mapping from timestamp to a description of the
     *         revision
     */
    public abstract Map<Timestamp, String> audit(long record, Timestamp start,
            Timestamp end);

    /**
     * List all the changes ever made to the {@code key} field in {@code record}
     *
     * @param key the field name
     * @param record the record id
     * @return for each change, a mapping from timestamp to a description of the
     *         revision
     */
    public abstract Map<Timestamp, String> audit(String key, long record);

    /**
     * List all the changes made to the {@code key} field in {@code record}
     * since {@code start} (inclusive).
     * 
     * @param key the field name
     * @param record the record id
     * @param start an inclusive {@link Timestamp} for the oldest change that
     *            should possibly be included in the audit
     * @return for each change, a mapping from timestamp to a description of the
     *         revision
     */
    public abstract Map<Timestamp, String> audit(String key, long record,
            Timestamp start);

    /**
     * List all the changes made to the {@code key} field in {@code record}
     * between {@code start} (inclusive) and {@code end} (non-inclusive).
     * 
     * @param key the field name
     * @param record the record id
     * @param start an inclusive {@link Timestamp} for the oldest change that
     *            should possibly be included in the audit
     * @param end a non-inclusive {@link Timestamp} for the most recent change
     *            that should possibly be included in the audit
     * @return for each change, a mapping from timestamp to a description of the
     *         revision
     */
    public abstract Map<Timestamp, String> audit(String key, long record,
            Timestamp start, Timestamp end);

    /**
     * View the values from all records that are currently stored for each of
     * the {@code keys}.
     * 
     * @param keys a collection of field names
     * @return a {@link Map} associating each key to a {@link Map} associating
     *         each value to the {@link Set} of records that contain that value
     *         in the {@code key} field
     */
    public abstract Map<String, Map<Object, Set<Long>>> browse(
            Collection<String> keys);

    /**
     * View the values from all records that were stored for each of the
     * {@code keys} at {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param timestamp the historical timestamp to use in the lookup
     * @return a {@link Map} associating each key to a {@link Map} associating
     *         each value to the {@link Set} of records that contained that
     *         value in the {@code key} field at {@code timestamp}
     */
    public abstract Map<String, Map<Object, Set<Long>>> browse(
            Collection<String> keys, Timestamp timestamp);

    /**
     * View the values from all records that are currently stored for
     * {@code key}.
     * 
     * @param key the field name
     * @return a {@link Map} associating each value to the {@link Set} of
     *         records that contain that value in the {@code key} field
     */
    public abstract Map<Object, Set<Long>> browse(String key);

    /**
     * View the values from all records that were stored for {@code key} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param timestamp the historical timestamp to use in the lookup
     * @return a {@link Map} associating each value to the {@link Set} of
     *         records that contained that value in the {@code key} field at
     *         {@code timestamp}
     */
    public abstract Map<Object, Set<Long>> browse(String key,
            Timestamp timestamp);

    /**
     * View a time series that associates the timestamp of each modification for
     * {@code key} in {@code record} to a snapshot containing the values that
     * were stored in the field after the change.
     * 
     * @param key the field name
     * @param record the record id
     * @return a {@link Map} associating each modification timestamp to the
     *         {@link Set} of values that were stored in the field after the
     *         change.
     */
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record);

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
     * @return a {@link Map} associating each modification timestamp to the
     *         {@link Set} of values that were stored in the field after the
     *         change.
     */
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record, Timestamp start);

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
     * @return a {@link Map} associating each modification timestamp to the
     *         {@link Set} of values that were stored in the field after the
     *         change.
     */
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record, Timestamp start, Timestamp end);

    /**
     * Atomically remove all the values stored for every key in each of the
     * {@code records}.
     * 
     * @param records a collection of record ids
     */
    public abstract void clear(Collection<Long> records);

    /**
     * Atomically remove all the values stored for each of the {@code keys} in
     * each of the {@code records}.
     * 
     * @param keys a collection of field names
     * @param records a collection of record ids.
     */
    public abstract void clear(Collection<String> keys, Collection<Long> records);

    /**
     * Atomically remove all the values stored for each of the {@code keys} in
     * {@code record}.
     * 
     * @param keys a collection of field names
     * @param record the record id
     */
    public abstract void clear(Collection<String> keys, long record);

    /**
     * Atomically remove all the values stored for every key in {@code record}.
     * 
     * @param record the record id
     */
    public abstract void clear(long record);

    /**
     * Atomically remove all the values stored for {@code key} in each of the
     * {@code records}.
     * 
     * @param key the field name
     * @param records a collection of record ids
     */
    public abstract void clear(String key, Collection<Long> records);

    /**
     * Atomically remove all the values stored for {@code key} in {@code record}
     * 
     * @param key the field name
     * @param record the record id
     */
    public abstract void clear(String key, long record);

    /**
     * <p>
     * <em>An alias for the {@link #exit()} method.</em>
     * </p>
     * {@inheritDoc}
     */
    @Override
    public final void close() {
        exit();
    }

    /**
     * Attempt to permanently commit any changes that are staged in a
     * transaction and return {@code true} if and only if all the changes can be
     * applied. Otherwise, returns {@code false} and all the changes are
     * discarded.
     * <p>
     * After returning, the driver will return to {@code autocommit} mode and
     * all subsequent changes will be committed immediately.
     * </p>
     * <p>
     * This method will return {@code false} if it is called when the driver is
     * not in {@code staging} mode.
     * </p>
     * 
     * @return {@code true} if all staged changes are committed, otherwise
     *         {@code false}
     */
    public abstract boolean commit();

    /**
     * For each of the {@code records}, list all of the keys that have at least
     * one value.
     * 
     * @param records a collection of record ids
     * @return a {@link Map} associating each record id to the {@link Set} of
     *         keys in that record
     */
    public abstract Map<Long, Set<String>> describe(Collection<Long> records);

    /**
     * For each of the {@code records}, list all the keys that had at least one
     * value at {@code timestamp}.
     * 
     * @param records a collection of record ids
     * @param timestamp the historical timestamp to use in the lookup
     * @return a {@link Map} associating each record id to the {@link Set} of
     *         keys that were in that record at {@code timestamp}
     */
    public abstract Map<Long, Set<String>> describe(Collection<Long> records,
            Timestamp timestamp);

    /**
     * List all the keys in {@code record} that have at least one value.
     * 
     * @param record the record id
     * @return the {@link Set} of keys in {@code record}
     */
    public abstract Set<String> describe(long record);

    /**
     * List all the keys in {@code record} that had at least one value at
     * {@code timestamp}.
     * 
     * @param record the record id
     * @param timestamp the historical timestamp to use in the lookup
     * @return the {@link Set} of keys that were in {@code record} at
     *         {@code timestamp}
     */
    public abstract Set<String> describe(long record, Timestamp timestamp);

    /**
     * List the net changes made to {@code record} since {@code start}.
     * <p>
     * If you begin with the state of the {@code record} at {@code start} and
     * re-apply all the changes in the diff, you'll re-create the state of the
     * same {@code record} at the present.
     * </p>
     * 
     * @param record the record id
     * @param start the base timestamp from which the diff is calculated
     * @return a {@link Map} that associates each key in the {@code record} to
     *         another {@link Map} that associates a {@link Diff change
     *         description} to the {@link Set} of values that fit the
     *         description (i.e. <code>
     *         {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}
     *         </code> )
     */
    public abstract <T> Map<String, Map<Diff, Set<T>>> diff(long record,
            Timestamp start);

    /**
     * List the net changes made to {@code record} from {@code start} to
     * {@code end}.
     *
     * <p>
     * If you begin with the state of the {@code record} at {@code start} and
     * re-apply all the changes in the diff, you'll re-create the state of the
     * same {@code record} at {@code end}.
     * </p>
     * 
     * @param record the record id
     * @param start the base timestamp from which the diff is calculated
     * @param end the comparison timestamp to which the diff is calculated
     * @return a {@link Map} that associates each key in the {@code record} to
     *         another {@link Map} that associates a {@link Diff change
     *         description} to the {@link Set} of values that fit the
     *         description (i.e. <code>
     *         {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}
     *         </code> )
     */
    public abstract <T> Map<String, Map<Diff, Set<T>>> diff(long record,
            Timestamp start, Timestamp end);

    /**
     * List the net changes made to {@code key} in {@code record} since
     * {@code start}.
     * 
     * <p>
     * If you begin with the state of the field at {@code start} and re-apply
     * all the changes in the diff, you'll re-create the state of the same field
     * at the present.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @param start the base timestamp from which the diff is calculated
     * @return a {@link Map} that associates a {@link Diff change
     *         description} to the {@link Set} of values that fit the
     *         description (i.e. <code>
     *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
     *         </code> )
     */
    public abstract <T> Map<Diff, Set<T>> diff(String key, long record,
            Timestamp start);

    /**
     * List the net changes made to {@code key} in {@code record} from
     * {@code start} to {@code end}.
     * 
     * <p>
     * If you begin with the state of the field at {@code start} and re-apply
     * all the changes in the diff, you'll re-create the state of the same field
     * at {@code end}.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @param start the base timestamp from which the diff is calculated
     * @param end the comparison timestamp to which the diff is calculated
     * @return a {@link Map} that associates a {@link Diff change
     *         description} to the {@link Set} of values that fit the
     *         description (i.e. <code>
     *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
     *         </code> )
     */
    public abstract <T> Map<Diff, Set<T>> diff(String key, long record,
            Timestamp start, Timestamp end);

    /**
     * List the net changes made to the {@code key} field across all records
     * since {@code start}.
     * 
     * <p>
     * If you begin with the state of the inverted index for {@code key} at
     * {@code start} and re-apply all the changes in the diff, you'll re-create
     * the state of the same index at the present.
     * </p>
     * 
     * @param key the field name
     * @param start the base timestamp from which the diff is calculated
     * @return a {@link Map} that associates each value stored for {@code key}
     *         across all records to another {@link Map} that associates a
     *         {@link Diff change description} to the {@link Set} of records
     *         where the description applies to that value in the {@code key}
     *         field (i.e. <code>
     *         {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
     *         </code>)
     */
    public abstract <T> Map<T, Map<Diff, Set<Long>>> diff(String key,
            Timestamp start);

    /**
     * List the net changes made to the {@code key} field across all records
     * from {@code start} to {@code end}.
     * 
     * <p>
     * If you begin with the state of the inverted index for {@code key} at
     * {@code start} and re-apply all the changes in the diff, you'll re-create
     * the state of the same index at {@code end}.
     * </p>
     * 
     * @param key the field name
     * @param start the base timestamp from which the diff is calculated
     * @param end the comparison timestamp to which the diff is calculated
     * @return a {@link Map} that associates each value stored for {@code key}
     *         across all records to another {@link Map} that associates a
     *         {@link Diff change description} to the {@link Set} of records
     *         where the description applies to that value in the {@code key}
     *         field (i.e. <code>
     *         {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
     *         </code>)
     */
    public abstract <T> Map<T, Map<Diff, Set<Long>>> diff(String key,
            Timestamp start, Timestamp end);

    /**
     * Terminate the client's session and close this connection.
     */
    public abstract void exit();

    /**
     * Find and return the set of records that satisfy the {@code criteria}.
     * This is analogous to the SELECT action in SQL.
     * 
     * @param criteria
     * @return the records that match the {@code criteria}
     */
    public abstract Set<Long> find(Criteria criteria);

    /**
     * Find and return the set of records that satisfy the {@code criteria}.
     * This is analogous to the SELECT action in SQL.
     * 
     * @param criteria
     * @return the records that match the {@code criteria}
     */
    public abstract Set<Long> find(Object criteria); // this method exists in
                                                     // case the caller
                                                     // forgets
                                                     // to called #build() on
                                                     // the CriteriaBuilder

    /**
     * Find and return the set of records that satisfy the {@code ccl} criteria.
     * 
     * @param ccl
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String ccl);

    /**
     * Find and return the set of records where {@code key} is equal to
     * {@code value}. This method is a shortcut for calling
     * {@link #find(String, Operator, Object)} with {@link Operator#EQUALS}.
     * 
     * @param key
     * @param value
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Object value);

    /**
     * Find {@code key} {@code operator} {@code value} and return the set of
     * records that satisfy the criteria. This is analogous to the SELECT action
     * in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value);

    /**
     * Find {@code key} {@code operator} {@code value} and {@code value2} and
     * return the set of records that satisfy the criteria. This is analogous to
     * the SELECT action in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @param value2
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value,
            Object value2);

    /**
     * Find {@code key} {@code operator} {@code value} and {@code value2} at
     * {@code timestamp} and return the set of records that satisfy the
     * criteria. This is analogous to the SELECT action in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @param value2
     * @param timestamp
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp);

    /**
     * Find {@code key} {@code operator} {@code value} at {@code timestamp} and
     * return the set of records that satisfy the criteria. This is analogous to
     * the SELECT action in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp);

    /**
     * Find {@code key} {@code operator} {@code value} and return the set of
     * records that satisfy the criteria. This is analogous to the SELECT action
     * in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value);

    /**
     * Find {@code key} {@code operator} {@code value} and {@code value2} and
     * return the set of records that satisfy the criteria. This is analogous to
     * the SELECT action in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @param value2
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value,
            Object value2);

    /**
     * Find {@code key} {@code operator} {@code value} and {@code value2} at
     * {@code timestamp} and return the set of records that satisfy the
     * criteria. This is analogous to the SELECT action in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @param value2
     * @param timestamp
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp);

    /**
     * Find {@code key} {@code operator} {@code value} at {@code timestamp} and
     * return the set of records that satisfy the criteria. This is analogous to
     * the SELECT action in SQL.
     * 
     * @param key
     * @param operator
     * @param value
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp);

    /**
     * Find and return the unique record where {@code key}
     * {@link Operator#EQUALS
     * equals} {@code value}, if if exists. If no record matches, then add
     * {@code key} as {@code value} into an new record and return the id
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only adds data if that condition isn't
     * currently satisfied.
     * </p>
     * 
     * @param key
     * @param value
     * @return the unique record where {@code key} = {@code value}, if one exist
     *         or the record where the {@code key} as {@code value} is added
     * @throws DuplicateEntryException
     */
    public abstract <T> long findOrAdd(String key, T value)
            throws DuplicateEntryException;

    /**
     * Find and return the unique record that matches the {@code criteria}, if
     * one exist. If and only if no record matches, insert the key/value
     * associations in {@code data} into a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param criteria A {@link Criteria} builder sequence that has reached a
     *            buildable state, but that has not be officially
     *            {@link BuildableState#build() built}.
     * @param data a {@link Map} with key/value associations to insert into the
     *            new record
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public final long findOrInsert(BuildableState criteria,
            Map<String, Object> data) throws DuplicateEntryException {
        String json = Convert.mapToJson(data);
        return findOrInsert(criteria, json);
    }

    /**
     * Find and return the unique record that matches the {@code criteria}, if
     * one exist. If and only if no record matches, insert the key/value
     * associations in {@code data} into a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param criteria A {@link Criteria} builder sequence that has reached a
     *            buildable state, but that has not be officially
     *            {@link BuildableState#build() built}.
     * @param data a {@link Multimap} with key/value associations to insert into
     *            the new record
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public final long findOrInsert(BuildableState criteria,
            Multimap<String, Object> data) throws DuplicateEntryException {
        String json = Convert.mapToJson(data);
        return findOrInsert(criteria, json);
    }

    /**
     * Find and return the unique record that matches the {@code ccl} string, if
     * one exist. If no records match, then insert the data in the {@code json}
     * string a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * 
     * @param criteria A {@link Criteria} builder sequence that has reached a
     *            buildable state, but that has not be officially
     *            {@link BuildableState#build() built}.
     * @param json a JSON blob describing a single object
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public long findOrInsert(BuildableState criteria, String json)
            throws DuplicateEntryException {
        return findOrInsert(criteria.build(), json);
    }

    /**
     * Find and return the unique record that matches the {@code criteria}, if
     * one exist. If and only if no record matches, insert the key/value
     * associations in {@code data} into a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param criteria a well formed {@link Criteria}
     * @param data a {@link Map} with key/value associations to insert into the
     *            new record
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public final long findOrInsert(Criteria criteria, Map<String, Object> data)
            throws DuplicateEntryException {
        String json = Convert.mapToJson(data);
        return findOrInsert(criteria, json);
    }

    /**
     * Find and return the unique record that matches the {@code criteria}, if
     * one exist. If and only if no record matches, insert the key/value
     * associations in {@code data} into a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param criteria a well formed {@link Criteria}
     * @param data a {@link Multimap} with key/value associations to insert into
     *            the new record
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public final long findOrInsert(Criteria criteria,
            Multimap<String, Object> data) throws DuplicateEntryException {
        String json = Convert.mapToJson(data);
        return findOrInsert(criteria, json);
    }

    /**
     * Find and return the unique record that matches the {@code ccl} string, if
     * one exist. If no records match, then insert the data in the {@code json}
     * string a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * 
     * @param criteria A {@link Criteria} builder sequence that has reached a
     *            buildable state, but that has not be officially
     *            {@link BuildableState#build() built}.
     * @param data a JSON blob describing a single object
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public abstract long findOrInsert(Criteria criteria, String json)
            throws DuplicateEntryException;

    /**
     * Find and return the unique record that matches the {@code ccl} criteria,
     * if one exist. If and only if no record matches, insert the key/value
     * associations in {@code data} into a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param ccl the well formed criteria expressed using CCL
     * @param data a {@link Map} with key/value associations to insert into the
     *            new record
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public final long findOrInsert(String ccl, Map<String, Object> data)
            throws DuplicateEntryException {
        String json = Convert.mapToJson(data);
        return findOrInsert(ccl, json);
    }

    /**
     * Find and return the unique record that matches the {@code ccl} criteria,
     * if one exist. If and only if no record matches, insert the key/value
     * associations in {@code data} into a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param ccl the well formed criteria expressed using CCL
     * @param data a {@link Multimap} with key/value associations to insert into
     *            the new record
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public final long findOrInsert(String ccl, Multimap<String, Object> data)
            throws DuplicateEntryException {
        String json = Convert.mapToJson(data);
        return findOrInsert(ccl, json);
    }

    /**
     * Find and return the unique record that matches the {@code ccl} string, if
     * one exist. If no records match, then insert the data in the {@code json}
     * string a new record and return the id.
     * 
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * 
     * @param ccl the criteria expressed using CCL
     * @param json a JSON blob describing a single object
     * @return the unique record that matches {@code ccl} string, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public abstract long findOrInsert(String ccl, String json)
            throws DuplicateEntryException;

    /**
     * Get each of the {@code keys} from each of the {@code records} and return
     * a mapping from each record to a mapping of each key to the first
     * contained value.
     * 
     * @param keys
     * @param records
     * @return the first contained value for each of the {@code keys} in each of
     *         the {@code records}
     */
    @CompoundOperation
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records);

    /**
     * Get each of the {@code keys} from each of the {@code records} at
     * {@code timestamp} and return a mapping from each record to a mapping of
     * each key to the first contained value.
     * 
     * @param keys
     * @param records
     * @param timestamp
     * @return the first contained value for each of the {@code keys} in each of
     *         the {@code records} at {@code timestamp}
     */
    @CompoundOperation
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp);

    /**
     * Get the most recently added value for each of the {@code keys} in all the
     * records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria);

    /**
     * Get the most recently added value for each of the {@code keys} at
     * {@code timestamp} in all the records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp);

    /**
     * Get each of the {@code keys} from {@code record} and return a mapping
     * from each key to the first contained value.
     * 
     * @param keys
     * @param record
     * @return the first contained value for each of the {@code keys} in
     *         {@code record}
     */
    @CompoundOperation
    public abstract <T> Map<String, T> get(Collection<String> keys, long record);

    /**
     * Get each of the {@code keys} from {@code record} at {@code timestamp} and
     * return a mapping from each key to the first contained value.
     * 
     * @param keys
     * @param record
     * @param timestamp
     * @return the first contained value for each of the {@code keys} in
     *         {@code record} at {@code timestamp}
     */
    @CompoundOperation
    public abstract <T> Map<String, T> get(Collection<String> keys,
            long record, Timestamp timestamp);

    /**
     * Get the most recently added value for each of the {@code keys} in all the
     * records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Object criteria);

    /**
     * Get the most recently added value for each of the {@code keys} at
     * {@code timestamp} in all the records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Object criteria, Timestamp timestamp);

    /**
     * Get the most recently added value for each of the {@code keys} in all the
     * records that match {@code ccl} criteria.
     * 
     * @param keys
     * @param ccl
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl);

    /**
     * Get the most recently added value for each of the {@code keys} at
     * {@code timestamp} in all the records that match {@code ccl} criteria.
     * 
     * @param keys
     * @param ccl
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp);

    /**
     * Get the most recently added values for each key in all of the records
     * that match {@code criteria}.
     * 
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Criteria criteria);

    /**
     * Get the most recently added values at {@code timestamp} for each key in
     * all of the records that match {@code criteria}.
     * 
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp);

    /**
     * Get the most recently added values for each key in all of the records
     * that match {@code criteria}.
     * 
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Object criteria);

    /**
     * Get the most recently added values at {@code timestamp} for each key in
     * all of the records that match {@code criteria}.
     * 
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(Object criteria,
            Timestamp timestamp);

    /**
     * Get the most recently added values for each key in all of the records
     * that match {@code ccl} criteria.
     * 
     * @param ccl
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(String ccl);

    /**
     * Get {@code key} from each of the {@code records} and return a mapping
     * from each record to the first contained value.
     * 
     * @param key
     * @param records
     * @return the first contained value for {@code key} in each of the
     *         {@code records}
     */
    @CompoundOperation
    public abstract <T> Map<Long, T> get(String key, Collection<Long> records);

    /**
     * Get {@code key} from each of the {@code records} at {@code timestamp} and
     * return a mapping from each record to the first contained value.
     * 
     * @param key
     * @param records
     * @param timestamp
     * @return the first contained value for {@code key} in each of the
     *         {@code records} at {@code timestamp}
     */
    @CompoundOperation
    public abstract <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp);

    /**
     * Get the most recently added value for {@code key} in all the records that
     * match {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, T> get(String key, Criteria criteria);

    /**
     * Get the most recently added value for {@code key} at {@code timestamp}
     * for all the records that match the {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp);

    /**
     * Get {@code key} from {@code record} and return the first contained value
     * or {@code null} if there is none. Compared to
     * {@link #select(String, long)}, this method is suited for cases when the
     * caller is certain that {@code key} in {@code record} maps to a single
     * value of type {@code T}.
     * 
     * @param key
     * @param record
     * @return the first contained value
     */
    public abstract <T> T get(String key, long record);

    /**
     * Get {@code key} from {@code record} at {@code timestamp} and return the
     * first contained value or {@code null} if there was none. Compared to
     * {@link #select(String, long, Timestamp)}, this method is suited for cases
     * when the caller is certain that {@code key} in {@code record} mapped to a
     * single value of type {@code T} at {@code timestamp}.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return the first contained value
     */
    public abstract <T> T get(String key, long record, Timestamp timestamp);

    /**
     * Get the most recently added value for {@code key} in all the records that
     * match {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, T> get(String key, Object criteria);

    /**
     * Get the most recently added value for {@code key} at {@code timestamp}
     * for all the records that match the {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, T> get(String key, Object criteria,
            Timestamp timestamp);

    /**
     * Get the most recently added value for {@code key} in all the records that
     * match {@code ccl} criteria.
     * 
     * @param key
     * @param ccl
     * @return the result set
     */
    public abstract <T> Map<Long, T> get(String key, String ccl);

    /**
     * Get the most recently added value for {@code key} at {@code timestamp}
     * for all the records that match the {@code ccl} criteria.
     * 
     * @param key
     * @param ccl
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, T> get(String key, String ccl,
            Timestamp timestamp);

    /**
     * Get the most recently added values at {@code timestamp} for each key in
     * all of the records that match {@code ccl} criteria.
     * 
     * @param ccl
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, T>> get(String ccl,
            Timestamp timestamp);

    /**
     * Return the environment of the server that is currently in use by this
     * client.
     * 
     * @return the server environment
     */
    public abstract String getServerEnvironment();

    /**
     * Return the version of the server to which this client is currently
     * connected.
     * 
     * @return the server version
     */
    public abstract String getServerVersion();

    /**
     * Within a single atomic operation, for each of the {@link Multimap maps}
     * in {@code data}, insert the key/value associations into a new and
     * distinct record.
     * <p>
     * Each of the values in each map in {@code data} must be a primitive or one
     * dimensional object (e.g. no nested {@link Map maps} or {@link Multimap
     * multimaps}).
     * </p>
     * 
     * @param data a {@link List} of {@link Multimap maps}, each with key/value
     *            associations to insert into a new record
     * @return a {@link Set} of ids containing the ids of the new records where
     *         the maps in {@code data} were inserted, respectively
     */
    public final Set<Long> insert(Collection<Multimap<String, Object>> data) {
        String json = Convert.mapsToJson(data);
        return insert(json);
    }

    /**
     * Atomically insert the key/value associations from {@code data} into a new
     * record.
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param data a {@link Map} with key/value associations to insert into the
     *            new record
     * @return the id of the new record where the {@code data} was inserted
     */
    public final long insert(Map<String, Object> data) {
        String json = Convert.mapToJson(data);
        return insert(json).iterator().next();
    }

    /**
     * Atomically insert the key/value associations in {@code data} into each of
     * the {@code records}.
     * 
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param data a {@link Map} with key/value associations to insert into each
     *            of the {@code records}
     * @param records a collection of ids for records where the {@code data}
     *            should attempt to be inserted
     * @return a {@link Map} associating each record id to a boolean that
     *         indicates if the data was successfully inserted in that record
     */
    public final Map<Long, Boolean> insert(Map<String, Object> data,
            Collection<Long> records) {
        String json = Convert.mapToJson(data);
        return insert(json, records);

    }

    /**
     * Atomically insert the key/value associations in {@code data} into
     * {@code record}.
     * 
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param data a {@link Map} with key/value associations to insert into
     *            {@code record}
     * @param record the record id
     * @return {@code true} if all of the {@code data} is successfully inserted
     *         into {@code record}, otherwise {@code false}
     */
    public final boolean insert(Map<String, Object> data, long record) {
        String json = Convert.mapToJson(data);
        return insert(json, record);
    }

    /**
     * Atomically insert the key/value associations from {@code data} into a new
     * record.
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param data a {@link Multimap} with key/value associations to insert into
     *            the new record
     * @return the id of the new record where the {@code data} was inserted
     */
    public final long insert(Multimap<String, Object> data) {
        String json = Convert.mapToJson(data);
        return insert(json).iterator().next();
    }

    /**
     * Atomically insert the key/value associations in {@code data} into each of
     * the {@code records}.
     * 
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param data a {@link Multimap} with key/value associations to insert into
     *            each of the {@code records}
     * @param records a collection of ids for records where the {@code data}
     *            should attempt to be inserted
     * @return a {@link Map} associating each record id to a boolean that
     *         indicates if the data was successfully inserted in that record
     */
    public final Map<Long, Boolean> insert(Multimap<String, Object> data,
            Collection<Long> records) {
        String json = Convert.mapToJson(data);
        return insert(json, records);
    }

    /**
     * Atomically insert the key/value associations in {@code data} into
     * {@code record}.
     * 
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * 
     * @param data a {@link Multimap} with key/value associations to insert into
     *            {@code record}
     * @param record the record id
     * @return {@code true} if all of the {@code data} is successfully inserted
     *         into {@code record}, otherwise {@code false}
     */
    public final boolean insert(Multimap<String, Object> data, long record) {
        String json = Convert.mapToJson(data);
        return insert(json, record);
    }

    /**
     * Atomically insert the key/value mappings described in the {@code json}
     * formatted string into a new record.
     * <p>
     * The {@code json} formatted string must describe an JSON object that
     * contains one or more keys, each of which maps to a JSON primitive or an
     * array of JSON primitives.
     * </p>
     * 
     * @param json
     * @return the primary key of the new record or {@code null} if the insert
     *         is unsuccessful
     */
    public abstract Set<Long> insert(String json);

    /**
     * Insert the key/value mappings described in the {@code json} formated
     * string into each of the {@code records}.
     * <p>
     * The {@code json} formatted string must describe an JSON object that
     * contains one or more keys, each of which maps to a JSON primitive or an
     * array of JSON primitives.
     * </p>
     * 
     * @param json
     * @param records
     * @return a mapping from each primary key to a boolean describing if the
     *         data was successfully inserted into that record
     */
    public abstract Map<Long, Boolean> insert(String json,
            Collection<Long> records);

    /**
     * Atomically insert the key/value mappings described in the {@code json}
     * formatted string into {@code record}.
     * <p>
     * The {@code json} formatted string must describe an JSON object that
     * contains one or more keys, each of which maps to a JSON primitive or an
     * array of JSON primitives.
     * </p>
     * 
     * @param json
     * @param record
     * @return {@code true} if the data is inserted into {@code record}
     */
    public abstract boolean insert(String json, long record);

    /**
     * Return a list of all the records that have ever contained data.
     * 
     * @return the full list of records
     */
    public abstract Set<Long> inventory();

    /**
     * Dump each of the {@code records} to a JSON string.
     * 
     * @param records
     * @return the json string dump
     */
    public abstract String jsonify(Collection<Long> records);

    /**
     * Dump each of the {@code records} to a JSON string that optionally
     * includes an {@code identifier} for each {@code record}.
     * 
     * @param records
     * @param identifier
     * @return the json string dump
     */
    public abstract String jsonify(Collection<Long> records, boolean identifier);

    /**
     * Dump the state of each of the {@code records} at {@code timestamp} to a
     * JSON string.
     * 
     * @param records
     * @param timestamp
     * @return the json string dump
     */
    public abstract String jsonify(Collection<Long> records, Timestamp timestamp);

    /**
     * Dump the state of each of the {@code records} at {@code timestamp} to a
     * JSON string that optionally includes an {@code identifier} for each
     * {@code record}.
     * 
     * @param records
     * @param timestamp
     * @param identifier
     * @return the json string dump
     */
    public abstract String jsonify(Collection<Long> records,
            Timestamp timestamp, boolean identifier);

    /**
     * Dump {@code record} to a JSON string.
     * 
     * @param record
     * @return the json string dump
     */
    public abstract String jsonify(long record);

    /**
     * Dump the {@code record} to a JSON string that optionally includes an
     * {@code identifier} for the record.
     * 
     * @param record
     * @param identifier
     * @return the json string dump
     */
    public abstract String jsonify(long record, boolean identifier);

    /**
     * Dump the state of the {@code record} at {@code timestamp} to a JSON
     * string.
     * 
     * @param record
     * @param timestamp
     * @return the json string dump
     */
    public abstract String jsonify(long record, Timestamp timestamp);

    /**
     * Dump the state of the {@code record} at {@code timestamp} to a JSON
     * string that optionally includes an {@code identifier} for the record.
     * 
     * @param record
     * @param timestamp
     * @param identifier
     * @return the json string dump
     */
    public abstract String jsonify(long record, Timestamp timestamp,
            boolean identifier);

    /**
     * Link {@code key} in {@code source} to each of the {@code destinations}.
     * 
     * @param key
     * @param source
     * @param destinations
     * @return a mapping from each destination to a boolean indicating if the
     *         link was added
     */
    public abstract Map<Long, Boolean> link(String key, long source,
            Collection<Long> destinations);

    /**
     * Link {@code key} in {@code source} to {@code destination}.
     * 
     * @param key
     * @param source
     * @param destination
     * @return {@code true} if the link is added
     */
    public abstract boolean link(String key, long source, long destination);

    /**
     * Ping each of the {@code records}.
     * 
     * @param records
     * @return a mapping from each record to a boolean indicating if the record
     *         currently has at least one populated key
     */
    @CompoundOperation
    public abstract Map<Long, Boolean> ping(Collection<Long> records);

    /**
     * Ping {@code record}.
     * 
     * @param record
     * @return {@code true} if {@code record} currently has at least one
     *         populated key
     */
    public abstract boolean ping(long record);

    /**
     * Remove {@code key} as {@code value} in each of the {@code records} if it
     * is contained.
     * 
     * @param key
     * @param value
     * @param records
     * @return a mapping from each record to a boolean indicating if
     *         {@code value} is removed
     */
    @CompoundOperation
    public abstract <T> Map<Long, Boolean> remove(String key, T value,
            Collection<Long> records);

    /**
     * Remove {@code key} as {@code value} to {@code record} if it is contained.
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if {@code value} is removed
     */
    public abstract <T> boolean remove(String key, T value, long record);

    /**
     * Revert each of the {@code keys} in each of the {@code records} to
     * {@code timestamp} by creating new revisions that the relevant changes
     * that have occurred since {@code timestamp}.
     * 
     * @param keys
     * @param records
     * @param timestamp
     */
    @CompoundOperation
    public abstract void revert(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp);

    /**
     * Revert each of the {@code keys} in {@code record} to {@code timestamp} by
     * creating new revisions that the relevant changes that have occurred since
     * {@code timestamp}.
     * 
     * @param keys
     * @param record
     * @param timestamp
     */
    @CompoundOperation
    public abstract void revert(Collection<String> keys, long record,
            Timestamp timestamp);

    /**
     * Revert {@code key} in each of the {@code records} to {@code timestamp} by
     * creating new revisions that the relevant changes that have occurred since
     * {@code timestamp}.
     * 
     * @param key
     * @param records
     * @param timestamp
     */
    @CompoundOperation
    public abstract void revert(String key, Collection<Long> records,
            Timestamp timestamp);

    /**
     * Atomically revert {@code key} in {@code record} to {@code timestamp} by
     * creating new revisions that undo the relevant changes that have occurred
     * since {@code timestamp}.
     * 
     * @param key
     * @param record
     * @param timestamp
     */
    public abstract void revert(String key, long record, Timestamp timestamp);

    /**
     * Search {@code key} for {@code query} and return the set of records that
     * match.
     * 
     * @param key
     * @param query
     * @return the records that match the query
     */
    public abstract Set<Long> search(String key, String query);

    /**
     * Select the {@code records} and return a mapping from each record to all
     * the data that is contained as a mapping from key name to value set.
     * 
     * @param records
     * @return a mapping of all the contained keys and their mapped values in
     *         each record
     */
    public abstract Map<Long, Map<String, Set<Object>>> select(
            Collection<Long> records);

    /**
     * Select the {@code records} at {@code timestamp} and return a mapping from
     * each record to all the data that was contained as a mapping from key name
     * to value set.
     * 
     * @param records
     * @param timestamp
     * @return a mapping of all the contained keys and their mapped values in
     *         each record
     */
    public abstract Map<Long, Map<String, Set<Object>>> select(
            Collection<Long> records, Timestamp timestamp);

    /**
     * Select each of the {@code keys} from each of the {@code records} and
     * return a mapping from each record to a mapping from each key to the
     * contained values.
     * 
     * @param keys
     * @param records
     * @return the contained values for each of the {@code keys} in each of the
     *         {@code records}
     */
    @CompoundOperation
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Collection<Long> records);

    /**
     * Select each of the {@code keys} from each of the {@code records} at
     * {@code timestamp} and return a mapping from each record to a mapping from
     * each key to the contained values.
     * 
     * @param keys
     * @param records
     * @param timestamp
     * @return the contained values for each of the {@code keys} in each of the
     *         {@code records} at {@code timestamp}
     */
    @CompoundOperation
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Collection<Long> records,
            Timestamp timestamp);

    /**
     * Select all of the values for each of the {@code keys} in all the records
     * that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Criteria criteria);

    /**
     * Select all of the values for each of the {@code keys} at
     * {@code timestamp} in all the records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Criteria criteria, Timestamp timestamp);

    /**
     * Select each of the {@code keys} from {@code record} and return a mapping
     * from each key to the contained values.
     * 
     * @param keys
     * @param record
     * @return the contained values for each of the {@code keys} in
     *         {@code record}
     */
    @CompoundOperation
    public abstract <T> Map<String, Set<T>> select(Collection<String> keys,
            long record);

    /**
     * Select each of the {@code keys} from {@code record} at {@code timestamp}
     * and return a mapping from each key to the contained values.
     * 
     * @param keys
     * @param record
     * @param timestamp
     * @return the contained values for each of the {@code keys} in
     *         {@code record} at {@code timestamp}
     */
    @CompoundOperation
    public abstract <T> Map<String, Set<T>> select(Collection<String> keys,
            long record, Timestamp timestamp);

    /**
     * Select all of the values for each of the {@code keys} in all the records
     * that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Object criteria);

    /**
     * Select all of the values for each of the {@code keys} at
     * {@code timestamp} in all the records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Object criteria, Timestamp timestamp);

    /**
     * Select all of the values for each of the {@code keys} in all the records
     * that match {@code ccl} criteria.
     * 
     * @param keys
     * @param ccl
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, String ccl);

    /**
     * Select all of the values for each of the {@code keys} at
     * {@code timestamp} in all the records that match {@code ccl} criteria.
     * 
     * @param keys
     * @param ccl
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, String ccl, Timestamp timestamp);

    /**
     * Select all of the values for every key in all the records that match
     * {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria);

    /**
     * Select all of the values for every key at {@code timestamp} in all the
     * records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Criteria criteria, Timestamp timestamp);

    /**
     * Select {@code record} and return all the data that is presently contained
     * as a mapping from key name to value set.
     * <p>
     * <em>This method is the atomic equivalent of calling
     * {@code fetch(describe(record), record)}</em>
     * </p>
     * 
     * @param record
     * @return a mapping of all the presently contained keys and their mapped
     *         values
     */
    public abstract Map<String, Set<Object>> select(long record);

    /**
     * Select {@code record} at {@code timestamp} and return all the data that
     * was contained as a mapping from key name to value set.
     * <p>
     * <em>This method is the atomic equivalent of calling
     * {@code fetch(describe(record, timestamp), record, timestamp)}</em>
     * </p>
     * 
     * @param record
     * @param timestamp
     * @return a mapping of all the contained keys and their mapped values
     */
    public abstract Map<String, Set<Object>> select(long record,
            Timestamp timestamp);

    /**
     * Select all of the values for every key in all the records that match
     * {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(Object criteria);

    /**
     * Select all of the values for every key at {@code timestamp} in all the
     * records that match {@code criteria}.
     * 
     * @param keys
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(Object criteria,
            Timestamp timestamp);

    /**
     * Select all of the values for every key in all the records that match
     * {@code ccl} criteria.
     * 
     * @param keys
     * @param ccl
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(String ccl);

    /**
     * Select {@code key} from each of the {@code records} and return a mapping
     * from each record to contained values.
     * 
     * @param key
     * @param records
     * @return the contained values for {@code key} in each {@code record}
     */
    @CompoundOperation
    public abstract <T> Map<Long, Set<T>> select(String key,
            Collection<Long> records);

    /**
     * Select {@code key} from} each of the {@code records} at {@code timestamp}
     * and return a mapping from each record to the contained values.
     * 
     * @param key
     * @param records
     * @param timestamp
     * @return the contained values for {@code key} in each of the
     *         {@code records} at {@code timestamp}
     */
    @CompoundOperation
    public abstract <T> Map<Long, Set<T>> select(String key,
            Collection<Long> records, Timestamp timestamp);

    /**
     * Select all of the values for {@code key} in all the records that match
     * {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Criteria criteria);

    /**
     * Select all of the values for {@code key} at {@code timestamp} in all the
     * records that match {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp);

    /**
     * Select {@code key} from {@code record} and return all the contained
     * values.
     * 
     * @param key
     * @param record
     * @return the contained values
     */
    public abstract <T> Set<T> select(String key, long record);

    /**
     * Select {@code key} from {@code record} at {@code timestamp} and return
     * the set of values that were mapped.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return the contained values
     */
    public abstract <T> Set<T> select(String key, long record,
            Timestamp timestamp);

    /**
     * Select all of the values for {@code key} in all the records that match
     * {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @return the result set
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Object criteria);

    /**
     * Select all of the values for {@code key} at {@code timestamp} in all the
     * records that match {@code criteria}.
     * 
     * @param key
     * @param criteria
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Object criteria,
            Timestamp timestamp);

    /**
     * Select all of the values for {@code key} in all the records that match
     * {@code ccl} criteria.
     * 
     * @param key
     * @param ccl
     * @return the result set
     */
    public abstract <T> Map<Long, Set<T>> select(String key, String ccl);

    /**
     * Select all of the values for {@code key} at {@code timestamp} in all the
     * records that match {@code ccl} criteria.
     * 
     * @param key
     * @param ccl
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp);

    /**
     * Select all of the values for every key at {@code timestamp} in all the
     * records that match {@code ccl} criteria.
     * 
     * @param keys
     * @param ccl
     * @param timestamp
     * @return the result set
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp);

    /**
     * Set {@code key} as {@code value} in each of the {@code records}.
     * 
     * @param key
     * @param value
     * @param records
     */
    @CompoundOperation
    public abstract void set(String key, Object value, Collection<Long> records);

    /**
     * Atomically set {@code key} as {@code value} in {@code record}. This is a
     * convenience method that clears the values for {@code key} and adds
     * {@code value}.
     * 
     * @param key
     * @param value
     * @param record
     */
    public abstract <T> void set(String key, T value, long record);

    /**
     * Start a new transaction.
     * <p>
     * This method will turn on STAGING mode so that all subsequent changes are
     * collected in an isolated buffer before possibly being committed to the
     * database. Staged operations are guaranteed to be reliable, all or nothing
     * units of work that allow correct recovery from failures and provide
     * isolation between clients so the database is always in a consistent
     * state.
     * </p>
     * <p>
     * After this method returns, all subsequent operations will be done in
     * {@code staging} mode until either {@link #abort()} or {@link #commit()}
     * is invoked.
     * </p>
     * <p>
     * All operations that occur within a transaction should be wrapped in a
     * try-catch block so that transaction exceptions can be caught and the
     * transaction can be properly aborted.
     * 
     * <pre>
     * concourse.stage();
     * try {
     *     concourse.get(&quot;foo&quot;, 1);
     *     concourse.add(&quot;foo&quot;, &quot;bar&quot;, 1);
     *     concourse.commit();
     * }
     * catch (TransactionException e) {
     *     concourse.abort();
     * }
     * </pre>
     * 
     * </p>
     */
    public abstract void stage() throws TransactionException;

    /**
     * Execute {@code task} within a new transaction.
     * <p>
     * This method will automatically start a transaction for {@code task} and
     * attempt to commit. There is also logic to gracefully handle exceptions
     * that may result from any actions in the {@code task}.
     * </p>
     * 
     * @param task the group of operations to execute in the transaction
     * @return a boolean that indicates if the transaction successfully
     *         committed
     * @throws TransactionException
     */
    public final boolean stage(Runnable task) throws TransactionException {
        stage();
        try {
            task.run();
            return commit();
        }
        catch (TransactionException e) {
            abort();
            throw e;
        }
    }

    /**
     * Return the current {@link Timestamp}.
     * 
     * @return the current Timestamp
     */
    public abstract Timestamp time();

    /**
     * Return the {@link Timestamp} that corresponds to the specified number of
     * {@code micros} from the Unix epoch.
     * 
     * @param micros
     * @return the Timestamp
     */
    public final Timestamp time(long micros) {
        return Timestamp.fromMicros(micros);
    }

    /**
     * Return the {@link Timestamp} that corresponds to the specified number of
     * {@code micros} from the Unix epoch.
     * 
     * @param micros
     * @return the Timestamp
     */
    public final Timestamp time(Number micros) {
        return time(micros.longValue());
    }

    /**
     * Return the {@link Timestamp} described by {@code phrase}.
     * 
     * @param phrase
     * @return the Timestamp
     */
    public abstract Timestamp time(String phrase);

    /**
     * Remove link from {@code key} in {@code source} to {@code destination}.
     * 
     * @param key
     * @param source
     * @param destination
     * @return {@code true} if the link is removed
     */
    public abstract boolean unlink(String key, long source, long destination);

    /**
     * Verify {@code key} equals {@code value} in {@code record} and return
     * {@code true} if {@code value} is currently mapped from {@code key} in
     * {@code record}.
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if {@code key} equals {@code value} in
     *         {@code record}
     */
    public abstract boolean verify(String key, Object value, long record);

    /**
     * Verify {@code key} equaled {@code value} in {@code record} at
     * {@code timestamp} and return {@code true} if {@code value} was mapped
     * from {@code key} in {@code record}.
     * 
     * @param key
     * @param value
     * @param record
     * @param timestamp
     * @return {@code true} if {@code key} equaled {@code value} in
     *         {@code record} at {@code timestamp}
     */
    public abstract boolean verify(String key, Object value, long record,
            Timestamp timestamp);

    /**
     * Atomically verify {@code key} equals {@code expected} in {@code record}
     * and swap with {@code replacement}.
     * 
     * @param key
     * @param expected
     * @param record
     * @param replacement
     * @return {@code true} if the swap is successful
     */
    public abstract boolean verifyAndSwap(String key, Object expected,
            long record, Object replacement);

    /**
     * Atomically verify that {@code key} equals {@code expected} in
     * {@code record} or set it as such.
     * <p>
     * Please note that after returning, this method guarantees that {@code key}
     * in {@code record} will only contain {@code value}, even if {@code value}
     * already existed alongside other values [e.g. calling verifyOrSet("foo",
     * "bar", 1) will mean that "foo" in 1 only has "bar" as a value after
     * returning, even if "foo" in 1 already had "bar", "baz", and "apple" as
     * values].
     * </p>
     * <p>
     * <em>So, basically, this function has the same guarantee as the
     * {@link #set(String, Object, long)} method, except it will not create any
     * new revisions unless it is necessary to do so.</em> The {@code set}
     * method, on the other hand, would indiscriminately clear all the values
     * for {@code key} in {@code record} before adding {@code value}, even if
     * {@code value} already existed.
     * </p>
     * <p>
     * If you want to add a new value if it does not exist while also preserving
     * other values, you should use the {@link #add(String, Object, long)}
     * method instead.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     */
    public abstract void verifyOrSet(String key, Object value, long record);

    /**
     * The implementation of the {@link Concourse} interface that establishes a
     * connection with the remote server and handles communication. This class
     * is a more user friendly wrapper around a Thrift
     * {@link ConcourseService.Client}.
     * 
     * @author Jeff Nelson
     */
    private final static class Client extends Concourse {

        private static String ENVIRONMENT;
        private static String PASSWORD;
        private static String SERVER_HOST;
        private static int SERVER_PORT;
        private static String USERNAME;
        static {
            // If there is a concourse_client.prefs file located in the working
            // directory, parse it and use its values as defaults.
            ConcourseClientPreferences config;
            try {
                config = ConcourseClientPreferences
                        .open("concourse_client.prefs");
            }
            catch (Exception e) {
                config = null;
            }
            SERVER_HOST = "localhost";
            SERVER_PORT = 1717;
            USERNAME = "admin";
            PASSWORD = "admin";
            ENVIRONMENT = "";
            if(config != null) {
                SERVER_HOST = config.getString("host", SERVER_HOST);
                SERVER_PORT = config.getInt("port", SERVER_PORT);
                USERNAME = config.getString("username", USERNAME);
                PASSWORD = config.getString("password", PASSWORD);
                ENVIRONMENT = config.getString("environment", ENVIRONMENT);
            }
        }

        /**
         * The Thrift client that actually handles all RPC communication.
         */
        private final ConcourseService.Client client;

        /**
         * The client keeps a copy of its {@link AccessToken} and passes it to
         * the server for each remote procedure call. The client will
         * re-authenticate when necessary using the username/password read from
         * the prefs file.
         */
        private AccessToken creds = null;

        /**
         * The environment to which the client is connected.
         */
        private final String environment;

        /**
         * The host of the connection.
         */
        private final String host;

        /**
         * An encrypted copy of the password passed to the constructor.
         */
        private final ByteBuffer password;

        /**
         * The port of the connection.
         */
        private final int port;

        /**
         * Whenever the client starts a Transaction, it keeps a
         * {@link TransactionToken} so that the server can stage the changes in
         * the appropriate place.
         */
        private TransactionToken transaction = null;

        /**
         * An encrypted copy of the username passed to the constructor.
         */
        private final ByteBuffer username;

        /**
         * Create a new Client connection to the environment of the Concourse
         * Server described in {@code concourse_client.prefs} (or the default
         * environment and server if the prefs file does not exist) and return a
         * handler to facilitate database interaction.
         */
        public Client() {
            this(ENVIRONMENT);
        }

        /**
         * Create a new Client connection to the specified {@code environment}
         * of the Concourse Server described in {@code concourse_client.prefs}
         * (or the default server if the prefs file does not exist) and return a
         * handler to facilitate database interaction.
         * 
         * @param environment
         */
        public Client(String environment) {
            this(SERVER_HOST, SERVER_PORT, USERNAME, PASSWORD, environment);
        }

        /**
         * Create a new Client connection to the default environment of the
         * specified Concourse Server and return a handler to facilitate
         * database interaction.
         * 
         * @param host
         * @param port
         * @param username
         * @param password
         */
        public Client(String host, int port, String username, String password) {
            this(host, port, username, password, "");
        }

        /**
         * Create a new Client connection to the specified {@code environment}
         * of the specified Concourse Server and return a handler to facilitate
         * database interaction.
         * 
         * @param host
         * @param port
         * @param username
         * @param password
         * @param environment
         */
        public Client(String host, int port, String username, String password,
                String environment) {
            this.host = host;
            this.port = port;
            this.username = ClientSecurity.encrypt(username);
            this.password = ClientSecurity.encrypt(password);
            this.environment = environment;
            final TTransport transport = new TSocket(host, port);
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                client = new ConcourseService.Client(protocol);
                authenticate();
                Runtime.getRuntime().addShutdownHook(new Thread("shutdown") {

                    @Override
                    public void run() {
                        if(transaction != null && transport.isOpen()) {
                            abort();
                            transport.close();
                        }
                    }

                });
            }
            catch (TTransportException e) {
                throw new RuntimeException(
                        "Could not connect to the Concourse Server at " + host
                                + ":" + port);
            }
        }

        @Override
        public void abort() {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    if(transaction != null) {
                        final TransactionToken token = transaction;
                        transaction = null;
                        client.abort(creds, token, environment);
                    }
                    return null;
                }

            });
        }

        @Override
        public <T> long add(final String key, final T value) {
            return execute(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    return client.addKeyValue(key, Convert.javaToThrift(value),
                            creds, transaction, environment);
                }

            });
        }

        @Override
        public <T> Map<Long, Boolean> add(final String key, final T value,
                final Collection<Long> records) {
            return execute(new Callable<Map<Long, Boolean>>() {

                @Override
                public Map<Long, Boolean> call() throws Exception {
                    Map<Long, Boolean> raw = client.addKeyValueRecords(key,
                            Convert.javaToThrift(value),
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    Map<Long, Boolean> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", "Successful");
                    for (long record : records) {
                        pretty.put(record, raw.get(record));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> boolean add(final String key, final T value,
                final long record) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.addKeyValueRecord(key,
                            Convert.javaToThrift(value), record, creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public Map<Timestamp, String> audit(final long record) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.auditRecord(record, creds,
                            transaction, environment);
                    return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
                }

            });
        }

        @Override
        public Map<Timestamp, String> audit(final long record,
                final Timestamp start) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit;
                    if(start.isString()) {
                        audit = client.auditRecordStartstr(record,
                                start.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        audit = client.auditRecordStart(record,
                                start.getMicros(), creds, transaction,
                                environment);
                    }
                    return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
                }

            });
        }

        @Override
        public Map<Timestamp, String> audit(final long record,
                final Timestamp start, final Timestamp end) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit;
                    if(start.isString()) {
                        audit = client.auditRecordStartstrEndstr(record,
                                start.toString(), end.toString(), creds,
                                transaction, environment);
                    }
                    else {
                        audit = client.auditRecordStartEnd(record,
                                start.getMicros(), end.getMicros(), creds,
                                transaction, environment);
                    }
                    return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
                }

            });
        }

        @Override
        public Map<Timestamp, String> audit(final String key, final long record) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.auditKeyRecord(key,
                            record, creds, transaction, environment);
                    return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
                }

            });
        }

        @Override
        public Map<Timestamp, String> audit(final String key,
                final long record, final Timestamp start) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit;
                    if(start.isString()) {
                        audit = client.auditKeyRecordStartstr(key, record,
                                start.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        audit = client.auditKeyRecordStart(key, record,
                                start.getMicros(), creds, transaction,
                                environment);
                    }
                    return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
                }

            });
        }

        @Override
        public Map<Timestamp, String> audit(final String key,
                final long record, final Timestamp start, final Timestamp end) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit;
                    if(start.isString()) {
                        audit = client.auditKeyRecordStartstrEndstr(key,
                                record, start.toString(), end.toString(),
                                creds, transaction, environment);
                    }
                    else {
                        audit = client.auditKeyRecordStartEnd(key, record,
                                start.getMicros(), end.getMicros(), creds,
                                transaction, environment);
                    }
                    return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
                }

            });
        }

        @Override
        public Map<String, Map<Object, Set<Long>>> browse(
                final Collection<String> keys) {
            return execute(new Callable<Map<String, Map<Object, Set<Long>>>>() {

                @Override
                public Map<String, Map<Object, Set<Long>>> call()
                        throws Exception {
                    Map<String, Map<TObject, Set<Long>>> raw = client
                            .browseKeys(Collections.toList(keys), creds,
                                    transaction, environment);
                    Map<String, Map<Object, Set<Long>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Key");
                    for (Entry<String, Map<TObject, Set<Long>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.thriftToJava(),
                                        Conversions.<Long> none()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<String, Map<Object, Set<Long>>> browse(
                final Collection<String> keys, final Timestamp timestamp) {
            return execute(new Callable<Map<String, Map<Object, Set<Long>>>>() {

                @Override
                public Map<String, Map<Object, Set<Long>>> call()
                        throws Exception {
                    Map<String, Map<TObject, Set<Long>>> raw;
                    if(timestamp.isString()) {
                        raw = client.browseKeysTimestr(
                                Collections.toList(keys), timestamp.toString(),
                                creds, transaction, environment);
                    }
                    else {
                        raw = client.browseKeysTime(Collections.toList(keys),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<String, Map<Object, Set<Long>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Key");
                    for (Entry<String, Map<TObject, Set<Long>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.thriftToJava(),
                                        Conversions.<Long> none()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<Object, Set<Long>> browse(final String key) {
            return execute(new Callable<Map<Object, Set<Long>>>() {

                @Override
                public Map<Object, Set<Long>> call() throws Exception {
                    Map<TObject, Set<Long>> raw = client.browseKey(key, creds,
                            transaction, environment);
                    Map<Object, Set<Long>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap(key, "Records");
                    for (Entry<TObject, Set<Long>> entry : raw.entrySet()) {
                        pretty.put(Convert.thriftToJava(entry.getKey()),
                                entry.getValue());
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<Object, Set<Long>> browse(final String key,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Object, Set<Long>>>() {

                @Override
                public Map<Object, Set<Long>> call() throws Exception {
                    Map<TObject, Set<Long>> raw;
                    if(timestamp.isString()) {
                        raw = client.browseKeyTimestr(key,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.browseKeyTime(key, timestamp.getMicros(),
                                creds, transaction, environment);
                    }
                    Map<Object, Set<Long>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap(key, "Records");
                    for (Entry<TObject, Set<Long>> entry : raw.entrySet()) {
                        pretty.put(Convert.thriftToJava(entry.getKey()),
                                entry.getValue());
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(final String key,
                final long record) {
            return execute(new Callable<Map<Timestamp, Set<Object>>>() {

                @Override
                public Map<Timestamp, Set<Object>> call() throws Exception {
                    Map<Long, Set<TObject>> raw = client.chronologizeKeyRecord(
                            key, record, creds, transaction, environment);
                    Map<Timestamp, Set<Object>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("DateTime", "Values");
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(Timestamp.fromMicros(entry.getKey()),
                                Transformers.transformSet(entry.getValue(),
                                        Conversions.thriftToJava()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(final String key,
                final long record, final Timestamp start) {
            Preconditions.checkArgument(start.getMicros() <= Time.now(),
                    "Start of range cannot be greater than the present");
            return execute(new Callable<Map<Timestamp, Set<Object>>>() {

                @Override
                public Map<Timestamp, Set<Object>> call() throws Exception {
                    Map<Long, Set<TObject>> raw;
                    if(start.isString()) {
                        raw = client.chronologizeKeyRecordStartstr(key, record,
                                start.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.chronologizeKeyRecordStart(key, record,
                                start.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Timestamp, Set<Object>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("DateTime", "Values");
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(Timestamp.fromMicros(entry.getKey()),
                                Transformers.transformSet(entry.getValue(),
                                        Conversions.thriftToJava()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(final String key,
                final long record, final Timestamp start, final Timestamp end) {
            return execute(new Callable<Map<Timestamp, Set<Object>>>() {

                @Override
                public Map<Timestamp, Set<Object>> call() throws Exception {
                    Map<Long, Set<TObject>> raw;
                    if(start.isString()) {
                        raw = client.chronologizeKeyRecordStartstrEndstr(key,
                                record, start.toString(), end.toString(),
                                creds, transaction, environment);
                    }
                    else {
                        raw = client.chronologizeKeyRecordStartEnd(key, record,
                                start.getMicros(), end.getMicros(), creds,
                                transaction, environment);
                    }
                    Map<Timestamp, Set<Object>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("DateTime", "Values");
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(Timestamp.fromMicros(entry.getKey()),
                                Transformers.transformSet(entry.getValue(),
                                        Conversions.thriftToJava()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public void clear(final Collection<Long> records) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clearRecords(Collections.toLongList(records), creds,
                            transaction, environment);
                    return null;
                }

            });
        }

        @Override
        public void clear(final Collection<String> keys,
                final Collection<Long> records) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clearKeysRecords(Collections.toList(keys),
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    return null;
                }

            });
        }

        @Override
        public void clear(final Collection<String> keys, final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clearKeysRecord(Collections.toList(keys), record,
                            creds, transaction, environment);
                    return null;
                }

            });
        }

        @Override
        public void clear(final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clearRecord(record, creds, transaction, environment);
                    return null;
                }

            });

        }

        @Override
        public void clear(final String key, final Collection<Long> records) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clearKeyRecords(key,
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    return null;
                }

            });
        }

        @Override
        public void clear(final String key, final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clearKeyRecord(key, record, creds, transaction,
                            environment);
                    return null;
                }

            });
        }

        @Override
        public boolean commit() {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    final TransactionToken token = transaction;
                    transaction = null;
                    return token != null ? client.commit(creds, token,
                            environment) : false;
                }

            });
        }

        @Override
        public Map<Long, Set<String>> describe(final Collection<Long> records) {
            return execute(new Callable<Map<Long, Set<String>>>() {

                @Override
                public Map<Long, Set<String>> call() throws Exception {
                    Map<Long, Set<String>> raw = client.describeRecords(
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    Map<Long, Set<String>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", "Keys");
                    for (Entry<Long, Set<String>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), entry.getValue());
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<Long, Set<String>> describe(final Collection<Long> records,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Set<String>>>() {

                @Override
                public Map<Long, Set<String>> call() throws Exception {
                    Map<Long, Set<String>> raw;
                    if(timestamp.isString()) {
                        raw = client.describeRecordsTimestr(
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.describeRecordsTime(
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Set<String>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", "Keys");
                    for (Entry<Long, Set<String>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), entry.getValue());
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Set<String> describe(final long record) {
            return execute(new Callable<Set<String>>() {

                @Override
                public Set<String> call() throws Exception {
                    Set<String> result = client.describeRecord(record, creds,
                            transaction, environment);
                    return result;
                }
            });
        }

        @Override
        public Set<String> describe(final long record, final Timestamp timestamp) {
            return execute(new Callable<Set<String>>() {

                @Override
                public Set<String> call() throws Exception {
                    if(timestamp.isString()) {
                        return client.describeRecordTimestr(record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        return client.describeRecordTime(record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                }
            });
        }

        @Override
        public <T> Map<String, Map<Diff, Set<T>>> diff(final long record,
                final Timestamp start) {
            return execute(new Callable<Map<String, Map<Diff, Set<T>>>>() {

                @Override
                public Map<String, Map<Diff, Set<T>>> call() throws Exception {
                    Map<String, Map<Diff, Set<TObject>>> raw;
                    if(start.isString()) {
                        raw = client.diffRecordStartstr(record,
                                start.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.diffRecordStart(record, start.getMicros(),
                                creds, transaction, environment);
                    }
                    PrettyLinkedTableMap<String, Diff, Set<T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap();
                    pretty.setRowName("Key");
                    for (Entry<String, Map<Diff, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<Diff> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public <T> Map<String, Map<Diff, Set<T>>> diff(final long record,
                final Timestamp start, final Timestamp end) {
            return execute(new Callable<Map<String, Map<Diff, Set<T>>>>() {

                @Override
                public Map<String, Map<Diff, Set<T>>> call() throws Exception {
                    Map<String, Map<Diff, Set<TObject>>> raw;
                    if(start.isString()) {
                        raw = client.diffRecordStartstrEndstr(record,
                                start.toString(), end.toString(), creds,
                                transaction, environment);
                    }
                    else {
                        raw = client.diffRecordStartEnd(record,
                                start.getMicros(), end.getMicros(), creds,
                                transaction, environment);
                    }
                    PrettyLinkedTableMap<String, Diff, Set<T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap();
                    pretty.setRowName("Key");
                    for (Entry<String, Map<Diff, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<Diff> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public <T> Map<Diff, Set<T>> diff(final String key, final long record,
                final Timestamp start) {
            return execute(new Callable<Map<Diff, Set<T>>>() {

                @Override
                public Map<Diff, Set<T>> call() throws Exception {
                    Map<Diff, Set<TObject>> raw;
                    if(start.isString()) {
                        raw = client.diffKeyRecordStartstr(key, record,
                                start.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.diffKeyRecordStart(key, record,
                                start.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Diff, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Operation", "Value");
                    for (Entry<Diff, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public <T> Map<Diff, Set<T>> diff(final String key, final long record,
                final Timestamp start, final Timestamp end) {
            return execute(new Callable<Map<Diff, Set<T>>>() {

                @Override
                public Map<Diff, Set<T>> call() throws Exception {
                    Map<Diff, Set<TObject>> raw;
                    if(start.isString()) {
                        raw = client.diffKeyRecordStartstrEndstr(key, record,
                                start.toString(), end.toString(), creds,
                                transaction, environment);
                    }
                    else {
                        raw = client.diffKeyRecordStartEnd(key, record,
                                start.getMicros(), end.getMicros(), creds,
                                transaction, environment);
                    }
                    Map<Diff, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Operation", "Value");
                    for (Entry<Diff, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Map<T, Map<Diff, Set<Long>>> diff(final String key,
                final Timestamp start) {
            return execute(new Callable<Map<T, Map<Diff, Set<Long>>>>() {

                @Override
                public Map<T, Map<Diff, Set<Long>>> call() throws Exception {
                    Map<TObject, Map<Diff, Set<Long>>> raw;
                    if(start.isString()) {
                        raw = client.diffKeyStartstr(key, start.toString(),
                                creds, transaction, environment);
                    }
                    else {
                        raw = client.diffKeyStart(key, start.getMicros(),
                                creds, transaction, environment);
                    }
                    PrettyLinkedTableMap<T, Diff, Set<Long>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap();
                    pretty.setRowName("Value");
                    for (Entry<TObject, Map<Diff, Set<Long>>> entry : raw
                            .entrySet()) {
                        pretty.put((T) Convert.thriftToJava(entry.getKey()),
                                entry.getValue());
                    }
                    return pretty;
                }
            });
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Map<T, Map<Diff, Set<Long>>> diff(final String key,
                final Timestamp start, final Timestamp end) {
            return execute(new Callable<Map<T, Map<Diff, Set<Long>>>>() {

                @Override
                public Map<T, Map<Diff, Set<Long>>> call() throws Exception {
                    Map<TObject, Map<Diff, Set<Long>>> raw;
                    if(start.isString()) {
                        raw = client.diffKeyStartstrEndstr(key,
                                start.toString(), end.toString(), creds,
                                transaction, environment);
                    }
                    else {
                        raw = client.diffKeyStartEnd(key, start.getMicros(),
                                end.getMicros(), creds, transaction,
                                environment);
                    }
                    PrettyLinkedTableMap<T, Diff, Set<Long>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap();
                    pretty.setRowName("Value");
                    for (Entry<TObject, Map<Diff, Set<Long>>> entry : raw
                            .entrySet()) {
                        pretty.put((T) Convert.thriftToJava(entry.getKey()),
                                entry.getValue());
                    }
                    return pretty;
                }
            });
        }

        @Override
        public void exit() {
            try {
                client.logout(creds, environment);
                client.getInputProtocol().getTransport().close();
                client.getOutputProtocol().getTransport().close();
            }
            catch (com.cinchapi.concourse.thrift.SecurityException
                    | TTransportException e) {
                // Handle corner case where the client is existing because of
                // (or after the occurrence of) a password change, which means
                // it can't perform a traditional logout. Its worth nothing that
                // we're okay with this scenario because a password change will
                // delete all previously issued tokens.
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public Set<Long> find(final Criteria criteria) {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.findCriteria(
                            Language.translateToThriftCriteria(criteria),
                            creds, transaction, environment);
                }

            });
        }

        @Override
        public Set<Long> find(Object object) {
            if(object instanceof BuildableState) {
                return find(((BuildableState) object).build());
            }
            else {
                throw new IllegalArgumentException(object
                        + " is not a valid argument for the find method");
            }
        }

        @Override
        public Set<Long> find(final String ccl) {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.findCcl(ccl, creds, transaction, environment);
                }

            });
        }

        @Override
        public Set<Long> find(String key, Object value) {
            return find0(key, Operator.EQUALS, value);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value) {
            return find0(key, operator, value);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2) {
            return find0(key, operator, value, value2);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2, Timestamp timestamp) {
            return find0(timestamp, key, operator, value, value2);
        }

        @Override
        public Set<Long> find(final String key, final Operator operator,
                final Object value, final Timestamp timestamp) {
            return find0(timestamp, key, operator, value);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value) {
            return find0(key, operator, value);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                final Object value2) {
            return find0(key, operator, value, value2);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                final Object value2, Timestamp timestamp) {
            return find0(timestamp, key, operator, value, value2);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Timestamp timestamp) {
            return find0(timestamp, key, operator, value);
        }

        @Override
        public <T> long findOrAdd(final String key, final T value) {
            return execute(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    return client.findOrAddKeyValue(key,
                            Convert.javaToThrift(value), creds, transaction,
                            environment);
                }

            });
        }

        @Override
        public long findOrInsert(final Criteria criteria, final String json) {
            return execute(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    return client.findOrInsertCriteriaJson(
                            Language.translateToThriftCriteria(criteria), json,
                            creds, transaction, environment);
                }

            });
        }

        @Override
        public long findOrInsert(final String ccl, final String json) {
            return execute(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    return client.findOrInsertCclJson(ccl, json, creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Collection<String> keys,
                final Collection<Long> records) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw = client
                            .getKeysRecords(Collections.toList(keys),
                                    Collections.toLongList(records), creds,
                                    transaction, environment);
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Collection<String> keys,
                final Collection<Long> records, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.getKeysRecordsTimestr(
                                Collections.toList(keys),
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeysRecordsTime(
                                Collections.toList(keys),
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Collection<String> keys,
                final Criteria criteria) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw = client
                            .getKeysCriteria(Collections.toList(keys), Language
                                    .translateToThriftCriteria(criteria),
                                    creds, transaction, environment);
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Collection<String> keys,
                final Criteria criteria, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.getKeysCriteriaTimestr(
                                Collections.toList(keys),
                                Language.translateToThriftCriteria(criteria),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeysCriteriaTime(
                                Collections.toList(keys),
                                Language.translateToThriftCriteria(criteria),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<String, T> get(final Collection<String> keys,
                final long record) {
            return execute(new Callable<Map<String, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<String, T> call() throws Exception {
                    Map<String, TObject> raw = client.getKeysRecord(
                            Collections.toList(keys), record, creds,
                            transaction, environment);
                    Map<String, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Key", "Value");
                    for (Entry<String, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<String, T> get(final Collection<String> keys,
                final long record, final Timestamp timestamp) {
            return execute(new Callable<Map<String, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<String, T> call() throws Exception {
                    Map<String, TObject> raw;
                    if(timestamp.isString()) {
                        raw = client.getKeysRecordTimestr(
                                Collections.toList(keys), record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeysRecordTime(
                                Collections.toList(keys), record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<String, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Key", "Value");
                    for (Entry<String, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Object criteria) {
            if(criteria instanceof BuildableState) {
                return get(keys, ((BuildableState) criteria).build());
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Object criteria, Timestamp timestamp) {
            if(criteria instanceof BuildableState) {
                return get(keys, ((BuildableState) criteria).build(), timestamp);
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Collection<String> keys,
                final String ccl) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw = client.getKeysCcl(
                            Collections.toList(keys), ccl, creds, transaction,
                            environment);
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Collection<String> keys,
                final String ccl, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.getKeysCclTimestr(
                                Collections.toList(keys), ccl,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeysCclTime(Collections.toList(keys),
                                ccl, timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Criteria criteria) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw = client.getCriteria(
                            Language.translateToThriftCriteria(criteria),
                            creds, transaction, environment);
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final Criteria criteria,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.getCriteriaTimestr(
                                Language.translateToThriftCriteria(criteria),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getCriteriaTime(
                                Language.translateToThriftCriteria(criteria),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Object criteria) {
            if(criteria instanceof BuildableState) {
                return get(((BuildableState) criteria).build());
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Object criteria,
                Timestamp timestamp) {
            if(criteria instanceof BuildableState) {
                return get(((BuildableState) criteria).build(), timestamp);
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final String ccl) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw = client.getCcl(ccl,
                            creds, transaction, environment);
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, T> get(final String key,
                final Collection<Long> records) {
            return execute(new Callable<Map<Long, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<Long, T> call() throws Exception {
                    Map<Long, TObject> raw = client.getKeyRecords(key,
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    Map<Long, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, T> get(final String key,
                final Collection<Long> records, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<Long, T> call() throws Exception {
                    Map<Long, TObject> raw;
                    if(timestamp.isString()) {
                        raw = client.getKeyRecordsTimestr(key,
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeyRecordsTime(key,
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, T> get(final String key, final Criteria criteria) {
            return execute(new Callable<Map<Long, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<Long, T> call() throws Exception {
                    Map<Long, TObject> raw = client.getKeyCriteria(key,
                            Language.translateToThriftCriteria(criteria),
                            creds, transaction, environment);
                    Map<Long, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, T> get(final String key, final Criteria criteria,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<Long, T> call() throws Exception {
                    Map<Long, TObject> raw;
                    if(timestamp.isString()) {
                        raw = client.getKeyCriteriaTimestr(key,
                                Language.translateToThriftCriteria(criteria),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeyCriteriaTime(key,
                                Language.translateToThriftCriteria(criteria),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        @Nullable
        public <T> T get(final String key, final long record) {
            return execute(new Callable<T>() {

                @SuppressWarnings("unchecked")
                @Override
                public T call() throws Exception {
                    TObject raw = client.getKeyRecord(key, record, creds,
                            transaction, environment);
                    return raw == TObject.NULL ? null : (T) Convert
                            .thriftToJava(raw);

                }

            });
        }

        @Override
        @Nullable
        public <T> T get(final String key, final long record,
                final Timestamp timestamp) {
            return execute(new Callable<T>() {

                @SuppressWarnings("unchecked")
                @Override
                public T call() throws Exception {
                    TObject raw;
                    if(timestamp.isString()) {
                        raw = client.getKeyRecordTimestr(key, record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeyRecordTime(key, record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    return raw == TObject.NULL ? null : (T) Convert
                            .thriftToJava(raw);

                }

            });
        }

        @Override
        public <T> Map<Long, T> get(String key, Object criteria) {
            if(criteria instanceof BuildableState) {
                return get(key, ((BuildableState) criteria).build());
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, T> get(String key, Object criteria,
                Timestamp timestamp) {
            if(criteria instanceof BuildableState) {
                return get(key, ((BuildableState) criteria).build(), timestamp);
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, T> get(final String key, final String ccl) {
            return execute(new Callable<Map<Long, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<Long, T> call() throws Exception {
                    Map<Long, TObject> raw = client.getKeyCcl(key, ccl, creds,
                            transaction, environment);
                    Map<Long, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, T> get(final String key, final String ccl,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, T>>() {

                @SuppressWarnings("unchecked")
                @Override
                public Map<Long, T> call() throws Exception {
                    Map<Long, TObject> raw;
                    if(timestamp.isString()) {
                        raw = client.getKeyCclTimestr(key, ccl,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.getKeyCclTime(key, ccl,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, T> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, TObject> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(),
                                (T) Convert.thriftToJava(entry.getValue()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(final String ccl,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, T>>>() {

                @Override
                public Map<Long, Map<String, T>> call() throws Exception {
                    Map<Long, Map<String, TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.getCclTimestr(ccl, timestamp.toString(),
                                creds, transaction, environment);
                    }
                    else {
                        raw = client.getCclTime(ccl, timestamp.getMicros(),
                                creds, transaction, environment);
                    }
                    Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, TObject>> entry : raw
                            .entrySet()) {
                        pretty.put(
                                entry.getKey(),
                                Transformers.transformMapValues(
                                        entry.getValue(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public String getServerEnvironment() {
            return execute(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    return client.getServerEnvironment(creds, transaction,
                            environment);
                }

            });
        }

        @Override
        public String getServerVersion() {
            return execute(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    return client.getServerVersion();
                }

            });
        }

        @Override
        public Set<Long> insert(final String json) {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.insertJson(json, creds, transaction,
                            environment);
                }

            });

        }

        @Override
        public Map<Long, Boolean> insert(final String json,
                final Collection<Long> records) {
            return execute(new Callable<Map<Long, Boolean>>() {

                @Override
                public Map<Long, Boolean> call() throws Exception {
                    return client.insertJsonRecords(json,
                            Collections.toLongList(records), creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public boolean insert(final String json, final long record) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.insertJsonRecord(json, record, creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public Set<Long> inventory() {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.inventory(creds, transaction, environment);
                }

            });
        }

        @Override
        public String jsonify(Collection<Long> records) {
            return jsonify(records, true);
        }

        @Override
        public String jsonify(final Collection<Long> records,
                final boolean identifier) {
            return execute(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    return client.jsonifyRecords(
                            Collections.toLongList(records), identifier, creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public String jsonify(Collection<Long> records, Timestamp timestamp) {
            return jsonify(records, timestamp, true);
        }

        @Override
        public String jsonify(final Collection<Long> records,
                final Timestamp timestamp, final boolean identifier) {
            return execute(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    if(timestamp.isString()) {
                        return client.jsonifyRecordsTimestr(
                                Collections.toLongList(records),
                                timestamp.toString(), identifier, creds,
                                transaction, environment);
                    }
                    else {
                        return client.jsonifyRecordsTime(
                                Collections.toLongList(records),
                                timestamp.getMicros(), identifier, creds,
                                transaction, environment);
                    }
                }

            });
        }

        @Override
        public String jsonify(long record) {
            return jsonify(Lists.newArrayList(record), true);
        }

        @Override
        public String jsonify(long record, boolean identifier) {
            return jsonify(Lists.newArrayList(record), identifier);
        }

        @Override
        public String jsonify(long record, Timestamp timestamp) {
            return jsonify(Lists.newArrayList(record), timestamp, true);
        }

        @Override
        public String jsonify(long record, Timestamp timestamp,
                boolean identifier) {
            return jsonify(Lists.newArrayList(record), timestamp, identifier);
        }

        @Override
        public Map<Long, Boolean> link(String key, long source,
                Collection<Long> destinations) {
            Map<Long, Boolean> result = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", "Result");
            for (long destination : destinations) {
                result.put(destination, link(key, source, destination));
            }
            return result;
        }

        @Override
        public boolean link(String key, long source, long destination) {
            return add(key, Link.to(destination), source);
        }

        @Override
        public Map<Long, Boolean> ping(final Collection<Long> records) {
            return execute(new Callable<Map<Long, Boolean>>() {

                @Override
                public Map<Long, Boolean> call() throws Exception {
                    return client.pingRecords(Collections.toLongList(records),
                            creds, transaction, environment);
                }

            });
        }

        @Override
        public boolean ping(final long record) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.pingRecord(record, creds, transaction,
                            environment);
                }

            });
        }

        @Override
        public <T> Map<Long, Boolean> remove(final String key, final T value,
                final Collection<Long> records) {
            return execute(new Callable<Map<Long, Boolean>>() {

                @Override
                public Map<Long, Boolean> call() throws Exception {
                    Map<Long, Boolean> raw = client.removeKeyValueRecords(key,
                            Convert.javaToThrift(value),
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    Map<Long, Boolean> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", "Result");
                    for (long record : records) {
                        pretty.put(record, raw.get(record));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> boolean remove(final String key, final T value,
                final long record) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.removeKeyValueRecord(key,
                            Convert.javaToThrift(value), record, creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public void revert(final Collection<String> keys,
                final Collection<Long> records, final Timestamp timestamp) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    if(timestamp.isString()) {
                        client.revertKeysRecordsTimestr(
                                Collections.toList(keys),
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        client.revertKeysRecordsTime(Collections.toList(keys),
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    return null;
                }

            });
        }

        @Override
        public void revert(final Collection<String> keys, final long record,
                final Timestamp timestamp) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    if(timestamp.isString()) {
                        client.revertKeysRecordTimestr(
                                Collections.toList(keys), record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        client.revertKeysRecordTime(Collections.toList(keys),
                                record, timestamp.getMicros(), creds,
                                transaction, environment);
                    }
                    return null;
                }

            });

        }

        @Override
        public void revert(final String key, final Collection<Long> records,
                final Timestamp timestamp) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    if(timestamp.isString()) {
                        client.revertKeyRecordsTimestr(key,
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        client.revertKeyRecordsTime(key,
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    return null;
                }

            });

        }

        @Override
        public void revert(final String key, final long record,
                final Timestamp timestamp) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    if(timestamp.isString()) {
                        client.revertKeyRecordTimestr(key, record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        client.revertKeyRecordTime(key, record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    return null;
                }

            });

        }

        @Override
        public Set<Long> search(final String key, final String query) {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.search(key, query, creds, transaction,
                            environment);
                }

            });
        }

        @Override
        public Map<Long, Map<String, Set<Object>>> select(
                final Collection<Long> records) {
            return execute(new Callable<Map<Long, Map<String, Set<Object>>>>() {

                @Override
                public Map<Long, Map<String, Set<Object>>> call()
                        throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectRecords(Collections.toLongList(records),
                                    creds, transaction, environment);
                    Map<Long, Map<String, Set<Object>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.thriftToJava()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<Long, Map<String, Set<Object>>> select(
                final Collection<Long> records, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, Set<Object>>>>() {

                @Override
                public Map<Long, Map<String, Set<Object>>> call()
                        throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectRecordsTimestr(
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectRecordsTime(
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, Set<Object>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.thriftToJava()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                final Collection<String> keys, final Collection<Long> records) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectKeysRecords(Collections.toList(keys),
                                    Collections.toLongList(records), creds,
                                    transaction, environment);
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                final Collection<String> keys, final Collection<Long> records,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectKeysRecordsTimestr(
                                Collections.toList(keys),
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectKeysRecordsTime(
                                Collections.toList(keys),
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                final Collection<String> keys, final Criteria criteria) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectKeysCriteria(
                                    Collections.toList(keys),
                                    Language.translateToThriftCriteria(criteria),
                                    creds, transaction, environment);
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                final Collection<String> keys, final Criteria criteria,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectKeysCriteriaTimestr(
                                Collections.toList(keys),
                                Language.translateToThriftCriteria(criteria),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectKeysCriteriaTime(
                                Collections.toList(keys),
                                Language.translateToThriftCriteria(criteria),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<String, Set<T>> select(final Collection<String> keys,
                final long record) {
            return execute(new Callable<Map<String, Set<T>>>() {

                @Override
                public Map<String, Set<T>> call() throws Exception {
                    Map<String, Set<TObject>> raw = client.selectKeysRecord(
                            Collections.toList(keys), record, creds,
                            transaction, environment);
                    Map<String, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Key", "Values");
                    for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<String, Set<T>> select(final Collection<String> keys,
                final long record, final Timestamp timestamp) {
            return execute(new Callable<Map<String, Set<T>>>() {

                @Override
                public Map<String, Set<T>> call() throws Exception {
                    Map<String, Set<TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectKeysRecordTimestr(
                                Collections.toList(keys), record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectKeysRecordTime(
                                Collections.toList(keys), record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<String, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Key", "Values");
                    for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Object criteria) {
            if(criteria instanceof BuildableState) {
                return select(keys, ((BuildableState) criteria).build());
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the select method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Object criteria, Timestamp timestamp) {
            if(criteria instanceof BuildableState) {
                return select(keys, ((BuildableState) criteria).build(),
                        timestamp);
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the select method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                final Collection<String> keys, final String ccl) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectKeysCcl(Collections.toList(keys), ccl,
                                    creds, transaction, environment);
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                final Collection<String> keys, final String ccl,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectKeysCclTimestr(
                                Collections.toList(keys), ccl,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectKeysCclTime(
                                Collections.toList(keys), ccl,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(final Criteria criteria) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectCriteria(Language
                                    .translateToThriftCriteria(criteria),
                                    creds, transaction, environment);
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                final Criteria criteria, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectCriteriaTimestr(
                                Language.translateToThriftCriteria(criteria),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectCriteriaTime(
                                Language.translateToThriftCriteria(criteria),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public Map<String, Set<Object>> select(final long record) {
            return execute(new Callable<Map<String, Set<Object>>>() {

                @Override
                public Map<String, Set<Object>> call() throws Exception {
                    Map<String, Set<TObject>> raw = client.selectRecord(record,
                            creds, transaction, environment);
                    Map<String, Set<Object>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Key", "Values");
                    for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(), Conversions.thriftToJava()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public Map<String, Set<Object>> select(final long record,
                final Timestamp timestamp) {
            return execute(new Callable<Map<String, Set<Object>>>() {

                @Override
                public Map<String, Set<Object>> call() throws Exception {
                    Map<String, Set<TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectRecordTimestr(record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectRecordTime(record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<String, Set<Object>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Key", "Values");
                    for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(), Conversions.thriftToJava()));
                    }
                    return pretty;
                }
            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Object criteria) {
            if(criteria instanceof BuildableState) {
                return select(((BuildableState) criteria).build());
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Object criteria,
                Timestamp timestamp) {
            if(criteria instanceof BuildableState) {
                return select(((BuildableState) criteria).build(), timestamp);
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the get method");
            }
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(final String ccl) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectCcl(ccl, creds, transaction, environment);
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Set<T>> select(final String key,
                final Collection<Long> records) {
            return execute(new Callable<Map<Long, Set<T>>>() {

                @Override
                public Map<Long, Set<T>> call() throws Exception {
                    Map<Long, Set<TObject>> raw = client.selectKeyRecords(key,
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Set<T>> select(final String key,
                final Collection<Long> records, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Set<T>>>() {

                @Override
                public Map<Long, Set<T>> call() throws Exception {
                    Map<Long, Set<TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectKeyRecordsTimestr(key,
                                Collections.toLongList(records),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectKeyRecordsTime(key,
                                Collections.toLongList(records),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Set<T>> select(final String key,
                final Criteria criteria) {
            return execute(new Callable<Map<Long, Set<T>>>() {

                @Override
                public Map<Long, Set<T>> call() throws Exception {
                    Map<Long, Set<TObject>> raw = client.selectKeyCriteria(key,
                            Language.translateToThriftCriteria(criteria),
                            creds, transaction, environment);
                    Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Set<T>> select(final String key,
                final Criteria criteria, final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Set<T>>>() {

                @Override
                public Map<Long, Set<T>> call() throws Exception {
                    Map<Long, Set<TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectKeyCriteriaTimestr(key,
                                Language.translateToThriftCriteria(criteria),
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectKeyCriteriaTime(key,
                                Language.translateToThriftCriteria(criteria),
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Set<T> select(final String key, final long record) {
            return execute(new Callable<Set<T>>() {

                @Override
                public Set<T> call() throws Exception {
                    Set<TObject> values = client.selectKeyRecord(key, record,
                            creds, transaction, environment);
                    return Transformers.transformSet(values,
                            Conversions.<T> thriftToJavaCasted());
                }

            });
        }

        @Override
        public <T> Set<T> select(final String key, final long record,
                final Timestamp timestamp) {
            return execute(new Callable<Set<T>>() {

                @Override
                public Set<T> call() throws Exception {
                    Set<TObject> values;
                    if(timestamp.isString()) {
                        values = client.selectKeyRecordTimestr(key, record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        values = client.selectKeyRecordTime(key, record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    return Transformers.transformSet(values,
                            Conversions.<T> thriftToJavaCasted());
                }

            });
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Object criteria) {
            if(criteria instanceof BuildableState) {
                return select(key, ((BuildableState) criteria).build());
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the select method");
            }
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Object criteria,
                Timestamp timestamp) {
            if(criteria instanceof BuildableState) {
                return select(key, ((BuildableState) criteria).build(),
                        timestamp);
            }
            else {
                throw new IllegalArgumentException(criteria
                        + " is not a valid argument for the select method");
            }
        }

        @Override
        public <T> Map<Long, Set<T>> select(final String key, final String ccl) {
            return execute(new Callable<Map<Long, Set<T>>>() {

                @Override
                public Map<Long, Set<T>> call() throws Exception {
                    Map<Long, Set<TObject>> raw = client.selectKeyCcl(key, ccl,
                            creds, transaction, environment);
                    Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Set<T>> select(final String key, final String ccl,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Set<T>>>() {

                @Override
                public Map<Long, Set<T>> call() throws Exception {
                    Map<Long, Set<TObject>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectKeyCclTimestr(key, ccl,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectKeyCclTime(key, ccl,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                            .newPrettyLinkedHashMap("Record", key);
                    for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                        pretty.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(final String ccl,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Long, Map<String, Set<T>>>>() {

                @Override
                public Map<Long, Map<String, Set<T>>> call() throws Exception {
                    Map<Long, Map<String, Set<TObject>>> raw;
                    if(timestamp.isString()) {
                        raw = client.selectCclTimestr(ccl,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        raw = client.selectCclTime(ccl, timestamp.getMicros(),
                                creds, transaction, environment);
                    }
                    Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                            .entrySet()) {
                        pretty.put(entry.getKey(), Transformers
                                .transformMapSet(entry.getValue(),
                                        Conversions.<String> none(),
                                        Conversions.<T> thriftToJavaCasted()));
                    }
                    return pretty;
                }

            });
        }

        @Override
        public void set(final String key, final Object value,
                final Collection<Long> records) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.setKeyValueRecords(key, Convert.javaToThrift(value),
                            Collections.toLongList(records), creds,
                            transaction, environment);
                    return null;
                }

            });
        }

        @Override
        public <T> void set(final String key, final T value, final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.setKeyValueRecord(key, Convert.javaToThrift(value),
                            record, creds, transaction, environment);
                    return null;
                }

            });
        }

        @Override
        public void stage() throws TransactionException {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    transaction = client.stage(creds, environment);
                    return null;
                }

            });
        }

        @Override
        public Timestamp time() {
            return execute(new Callable<Timestamp>() {

                @Override
                public Timestamp call() throws Exception {
                    return Timestamp.fromMicros(client.time(creds, transaction,
                            environment));
                }

            });
        }

        @Override
        public Timestamp time(final String phrase) {
            return execute(new Callable<Timestamp>() {

                @Override
                public Timestamp call() throws Exception {
                    return Timestamp.fromMicros(client.timePhrase(phrase,
                            creds, transaction, environment));
                }

            });
        }

        @Override
        public String toString() {
            return "Connected to " + host + ":" + port + " as "
                    + new String(ClientSecurity.decrypt(username).array());
        }

        @Override
        public boolean unlink(String key, long source, long destination) {
            return remove(key, Link.to(destination), source);
        }

        @Override
        public boolean verify(final String key, final Object value,
                final long record) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.verifyKeyValueRecord(key,
                            Convert.javaToThrift(value), record, creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public boolean verify(final String key, final Object value,
                final long record, final Timestamp timestamp) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    if(timestamp.isString()) {
                        return client.verifyKeyValueRecordTimestr(key,
                                Convert.javaToThrift(value), record,
                                timestamp.toString(), creds, transaction,
                                environment);
                    }
                    else {
                        return client.verifyKeyValueRecordTime(key,
                                Convert.javaToThrift(value), record,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                }

            });
        }

        @Override
        public boolean verifyAndSwap(final String key, final Object expected,
                final long record, final Object replacement) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.verifyAndSwap(key,
                            Convert.javaToThrift(expected), record,
                            Convert.javaToThrift(replacement), creds,
                            transaction, environment);
                }

            });
        }

        @Override
        public void verifyOrSet(final String key, final Object value,
                final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.verifyOrSet(key, Convert.javaToThrift(value),
                            record, creds, transaction, environment);
                    return null;
                }

            });
        }

        /**
         * Authenticate the {@link #username} and {@link #password} and populate
         * {@link #creds} with the appropriate AccessToken.
         */
        private void authenticate() {
            try {
                creds = client.login(ClientSecurity.decrypt(username),
                        ClientSecurity.decrypt(password), environment);
            }
            catch (TException e) {
                throw Throwables.propagate(e);
            }
        }

        /**
         * Execute the task defined in {@code callable}. This method contains
         * retry logic to handle cases when {@code creds} expires and must be
         * updated.
         * 
         * @param callable
         * @return the task result
         */
        private <T> T execute(Callable<T> callable) {
            try {
                return callable.call();
            }
            catch (SecurityException e) {
                authenticate();
                return execute(callable);
            }
            catch (com.cinchapi.concourse.thrift.TransactionException e) {
                throw new TransactionException();
            }
            catch (com.cinchapi.concourse.thrift.DuplicateEntryException e) {
                throw new DuplicateEntryException(e);
            }
            catch (com.cinchapi.concourse.thrift.InvalidArgumentException e) {
                throw new InvalidArgumentException(e);
            }
            catch (com.cinchapi.concourse.thrift.ParseException e) {
                throw new ParseException(e);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        /**
         * Perform an old-school/simple find operation where {@code key}
         * satisfied {@code operation} in relation to the specified
         * {@code values}.
         * 
         * @param key
         * @param operator
         * @param values
         * @return the records that match the criteria.
         */
        private Set<Long> find0(final String key, final Object operator,
                final Object... values) {
            final List<TObject> tValues = Lists.transform(
                    Lists.newArrayList(values), Conversions.javaToThrift());
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    if(operator instanceof Operator) {
                        return client.findKeyOperatorValues(key,
                                (Operator) operator, tValues, creds,
                                transaction, environment);
                    }
                    else {
                        return client.findKeyOperatorstrValues(key,
                                operator.toString(), tValues, creds,
                                transaction, environment);
                    }

                }

            });
        }

        /**
         * Perform an old-school/simple find operation where {@code key}
         * satisfied {@code operation} in relation to the specified
         * {@code values} at {@code timestamp}.
         * 
         * @param key
         * @param operator
         * @param values
         * @param timestamp
         * @return the records that match the criteria.
         */
        private Set<Long> find0(final Timestamp timestamp, final String key,
                final Object operator, final Object... values) {
            final List<TObject> tValues = Lists.transform(
                    Lists.newArrayList(values), Conversions.javaToThrift());
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    if(operator instanceof Operator) {
                        return client.findKeyOperatorValuesTime(key,
                                (Operator) operator, tValues,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }
                    else {
                        return client.findKeyOperatorstrValuesTime(key,
                                operator.toString(), tValues,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }

                }

            });
        }

    }
}
