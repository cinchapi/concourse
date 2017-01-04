/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.annotate.Incubating;
import com.cinchapi.concourse.config.ConcourseClientPreferences;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.Sets;
import com.google.common.collect.Multimap;

/**
 * A client connection to a Concourse node or cluster. Use one of the
 * {@link Concourse#connect()} methods to instantiate.
 * 
 * <h2>Overview</h2>
 * <p>
 * Concourse is a self-tuning database that enables live analytics for large
 * streams of operational data. Developers use Concourse to quickly build
 * software that requires both ACID transactions and the ability to get data
 * insights on demand. With Concourse, end-to-end data management requires no
 * extra infrastructure, no prior configuration and no additional coding–all of
 * which greatly reduce costs and allow developers to focus on core business
 * problems.
 * </p>
 * 
 * <h2>Using Transactions</h2>
 * <p>
 * By default, Concourse conducts every operation in {@code autocommit} mode
 * where every change is immediately written. Concourse also supports the
 * ability to stage a group of operations within transactions that are atomic,
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
        return new ConcourseThriftDriver();
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
        return new ConcourseThriftDriver(environment);
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
        return new ConcourseThriftDriver(host, port, username, password);
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
        return new ConcourseThriftDriver(host, port, username, password,
                environment);
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
     * Create a new connecting by copying the connection information from the
     * provided {@code concourse} handle.
     * 
     * @param concourse an existing {@link Concourse} connection handle
     * @return the handle
     */
    public static Concourse copyExistingConnection(Concourse concourse) {
        return concourse.copyConnection();
    }

    /**
     * The interface to use for all {@link #calculate() calculation} methods.
     */
    private Calculator calculator = null;

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
     * Atomically append {@code key} as {@code value} in each of the
     * {@code records} where it doesn't exist.
     * 
     * @param key the field name
     * @param value the value to add
     * @param records a collection of record ids where an attempt is made to
     *            add the data
     * @return a {@link Map} associating each record id to a boolean that
     *         indicates if the data was added
     */
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
     * Return a list all the changes ever made to {@code record}.
     * 
     * @param record the record id
     * @return a {@link Map} associating the {@link Timestamp} of each change
     *         to the respective description of the change
     */
    public abstract Map<Timestamp, String> audit(long record);

    /**
     * Return a list all the changes made to {@code record} since {@code start}
     * (inclusive).
     *
     * @param record the record id
     * @param start an inclusive {@link Timestamp} of the oldest change that
     *            should possibly be included in the audit – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating the {@link Timestamp} of each change
     *         to the respective description of the change
     */
    public abstract Map<Timestamp, String> audit(long record, Timestamp start);

    /**
     * Return a list all the changes made to {@code record} between
     * {@code start} (inclusive) and {@code end} (non-inclusive).
     *
     * @param record the record id
     * @param start an inclusive {@link Timestamp} for the oldest change that
     *            should possibly be included in the audit – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @param end a non-inclusive {@link Timestamp} for the most recent change
     *            that should possibly be included in the audit – created from
     *            either a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating the {@link Timestamp} of each change
     *         to the respective description of the change
     */
    public abstract Map<Timestamp, String> audit(long record, Timestamp start,
            Timestamp end);

    /**
     * Return a list all the changes ever made to the {@code key} field in
     * {@code record}
     *
     * @param key the field name
     * @param record the record id
     * @return a {@link Map} associating the {@link Timestamp} of each change
     *         to the respective description of the change
     */
    public abstract Map<Timestamp, String> audit(String key, long record);

    /**
     * Return a list of all the changes made to the {@code key} field in
     * {@code record} since {@code start} (inclusive).
     * 
     * @param key the field name
     * @param record the record id
     * @param start an inclusive {@link Timestamp} for the oldest change that
     *            should possibly be included in the audit – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating the {@link Timestamp} of each change
     *         to the respective description of the change
     */
    public abstract Map<Timestamp, String> audit(String key, long record,
            Timestamp start);

    /**
     * Return a list of all the changes made to the {@code key} field in
     * {@code record} between {@code start} (inclusive) and {@code end}
     * (non-inclusive).
     * 
     * @param key the field name
     * @param record the record id
     * @param start an inclusive {@link Timestamp} for the oldest change that
     *            should possibly be included in the audit – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @param end a non-inclusive {@link Timestamp} for the most recent change
     *            that should possibly be included in the audit – created from
     *            either a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating the {@link Timestamp} of each change
     *         to the respective description of the change
     */
    public abstract Map<Timestamp, String> audit(String key, long record,
            Timestamp start, Timestamp end);

    /**
     * Return a view of the values from all records that are currently stored
     * for each of the {@code keys}.
     * 
     * @param keys a collection of field names
     * @return a {@link Map} associating each of the {@code keys} to a
     *         another {@link Map} associating each indexed value to the
     *         {@link Set} of records that contain that value in the {@code key}
     *         field
     */
    public abstract Map<String, Map<Object, Set<Long>>> browse(
            Collection<String> keys);

    /**
     * Return a view of the values from all records that were stored for each of
     * the {@code keys} at {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code keys} to a
     *         another {@link Map} associating each indexed value to the
     *         {@link Set} of records that contained that value in the
     *         {@code key} field at {@code timestamp}
     */
    public abstract Map<String, Map<Object, Set<Long>>> browse(
            Collection<String> keys, Timestamp timestamp);

    /**
     * Return a view of the values from all records that are currently stored
     * for {@code key}.
     * 
     * @param key the field name
     * @return a {@link Map} associating each indexed value to the {@link Set}
     *         of records that contain that value in the {@code key} field
     */
    public abstract Map<Object, Set<Long>> browse(String key);

    /**
     * Return a view of the values from all records that were stored for
     * {@code key} at {@code timestamp}.
     * 
     * @param key the field name
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each indexed value to the {@link Set}
     *         of records that contained that value in the {@code key} field at
     *         {@code timestamp}
     */
    public abstract Map<Object, Set<Long>> browse(String key,
            Timestamp timestamp);

    /**
     * Return a {@link Calculator} to use for calculations across data.
     * 
     * @return a {@link Calculator}
     */
    public Calculator calculate() {
        if(calculator == null) {
            calculator = new Calculator(this);
        }
        return calculator;
    }

    /**
     * Perform the specified calculation {@code method} using the provided
     * {@code args}.
     * 
     * @param method the name of the calculation method in the
     *            {@link Calculator} interface
     * @param args the args to pass to the method
     * @return the result of the calculation
     */
    public Object calculate(String method, Object... args) {
        if(calculator == null) {
            calculator = new Calculator(this);
        }
        return Reflection.call(calculator, method, args);
    }

    /**
     * Return a time series that contains a snapshot of the values stored for
     * {@code key} in {@code record} after every change made to the field.
     * 
     * @param key the field name
     * @param record the record id
     * @return a {@link Map} associating the {@link Timestamp} of each change to
     *         the {@link Set} of values that were stored in the field after
     *         that change
     */
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record);

    /**
     * Return a time series between {@code start} (inclusive) and the present
     * that contains a snapshot of the values stored for {@code key} in
     * {@code record} after every change made to the field during the time span.
     * 
     * @param key the field name
     * @param record the record id
     * @param start the first possible {@link Timestamp} to include in the
     *            time series – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating the {@link Timestamp} of each change to
     *         the {@link Set} of values that were stored in the field after
     *         that change
     */
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record, Timestamp start);

    /**
     * Return a time series between {@code start} (inclusive) and {@code end}
     * (non-inclusive) that contains a snapshot of the values stored for
     * {@code key} in {@code record} after every change made to the field during
     * the time span.
     * 
     * @param key the field name
     * @param record the record id
     * @param start the first possible {@link Timestamp} to include in the
     *            time series – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @param end the {@link Timestamp} that should be greater than every
     *            timestamp in the time series – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating the {@link Timestamp} of each change to
     *         the {@link Set} of values that were stored in the field after
     *         that change
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
    public abstract void clear(Collection<String> keys,
            Collection<Long> records);

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
     * For each of the {@code records}, return all of the keys that have at
     * least one value.
     * 
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to the
     *         {@link Set} of keys in that record
     */
    public abstract Map<Long, Set<String>> describe(Collection<Long> records);

    /**
     * For each of the {@code records}, return all the keys that had at least
     * one value at {@code timestamp}.
     * 
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code records} to the
     *         {@link Set} of keys that were in that record at {@code timestamp}
     */
    public abstract Map<Long, Set<String>> describe(Collection<Long> records,
            Timestamp timestamp);

    /**
     * Return all the keys in {@code record} that have at least one value.
     * 
     * @param record the record id
     * @return the {@link Set} of keys in {@code record}
     */
    public abstract Set<String> describe(long record);

    /**
     * Return all the keys in {@code record} that had at least one value at
     * {@code timestamp}.
     * 
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the {@link Set} of keys that were in {@code record} at
     *         {@code timestamp}
     */
    public abstract Set<String> describe(long record, Timestamp timestamp);

    /**
     * Return the <em>net</em> changes made to {@code record} since
     * {@code start}.
     * <p>
     * If you begin with the state of the {@code record} at {@code start} and
     * re-apply all the changes in the diff, you'll re-create the state of the
     * {@code record} at the present.
     * </p>
     * <p>
     * Unlike the {@link #audit(long, Timestamp) audit} method,
     * {@link #diff(long, Timestamp) diff} does not necessarily reflect ALL the
     * changes made to {@code record} during the time span.
     * </p>
     * 
     * @param record the record id
     * @param start the base timestamp from which the diff is calculated
     * @return a {@link Map} associating each key in the {@code record} to
     *         another {@link Map} associating a {@link Diff change
     *         description} to the {@link Set} of values that fit the
     *         description (i.e. <code>
     *         {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}
     *         </code> )
     */
    public abstract <T> Map<String, Map<Diff, Set<T>>> diff(long record,
            Timestamp start);

    /**
     * Return the <em>net</em> changes made to {@code record} from {@code start}
     * to {@code end}.
     * <p>
     * If you begin with the state of the {@code record} at {@code start} and
     * re-apply all the changes in the diff, you'll re-create the state of the
     * same {@code record} at {@code end}.
     * </p>
     * <p>
     * Unlike the {@link #audit(long, Timestamp, Timestamp) audit} method,
     * {@link #diff(long, Timestamp) diff} does not necessarily reflect ALL the
     * changes made to {@code record} during the time span.
     * </p>
     * 
     * @param record the record id
     * @param start the base timestamp from which the diff is calculated
     * @param end the comparison timestamp to which the diff is calculated
     * @return a {@link Map} associating each key in the {@code record} to
     *         another {@link Map} associating a {@link Diff change
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
     * <p>
     * If you begin with the state of the field at {@code start} and re-apply
     * all the changes in the diff, you'll re-create the state of the same field
     * at the present.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @param start the base timestamp from which the diff is calculated
     * @return a {@link Map} associating a {@link Diff change
     *         description} to the {@link Set} of values that fit the
     *         description (i.e. <code>
     *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
     *         </code> )
     */
    public abstract <T> Map<Diff, Set<T>> diff(String key, long record,
            Timestamp start);

    /**
     * Return the <em>net</em> changes made to {@code key} in {@code record}
     * from {@code start} to {@code end}.
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
     * @return a {@link Map} associating a {@link Diff change
     *         description} to the {@link Set} of values that fit the
     *         description (i.e. <code>
     *         {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
     *         </code> )
     */
    public abstract <T> Map<Diff, Set<T>> diff(String key, long record,
            Timestamp start, Timestamp end);

    /**
     * Return the <em>net</em> changes made to the {@code key} field across all
     * records since {@code start}.
     * <p>
     * If you begin with the state of the inverted index for {@code key} at
     * {@code start} and re-apply all the changes in the diff, you'll re-create
     * the state of the same index at the present.
     * </p>
     * <p>
     * Unlike the {@link #audit(String, long, Timestamp) audit} method,
     * {@link #diff(long, Timestamp) diff} does not necessarily reflect ALL the
     * changes made to {@code key} in {@code record} during the time span.
     * </p>
     * 
     * @param key the field name
     * @param start the base timestamp from which the diff is calculated
     * @return a {@link Map} associating each value stored for {@code key}
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
     * Return the <em>net</em> changes made to the {@code key} field across all
     * records from {@code start} to {@code end}.
     * <p>
     * If you begin with the state of the inverted index for {@code key} at
     * {@code start} and re-apply all the changes in the diff, you'll re-create
     * the state of the same index at {@code end}.
     * </p>
     * <p>
     * Unlike the {@link #audit(String, long, Timestamp, Timestamp) audit}
     * method, {@link #diff(long, Timestamp) diff} does not necessarily return
     * ALL the changes made to {@code key} in {@code record} during the time
     * span.
     * </p>
     * 
     * @param key the field name
     * @param start the base timestamp from which the diff is calculated
     * @param end the comparison timestamp to which the diff is calculated
     * @return a {@link Map} associating each value stored for {@code key}
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
     * Return the set of records that satisfy the {@link Criteria criteria}.
     * 
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @return the records that match the {@code criteria}
     */
    public abstract Set<Long> find(Criteria criteria);

    /**
     * Return the set of records that satisfy the {@code criteria}.
     * <p>
     * This method is syntactic sugar for {@link #find(Criteria)}. The only
     * difference is that this method takes a in-process {@link Criteria}
     * building sequence for convenience.
     * </p>
     * 
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired records
     * @return the records that match the {@code criteria}
     */
    public abstract Set<Long> find(Object criteria); // this method exists in
                                                     // case the caller
                                                     // forgets
                                                     // to called #build() on
                                                     // the CriteriaBuilder

    /**
     * Return the set of records that satisfy the {@code ccl} filter.
     * 
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String ccl);

    /**
     * Return the set of records where {@code key} {@link Operator#EQUALS
     * equals} {@code value}.
     * <p>
     * This method is a shortcut for calling
     * {@link #find(String, Operator, Object)} with {@link Operator#EQUALS}.
     * </p>
     * 
     * @param key the field name
     * @param value the value that must exist in the {@code key} field for the
     *            record to match
     * @return the records where {@code key} = {@code value}
     */
    public abstract Set<Long> find(String key, Object value);

    /**
     * Return the set of records where {@code key} was {@link Operator#EQUALS
     * equal} to {@code value} at {@code timestamp}.
     * <p>
     * This method is a shortcut for calling
     * {@link #find(String, Operator, Object, Timestamp)} with
     * {@link Operator#EQUALS}.
     * </p>
     * 
     * @param key the field name
     * @param value the value that must exist in the {@code key} field for the
     *            record to match
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use when checking for matches – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records where {@code key} was equal to {@code value} at
     *         {@code timestamp}
     */
    public abstract Set<Long> find(String key, Object value,
            Timestamp timestamp);

    /**
     * Return the set of {@code records} where the {@code key} field contains at
     * least one value that satisfies the {@code operator} in relation to the
     * {@code value}.
     * 
     * @param key the field name
     * @param operator the {@link Operator} to use when comparing the specified
     *            {@code value} to those stored across the {@code key} field
     *            while determining which records are matches
     * @param value the comparison value for the {@code operator}
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value);

    /**
     * Return the set of {@code records} where the {@code key} field contains at
     * least one value that satisfies the {@code operator} in relation to
     * {@code value} and {@code value2}.
     * 
     * @param key the field name
     * @param operator the {@link Operator} to use when comparing the specified
     *            values to those stored across the {@code key} field while
     *            determining which records are matches
     * @param value the first comparison value for the {@code operator}
     * @param value2 the second comparison value for the {@code operator}
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value,
            Object value2);

    /**
     * Return the set of {@code records} where the {@code key} field contained
     * at least one value at {@code timestamp} that satisfies the
     * {@code operator} in relation to {@code value} and {@code value2}.
     * 
     * @param key the field name
     * @param operator the {@link Operator} to use when comparing the specified
     *            values to those stored across the {@code key} field while
     *            determining which records are matches
     * @param value the first comparison value for the {@code operator}
     * @param value2 the second comparison value for the {@code operator}
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use when checking for matches – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp);

    /**
     * Return the set of {@code records} where the {@code key} field contained
     * at least one value at {@timestamp} that satisfies the {@code operator} in
     * relation to the {@code value}.
     * 
     * @param key the field name
     * @param operator the {@link Operator} to use when comparing the specified
     *            {@code value} to those stored across the {@code key} field
     *            while determining which records are matches
     * @param value the comparison value for the {@code operator}
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use when checking for matches – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp);

    /**
     * Return the set of {@code records} where the {@code key} field contains at
     * least one value that satisfies the {@code operator} in relation to the
     * {@code value}.
     * 
     * @param key the field name
     * @param operator a valid {@link Convert#stringToOperator(String)
     *            description} of an {@link Operator} to use when comparing the
     *            specified {@code value} to those stored across the {@code key}
     *            field while determining which records are matches
     * @param value the comparison value for the {@code operator}
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value);

    /**
     * Return the set of {@code records} where the {@code key} field contains at
     * least one value that satisfies the {@code operator} in relation to
     * {@code value} and {@code value2}.
     * 
     * @param key the field name
     * @param operator a valid {@link Convert#stringToOperator(String)
     *            description} of an {@link Operator} to use when comparing the
     *            specified {@code value} to those stored across the {@code key}
     *            field while determining which records are matches
     * @param value the first comparison value for the {@code operator}
     * @param value2 the second comparison value for the {@code operator}
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value,
            Object value2);

    /**
     * Return the set of {@code records} where the {@code key} field contained
     * at least one value at {@code timestamp} that satisfies the
     * {@code operator} in relation to {@code value} and {@code value2}.
     * 
     * @param key the field name
     * @param operator a valid {@link Convert#stringToOperator(String)
     *            description} of an {@link Operator} to use when comparing the
     *            specified {@code value} to those stored across the {@code key}
     *            field while determining which records are matches
     * @param value the first comparison value for the {@code operator}
     * @param value2 the second comparison value for the {@code operator}
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use when checking for matches – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp);

    /**
     * Return the set of {@code records} where the {@code key} field contained
     * at least one value at {@code timestamp} that satisfies the
     * {@code operator} in relation to the {@code value}.
     * 
     * @param key the field name
     * @param operator a valid {@link Convert#stringToOperator(String)
     *            description} of an {@link Operator} to use when comparing the
     *            specified {@code value} to those stored across the {@code key}
     *            field while determining which records are matches
     * @param value the comparison value for the {@code operator}
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use when checking for matches – created from either
     *            a {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria
     */
    public abstract Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp);

    /**
     * Return the unique record where {@code key} {@link Operator#EQUALS equals}
     * {@code value}, or throw a {@link DuplicateEntryException} if multiple
     * records match the condition. If no record matches,
     * {@link #add(String, Object) add} {@code key} as {@code value} into an new
     * record and return the id.
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only adds data if that condition isn't
     * currently satisfied.
     * </p>
     * 
     * @param key the field name
     * @param value the value that must exist in the {@code key} field of a
     *            single record for that record to match or the value that is
     *            added to the {@code key} field in a new record if no existing
     *            record matches the condition
     * @return the unique record where {@code key} = {@code value}, if one exist
     *         or the record where the {@code key} as {@code value} is added
     * @throws DuplicateEntryException
     */
    public abstract <T> long findOrAdd(String key, T value)
            throws DuplicateEntryException;

    /**
     * Return the unique record that matches the {@code criteria}, if
     * one exist or throw a {@link DuplicateEntryException} if multiple records
     * match. If no record matches, {@link #insert(Map)} the {@code data} into a
     * new record and return the id.
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * <p>
     * This method is syntactic sugar for {@link #findOrInsert(Criteria, Map)}.
     * The only difference is that this method takes a in-process
     * {@link Criteria} building sequence for convenience.
     * </p>
     * 
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired record
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
     * Return the unique record that matches the {@code criteria}, if one exist
     * or throw a {@link DuplicateEntryException} if multiple records match. If
     * no record matches, {@link #insert(Multimap)} the {@code data} into a new
     * record and return the id.
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * Each of the values in {@code data} must be a primitive or one dimensional
     * object (e.g. no nested {@link Map maps} or {@link Multimap multimaps}).
     * </p>
     * <p>
     * This method is syntactic sugar for
     * {@link #findOrInsert(Criteria, Multimap)}. The only difference is that
     * this method takes a in-process {@link Criteria} building sequence for
     * convenience.
     * </p>
     * 
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            record
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
     * Return the unique record that matches the {@code criteria}, if one exist
     * or throw a {@link DuplicateEntryException} if multiple records match. If
     * no record matches, {@link #insert(String)} the {@code json} into a new
     * record and return the id.
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * <p>
     * This method is syntactic sugar for
     * {@link #findOrInsert(Criteria, String)}. The only difference is that this
     * method takes a in-process {@link Criteria} building sequence for
     * convenience.
     * </p>
     * 
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            record
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
     * Return the unique record that matches the {@code criteria}, if one exist
     * or throw a {@link DuplicateEntryException} if multiple records match. If
     * no record matches, {@link #insert(Map)} the {@code data} into a
     * new record and return the id.
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
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired record
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
     * Return the unique record that matches the {@code criteria}, if one exist
     * or throw a {@link DuplicateEntryException} if multiple records match. If
     * no record matches, {@link #insert(Multimap)} the {@code data} into a new
     * record and return the id.
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
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired record
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
     * Return the unique record that matches the {@code criteria}, if one exist
     * or throw a {@link DuplicateEntryException} if multiple records match. If
     * no record matches, {@link #insert(String)} the {@code json} into a new
     * record and return the id.
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * 
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired record
     * @param data a JSON blob describing a single object
     * @return the unique record that matches {@code criteria}, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public abstract long findOrInsert(Criteria criteria, String json)
            throws DuplicateEntryException;

    /**
     * Return the unique record that matches the {@code ccl} filter, if one
     * exist or throw a {@link DuplicateEntryException} if multiple records
     * match. If no record matches, {@link #insert(Map)} the {@code data} into a
     * new record and return the id.
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
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
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
     * Return the unique record that matches the {@code ccl} filter, if one
     * exist or throw a {@link DuplicateEntryException} if multiple records
     * match. If no record matches, {@link #insert(Multimap)} the {@code data}
     * into a new record and return the id.
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
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
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
     * Return the unique record that matches the {@code ccl} filter, if one
     * exist or throw a {@link DuplicateEntryException} if multiple records
     * match. If no record matches, {@link #insert(String)} the {@code json}
     * into a new record and return the id.
     * <p>
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if that condition isn't
     * currently satisfied.
     * </p>
     * 
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @param json a JSON blob describing a single object
     * @return the unique record that matches {@code ccl} string, if one exist
     *         or the record where the {@code json} data is inserted
     * @throws DuplicateEntryException
     */
    public abstract long findOrInsert(String ccl, String json)
            throws DuplicateEntryException;

    /**
     * For each of the {@code keys} in each of the {@code records}, return the
     * stored value that was most recently added.
     * 
     * @param keys a collection of field names
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records);

    /**
     * For each of the {@code keys} in each of the {@code records}, return the
     * stored value that was most recently added at {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code records} to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp);

    /**
     * For each of the {@code keys} in every record that matches the
     * {@code criteria}, return the stored value that was most recently
     * added.
     * 
     * @param keys a collection of field names
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria);

    /**
     * For each of the {@code keys} in every record that matches the
     * {@code criteria}, return the stored value that was most recently
     * added at {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp);

    /**
     * For each of the {@code keys} in {@code record}, return the stored value
     * that was most recently added.
     * 
     * @param keys a collection of field names
     * @param record the record id
     * @return a {@link Map} associating each of the {@code keys} to the
     *         freshest value in the field
     */
    public abstract <T> Map<String, T> get(Collection<String> keys,
            long record);

    /**
     * For each of the {@code keys} in {@code record}, return the stored value
     * that was most recently added at {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code keys} to the
     *         freshest
     *         value in the field at {@code timestamp}
     */
    public abstract <T> Map<String, T> get(Collection<String> keys, long record,
            Timestamp timestamp);

    /**
     * For each of the {@code keys} in every record that matches the
     * {@code criteria}, return the stored value that was most recently
     * added.
     * <p>
     * This method is syntactic sugar for {@link #get(Collection, Criteria)}.
     * The only difference is that this method takes a in-process
     * {@link Criteria} building sequence for convenience.
     * </p>
     * 
     * @param keys a collection of field names
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Object criteria);

    /**
     * For each of the {@code keys} in every record that matches the
     * {@code criteria}, return the stored value that was most recently
     * added at {@code timestamp}.
     * <p>
     * This method is syntactic sugar for
     * {@link #get(Collection, Criteria, Timestamp)}. The only difference is
     * that this method takes a in-process {@link Criteria} building sequence
     * for convenience.
     * </p>
     * 
     * @param keys a collection of field names
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Object criteria, Timestamp timestamp);

    /**
     * For each of the {@code keys} in every record that matches the {@code ccl}
     * filter, return the stored value that was most recently added.
     * 
     * @param keys a collection of field names
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl);

    /**
     * For each of the {@code keys} in every record that matches the {@code ccl}
     * filter, return the stored value that was most recently added at
     * {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} to the freshest
     *         value in the field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp);

    /**
     * For every key in every record that matches the {@code criteria}, return
     * the stored value that was most recently added.
     * 
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the record's keys to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(Criteria criteria);

    /**
     * For every key in every record that matches the {@code criteria}, return
     * the stored value that was most recently added at {@code timestamp} .
     * 
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the record's keys to the freshest
     *         value in the field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp);

    /**
     * For every key in every record that matches the {@code criteria}, return
     * the stored value that was most recently added.
     * 
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the record's keys to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(Object criteria);

    /**
     * For every key in every record that matches the {@code criteria}, return
     * the stored value that was most recently added at {@code timestamp}.
     * 
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the record's keys to the freshest
     *         value in the field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, T>> get(Object criteria,
            Timestamp timestamp);

    /**
     * For every key in every record that matches the {@code ccl} filter, return
     * the stored value that was most recently added.
     * 
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the record's keys to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(String ccl);

    /**
     * For each of the {@code records}, return the stored value in the
     * {@code key} field that was most recently added.
     * 
     * @param key the field name
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to the
     *         freshest value in the {@code key} field
     */
    public abstract <T> Map<Long, T> get(String key, Collection<Long> records);

    /**
     * For each of the {@code records}, return the stored value in the
     * {@code key} field that was most recently added at {@code timestamp}
     * 
     * @param key the field name
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code records} to the
     *         freshest value in the {@code key} field at {@code timestamp}
     */
    public abstract <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp);

    /**
     * For every record that matches the {@code criteria}, return the stored
     * value in the {@code key} field that was most recently added.
     * 
     * @param key the field name
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @return a {@link Map} associating each of the matching records to the
     *         freshest value in the {@code key} field
     */
    public abstract <T> Map<Long, T> get(String key, Criteria criteria);

    /**
     * For every record that matches the {@code criteria}, return the
     * stored value in the {@code key} field that was most recently added at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to the
     *         freshest value in the {@code key} field
     */
    public abstract <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp);

    /**
     * Return the stored value that was most recently added for {@code key} in
     * {@code record}. If the field is empty, return {@code null}.
     * 
     * @param key the field name
     * @param record the record id
     * @return the freshest value in the field
     */
    @Nullable
    public abstract <T> T get(String key, long record);

    /**
     * Return the stored value that was most recently added for {@code key} in
     * {@code record} at {@code timestamp}. If the field was empty at
     * {@code timestamp}, return {@code null}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the freshest value in the field at {@code timestamp}
     */
    @Nullable
    public abstract <T> T get(String key, long record, Timestamp timestamp);

    /**
     * For every record that matches the {@code criteria}, return the stored
     * value in the {@code key} field that was most recently added.
     * <p>
     * This method is syntactic sugar for {@link #get(String, Criteria)}. The
     * only difference is that this method takes a in-process {@link Criteria}
     * building sequence for convenience.
     * </p>
     * 
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the record's keys to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, T> get(String key, Object criteria);

    /**
     * For every record that matches the {@code criteria}, return the
     * stored value in the {@code key} field that was most recently added at
     * {@code timestamp}.
     * <p>
     * This method is syntactic sugar for
     * {@link #get(String, Criteria, Timestamp)}. The only difference is that
     * this method takes a in-process {@link Criteria} building sequence for
     * convenience.
     * </p>
     * 
     * @param key the field name
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to the
     *         freshest value in the {@code key} field
     */
    public abstract <T> Map<Long, T> get(String key, Object criteria,
            Timestamp timestamp);

    /**
     * For every record that matches the {@code ccl} filter, return the
     * stored value in the {@code key} field that was most recently added.
     * <p>
     * This method is syntactic sugar for {@link #get(String, Criteria)}. The
     * only difference is that this method takes a in-process {@link Criteria}
     * building sequence for convenience.
     * </p>
     * 
     * @param key the field name
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @return a {@link Map} associating each of the matching records to the
     *         freshest value in the {@code key} field
     */
    public abstract <T> Map<Long, T> get(String key, String ccl);

    /**
     * For every record that matches the {@code ccl} filter, return the
     * stored value in the {@code key} field that was most recently added at
     * {@code timestamp}.
     * <p>
     * This method is syntactic sugar for
     * {@link #get(String, Criteria, Timestamp)}. The only difference is that
     * this method takes a in-process {@link Criteria} building sequence for
     * convenience.
     * </p>
     * 
     * @param key the field name
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to the
     *         freshest value in the {@code key} field at {@code timestamp}
     */
    public abstract <T> Map<Long, T> get(String key, String ccl,
            Timestamp timestamp);

    /**
     * For every key in every record that matches the {@code ccl} filter,
     * return the stored value that was most recently added.
     * 
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the record's keys to the freshest
     *         value in the field
     */
    public abstract <T> Map<Long, Map<String, T>> get(String ccl,
            Timestamp timestamp);

    /**
     * Return the name of the connected environment.
     * 
     * @return the server environment to which this client is connected
     */
    public abstract String getServerEnvironment();

    /**
     * Return the version of the connected server.
     * 
     * @return the server version
     */
    public abstract String getServerVersion();

    /**
     * Atomically insert the key/value associations from each of the
     * {@link Multimap maps} in {@code data} into new and distinct records.
     * <p>
     * Each of the values in each map in {@code data} must be a primitive or one
     * dimensional object (e.g. no nested {@link Map maps} or {@link Multimap
     * multimaps}).
     * </p>
     * 
     * @param data a {@link List} of {@link Multimap maps}, each with key/value
     *            associations to insert into a new record
     * @return a {@link Set} containing the ids of the new records where the
     *         maps in {@code data} were inserted, respectively
     */
    public final Set<Long> insert(Collection<Multimap<String, Object>> data) {
        String json = Convert.mapsToJson(data);
        return insert(json);
    }

    /**
     * Atomically insert the key/value associations from {@link Map data} into a
     * new record.
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
     * Atomically insert the key/value associations from {@link Map data} into
     * each of the {@code records}, if possible.
     * <p>
     * An insert will fail for a given record if any of the key/value
     * associations in {@code data} currently exist in that record (e.g.
     * {@link #add(String, Object, long) adding} the key/value association would
     * fail).
     * </p>
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
     *         indicates if the {@code data} was successfully inserted in that
     *         record
     */
    public final Map<Long, Boolean> insert(Map<String, Object> data,
            Collection<Long> records) {
        String json = Convert.mapToJson(data);
        return insert(json, records);

    }

    /**
     * Atomically insert the key/value associations from {@link Map data} into
     * {@code record}, if possible.
     * <p>
     * The insert will fail if any of the key/value associations in {@code data}
     * currently exist in {@code record} (e.g.
     * {@link #add(String, Object, long) adding} the key/value association would
     * fail).
     * </p>
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
     * Atomically insert the key/value associations from {@code Multimap data}
     * into a new record.
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
     * Atomically insert the key/value associations from {@code Multimap data}
     * into each of the {@code records}, if possible.
     * <p>
     * An insert will fail for a given record if any of the key/value
     * associations in {@code data} currently exist in that record (e.g.
     * {@link #add(String, Object, long) adding} the key/value association would
     * fail).
     * </p>
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
     *         indicates if the {@code data} was successfully inserted in that
     *         record
     */
    public final Map<Long, Boolean> insert(Multimap<String, Object> data,
            Collection<Long> records) {
        String json = Convert.mapToJson(data);
        return insert(json, records);
    }

    /**
     * Atomically insert the key/value associations in {@link Multimap data}
     * into {@code record}, if possible.
     * <p>
     * The insert will fail if any of the key/value associations in {@code data}
     * currently exist in {@code record} (e.g.
     * {@link #add(String, Object, long) adding} the key/value association would
     * fail).
     * </p>
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
     * Atomically insert the key/value associations from the {@code json} string
     * into as many new records as necessary.
     * <p>
     * If the {@code json} string contains a top-level array (of objects), this
     * method will insert each of the objects in a new and distinct record. The
     * {@link Set} that is returned will contain the ids of all those records.
     * On the other hand, if the {@code json} string contains a single top-level
     * object, this method will insert that object in a single new record. The
     * {@link Set} that is returned will only contain the id of that record.
     * </p>
     * <p>
     * Regardless of whether the top-level element is an object or an array,
     * each object in the {@code json} string contains one or more keys, each of
     * which maps to a JSON primitive or an array of JSON primitives (e.g. no
     * nested objects or arrays).
     * </p>
     * 
     * @param json a valid json string with either a top-level object or array
     * @return a {@link Set} that contains one or more records ids where the
     *         objects in {@code json} are inserted, respectively
     */
    public abstract Set<Long> insert(String json);

    /**
     * Atomically insert the key/value associations from the {@code json} object
     * into each of the {@code records}, if possible.
     * <p>
     * An insert will fail for a given record if any of the key/value
     * associations in the {@code json} object currently exist in that record
     * (e.g. {@link #add(String, Object, long) adding} the key/value association
     * would fail).
     * </p>
     * <p>
     * The {@code json} must contain a top-level object that contains one or
     * more keys, each of which maps to a JSON primitive or an array of JSON
     * primitives (e.g. no nested objects or arrays).
     * </p>
     * 
     * @param json a valid json string containing a top-level object
     * @param records a collection of record ids
     * @return a {@link Map} associating each record id to a boolean that
     *         indicates if the {@code json} was successfully inserted in that
     *         record
     */
    public abstract Map<Long, Boolean> insert(String json,
            Collection<Long> records);

    /**
     * Atomically insert the key/value associations from the {@code json} object
     * into {@code record}, if possible.
     * <p>
     * The insert will fail if any of the key/value associations in the
     * {@code json} object currently exist in {@code record} (e.g.
     * {@link #add(String, Object, long)
     * adding} the key/value association would fail).
     * </p>
     * <p>
     * The {@code json} must contain a JSON object that contains one or more
     * keys, each of which maps to a JSON primitive or an array of JSON
     * primitives.
     * </p>
     * 
     * @param json json a valid json string containing a top-level object
     * @param record the record id
     * @return {@code true} if the {@code json} is inserted into {@code record}
     */
    public abstract boolean insert(String json, long record);

    /**
     * Return all the records that have current or historical data.
     * 
     * @return a {@link Set} containing the ids of records that have current or
     *         historical data
     */
    public abstract Set<Long> inventory();

    /**
     * Invoke {@code method} using {@code args} within the plugin identified by
     * {@code id}.
     * 
     * <p>
     * There must be a class named {@code id} available in Concourse Server via
     * a plugin distribution. The {@code method} must also be accessible within
     * the class.
     * </p>
     * <p>
     * If the plugin throws any {@link Exception}, it'll be re-thrown here as a
     * {@link RuntimeException}.
     * </p>
     * 
     * @param id the fully qualified name of the plugin class (e.g.
     *            com.cinchapi.plugin.PluginClass)
     * @param method the name of the method within the {@code pluginClass}
     * @param args the arguments to pass to the {@code method}
     * @return the result returned from the plugin
     */
    public abstract <T> T invokePlugin(String id, String method,
            Object... args);

    /**
     * Atomically dump the data in each of the {@code records} as a JSON array
     * of objects.
     * 
     * @param records a collection of record ids
     * @return a JSON array of objects, each of which contains the data in the
     *         one of the {@code records}, respectively
     */
    public abstract String jsonify(Collection<Long> records);

    /**
     * Atomically dump the data in each of the {@code records} as a JSON array
     * of objects and optionally include a special {@code identifier} key that
     * contains the record id for each of the dumped objects.
     * 
     * @param records a collection of record ids
     * @param identifier a boolean that indicates whether to include a special
     *            key ({@link Constants#JSON_RESERVED_IDENTIFIER_NAME}) that
     *            maps to the record id in each of the dumped objects
     * @return a JSON array of objects, each of which contains the data in the
     *         one of the {@code records}, respectively
     */
    public abstract String jsonify(Collection<Long> records,
            boolean identifier);

    /**
     * Atomically dump the data in each of the {@code records} at
     * {@code timestamp} as a JSON array of objects.
     * 
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a JSON array of objects, each of which contains the data in the
     *         one of the {@code records} at {@code timestamp}, respectively
     */
    public abstract String jsonify(Collection<Long> records,
            Timestamp timestamp);

    /**
     * Atomically dump the data in each of the {@code records} at
     * {@code timestamp} as a JSON array of objects and optionally include a
     * special {@code identifier} key that contains the record id for each of
     * the dumped objects.
     * 
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @param identifier a boolean that indicates whether to include a special
     *            key ({@link Constants#JSON_RESERVED_IDENTIFIER_NAME}) that
     *            maps to the record id in each of the dumped objects
     * @return a JSON array of objects, each of which contains the data in the
     *         one of the {@code records} at {@code timestamp}, respectively
     */
    public abstract String jsonify(Collection<Long> records,
            Timestamp timestamp, boolean identifier);

    /**
     * Atomically dump all the data in {@code record} as a JSON object.
     * 
     * @param record the record id
     * @return a JSON object that contains all the data in {@code record}
     */
    public abstract String jsonify(long record);

    /**
     * Atomically dump all the data in {@code record} as a JSON object and
     * optionally include a special {@code identifier} key that contains the
     * record id.
     * 
     * @param record the record id
     * @param identifier a boolean that indicates whether to include a special
     *            key ({@link Constants#JSON_RESERVED_IDENTIFIER_NAME}) that
     *            maps to the record id in each of the dumped objects
     * @return a JSON object that contains all the data in {@code record}
     */
    public abstract String jsonify(long record, boolean identifier);

    /**
     * Atomically dump all the data in {@code record} at {@code timestamp} as a
     * JSON object.
     * 
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a JSON object that contains all the data in {@code record} at
     *         {@code timestamp}
     */
    public abstract String jsonify(long record, Timestamp timestamp);

    /**
     * Atomically dump all the data in {@code record} at {@code timestamp} as a
     * JSON object and optionally include a special {@code identifier} key that
     * contains the record id.
     * 
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @param identifier a boolean that indicates whether to include a special
     *            key ({@link Constants#JSON_RESERVED_IDENTIFIER_NAME}) that
     *            maps to the record id in each of the dumped objects
     * @return a JSON object that contains all the data in {@code record} at
     *         {@code timestamp}
     */
    public abstract String jsonify(long record, Timestamp timestamp,
            boolean identifier);

    /**
     * Append links from {@code key} in {@code source} to each of the
     * {@code destinations}.
     * 
     * @param key the field name
     * @param destinations a collection of ids for the records where each of the
     *            links points, respectively
     * @param source the id of the record where each of the links originate
     * @return a {@link Map} associating the ids for each of the
     *         {@code destinations} to a boolean that indicates whether the link
     *         was successfully added
     */
    public abstract Map<Long, Boolean> link(String key,
            Collection<Long> destinations, long source);

    /**
     * Append a link from {@code key} in {@code source} to {@code destination}.
     * 
     * @param key the field name
     * @param destination the id of the record where the link points
     * @param source the id of the record where the link originates
     * @return {@code true} if the link is added
     */
    public abstract boolean link(String key, long destination, long source);

    /**
     * Atomically check to see if each of the {@code records} currently contains
     * any data.
     * 
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to a
     *         boolean that indicates whether that record currently contains any
     *         data.
     */
    public abstract Map<Long, Boolean> ping(Collection<Long> records);

    /**
     * Check to see if {@code record} currently contains any data.
     * 
     * @param record the record id
     * @return {@code true} if {@code record} currently contains any data,
     *         otherwise {@code false}
     */
    public abstract boolean ping(long record);

    /**
     * Make the necessary changes to the data stored for {@code key} in
     * {@code record} so that it contains the exact same {@code values} as the
     * specified collection.
     * 
     * @param key the field name
     * @param record the record id
     * @param values the collection of values that should be exactly what is
     *            contained in the field after this method executes
     */
    @Incubating
    public abstract <T> void reconcile(String key, long record,
            Collection<T> values);

    /**
     * Make the necessary changes to the data stored for {@code key} in
     * {@code record} so that it contains the exact same {@code values} as the
     * specified array.
     * 
     * @param key the field name
     * @param record the record id
     * @param values the array of values that should be exactly what is
     *            contained in the field after this method executes
     */
    @SuppressWarnings("unchecked")
    @Incubating
    public final <T> void reconcile(String key, long record, T... values) {
        reconcile(key, record, Sets.newHashSet(values));
    }

    /**
     * Atomically remove {@code key} as {@code value} from each of the
     * {@code records} where it currently exists.
     * 
     * @param key the field name
     * @param value the value to remove
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to a
     *         boolean that indicates whether the data was removed
     */
    public abstract <T> Map<Long, Boolean> remove(String key, T value,
            Collection<Long> records);

    /**
     * Remove {@code key} as {@code value} from {@code record} if it currently
     * exists.
     * 
     * @param key the field name
     * @param value the value to remove
     * @param record the record id
     * @return {@code true} if the data is removed
     */
    public abstract <T> boolean remove(String key, T value, long record);

    /**
     * Atomically revert each of the {@code keys} in each of the {@code records}
     * to their state at {@code timestamp} by creating new revisions that undo
     * the net changes that have occurred since {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     */
    public abstract void revert(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp);

    /**
     * Atomically revert each of the {@code keys} in {@code record} to their
     * state at {@code timestamp} by creating new revisions that undo the net
     * changes that have occurred since {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     */
    public abstract void revert(Collection<String> keys, long record,
            Timestamp timestamp);

    /**
     * Atomically revert {@code key} in each of the {@code records} to its state
     * at {@code timestamp} by creating new revisions that undo the net
     * changes that have occurred since {@code timestamp}.
     * 
     * @param key the field name
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     */
    public abstract void revert(String key, Collection<Long> records,
            Timestamp timestamp);

    /**
     * Atomically revert {@code key} in {@code record} to its state at
     * {@code timestamp} by creating new revisions that undo the net
     * changes that have occurred since {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     */
    public abstract void revert(String key, long record, Timestamp timestamp);

    /**
     * Perform a full text search for {@code query} against the {@code key}
     * field and return the records that contain a {@link String} or {@link Tag}
     * value that matches.
     * 
     * @param key
     * @param query
     * @return a {@link Set} of ids for records that match the search query
     */
    public abstract Set<Long> search(String key, String query);

    /**
     * Return all the data that is currently stored in each of the
     * {@code records}.
     * 
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to another
     *         {@link Map} associating every key in that record to a {@link Set}
     *         containing all the values stored in the respective field
     */
    public abstract Map<Long, Map<String, Set<Object>>> select(
            Collection<Long> records);

    /**
     * Return all the data that was stored in each of the {@code records} at
     * {@code timestamp}.
     * 
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code records} to another
     *         {@link Map} associating every key in that record at
     *         {@code timestamp} to a {@link Set} containing all the values
     *         stored in the respective field at {@code timestamp}
     */
    public abstract Map<Long, Map<String, Set<Object>>> select(
            Collection<Long> records, Timestamp timestamp);

    /**
     * Return all the values stored for each of the {@code keys} in each of the
     * {@code records}.
     * 
     * @param keys a collection of field names
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to another
     *         {@link Map} associating each of the {@code keys} to a {@link Set}
     *         containing all the values stored in the respective field
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Collection<Long> records);

    /**
     * Return all the values stored for each of the {@code keys} in each of the
     * {@code records} at {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code records} to another
     *         {@link Map} associating each of the {@code keys} to a {@link Set}
     *         containing all the values stored in the respective field at
     *         {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Collection<Long> records,
            Timestamp timestamp);

    /**
     * Return all the values stored for each of the {@code keys} in every record
     * that matches the {@code criteria}.
     * 
     * @param keys a collection of field names
     * @param criteria a {@link Criteria} that contains a
     *            well-formed filter for the desired records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Criteria criteria);

    /**
     * Return all the values stored for each of the {@code keys} at
     * {@code timestamp} in every record that matches the {@code criteria}
     * 
     * @param keys a collection of field names
     * @param criteria a {@link Criteria} that contains a
     *            well-formed filter for the desired records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Criteria criteria, Timestamp timestamp);

    /**
     * Return all the values stored for each of the {@code keys} in
     * {@code record}.
     * 
     * @param keys a collection of field names
     * @param record the record id
     * @return a {@link Map} associating each of the {@code keys} to a
     *         {@link Set} containing all the values stored in the respective
     *         field
     */
    public abstract <T> Map<String, Set<T>> select(Collection<String> keys,
            long record);

    /**
     * Return all the values stored for each of the {@code keys} in
     * {@code record} at {@code timestamp}.
     * 
     * @param keys a collection of field names
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code keys} to a
     *         {@link Set} containing all the values stored in the respective
     *         field at {@code timestamp}
     */
    public abstract <T> Map<String, Set<T>> select(Collection<String> keys,
            long record, Timestamp timestamp);

    /**
     * Return all the values stored for each of the {@code keys} in every record
     * that matches the {@code criteria}.
     * <p>
     * This method is syntactic sugar for {@link #select(Collection, Criteria)}.
     * The only difference is that this method takes a in-process
     * {@link Criteria} building sequence for convenience.
     * </p>
     * 
     * @param keys a collection of field names
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Object criteria);

    /**
     * Return all the values stored for each of the {@code keys} at
     * {@code timestamp} in every record that matches the {@code criteria}.
     * <p>
     * This method is syntactic sugar for
     * {@link #select(Collection, Criteria, Timestamp)}. The only difference is
     * that this method takes a in-process {@link Criteria} building sequence
     * for convenience.
     * </p>
     * 
     * @param keys a collection of field names
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, Object criteria, Timestamp timestamp);

    /**
     * Return all the values stored for each of the {@code keys} in every record
     * that matches the {@code ccl} filter.
     * 
     * @param keys a collection of field names
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, String ccl);

    /**
     * Return all the values stored for each of the {@code keys} at
     * {@code timestamp} in every record that matches the {@code ccl} filter.
     * 
     * @param keys a collection of field names
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Collection<String> keys, String ccl, Timestamp timestamp);

    /**
     * Return all the data from every record that matches {@code criteria}.
     * 
     * @param keys a collection of field names
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(
            Criteria criteria);

    /**
     * Return all the data at {@code timestamp} from every record that
     * matches the {@code criteria}.
     * 
     * @param keys a collection of field names
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp);

    /**
     * Return all the data from {@code record}.
     * 
     * @param record the record id
     * @return a {@link Map} associating each key in {@code record} to a
     *         {@link Set} containing all the values stored in the respective
     *         field
     */
    public abstract Map<String, Set<Object>> select(long record);

    /**
     * Return all the data from {@code record} at {@code timestamp}.
     * 
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each key in {@code record} to a
     *         {@link Set} containing all the values stored in the respective
     *         field at {@code timestamp}
     */
    public abstract Map<String, Set<Object>> select(long record,
            Timestamp timestamp);

    /**
     * Return all the data from every record that matches {@code criteria}.
     * <p>
     * This method is syntactic sugar for {@link #select(Criteria)}. The only
     * difference is that this method takes a in-process {@link Criteria}
     * building sequence for convenience.
     * </p>
     * 
     * @param keys a collection of field names
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(Object criteria);

    /**
     * Return all the data at {@code timestamp} from every record that
     * matches the {@code criteria}.
     * <p>
     * This method is syntactic sugar for {@link #select(Criteria, Timestamp)}.
     * The only difference is that this method takes a in-process
     * {@link Criteria} building sequence for convenience.
     * </p>
     * 
     * @param keys a collection of field names
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(Object criteria,
            Timestamp timestamp);

    /**
     * Return all the data from every record that matches {@code ccl} filter.
     * 
     * @param keys a collection of field names
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(String ccl);

    /**
     * Return all values stored for {@code key} in each of the {@code records}.
     * 
     * @param key the field name
     * @param records a collection of record ids
     * @return a {@link Map} associating each of the {@code records} to a
     *         {@link Set} containing all the values stored in the respective
     *         field
     */
    public abstract <T> Map<Long, Set<T>> select(String key,
            Collection<Long> records);

    /**
     * Return all values stored for {@code key} in each of the {@code records}
     * at {@code timestamp}.
     * 
     * @param key the field name
     * @param records a collection of record ids
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the {@code records} to a
     *         {@link Set} containing all the values stored in the respective
     *         field at {@code timestamp}
     */
    public abstract <T> Map<Long, Set<T>> select(String key,
            Collection<Long> records, Timestamp timestamp);

    /**
     * Return all the values stored for {@code key} in every record that
     * matches the {@code criteria}.
     * 
     * @param key the field name
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @return a {@link Map} associating each of the matching records to a
     *         {@link Set} containing all the values stored in the respective
     *         field
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Criteria criteria);

    /**
     * Return all the values stored for {@code key} at {@code timestamp} in
     * every record that matches the {@code criteria}.
     * 
     * @param key the field name
     * @param criteria a {@link Criteria} that contains a well-formed filter for
     *            the desired records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to a
     *         {@link Set} containing all the values stored in the respective
     *         field at {@code timestamp}
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp);

    /**
     * Return all the values stored for {@code key} in {@code record}.
     * 
     * @param key the field name
     * @param record the record id
     * @return a {@link Set} containing all the values stored in the field
     */
    public abstract <T> Set<T> select(String key, long record);

    /**
     * Return all the values stored for {@code key} in {@code record} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Set} containing all the values stored in the field at
     *         {@code timestamp}
     */
    public abstract <T> Set<T> select(String key, long record,
            Timestamp timestamp);

    /**
     * Return all the values stored for {@code key} in every record that
     * matches the {@code criteria}.
     * <p>
     * This method is syntactic sugar for {@link #select(String, Criteria)}. The
     * only difference is that this method takes a in-process {@link Criteria}
     * building sequence for convenience.
     * </p>
     * 
     * @param key the field name
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @return a {@link Map} associating each of the matching records to a
     *         {@link Set} containing all the values stored in the respective
     *         field
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Object criteria);

    /**
     * Return all the values stored for {@code key} at {@code timestamp} in
     * every record that matches the {@code criteria}.
     * <p>
     * This method is syntactic sugar for
     * {@link #select(String, Criteria, Timestamp)}. The only difference is that
     * this method takes a in-process {@link Criteria} building sequence for
     * convenience.
     * </p>
     * 
     * @param key the field name
     * @param criteria an in-process {@link Criteria} building sequence that
     *            contains an {@link BuildableState#build() unfinalized},
     *            but well-formed filter for the desired
     *            records
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to a
     *         {@link Set} containing all the values stored in the respective
     *         field at {@code timestamp}
     */
    public abstract <T> Map<Long, Set<T>> select(String key, Object criteria,
            Timestamp timestamp);

    /**
     * Return all the values stored for {@code key} in every record that
     * matches the {@code ccl} filter.
     * 
     * @param key the field name
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @return a {@link Map} associating each of the the matching records to a
     *         {@link Set} containing all the values stored in the respective
     *         field
     */
    public abstract <T> Map<Long, Set<T>> select(String key, String ccl);

    /**
     * Return all the values stored for {@code key} at {@code timestamp} in
     * every record that matches the {@code ccl} filter.
     * 
     * @param key the field name
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to a
     *         {@link Set} containing all the values stored in the respective
     *         field at {@code timestamp}
     */
    public abstract <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp);

    /**
     * Return all the data at {@code timestamp} from every record that
     * matches the {@code ccl} filter.
     * 
     * @param keys a collection of field names
     * @param ccl a well-formed criteria expressed using the Concourse Criteria
     *            Language
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return a {@link Map} associating each of the matching records to another
     *         {@link Map} associating each of the {@code keys} in that record
     *         to a {@link Set} containing all the values stored in the
     *         respective field at {@code timestamp}
     */
    public abstract <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp);

    /**
     * In each of the {@code records}, atomically remove all the values stored
     * for {@code key} and then add {@code key} as {@code value} in the
     * respective record.
     * 
     * @param key the field name
     * @param value the value to set
     * @param records a collection of record ids
     */
    public abstract void set(String key, Object value,
            Collection<Long> records);

    /**
     * Atomically remove all the values stored for {@code key} in {@code record}
     * and add then {@code key} as {@code value}.
     * 
     * @param key the field name
     * @param value the value to set
     * @param record the record id
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
     * @param task a {@link Runnable} that contains the group of operations to
     *            execute in the transaction
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
     * Return a {@link Timestamp} that represents the current instant according
     * to the server.
     * 
     * @return the current time
     */
    public abstract Timestamp time();

    /**
     * Return a {@link Timestamp} that corresponds to the specified number of
     * {@code micros}econds since the Unix epoch.
     * 
     * @param micros the number of microseconds since the unix epoch
     * @return the {@link Timestamp} that represents the desired instant
     */
    public final Timestamp time(long micros) {
        return Timestamp.fromMicros(micros);
    }

    /**
     * Return the {@link Timestamp} that corresponds to the specified number of
     * {@code micros}econds since the Unix epoch.
     * 
     * @param micros the number of microseconds since the unix epoch
     * @return the {@link Timestamp} that represents the desired instant
     */
    public final Timestamp time(Number micros) {
        return time(micros.longValue());
    }

    /**
     * Return the {@link Timestamp}, according to the server, that corresponds
     * to the instant described by the {@code phrase}.
     * 
     * @param phrase a natural language description of a point in time.
     * @return the {@link Timestamp} that represents the desired instant
     */
    public abstract Timestamp time(String phrase);

    /**
     * If it exists, remove the link from {@code key} in {@code source} to
     * {@code destination}.
     * 
     * @param key the field name
     * @param destination the id of the record where the link points
     * @param source the id of the record where the link originates
     * @return {@code true} if the link is removed
     */
    public abstract boolean unlink(String key, long destination, long source);

    /**
     * Return {@code true} if {@code value} is stored for {@code key} in
     * {@code record}.
     * 
     * @param key the field name
     * @param value the value to check
     * @param record the record id
     * @return {@code true} if {@code value} is stored in the field, otherwise
     *         {@code false}
     */
    public abstract boolean verify(String key, Object value, long record);

    /**
     * Return {@code true} if {@code value} was stored for {@code key} in
     * {@code record} at {@code timestamp}.
     * 
     * @param key the field name
     * @param value the value to check
     * @param record the record id
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return {@code true} if {@code value} is stored in the field, otherwise
     *         {@code false}
     */
    public abstract boolean verify(String key, Object value, long record,
            Timestamp timestamp);

    /**
     * Atomically replace {@code expected} with {@code replacement} for
     * {@code key} in {@code record} if and only if {@code expected} is
     * currently stored in the field.
     * 
     * @param key the field name
     * @param expected the value expected to currently exist in the field
     * @param record the record id
     * @param replacement the value with which to replace {@code expected} if
     *            and only if it currently exists in the field
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
     * If you want to add a new value only if it does not exist while also
     * preserving other values, you should use the
     * {@link #add(String, Object, long)} method instead.
     * </p>
     * 
     * @param key the field name
     * @param value the value to check
     * @param record the record id
     */
    public abstract void verifyOrSet(String key, Object value, long record);

    /**
     * Return a new {@link Concourse} connection that is connected to the same
     * deployment with the same credentials as this connection.
     * 
     * @return a copy of this connection handle
     */
    protected abstract Concourse copyConnection();
}