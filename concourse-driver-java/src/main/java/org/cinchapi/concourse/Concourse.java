/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse;

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
import org.cinchapi.concourse.annotate.CompoundOperation;
import org.cinchapi.concourse.config.ConcourseConfiguration;
import org.cinchapi.concourse.lang.BuildableState;
import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.lang.Language;
import org.cinchapi.concourse.security.ClientSecurity;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.TSecurityException;
import org.cinchapi.concourse.thrift.TTransactionException;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Collections;
import org.cinchapi.concourse.util.Conversions;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.PrettyLinkedTableMap;
import org.cinchapi.concourse.util.Transformers;
import org.cinchapi.concourse.util.PrettyLinkedHashMap;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * <p>
 * ConcourseDB is a self-tuning database that practically runs itself. Concourse
 * offers features like automatic indexing, version control and distributed ACID
 * transactions to provide a more efficient approach to data management that is
 * easy to deploy, access and scale while maintaining the strong consistency of
 * traditional database systems
 * </p>
 * <h2>Data Model</h2>
 * <p>
 * The Concourse data model is lightweight and flexible. Unlike other databases,
 * Concourse is completely schemaless and does not hold data in tables or
 * collections. Instead, Concourse is simply a distributed document-graph. Each
 * record/document has multiple keys. And each key has one or more distinct
 * values. Like any graph, you can link records to one another. And the
 * structure of one record does not affect the structure of another.
 * </p>
 * <p>
 * <ul>
 * <li><strong>Record</strong> &mdash; A logical grouping of data about a single
 * person, place, or thing (i.e. an object). Each {@code record} is a collection
 * of key/value pairs that are together identified by a unique
 * <em>primary key</em>.
 * <li><strong>Key</strong> &mdash; An attribute that maps to a set of
 * <em>one or more</em> distinct {@code values}. A {@code record} can have many
 * different {@code keys}, and the {@code keys} in one {@code record} do not
 * affect those in another {@code record}.
 * <li><strong>Value</strong> &mdash; A dynamically typed quantity that is
 * associated with a {@code key} in a {@code record}.
 * </ul>
 * </p>
 * <h4>Data Types</h4>
 * <p>
 * Concourse natively stores most of the Java primitives: boolean, double,
 * float, integer, long, and string (UTF-8). Otherwise, the value of the
 * {@link #toString()} method for the Object is stored.
 * </p>
 * <h4>Links</h4>
 * <p>
 * Concourse supports linking a {@code key} in one {@code record} to another
 * {@code record}. Links are one-directional, but it is possible to add two
 * links that are the inverse of each other to simulate bi-directionality (i.e.
 * link "friend" in Record 1 to Record 2 and link "friend" in Record 2 to Record
 * 1).
 * </p>
 * <h2>Transactions</h2>
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
     * Create a new connection to the environment of the Concourse Server
     * described in {@code concourse_client.prefs} (or, if the file does
     * not exist, the default environment of the server at localhost:1717) and
     * return a handle to facilitate interaction.
     * 
     * @return the handle
     */
    public static Concourse connect() {
        return new Client();
    }

    /**
     * Create a new connection to the specified {@code environment} of the
     * Concourse Server described in {@code concourse_client.prefs} (or, if the
     * file does not exist, the server at localhost:1717) and return a handle
     * to facilitate interaction.
     * 
     * @param environment
     * @return the handle
     */
    public static Concourse connect(String environment) {
        return new Client(environment);
    }

    /**
     * Create a new connection to the default environment of the specified
     * Concourse Server and return a handle to facilitate interaction.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
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
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @return the handle
     */
    public static Concourse connect(String host, int port, String username,
            String password, String environment) {
        return new Client(host, port, username, password, environment);
    }

    /**
     * Discard any changes that are currently staged for commit.
     * <p>
     * After this function returns, Concourse will return to {@code autocommit}
     * mode and all subsequent changes will be committed immediately.
     * </p>
     */
    public abstract void abort();

    /**
     * Add {@code key} as {@code value} in each of the {@code records} if it is
     * not already contained.
     * 
     * @param key
     * @param value
     * @param records
     * @return a mapping from each record to a boolean indicating if
     *         {@code value} is added
     */
    @CompoundOperation
    public abstract Map<Long, Boolean> add(String key, Object value,
            Collection<Long> records);

    /**
     * Add {@code key} as {@code value} in a new record and return the primary
     * key.
     * 
     * @param key
     * @param value
     * @return the primary key of the record in which the data was added
     */
    public abstract <T> long add(String key, T value);

    /**
     * Add {@code key} as {@code value} to {@code record} if it is not already
     * contained.
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if {@code value} is added
     */
    public abstract <T> boolean add(String key, T value, long record);

    /**
     * Audit {@code record} and return a log of revisions.
     * 
     * @param record
     * @return a mapping from timestamp to a description of a revision
     */
    public abstract Map<Timestamp, String> audit(long record);

    /**
     * Audit {@code record} and return a log of revisions.
     *
     * @param record
     * @param start
     * @return a mapping from timestamp to a description of a revision
     */
    public abstract Map<Timestamp, String> audit(long record, Timestamp start);

    /**
     * Audit {@code record} and return a log of revisions.
     *
     * @param record
     * @param start
     * @param end
     * @return a mapping from timestamp to a description of a revision
     */
    public abstract Map<Timestamp, String> audit(long record, Timestamp start,
            Timestamp end);

    /**
     * Audit {@code key} in {@code record} and return a log of revisions.
     * 
     * @param key
     * @param record
     * @return a mapping from timestamp to a description of a revision
     */
    public abstract Map<Timestamp, String> audit(String key, long record);

    /**
     * Audit {@code record} and return a log of revisions.
     * 
     * @param key
     * @param record
     * @param start
     * @return a mapping from timestamp to a description of a revision
     */
    public abstract Map<Timestamp, String> audit(String key, long record,
            Timestamp start);

    /**
     * Audit {@code record} and return a log of revisions.
     * 
     * @param key
     * @param record
     * @param start
     * @param end
     * @return a mapping from timestamp to a description of a revision
     */
    public abstract Map<Timestamp, String> audit(String key, long record,
            Timestamp start, Timestamp end);

    /**
     * Browse all of the {@code keys} and return all the data that is indexed as
     * a mapping from value to the set of records containing the value for each
     * {@code key}.
     * 
     * @param keys
     * @return a mapping of all the indexed values and their associated records.
     */
    public abstract Map<String, Map<Object, Set<Long>>> browse(
            Collection<String> keys);

    /**
     * Browse all of the {@code keys} at {@code timestamp} and return all the
     * data that was indexed as a mapping from value to the set of records
     * containing the value for each {@code key}.
     * 
     * @param keys
     * @param timestamp
     * @return a mapping of all the indexed values and their associated records.
     */
    public abstract Map<String, Map<Object, Set<Long>>> browse(
            Collection<String> keys, Timestamp timestamp);

    /**
     * Browse {@code key} and return all the data that is indexed as a mapping
     * from value to the set of records containing the value for {@code key}.
     * 
     * @param key
     * @return a mapping of all the indexed values and their associated records.
     */
    public abstract Map<Object, Set<Long>> browse(String key);

    /**
     * Browse {@code key} at {@code timestamp} and return all the data that was
     * indexed as a mapping from value to the set of records that contained the
     * value for {@code key} .
     * 
     * @param key
     * @param timestamp
     * @return a mapping of all the indexed values and their associated records.
     */
    public abstract Map<Object, Set<Long>> browse(String key,
            Timestamp timestamp);

    /**
     * Chronologize non-empty sets of values in {@code key} from {@code record}
     * and return a mapping from each timestamp to the non-empty set of values.
     * 
     * @param key
     * @param record
     * @return a chronological mapping from each timestamp to the set of values
     *         that were contained for the key in record
     */
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record);

    /**
     * Chronologize non-empty sets of values in {@code key} from {@code record}
     * from {@code start} timestamp inclusively to present and return a mapping
     * from each timestamp to the non-emtpy set of values.
     * 
     * @param key
     * @param record
     * @param start
     * @return a chronological mapping from each timestamp to the set of values
     *         that were contained for the key in record from specified start
     *         timestamp to present
     */
    @CompoundOperation
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record, Timestamp start);

    /**
     * Chronologize non-empty sets of values in {@code key} from {@code record}
     * from {@code start} timestamp inclusively to {@code end} timestamp
     * exclusively and return a mapping from each timestamp to the non-empty set
     * of values.
     * 
     * @param key
     * @param record
     * @param start
     * @param end
     * @return a chronological mapping from each timestamp to the set of values
     *         that were contained for the key in record from specified start
     *         timestamp to specified end timestamp
     */
    @CompoundOperation
    public abstract Map<Timestamp, Set<Object>> chronologize(String key,
            long record, Timestamp start, Timestamp end);

    /**
     * Clear every {@code key} and contained value in each of the
     * {@code records} by removing every value for each {@code key} in each
     * record.
     * 
     * @param records
     */
    @CompoundOperation
    public abstract void clear(Collection<Long> records);

    /**
     * Clear each of the {@code keys} in each of the {@code records} by removing
     * every value for each key in each record.
     * 
     * @param keys
     * @param records
     */
    @CompoundOperation
    public abstract void clear(Collection<String> keys, Collection<Long> records);

    /**
     * Clear each of the {@code keys} in {@code record} by removing every value
     * for each key.
     * 
     * @param keys
     * @param record
     */
    @CompoundOperation
    public abstract void clear(Collection<String> keys, long record);

    /**
     * Atomically clear {@code record} by removing each contained key and their
     * values.
     * 
     * @param record
     */
    public abstract void clear(long record);

    /**
     * Clear {@code key} in each of the {@code records} by removing every value
     * for {@code key} in each record.
     * 
     * @param key
     * @param records
     */
    @CompoundOperation
    public abstract void clear(String key, Collection<Long> records);

    /**
     * Atomically clear {@code key} in {@code record} by removing each contained
     * value.
     * 
     * @param record
     */
    public abstract void clear(String key, long record);

    @Override
    public final void close() {
        exit();
    }

    /**
     * Attempt to permanently commit all the currently staged changes. This
     * function returns {@code true} if and only if all the changes can be
     * successfully applied. Otherwise, this function returns {@code false} and
     * all the changes are aborted.
     * <p>
     * After this function returns, Concourse will return to {@code autocommit}
     * mode and all subsequent changes will be written immediately.
     * </p>
     * 
     * @return {@code true} if all staged changes are successfully committed
     */
    public abstract boolean commit();

    @Deprecated
    public abstract long create();

    /**
     * Describe each of the {@code records} and return a mapping from each
     * record to the keys that currently have at least one value.
     * 
     * @param records
     * @return the populated keys in each record
     */
    @CompoundOperation
    public abstract Map<Long, Set<String>> describe(Collection<Long> records);

    /**
     * Describe each of the {@code records} at {@code timestamp} and return a
     * mapping from each record to the keys that had at least one value.
     * 
     * @param records
     * @param timestamp
     * @return the populated keys in each record at {@code timestamp}
     */
    @CompoundOperation
    public abstract Map<Long, Set<String>> describe(Collection<Long> records,
            Timestamp timestamp);

    /**
     * Describe {@code record} and return the keys that currently have at least
     * one value.
     * 
     * @param record
     * @return the populated keys in {@code record}
     */
    public abstract Set<String> describe(long record);

    /**
     * Describe {@code record} at {@code timestamp} and return the keys that had
     * at least one value.
     * 
     * @param record
     * @param timestamp
     * @return the populated keys in {@code record} at {@code timestamp}
     */
    public abstract Set<String> describe(long record, Timestamp timestamp);

    /**
     * Return all the changes (Addition and Deletion) of {@code record} and
     * {@code value} in {@code record} for all {@code key} between {@code start}
     * and {@code end}.
     * 
     * @param record
     * @param start
     * @param end
     * @return the changes made to the record within the range
     */
    public abstract Map<String, Map<Diff, Set<Object>>> diff(long record,
            Timestamp start, Timestamp end);

    /**
     * Return all the changes (Addition and Deletion) of {@code value} of
     * {@code key} in {@code record} between {@code start} and current time.
     * 
     * @param key
     * @param record
     * @param start
     * @return the changes made to the {@code key}/{@code record} within the
     *         range
     */
    public abstract Map<Diff, Set<Object>> diff(String key, long record,
            Timestamp start);

    /**
     * Return all the changes (Addition and Deletion) of {@code value} of
     * {@code key} in {@code record} between {@code start} and {@code end}.
     * 
     * @param key
     * @param record
     * @param start
     * @param end
     * @return the changes made to the {@code key}/{@coee record} within the
     *         range
     */
    public abstract Map<Diff, Set<Object>> diff(String key, long record,
            Timestamp start, Timestamp end);

    /**
     * Return all the changes (Addition and Deletion) of {@code key} and it's
     * value between {@code start} and {@code end}.
     * 
     * @param key
     * @param start
     * @param end
     * @return the changes map to the key within the range
     */
    public abstract Map<Object, Map<Diff, Set<Long>>> diff(String key,
            Timestamp start, Timestamp end);

    /**
     * Close the Client connection.
     */
    public abstract void exit();

    /**
     * Return a list of all the records that have ever contained data.
     * 
     * @return the full list of records
     */
    public abstract Set<Long> find();

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
    @CompoundOperation
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
    public abstract Map<Long, Boolean> remove(String key, Object value,
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
     * Diff represent whether the {@code value} or {@code record} or both is
     * Added or Removed
     * 
     * @author dubex
     *
     */
    public enum Diff {
        ADDED, REMOVED
    }

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
        // NOTE: The configuration variables are static because we want to
        // guarantee that they are set before the client connection is
        // constructed. Even though these variables are static, it is still the
        // case that any changes to the configuration will be picked up
        // immediately for new client connections.
        private static String SERVER_HOST;
        private static int SERVER_PORT;
        private static String USERNAME;
        static {
            ConcourseConfiguration config;
            try {
                config = ConcourseConfiguration
                        .loadConfig("concourse_client.prefs");
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
        public Map<Long, Boolean> add(final String key, final Object value,
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
            Preconditions.checkArgument(start.getMicros() <= Time.now(),
                    "Start of range cannot be greater than the present");
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.auditRecordStart(record,
                            start.getMicros(), creds, transaction, environment);
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
            Preconditions.checkArgument(start.getMicros() <= end.getMicros(),
                    "Start of range cannot be greater than the end");
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.auditRecordStartEnd(
                            record, start.getMicros(), end.getMicros(), creds,
                            transaction, environment);
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
            Preconditions.checkArgument(start.getMicros() <= Time.now(),
                    "Start of range cannot be greater than the present");
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.auditKeyRecordStart(key,
                            record, start.getMicros(), creds, transaction,
                            environment);
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
            Preconditions.checkArgument(start.getMicros() <= end.getMicros(),
                    "Start of range cannot be greater than the end");
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.auditKeyRecordStartEnd(
                            key, record, start.getMicros(), end.getMicros(),
                            creds, transaction, environment);
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
                    Map<String, Map<TObject, Set<Long>>> raw = client
                            .browseKeysTime(Collections.toList(keys),
                                    timestamp.getMicros(), creds, transaction,
                                    environment);
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
                    Map<TObject, Set<Long>> raw = client.browseKeyTime(key,
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Map<Long, Set<TObject>> raw = client
                            .chronologizeKeyRecordStart(key, record,
                                    start.getMicros(), creds, transaction,
                                    environment);
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
            Preconditions.checkArgument(start.getMicros() <= end.getMicros(),
                    "Start of range cannot be greater than the end");
            return execute(new Callable<Map<Timestamp, Set<Object>>>() {

                @Override
                public Map<Timestamp, Set<Object>> call() throws Exception {
                    Map<Long, Set<TObject>> raw = client
                            .chronologizeKeyRecordStartEnd(key, record,
                                    start.getMicros(), end.getMicros(), creds,
                                    transaction, environment);
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
                    return client.commit(creds, token, environment);
                }

            });
        }

        @Override
        public long create() {
            return Time.now();
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
                    Map<Long, Set<String>> raw = client.describeRecordsTime(
                            Collections.toLongList(records),
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Set<String> result = client.describeRecordTime(record,
                            timestamp.getMicros(), creds, transaction,
                            environment);
                    return result;
                }
            });
        }

        @Override
        public Map<String, Map<Diff, Set<Object>>> diff(long record,
                Timestamp start, Timestamp end) {
            PrettyLinkedTableMap<String, Diff, Set<Object>> result = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap();
            result.setRowName("Value");
            Map<String, Set<Object>> startBrowse = select(record, start);
            Map<String, Set<Object>> endBrowse = select(record, end);
            Set<String> startBrowseKeySet = startBrowse.keySet();
            Set<String> endBrowseKeySet = endBrowse.keySet();
            Set<String> xor = Sets.symmetricDifference(startBrowseKeySet,
                    endBrowseKeySet);
            Set<String> intersection = Sets.intersection(startBrowseKeySet,
                    endBrowseKeySet);

            for (String current : xor) {
                if(!startBrowseKeySet.contains(current))
                    result.put(current, Diff.ADDED, endBrowse.get(current));
                else {
                    result.put(current, Diff.REMOVED, startBrowse.get(current));
                }
            }

            for (String currentKey : intersection) {
                Set<Object> startValue = startBrowse.get(currentKey);
                Set<Object> endValue = endBrowse.get(currentKey);
                Set<Object> xorValue = Sets.symmetricDifference(startValue,
                        endValue);
                for (Object currentValue : xorValue) {
                    if(!startValue.contains(currentValue))
                        result.put(currentKey, Diff.ADDED,
                                Sets.newHashSet(currentValue));
                    else {
                        result.put(currentKey, Diff.REMOVED,
                                Sets.newHashSet(currentValue));
                    }
                }
            }
            return result;
        }

        @Override
        public Map<Diff, Set<Object>> diff(String key, long record,
                Timestamp start) {
            return diff(key, record, start, Timestamp.now());
        }

        @Override
        public Map<Diff, Set<Object>> diff(String key, long record,
                Timestamp start, Timestamp end) {
            Map<Diff, Set<Object>> result = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Status", "Value");
            Set<Object> added = Sets.newHashSet();
            Set<Object> removed = Sets.newHashSet();
            Set<Object> startFetch = select(key, record, start);
            Set<Object> endFetch = select(key, record, end);
            Set<Object> xor = Sets.symmetricDifference(startFetch, endFetch);

            for (Object current : xor) {
                if(!startFetch.contains(current))
                    added.add(current);
                else {
                    removed.add(current);
                }
            }
            result.put(Diff.ADDED, added);
            result.put(Diff.REMOVED, removed);
            return result;
        }

        @Override
        public Map<Object, Map<Diff, Set<Long>>> diff(String key,
                Timestamp start, Timestamp end) {
            PrettyLinkedTableMap<Object, Diff, Set<Long>> result = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap();
            result.setRowName("Value");
            Map<Object, Set<Long>> startBrowse = browse(key, start);
            Map<Object, Set<Long>> endBrowse = browse(key, end);
            Set<Object> startBrowseKeySet = startBrowse.keySet();
            Set<Object> endBrowseKeySet = endBrowse.keySet();
            Set<Object> xor = Sets.symmetricDifference(startBrowseKeySet,
                    endBrowseKeySet);
            Set<Object> intersection = Sets.intersection(startBrowseKeySet,
                    endBrowseKeySet);

            for (Object current : xor) {
                if(!startBrowseKeySet.contains(current))
                    result.put(current, Diff.ADDED, endBrowse.get(current));
                else {
                    result.put(current, Diff.REMOVED, startBrowse.get(current));
                }
            }

            for (Object currentKey : intersection) {
                Set<Long> startValue = startBrowse.get(currentKey);
                Set<Long> endValue = endBrowse.get(currentKey);
                Set<Long> xorValue = Sets.symmetricDifference(startValue,
                        endValue);
                for (Long currentValue : xorValue) {
                    if(!startValue.contains(currentValue))
                        result.put(currentKey, Diff.ADDED,
                                Sets.newHashSet(currentValue));
                    else {
                        result.put(currentKey, Diff.REMOVED,
                                Sets.newHashSet(currentValue));
                    }
                }
            }
            return result;

        }

        @Override
        public void exit() {
            try {
                client.logout(creds, environment);
                client.getInputProtocol().getTransport().close();
                client.getOutputProtocol().getTransport().close();
            }
            catch (TSecurityException | TTransportException e) {
                // Handle corner case where the client is existing because of
                // (or after the occurence of) a password change, which means it
                // can't perform a traditional logout. Its worth nothing that
                // we're okay with this scenario because a password change will
                // delete all previously issued tokens.
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public Set<Long> find() {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.find(creds, transaction, environment);
                }

            });
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
                    Map<Long, Map<String, TObject>> raw = client
                            .getKeysRecordsTime(Collections.toList(keys),
                                    Collections.toLongList(records),
                                    timestamp.getMicros(), creds, transaction,
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
                    Map<Long, Map<String, TObject>> raw = client
                            .getKeysCriteriaTime(
                                    Collections.toList(keys),
                                    Language.translateToThriftCriteria(criteria),
                                    timestamp.getMicros(), creds, transaction,
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
                    Map<String, TObject> raw = client.getKeysRecordTime(
                            Collections.toList(keys), record,
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Map<Long, TObject> raw = client.getKeyRecordsTime(key,
                            Collections.toLongList(records),
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Map<Long, TObject> raw = client.getKeyCriteriaTime(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    TObject raw = client.getKeyRecordTime(key, record,
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
        public Map<Long, Boolean> remove(final String key, final Object value,
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
                    client.revertKeysRecordsTime(Collections.toList(keys),
                            Collections.toLongList(records),
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    client.revertKeysRecordTime(Collections.toList(keys),
                            record, timestamp.getMicros(), creds, transaction,
                            environment);
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
                    client.revertKeyRecordsTime(key,
                            Collections.toLongList(records),
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    client.revertKeyRecordTime(key, record,
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectRecordsTime(Collections.toLongList(records),
                                    timestamp.getMicros(), creds, transaction,
                                    environment);
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
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectKeysRecordsTime(Collections.toList(keys),
                                    Collections.toLongList(records),
                                    timestamp.getMicros(), creds, transaction,
                                    environment);
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
                    Map<Long, Map<String, Set<TObject>>> raw = client
                            .selectKeysCriteriaTime(
                                    Collections.toList(keys),
                                    Language.translateToThriftCriteria(criteria),
                                    timestamp.getMicros(), creds, transaction,
                                    environment);
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
                    Map<String, Set<TObject>> raw = client
                            .selectKeysRecordTime(Collections.toList(keys),
                                    record, timestamp.getMicros(), creds,
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
                    Map<String, Set<TObject>> raw = client.selectRecordTime(
                            record, timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Map<Long, Set<TObject>> raw = client.selectKeyRecordsTime(
                            key, Collections.toLongList(records),
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Map<Long, Set<TObject>> raw = client.selectKeyCriteriaTime(
                            key, Language.translateToThriftCriteria(criteria),
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
                    Set<TObject> values = client.selectKeyRecordTime(key,
                            record, timestamp.getMicros(), creds, transaction,
                            environment);
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
                    return client.verifyKeyValueRecordTime(key,
                            Convert.javaToThrift(value), record,
                            timestamp.getMicros(), creds, transaction,
                            environment);
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
            catch (TTransactionException e) {
                throw new TransactionException();
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
                        return client.findKeyStringOperatorValues(key,
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
                        return client.findKeyStringOperatorValuesTime(key,
                                operator.toString(), tValues,
                                timestamp.getMicros(), creds, transaction,
                                environment);
                    }

                }

            });
        }

    }
}
