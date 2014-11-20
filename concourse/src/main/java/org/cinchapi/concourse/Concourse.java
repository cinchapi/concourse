/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
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

import org.apache.commons.lang.StringUtils;
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
import org.cinchapi.concourse.lang.Translate;
import org.cinchapi.concourse.security.ClientSecurity;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.TTransactionException;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TLinkedTableMap;
import org.cinchapi.concourse.util.Timestamps;
import org.cinchapi.concourse.util.Transformers;
import org.cinchapi.concourse.util.TLinkedHashMap;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * <p>
 * Concourse is a schemaless and distributed version control database with
 * automatic indexing, acid transactions and full-text search. Concourse
 * provides a more intuitive approach to data management that is easy to deploy,
 * access and scale with minimal tuning while also maintaining the referential
 * integrity and ACID characteristics of traditional database systems.
 * </p>
 * <h2>Data Model</h2>
 * <p>
 * The Concourse data model is lightweight and flexible. Unlike other databases,
 * Concourse is completely schemaless and does not hold data in tables or
 * collections. Instead, Concourse is simply a distributed graph of records.
 * Each record has multiple keys. And each key has one or more distinct values.
 * Like any graph, you can link records to one another. And the structure of one
 * record does not affect the structure of another.
 * </p>
 * <p>
 * <ul>
 * <li><strong>Record</strong> &mdash; A logical grouping of data about a single
 * person, place, or thing (i.e. an object). Each {@code record} is a collection
 * of key/value pairs that are together identified by a unique primary key.
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
 * @author jnelson
 */
public abstract class Concourse implements AutoCloseable {

    /**
     * Create a new Client connection to the environment of the Concourse Server
     * described in {@code concourse_client.prefs} (or the default environment
     * and server if the prefs file does not exist) and return a handler to
     * facilitate database interaction.
     * 
     * @return the database handler
     */
    public static Concourse connect() {
        return new Client();
    }

    /**
     * /** Create a new Client connection to the specified {@code environment}
     * of the Concourse Server described in {@code concourse_client.prefs} (or
     * the default server if the prefs file does not exist) and return a handler
     * to facilitate database interaction.
     * 
     * @param environment
     * @return
     */
    public static Concourse connect(String environment) {
        return new Client(environment);
    }

    /**
     * Create a new Client connection to the default environment of the
     * specified Concourse Server and return a handler to facilitate database
     * interaction.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @return the database handler
     */
    public static Concourse connect(String host, int port, String username,
            String password) {
        return new Client(host, port, username, password);
    }

    /**
     * Create a new Client connection to the specified {@code environment} of
     * the specified Concourse Server and return a handler to facilitate
     * database interaction.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @return the database handler
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
     * Audit {@code key} in {@code record} and return a log of revisions.
     * 
     * @param key
     * @param record
     * @return a mapping from timestamp to a description of a revision
     */
    public abstract Map<Timestamp, String> audit(String key, long record);

    /**
     * Browse the {@code records} and return a mapping from each record to all
     * the data that is contained as a mapping from key name to value set.
     * 
     * @param records
     * @return a mapping of all the contained keys and their mapped values in
     *         each record
     */
    public abstract Map<Long, Map<String, Set<Object>>> browse(
            Collection<Long> records);

    /**
     * Browse the {@code records} at {@code timestamp} and return a mapping from
     * each record to all the data that was contained as a mapping from key name
     * to value set.
     * 
     * @param records
     * @param timestamp
     * @return a mapping of all the contained keys and their mapped values in
     *         each record
     */
    public abstract Map<Long, Map<String, Set<Object>>> browse(
            Collection<Long> records, Timestamp timestamp);

    /**
     * Browse {@code record} and return all the data that is presently contained
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
    public abstract Map<String, Set<Object>> browse(long record);

    /**
     * Browse {@code record} at {@code timestamp} and return all the data that
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
    public abstract Map<String, Set<Object>> browse(long record,
            Timestamp timestamp);

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
    public final void close() throws Exception {
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

    /**
     * Create a new Record and return its Primary Key.
     * 
     * @return the Primary Key of the new Record
     */
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
     * Close the Client connection.
     */
    public abstract void exit();

    /**
     * Fetch each of the {@code keys} from each of the {@code records} and
     * return a mapping from each record to a mapping from each key to the
     * contained values.
     * 
     * @param keys
     * @param records
     * @return the contained values for each of the {@code keys} in each of the
     *         {@code records}
     */
    @CompoundOperation
    public abstract Map<Long, Map<String, Set<Object>>> fetch(
            Collection<String> keys, Collection<Long> records);

    /**
     * Fetch each of the {@code keys} from each of the {@code records} at
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
    public abstract Map<Long, Map<String, Set<Object>>> fetch(
            Collection<String> keys, Collection<Long> records,
            Timestamp timestamp);

    /**
     * Fetch each of the {@code keys} from {@code record} and return a mapping
     * from each key to the contained values.
     * 
     * @param keys
     * @param record
     * @return the contained values for each of the {@code keys} in
     *         {@code record}
     */
    @CompoundOperation
    public abstract Map<String, Set<Object>> fetch(Collection<String> keys,
            long record);

    /**
     * Fetch each of the {@code keys} from {@code record} at {@code timestamp}
     * and return a mapping from each key to the contained values.
     * 
     * @param keys
     * @param record
     * @param timestamp
     * @return the contained values for each of the {@code keys} in
     *         {@code record} at {@code timestamp}
     */
    @CompoundOperation
    public abstract Map<String, Set<Object>> fetch(Collection<String> keys,
            long record, Timestamp timestamp);

    /**
     * Fetch {@code key} from each of the {@code records} and return a mapping
     * from each record to contained values.
     * 
     * @param key
     * @param records
     * @return the contained values for {@code key} in each {@code record}
     */
    @CompoundOperation
    public abstract Map<Long, Set<Object>> fetch(String key,
            Collection<Long> records);

    /**
     * Fetch {@code key} from} each of the {@code records} at {@code timestamp}
     * and return a mapping from each record to the contained values.
     * 
     * @param key
     * @param records
     * @param timestamp
     * @return the contained values for {@code key} in each of the
     *         {@code records} at {@code timestamp}
     */
    @CompoundOperation
    public abstract Map<Long, Set<Object>> fetch(String key,
            Collection<Long> records, Timestamp timestamp);

    /**
     * Fetch {@code key} from {@code record} and return all the contained
     * values.
     * 
     * @param key
     * @param record
     * @return the contained values
     */
    public abstract Set<Object> fetch(String key, long record);

    /**
     * Fetch {@code key} from {@code record} at {@code timestamp} and return the
     * set of values that were mapped.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return the contained values
     */
    public abstract Set<Object> fetch(String key, long record,
            Timestamp timestamp);

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
    public abstract Map<Long, Map<String, Object>> get(Collection<String> keys,
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
    public abstract Map<Long, Map<String, Object>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp);

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
    public abstract Map<String, Object> get(Collection<String> keys, long record);

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
    public abstract Map<String, Object> get(Collection<String> keys,
            long record, Timestamp timestamp);

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
    public abstract Map<Long, Object> get(String key, Collection<Long> records);

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
    public abstract Map<Long, Object> get(String key, Collection<Long> records,
            Timestamp timestamp);

    /**
     * Get {@code key} from {@code record} and return the first contained value
     * or {@code null} if there is none. Compared to
     * {@link #fetch(String, long)}, this method is suited for cases when the
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
     * {@link #fetch(String, long, Timestamp)}, this method is suited for cases
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
    public abstract long insert(String json);

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
     * Convert list of {@code record} into {@code json} syntax formatted String
     * 
     * @param records
     * @param includePrimaryKey
     * @return {@code JSON String} of the list of {@code records}
     */
    public abstract String jsonify(List<Long> records, boolean includePrimaryKey);
    
    /**
     * Convert list of {@code record} into {@code Json} syntax formatted String.
     * No flag parameter defaults to the flag being set to true.
     * 
     * @param records
     * @return {@code JSON String} of the list of {@code records}
     */
    public abstract String jsonify(List<Long> records);

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
     * Turn on {@code staging} mode so that all subsequent changes are collected
     * in a staging area before possibly being committed. Staged operations are
     * guaranteed to be reliable, all or nothing units of work that allow
     * correct recovery from failures and provide isolation between clients so
     * that Concourse is always in a consistent state (e.g. a transaction).
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
     * try {
     *     concourse.stage();
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
     * The implementation of the {@link Concourse} interface that establishes a
     * connection with the remote server and handles communication. This class
     * is a more user friendly wrapper around a Thrift
     * {@link ConcourseService.Client}.
     * 
     * @author jnelson
     */
    private final static class Client extends Concourse {

        // NOTE: The configuration variables are static because we want to
        // guarantee that they are set before the client connection is
        // constructed. Even though these variables are static, it is still the
        // case that any changes to the configuration will be picked up
        // immediately for new client connections.
        private static String SERVER_HOST;
        private static int SERVER_PORT;
        private static String USERNAME;
        private static String PASSWORD;
        private static String ENVIRONMENT;
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
         * Represents a request to respond to a query using the current state as
         * opposed to the history.
         */
        private static Timestamp now = Timestamp.fromMicros(0);

        /**
         * An encrypted copy of the username passed to the constructor.
         */
        private final ByteBuffer username;

        /**
         * An encrypted copy of the password passed to the constructor.
         */
        private final ByteBuffer password;

        /**
         * The host of the connection.
         */
        private final String host;

        /**
         * The port of the connection.
         */
        private final int port;

        /**
         * The environment to which the client is connected.
         */
        private final String environment;

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
         * Whenever the client starts a Transaction, it keeps a
         * {@link TransactionToken} so that the server can stage the changes in
         * the appropriate place.
         */
        private TransactionToken transaction = null;

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
        public Map<Long, Boolean> add(String key, Object value,
                Collection<Long> records) {
            Map<Long, Boolean> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", "Result");
            for (long record : records) {
                result.put(record, add(key, value, record));
            }
            return result;
        }

        public <T> long add(final String key, final T value) {
            if(!StringUtils.isBlank(key)
                    && (!(value instanceof String) || (value instanceof String && !StringUtils
                            .isBlank((String) value)))) { // CON-21
                return execute(new Callable<Long>() {

                    @Override
                    public Long call() throws Exception {
                        return client.add1(key, Convert.javaToThrift(value),
                                creds, transaction, environment);
                    }

                });
            }
            else {
                throw new IllegalArgumentException(
                        "Either your key is blank or value");
            }

        }

        @Override
        public <T> boolean add(final String key, final T value,
                final long record) {
            if(!StringUtils.isBlank(key)
                    && (!(value instanceof String) || (value instanceof String && !StringUtils
                            .isBlank((String) value)))) { // CON-21
                return execute(new Callable<Boolean>() {

                    @Override
                    public Boolean call() throws Exception {
                        return client.add(key, Convert.javaToThrift(value),
                                record, creds, transaction, environment);
                    }

                });
            }
            return false;
        }

        @Override
        public Map<Timestamp, String> audit(final long record) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.audit(record, null, creds,
                            transaction, environment);
                    return ((TLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    new Function<Long, Timestamp>() {

                                        @Override
                                        public Timestamp apply(Long input) {
                                            return Timestamp.fromMicros(input);
                                        }

                                    })).setKeyName("DateTime").setValueName(
                            "Revision");
                }

            });
        }

        @Override
        public Map<Timestamp, String> audit(final String key, final long record) {
            return execute(new Callable<Map<Timestamp, String>>() {

                @Override
                public Map<Timestamp, String> call() throws Exception {
                    Map<Long, String> audit = client.audit(record, key, creds,
                            transaction, environment);
                    return ((TLinkedHashMap<Timestamp, String>) Transformers
                            .transformMap(audit,
                                    new Function<Long, Timestamp>() {

                                        @Override
                                        public Timestamp apply(Long input) {
                                            return Timestamp.fromMicros(input);
                                        }

                                    })).setKeyName("DateTime").setValueName(
                            "Revision");
                }

            });
        }

        @CompoundOperation
        @Override
        public Map<Long, Map<String, Set<Object>>> browse(
                Collection<Long> records) {
            Map<Long, Map<String, Set<Object>>> data = TLinkedTableMap
                    .newTLinkedTableMap("Record");
            for (long record : records) {
                data.put(record, browse(record, now));
            }
            return data;
        }

        @CompoundOperation
        @Override
        public Map<Long, Map<String, Set<Object>>> browse(
                Collection<Long> records, Timestamp timestamp) {
            Map<Long, Map<String, Set<Object>>> data = TLinkedTableMap
                    .newTLinkedTableMap("Record");
            for (long record : records) {
                data.put(record, browse(record, timestamp));
            }
            return data;
        }

        @Override
        public Map<String, Set<Object>> browse(long record) {
            return browse(record, now);
        }

        @Override
        public Map<String, Set<Object>> browse(final long record,
                final Timestamp timestamp) {
            return execute(new Callable<Map<String, Set<Object>>>() {

                @Override
                public Map<String, Set<Object>> call() throws Exception {
                    Map<String, Set<Object>> data = TLinkedHashMap
                            .newTLinkedHashMap("Key", "Values");
                    for (Entry<String, Set<TObject>> entry : client.browse0(
                            record, timestamp.getMicros(), creds, transaction,
                            environment).entrySet()) {
                        data.put(entry.getKey(), Transformers.transformSet(
                                entry.getValue(),
                                new Function<TObject, Object>() {

                                    @Override
                                    public Object apply(TObject input) {
                                        return Convert.thriftToJava(input);
                                    }

                                }));
                    }
                    return data;
                }

            });
        }

        @Override
        public Map<Object, Set<Long>> browse(String key) {
            return browse(key, now);
        }

        @Override
        public Map<Object, Set<Long>> browse(final String key,
                final Timestamp timestamp) {
            return execute(new Callable<Map<Object, Set<Long>>>() {

                @Override
                public Map<Object, Set<Long>> call() throws Exception {
                    Map<Object, Set<Long>> data = TLinkedHashMap
                            .newTLinkedHashMap(key, "Records");
                    for (Entry<TObject, Set<Long>> entry : client.browse1(key,
                            timestamp.getMicros(), creds, transaction,
                            environment).entrySet()) {
                        data.put(Convert.thriftToJava(entry.getKey()),
                                entry.getValue());
                    }
                    return data;
                }

            });
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(final String key,
                final long record) {
            return execute(new Callable<Map<Timestamp, Set<Object>>>() {

                @Override
                public Map<Timestamp, Set<Object>> call() throws Exception {
                    Map<Long, Set<TObject>> chronologize = client.chronologize(
                            record, key, creds, transaction, environment);
                    Map<Timestamp, Set<Object>> result = TLinkedHashMap
                            .newTLinkedHashMap("DateTime", "Values");
                    for (Entry<Long, Set<TObject>> entry : chronologize
                            .entrySet()) {
                        result.put(Timestamp.fromMicros(entry.getKey()),
                                Transformers.transformSet(entry.getValue(),
                                        new Function<TObject, Object>() {

                                            @Override
                                            public Object apply(TObject input) {
                                                return Convert
                                                        .thriftToJava(input);
                                            }

                                        }));
                    }
                    return result;
                }

            });
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(final String key,
                final long record, final Timestamp start) {
            return chronologize(key, record, start, Timestamp.now());
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(final String key,
                final long record, final Timestamp start, final Timestamp end) {
            Preconditions.checkArgument(start.getMicros() <= end.getMicros(),
                    "Start of range cannot be greater than the end");
            Map<Timestamp, Set<Object>> result = TLinkedHashMap
                    .newTLinkedHashMap("DateTime", "Values");
            Map<Timestamp, Set<Object>> chronology = chronologize(key, record);
            int index = Timestamps.findNearestSuccessorForTimestamp(
                    chronology.keySet(), start);
            Entry<Timestamp, Set<Object>> entry = null;
            if(index > 0) {
                entry = Iterables.get(chronology.entrySet(), index - 1);
                result.put(entry.getKey(), entry.getValue());
            }
            for (int i = index; i < chronology.size(); ++i) {
                entry = Iterables.get(chronology.entrySet(), i);
                if(entry.getKey().getMicros() >= end.getMicros()) {
                    break;
                }
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }

        @Override
        public void clear(final Collection<Long> records) {
            for (Long record : records) {
                clear(record);
            }
        }

        @Override
        public void clear(Collection<String> keys, Collection<Long> records) {
            for (long record : records) {
                for (String key : keys) {
                    clear(key, record);
                }
            }
        }

        @Override
        public void clear(Collection<String> keys, long record) {
            for (String key : keys) {
                clear(key, record);
            }
        }

        @Override
        public void clear(final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clear1(record, creds, transaction, environment);
                    return null;
                }

            });

        }

        @Override
        public void clear(String key, Collection<Long> records) {
            for (long record : records) {
                clear(key, record);
            }
        }

        @Override
        public void clear(final String key, final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.clear(key, record, creds, transaction, environment);
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
            return Time.now(); // TODO get a primary key using a plugin
        }

        @Override
        public Map<Long, Set<String>> describe(Collection<Long> records) {
            Map<Long, Set<String>> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", "Keys");
            for (long record : records) {
                result.put(record, describe(record));
            }
            return result;
        }

        @Override
        public Map<Long, Set<String>> describe(Collection<Long> records,
                Timestamp timestamp) {
            Map<Long, Set<String>> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", "Keys");
            for (long record : records) {
                result.put(record, describe(record, timestamp));
            }
            return result;
        }

        @Override
        public Set<String> describe(long record) {
            return describe(record, now);
        }

        @Override
        public Set<String> describe(final long record, final Timestamp timestamp) {
            return execute(new Callable<Set<String>>() {

                @Override
                public Set<String> call() throws Exception {
                    return client.describe(record, timestamp.getMicros(),
                            creds, transaction, environment);
                }

            });
        }

        @Override
        public void exit() {
            client.getInputProtocol().getTransport().close();
            client.getOutputProtocol().getTransport().close();
        }

        @Override
        public Map<Long, Map<String, Set<Object>>> fetch(
                Collection<String> keys, Collection<Long> records) {
            TLinkedTableMap<Long, String, Set<Object>> result = TLinkedTableMap
                    .<Long, String, Set<Object>> newTLinkedTableMap("Record");
            for (long record : records) {
                for (String key : keys) {
                    result.put(record, key, fetch(key, record));
                }
            }
            return result;
        }

        @Override
        public Map<Long, Map<String, Set<Object>>> fetch(
                Collection<String> keys, Collection<Long> records,
                Timestamp timestamp) {
            TLinkedTableMap<Long, String, Set<Object>> result = TLinkedTableMap
                    .<Long, String, Set<Object>> newTLinkedTableMap("Record");
            for (long record : records) {
                for (String key : keys) {
                    result.put(record, key, fetch(key, record, timestamp));
                }
            }
            return result;
        }

        @Override
        public Map<String, Set<Object>> fetch(Collection<String> keys,
                long record) {
            Map<String, Set<Object>> result = TLinkedHashMap.newTLinkedHashMap(
                    "Key", "Values");
            for (String key : keys) {
                result.put(key, fetch(key, record));
            }
            return result;
        }

        @Override
        public Map<String, Set<Object>> fetch(Collection<String> keys,
                long record, Timestamp timestamp) {
            Map<String, Set<Object>> result = TLinkedHashMap.newTLinkedHashMap(
                    "Key", "Values");
            for (String key : keys) {
                result.put(key, fetch(key, record, timestamp));
            }
            return result;
        }

        @Override
        public Map<Long, Set<Object>> fetch(String key, Collection<Long> records) {
            Map<Long, Set<Object>> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", key);
            for (long record : records) {
                result.put(record, fetch(key, record));
            }
            return result;
        }

        @Override
        public Map<Long, Set<Object>> fetch(String key,
                Collection<Long> records, Timestamp timestamp) {
            Map<Long, Set<Object>> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", key);
            for (long record : records) {
                result.put(record, fetch(key, record, timestamp));
            }
            return result;
        }

        @Override
        public Set<Object> fetch(String key, long record) {
            return fetch(key, record, now);
        }

        @Override
        public Set<Object> fetch(final String key, final long record,
                final Timestamp timestamp) {
            return execute(new Callable<Set<Object>>() {

                @Override
                public Set<Object> call() throws Exception {
                    Set<TObject> values = client.fetch(key, record,
                            timestamp.getMicros(), creds, transaction,
                            environment);
                    return Transformers.transformSet(values,
                            new Function<TObject, Object>() {

                                @Override
                                public Object apply(TObject input) {
                                    return Convert.thriftToJava(input);
                                }

                            });
                }

            });
        }

        @Override
        public Set<Long> find(final Criteria criteria) {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.find1(Translate.toThrift(criteria), creds,
                            transaction, environment);
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
        public Set<Long> find(String key, Operator operator, Object value) {
            return find(key, operator, value, now);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2) {
            return find(key, operator, value, value2, now);
        }

        @Override
        public Set<Long> find(final String key, final Operator operator,
                final Object value, final Object value2,
                final Timestamp timestamp) {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.find(key, operator, Lists.transform(
                            Lists.newArrayList(value, value2),
                            new Function<Object, TObject>() {

                                @Override
                                public TObject apply(Object input) {
                                    return Convert.javaToThrift(input);
                                }

                            }), timestamp.getMicros(), creds, transaction,
                            environment);
                }

            });
        }

        @Override
        public Set<Long> find(final String key, final Operator operator,
                final Object value, final Timestamp timestamp) {
            return execute(new Callable<Set<Long>>() {

                @Override
                public Set<Long> call() throws Exception {
                    return client.find(key, operator, Lists.transform(
                            Lists.newArrayList(value),
                            new Function<Object, TObject>() {

                                @Override
                                public TObject apply(Object input) {
                                    return Convert.javaToThrift(input);
                                }

                            }), timestamp.getMicros(), creds, transaction,
                            environment);
                }

            });
        }

        @Override
        public Map<Long, Map<String, Object>> get(Collection<String> keys,
                Collection<Long> records) {
            TLinkedTableMap<Long, String, Object> result = TLinkedTableMap
                    .<Long, String, Object> newTLinkedTableMap("Record");
            for (long record : records) {
                for (String key : keys) {
                    Object value = get(key, record);
                    if(value != null) {
                        result.put(record, key, value);
                    }
                }
            }
            return result;
        }

        @Override
        public Map<Long, Map<String, Object>> get(Collection<String> keys,
                Collection<Long> records, Timestamp timestamp) {
            TLinkedTableMap<Long, String, Object> result = TLinkedTableMap
                    .<Long, String, Object> newTLinkedTableMap("Record");
            for (long record : records) {
                for (String key : keys) {
                    Object value = get(key, record, timestamp);
                    if(value != null) {
                        result.put(record, key, value);
                    }
                }
            }
            return result;
        }

        @Override
        public Map<String, Object> get(Collection<String> keys, long record) {
            Map<String, Object> result = TLinkedHashMap.newTLinkedHashMap(
                    "Key", "Value");
            for (String key : keys) {
                Object value = get(key, record);
                if(value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        @Override
        public Map<String, Object> get(Collection<String> keys, long record,
                Timestamp timestamp) {
            Map<String, Object> result = TLinkedHashMap.newTLinkedHashMap(
                    "Key", "Value");
            for (String key : keys) {
                Object value = get(key, record, timestamp);
                if(value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        @Override
        public Map<Long, Object> get(String key, Collection<Long> records) {
            Map<Long, Object> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", key);
            for (long record : records) {
                Object value = get(key, record);
                if(value != null) {
                    result.put(record, value);
                }
            }
            return result;
        }

        @Override
        public Map<Long, Object> get(String key, Collection<Long> records,
                Timestamp timestamp) {
            Map<Long, Object> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", key);
            for (long record : records) {
                Object value = get(key, record, timestamp);
                if(value != null) {
                    result.put(record, value);
                }
            }
            return result;
        }

        @Override
        @Nullable
        public <T> T get(String key, long record) {
            return get(key, record, now);
        }

        @SuppressWarnings("unchecked")
        @Override
        @Nullable
        public <T> T get(String key, long record, Timestamp timestamp) {
            Set<Object> values = fetch(key, record, timestamp);
            if(!values.isEmpty()) {
                return (T) values.iterator().next();
            }
            return null;
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
        public long insert(final String json) {
            return execute(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    return client
                            .insert1(json, creds, transaction, environment);
                }

            });

        }

        @Override
        public Map<Long, Boolean> insert(String json, Collection<Long> records) {
            Map<Long, Boolean> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", "Result");
            for (long record : records) {
                result.put(record, insert(json, record));
            }
            return result;
        }

        @Override
        public boolean insert(final String json, final long record) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.insert(json, record, creds, transaction,
                            environment);
                }

            });
        }
        
        @Override
        public String jsonify(final List<Long> records, final boolean flag) {
        	return execute(new Callable<String>() {
        		
        		@Override
        		public String call() throws Exception {
        			return client.jsonify(records, flag, creds, transaction,
        					environment);
        		}
        	});
        }
        
        @Override
        public String jsonify(final List<Long> records) {
        	return jsonify(records, true);
        }

        @Override
        public Map<Long, Boolean> link(String key, long source,
                Collection<Long> destinations) {
            Map<Long, Boolean> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", "Result");
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
        public Map<Long, Boolean> ping(Collection<Long> records) {
            Map<Long, Boolean> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", "Result");
            for (long record : records) {
                result.put(record, ping(record));
            }
            return result;
        }

        @Override
        public boolean ping(final long record) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.ping(record, creds, transaction, environment);
                }

            });
        }

        @Override
        public Map<Long, Boolean> remove(String key, Object value,
                Collection<Long> records) {
            Map<Long, Boolean> result = TLinkedHashMap.newTLinkedHashMap(
                    "Record", "Result");
            for (long record : records) {
                result.put(record, remove(key, value, record));
            }
            return result;
        }

        @Override
        public <T> boolean remove(final String key, final T value,
                final long record) {
            if(!StringUtils.isBlank(key)
                    && (!(value instanceof String) || (value instanceof String && !StringUtils
                            .isBlank((String) value)))) { // CON-21
                return execute(new Callable<Boolean>() {

                    @Override
                    public Boolean call() throws Exception {
                        return client.remove(key, Convert.javaToThrift(value),
                                record, creds, transaction, environment);
                    }

                });
            }
            return false;
        }

        @Override
        public void revert(Collection<String> keys, Collection<Long> records,
                Timestamp timestamp) {
            for (long record : records) {
                for (String key : keys) {
                    revert(key, record, timestamp);
                }
            }
        }

        @Override
        public void revert(Collection<String> keys, long record,
                Timestamp timestamp) {
            for (String key : keys) {
                revert(key, record, timestamp);
            }

        }

        @Override
        public void revert(String key, Collection<Long> records,
                Timestamp timestamp) {
            for (long record : records) {
                revert(key, record, timestamp);
            }

        }

        @Override
        public void revert(final String key, final long record,
                final Timestamp timestamp) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.revert(key, record, timestamp.getMicros(), creds,
                            transaction, environment);
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
        public void set(String key, Object value, Collection<Long> records) {
            for (long record : records) {
                set(key, value, record);
            }
        }

        @Override
        public <T> void set(final String key, final T value, final long record) {
            execute(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client.set0(key, Convert.javaToThrift(value), record,
                            creds, transaction, environment);
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
        public boolean verify(String key, Object value, long record) {
            return verify(key, value, record, now);
        }

        @Override
        public boolean verify(final String key, final Object value,
                final long record, final Timestamp timestamp) {
            return execute(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return client.verify(key, Convert.javaToThrift(value),
                            record, timestamp.getMicros(), creds, transaction,
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

    }
}
