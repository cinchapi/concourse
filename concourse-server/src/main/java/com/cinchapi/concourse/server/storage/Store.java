/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;

/**
 * <p>
 * A {@link Store} is a revisioning service that defines primitive operations to
 * read data from both current and previous states.
 * </p>
 * <p>
 * A {@code Store} can acquire data in one of two ways: directly if it is a
 * {@link Limbo} or <em>eventually</em> if it is a {@link DurableStore}.
 * </p>
 * <p>
 * In general, a {@code Limbo} and {@code PermanentStore} work together in a
 * {@link BufferedStore} to improve write performance by immediately committing
 * writes into a durable buffer before batch indexing them in the background.
 * </p>
 * 
 * @author Jeff Nelson
 */
public interface Store {

    /**
     * Audit {@code record}.
     * <p>
     * This method returns a log of revisions in {@code record} as a Map
     * associating timestamps (in milliseconds) to CAL statements:
     * </p>
     * 
     * <pre>
     * {
     *    "13703523370000" : "ADD 'foo' AS 'bar bang' TO 1", 
     *    "13703524350000" : "ADD 'baz' AS '10' TO 1",
     *    "13703526310000" : "REMOVE 'foo' AS 'bar bang' FROM 1"
     * }
     * </pre>
     * 
     * @param record
     * @return the the revision log
     */
    public Map<Long, String> audit(long record);

    /**
     * Audit {@code key} in {@code record}
     * <p>
     * This method returns a log of revisions in {@code record} as a Map
     * associating timestamps (in milliseconds) to CAL statements:
     * </p>
     * 
     * <pre>
     * {
     *    "13703523370000" : "ADD 'foo' AS 'bar bang' TO 1", 
     *    "13703524350000" : "ADD 'baz' AS '10' TO 1",
     *    "13703526310000" : "REMOVE 'foo' AS 'bar bang' FROM 1"
     * }
     * </pre>
     * 
     * @param key
     * @param record
     * @return the revision log
     */
    public Map<Long, String> audit(String key, long record);

    /**
     * Browse {@code key}.
     * <p>
     * This method returns a mapping from each of the values that is currently
     * indexed to {@code key} to a Set the records that contain {@code key} as
     * the associated value. If there are no such values, an empty Map is
     * returned.
     * </p>
     * 
     * @param key
     * @return a possibly empty Map of data
     */
    public Map<TObject, Set<Long>> browse(String key);

    /**
     * Browse {@code key} at {@code timestamp}.
     * <p>
     * This method returns a mapping from each of the values that was indexed to
     * {@code key} at {@code timestamp} to a Set the records that contained
     * {@code key} as the associated value at {@code timestamp}. If there were
     * no such values, an empty Map is returned.
     * </p>
     * 
     * @param key
     * @param timestamp
     * @return a possibly empty Map of data
     */
    public Map<TObject, Set<Long>> browse(String key, long timestamp);

    /**
     * Return a time series that contains the values stored for {@code key} in
     * {@code record} at each modification timestamp between {@code start}
     * (inclusive) and {@code end} WITHOUT grabbing any locks.
     * 
     * This method is ONLY appropriate to call from the methods of
     * {@link #AtomicOperation} class because in this case intermediate read
     * {@link #Lock} is not required.
     * 
     * @param key the field name
     * @param record the record id
     * @param start the start timestamp (inclusive)
     * @param end the end timestamp (exclusive)
     * @return a {@link Map mapping} from modification timestamp to a non-empty
     *         {@link Set} of values that were contained at that timestamp
     */
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end);

    /**
     * Return {@code true} if the store contains any data, present or
     * historical, for {@code record}.
     * 
     * @param record
     * @return {@code true} if the record exists
     */
    public boolean contains(long record);

    /**
     * Describe {@code record}.
     * <p>
     * This method returns the keys for fields that currently have at least one
     * mapped value in {@code record} such that {@link #select(String, long)}
     * for each key is nonempty. If there are no such keys, an empty Set is
     * returned.
     * </p>
     * 
     * @param record
     * @return a possibly empty Set of keys
     */
    public default Set<String> describe(long record) {
        return select(record).keySet();
    }

    /**
     * Describe {@code record} at {@code timestamp}.
     * <p>
     * This method returns the keys for fields that had at least one mapped
     * value in {@code record} at {@code timestamp} such that
     * {@link #select(String, long, long)} for each key at {@code timestamp} is
     * nonempty. If there are no such keys, an empty Set is returned.
     * </p>
     * 
     * @param record
     * @param timestamp
     * @return a possibly empty Set of keys
     */
    public default Set<String> describe(long record, long timestamp) {
        return select(record, timestamp).keySet();
    }

    /**
     * Explore {@code key} {@code operator} {@code values} at {@code timestamp}.
     * <p>
     * This method returns a mapping from the primary key of each record that
     * meets the criteria at the specified timestamp to the values that caused
     * the record to meet the criteria.
     * </p>
     * 
     * @param key
     * @param operator
     * @param values
     * @return the relevant data for all matching records
     */
    public default Map<Long, Set<TObject>> explore(long timestamp, String key,
            Operator operator, TObject... values) {
        Aliases aliases = TObject.alias(operator, values);
        return explore(key, aliases, timestamp);
    }

    /**
     * Explore {@code key} using an {@link TObject#alias(Operator, TObject...)
     * aliased} {@link Operator} and {@link TObject values}.
     * 
     * <p>
     * This method should only be used with aliased values. Call
     * {@link #explore(String, Operator, TObject...)} during the normal
     * course of operations.
     * </p>
     * 
     * @param key
     * @param aliases
     * @return the relevant data for all matching records
     */
    @DoNotInvoke
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases);

    /**
     * Explore {@code key} using an {@link TObject#alias(Operator, TObject...)
     * aliased} {@link Operator} and {@link TObject values} at
     * {@code timestamp}.
     * 
     * <p>
     * This method should only be used with aliased values. Call
     * {@link #explore(long, String, Operator, TObject...)} during the normal
     * course of operations.
     * </p>
     * 
     * @param key
     * @param aliases
     * @param timestamp
     * @return the relevant data for all matching records
     */
    @DoNotInvoke
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases,
            long timestamp);

    /**
     * Explore {@code key} {@code operator} {@code values}.
     * <p>
     * This method returns a mapping from the primary key of each record that
     * meets the criteria to the values that cause the record to meet the
     * criteria.
     * </p>
     * 
     * @param key
     * @param operator
     * @param values
     * @return the relevant data for all matching records
     */
    public default Map<Long, Set<TObject>> explore(String key,
            Operator operator, TObject... values) {
        Aliases aliases = TObject.alias(operator, values);
        return explore(key, aliases);
    }

    /**
     * Find {@code key} {@code operator} {@code values} at {@code timestamp}.
     * <p>
     * This method returns the records that satisfy {@code operator} in relation
     * to {@code key} at {@code timestamp} for the appropriate {@code values}.
     * This is analogous to a SELECT query in a RDBMS.
     * <p>
     * 
     * @param timestamp
     * @param key
     * @param operator
     * @param values
     * @return a possibly empty Set of primary keys
     */
    public default Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values) {
        return explore(timestamp, key, operator, values).keySet();
    }

    /**
     * Find {@code key} {@code operator} {@code values}
     * <p>
     * This method will return the records that satisfy {@code operator} in
     * relation to {@code key} for the appropriate {@code values}. This is
     * analogous to a SELECT query in a RDBMS.
     * </p>
     * 
     * @param key
     * @param operator
     * @param values
     * @return a possibly empty Set of primary keys
     * @see {@link Operator}
     */
    public default Set<Long> find(String key, Operator operator,
            TObject... values) {
        return explore(key, operator, values).keySet();
    }

    /**
     * Gather the values that are stored for {@code key} in {@code record}.
     * <p>
     * This method is slightly similar to {@link #select(String, long)}. All the
     * values that would be returned from the {@link #select(String, long)
     * select} method are returned here, but the order of the values returned
     * from this method are not necessarily in insertion order.
     * </p>
     * <p>
     * This performance of this method for a single gather is not likely to be
     * better than the performance of a single {@link #select(String, long)
     * selection}; especially, if a normalized index for the record in which the
     * key is held in memory. On the other hand, this method may perform better
     * when gathering one or a few keys across <strong>many</strong> records.
     * </p>
     * 
     * @param key
     * @param record
     * @return a possibly empty Set of values
     */
    public default Set<TObject> gather(String key, long record) {
        return select(key, record);
    }

    /**
     * Gather the values that are stored for {@code key} in {@code record} at
     * {@code timestamp}.
     * <p>
     * This method is slightly similar to {@link #select(String, long, long)}.
     * All the values that would be returned from the
     * {@link #select(String, long) select} method are returned here, but the
     * order of the values returned from this method are not necessarily in
     * insertion order.
     * </p>
     * <p>
     * This performance of this method for a single gather is not likely to be
     * better than the performance of a single
     * {@link #select(String, long, long) selection}; especially, if a
     * normalized index for the record in which the key is held in memory. On
     * the other hand, this method may perform better when gathering one or a
     * few keys across <strong>many</strong> records.
     * </p>
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return a possibly empty Set of values
     */
    public default Set<TObject> gather(String key, long record,
            long timestamp) {
        return select(key, record, timestamp);
    }

    /**
     * Return a {@link Set} which contains the ids of every record that has ever
     * contained data within this {@link Store}.
     *
     * @return the {@link Set} of record ids
     */
    public default Set<Long> getAllRecords() {
        throw new UnsupportedOperationException();
    }

    /**
     * Return the {@link Memory} of this {@link Store}.
     * 
     * @return the {@link Store} {@link Memory state}
     */
    public Memory memory();

    /**
     * Search {@code key} for {@code query}.
     * <p>
     * This method performs a fulltext search for {@code query} in all data
     * <em>currently</em> mapped from {@code key}.
     * </p>
     * 
     * @param key
     * @param query
     * @return the Set of primary keys identifying the records matching the
     *         search
     */
    public Set<Long> search(String key, String query);

    /**
     * Browse {@code record}.
     * <p>
     * This method returns a mapping from each of the nonempty keys in
     * {@code record} to a Set of associated values. If there are no such keys,
     * an empty Map is returned.
     * </p>
     * 
     * @param record
     * @return a possibly empty Map of data.
     */
    public Map<String, Set<TObject>> select(long record);

    /**
     * Browse {@code record} at {@code timestamp}.
     * <p>
     * This method returns a mapping from each of the nonempty keys in
     * {@code record} at {@code timestamp} to a Set of associated values. If
     * there were no such keys, an empty Map is returned.
     * </p>
     * 
     * @param record
     * @return a possibly empty Map of data.
     */
    public Map<String, Set<TObject>> select(long record, long timestamp);

    /**
     * Fetch {@code key} from {@code record}.
     * <p>
     * This method returns the values currently mapped from {@code key} in
     * {@code record}. The returned Set is nonempty if and only if {@code key}
     * is a member of the Set returned from {@link #describe(long)}.
     * </p>
     * 
     * @param key
     * @param record
     * @return a possibly empty Set of values
     */
    public Set<TObject> select(String key, long record);

    /**
     * Fetch {@code key} from {@code record} at {@code timestamp}.
     * <p>
     * This method return the values mapped from {@code key} at
     * {@code timestamp}. The returned Set is nonempty if and only if
     * {@code key} is a member of the Set returned from
     * {@link #describe(long, long)}.
     * </p>
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return a possibly empty Set of values
     */
    public Set<TObject> select(String key, long record, long timestamp);

    /**
     * Start the service.
     */
    public void start();

    /**
     * Stop the service.
     */
    public void stop();

    /**
     * Verify {@code key} equals {@code value} in {@code record}.
     * <p>
     * This method checks that there is <em>currently</em> a mapping from
     * {@code key} to {@code value} in {@code record}. This method has the same
     * affect as calling {@link #select(String, long)}
     * {@link Set#contains(Object)}.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if there is a an association from {@code key} to
     *         {@code value} in {@code record}
     */
    public default boolean verify(String key, TObject value, long record) {
        return verify(Write.notStorable(key, value, record));
    }

    /**
     * Verify {@code key} equals {@code value} in {@code record} at
     * {@code timestamp}.
     * <p>
     * This method checks that there was a mapping from {@code key} to
     * {@code value} in {@code record} at {@code timestamp}. This method has the
     * same affect as calling {@link #select(String, long, long)}
     * {@link Set#contains(Object)}.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     * @param timestamp
     * @return {@code true} if there is an association from {@code key} to
     *         {@code value} in {@code record} at {@code timestamp}
     */
    public default boolean verify(String key, TObject value, long record,
            long timestamp) {
        return verify(Write.notStorable(key, value, record), timestamp);
    }

    /**
     * Verify that {@link Write#getKey()} equals {@link Write#getValue()} in
     * {@link Write#getRecord()}.
     * <p>
     * This method checks that the element described in the {@link Write}
     * <em>currently</em> exists.
     * </p>
     * <p>
     * This method is intended to prevent the creation of a duplicate in
     * contexts where a {@link Write} was already generated (e.g. adding or
     * removing data) and therefore ignores the {@link Write#getType() action}
     * associated with the {@link Write}. It only considers the element (e.g.
     * {@link Write#getKey() key}/{@link Write#getValue()
     * value}/{@link Write#getRecord() record}.). It is possible to use this
     * method for a straight up verify, in which case it is customary to provide
     * a {@link Write#notStorable(String, TObject, long) comparison Write}, but
     * not required.
     * </p>
     * 
     * @param write
     * @return {@code true} if the {@link Write Write's} element currently
     *         exists
     */
    public boolean verify(Write write);

    /**
     * Verify that {@link Write#getKey()} equals {@link Write#getValue()} in
     * {@link Write#getRecord()} at {@code timestamp}.
     * <p>
     * This method checks that the element described in the {@link Write}
     * existed at {@code timestamp}.
     * </p>
     * <p>
     * This method is intended to prevent the creation of a duplicate in
     * contexts where a {@link Write} was already generated (e.g. adding or
     * removing data) and therefore ignores both the {@link Write#getType()
     * action} associated with the {@link Write} and the
     * {@link Write#getVersion()}. It only considers the element (e.g.
     * {@link Write#getKey() key}/{@link Write#getValue()
     * value}/{@link Write#getRecord() record}.). It is possible to use this
     * method for a straight up verify, in which case it is customary to provide
     * a {@link Write#notStorable(String, TObject, long) comparison Write}, but
     * not required.
     * </p>
     * 
     * @param write
     * @param timestamp
     * @return {@code true} if the {@link Write Write's} element existed at
     *         {@code timestamp}
     */
    public boolean verify(Write write, long timestamp);

}
