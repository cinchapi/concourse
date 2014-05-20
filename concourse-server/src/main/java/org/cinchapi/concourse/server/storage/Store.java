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
package org.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

/**
 * <p>
 * A {@link Store} is a revisioning service that defines primitive operations to
 * read data from both current and previous states.
 * </p>
 * <p>
 * A {@code Store} can acquire data in one of two ways: directly if it is a
 * {@link Limbo} or <em>eventually</em> if it is a {@link PermanentStore}.
 * </p>
 * <p>
 * In general, a {@code Limbo} and {@code PermanentStore} work together in a
 * {@link BufferedStore} to improve write performance by immediately committing
 * writes into a durable buffer before batch indexing them in the background.
 * </p>
 * 
 * @author jnelson
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
    public Map<String, Set<TObject>> browse(long record);

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
    public Map<String, Set<TObject>> browse(long record, long timestamp);

    /**
     * Describe {@code record}.
     * <p>
     * This method returns the keys for fields that currently have at least one
     * mapped value in {@code record} such that {@link #fetch(String, long)} for
     * each key is nonempty. If there are no such keys, an empty Set is
     * returned.
     * </p>
     * 
     * @param record
     * @return a possibly empty Set of keys
     */
    public Set<String> describe(long record);

    /**
     * Describe {@code record} at {@code timestamp}.
     * <p>
     * This method returns the keys for fields that had at least one mapped
     * value in {@code record} at {@code timestamp} such that
     * {@link #fetch(String, long, long)} for each key at {@code timestamp} is
     * nonempty. If there are no such keys, an empty Set is returned.
     * </p>
     * 
     * @param record
     * @param timestamp
     * @return a possibly empty Set of keys
     */
    public Set<String> describe(long record, long timestamp);

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
    public Set<TObject> fetch(String key, long record);

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
    public Set<TObject> fetch(String key, long record, long timestamp);

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
    public Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values);

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
    public Set<Long> find(String key, Operator operator, TObject... values);

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
     * affect as calling {@link #fetch(String, long)}
     * {@link Set#contains(Object)}.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if there is a an association from {@code key} to
     *         {@code value} in {@code record}
     */
    public boolean verify(short uid, String key, TObject value, long record);

    /**
     * Verify {@code key} equals {@code value} in {@code record} at
     * {@code timestamp}.
     * <p>
     * This method checks that there was a mapping from {@code key} to
     * {@code value} in {@code record} at {@code timestamp}. This method has the
     * same affect as calling {@link #fetch(String, long, DateTime)}
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
    public boolean verify(short uid, String key, TObject value, long record, long timestamp);

}
