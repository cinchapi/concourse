/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.engine;

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

/**
 * A {@link Store} is a revisioning service that manages data. The service
 * automatically journals writes so it is possible to read data from both
 * current and previous states and revert changes.
 * 
 * @author jnelson
 */
public interface Store {

	/**
	 * Add {@code key} as {@code value} to {@code record}.
	 * <p>
	 * This method maps {@code key} to {@code value} in {@code record}, if and
	 * only if that mapping does not <em>currently</em> exist (i.e.
	 * {@link #verify(String, Object, long)} is {@code false}). Adding
	 * {@code value} to {@code key} does not replace any existing mappings from
	 * {@code key} in {@code record} because a field may contain multiple
	 * distinct values.
	 * </p>
	 * <p>
	 * To overwrite existing mappings from {@code key} in {@code record}, use
	 * {@link #set(String, Object, long)} instead.
	 * </p>
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is added
	 * @throws UnsupportedOperationException if the implementing class cannot
	 *             process a Write using this method
	 */
	public boolean add(String key, TObject value, long record)
			throws UnsupportedOperationException;

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
	 * Ping {@code record}.
	 * <p>
	 * This method returns {@code true} if and only if {@code record} currently
	 * contains data (i.e. {@link #describe(long)} returns a nonempty Set).
	 * </p>
	 * 
	 * @param record
	 * @return {@code true} if {@code record} is currently not empty
	 */
	public boolean ping(long record);

	/**
	 * Remove {@code key} as {@code value} from {@code record}.
	 * <p>
	 * This method deletes the mapping from {@code key} to {@code value} in
	 * {@code record}, if that mapping <em>currently</em> exists (i.e.
	 * {@link #verify(String, Object, long)} is {@code true}. No other mappings
	 * from {@code key} in {@code record} are affected.
	 * </p>
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is removed
	 * @throws UnsupportedOperationException if the implementing class cannot
	 *             process a Write using this method
	 */
	public boolean remove(String key, TObject value, long record)
			throws UnsupportedOperationException;

	/**
	 * Revert {@code key} in {@code record} to {@code timestamp}.
	 * <p>
	 * This method returns {@code key} in {@code record} to its previous state
	 * at {@code timestamp} by reversing all revisions in the field that have
	 * occurred since. This method <em>does not rollback</em> any revisions, but
	 * creates new revisions that are the reverse of the reverted revisions:
	 * <table>
	 * <tr>
	 * <th>Time</th>
	 * <th>Revision</th>
	 * </tr>
	 * <tr>
	 * <td>T1</td>
	 * <td>ADD A</td>
	 * </tr>
	 * <tr>
	 * <td>T2</td>
	 * <td>ADD B</td>
	 * </tr>
	 * <tr>
	 * <td>T3</td>
	 * <td>REMOVE A</td>
	 * </tr>
	 * <tr>
	 * <td>T4</td>
	 * <td>ADD C</td>
	 * </tr>
	 * <tr>
	 * <td>T5</td>
	 * <td>REMOVE C</td>
	 * </tr>
	 * <tr>
	 * <td>T6</td>
	 * <td>REMOVE B</td>
	 * </tr>
	 * <tr>
	 * <td>T7</td>
	 * <td>ADD D</td>
	 * </tr>
	 * </table>
	 * In the example above, after {@code T7}, the field contains value
	 * {@code D}. If the field is reverted to T3, the following new revisions
	 * are added:
	 * <table>
	 * <tr>
	 * <th>Time</th>
	 * <th>Revision</th>
	 * </tr>
	 * <tr>
	 * <td>T8</td>
	 * <td>REMOVE D</td>
	 * </tr>
	 * <tr>
	 * <td>T9</td>
	 * <td>ADD B</td>
	 * </tr>
	 * <tr>
	 * <td>T10</td>
	 * <td>ADD C</td>
	 * </tr>
	 * <tr>
	 * <td>T11</td>
	 * <td>REMOVE C</td>
	 * </tr>
	 * </table>
	 * After {@code T11}, the field contains value {@code B}. Regardless of the
	 * current state, ever revision to the field exists in history so it is
	 * possible to revert to any previous state, even after reverting to a much
	 * earlier state (i.e. after reverting to {@code T3} it is possible to
	 * revert to {@code T5}).
	 * </p>
	 * 
	 * @param key
	 * @param record
	 * @param timestamp
	 * @throws UnsupportedOperationException if the implementing class cannot
	 *             revert data
	 */
	public void revert(String key, long record, long timestamp)
			throws UnsupportedOperationException;

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
	public boolean verify(String key, TObject value, long record);

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
	public boolean verify(String key, TObject value, long record, long timestamp);

}
