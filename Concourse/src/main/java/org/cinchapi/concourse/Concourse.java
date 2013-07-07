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
package org.cinchapi.concourse;

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.joda.time.DateTime;

/**
 * <p>
 * Concourse is a schemaless distributed version control database with
 * serializable transactions and full-text search. Concourse provides a more
 * intuitive approach to data management that is easy to deploy, access and
 * scale with minimal tuning while also maintaining the referential integrity,
 * atomicity, consistency, isolability and durability found in traditional
 * database systems.
 * </p>
 * <h2>Data Model</h2>
 * <p>
 * The Concourse data model is lightweight and flexible which enables it to
 * support any kind of data at very large scales. Concourse trades unnecessary
 * structural notions of predefined schemas, tables and indexes for a more
 * natural modeling of data based only on the following concepts:
 * </p>
 * <p>
 * <ul>
 * <li><strong>Record</strong> &mdash; A logical grouping of information about a
 * single person, place, thing (i.e. object). Each record is an independent
 * first class item in the database.
 * <li><strong>Primary Key</strong> &mdash; A value that is used to identify a
 * single Record. Each Record has a unique Primary Key.
 * <li><strong>Key</strong> &mdash; A label that maps to one or more distinct
 * Values. A Record can have many different Keys. And since Records are
 * independent, the Keys in one Record do not affect the Keys in any other
 * Record.
 * <li><strong>Field</strong> &mdash; A division within a Record that is used to
 * store the data mapped from a single Key.</li>
 * <li><strong>Value</strong> &mdash; A dynamically typed quantity that is
 * mapped from a Key and stored in a Field.
 * </ul>
 * </p>
 * <h4>Data Types</h4>
 * <p>
 * Concourse is a dynamically typed system. Values are cast as one of the
 * following native types: boolean, double, float, integer, long, link, string
 * (UTF-8) if possible, otherwise the {@link #toString()} method of the value is
 * stored. Therefore non-primitive objects should implement a string
 * representation from which the object can be reconstructed (i.e. JSON, base64
 * encoded binary, etc).
 * </p>
 * <h4>Links</h4>
 * <p>
 * Concourse supports links between Records and enforces referential integrity
 * with the ability to map a key in one Record to the PrimaryKey of another
 * Record using a {@link Pointer}.
 * 
 * <pre>
 * concourse.add("name", "John Doe", 1);
 * concourse.add("name", "Jane Doe", 2);
 * concourse.add("spouse", Pointer.to(1), 2); # adds a link from Record 2 to Record 1
 * concourse.add("spouse", Pointer.to(2), 1); # adds a link from Record 1 to Record 2
 * </pre>
 * 
 * </p>
 * <h2>Transactions</h2>
 * <p>
 * By default, Concourse conducts every operation in {@code autocommit} mode
 * where every change is immediately written. Concourse also supports the
 * ability to group and stage operations in transactions that are atomic,
 * consistent, isolated, and durable (ACID).
 * 
 * <pre>
 * concourse.stage(); # starts the transaction
 * concourse.add("name", "John Doe", 1);
 * concourse.verify("name", "John Doe", 1); # returns {@code true} for this client, but {@code false} for others since the operation is isolated
 * concourse.commit(); # permanently writes changes for other clients to see
 * </pre>
 * 
 * </p>
 * 
 * @author jnelson
 */
public abstract class Concourse {

	/**
	 * Create a new connection to the Concourse server specified in
	 * {@code concourse.prefs} and return a handler to facilitate database
	 * interaction.
	 * 
	 * @return the Concourse connection handler
	 */
	public static Concourse connect() {
		return new ConcourseHandler();
	}

	/**
	 * Represents a request to respond to a query using the current state as
	 * opposed to the history.
	 */
	private static DateTime now = new DateTime(0);

	/**
	 * Discard any changes that are currently staged for commit.
	 * <p>
	 * After this function returns, Concourse will return to {@code autocommit}
	 * mode and all subsequent changes will be immediately written to the
	 * database.
	 * </p>
	 */
	public abstract void abort();

	/**
	 * Add {@code key} as {@code value} in {@code record} if no such mapping
	 * currently exist. No other mappings are affected because a field may
	 * contain multiple distinct values.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is added
	 */
	public abstract boolean add(String key, Object value, long record);

	/**
	 * Audit {@code record} and return a log of revisions.
	 * 
	 * @param record
	 * @return a mapping of timestamps to revision descriptions
	 */
	public abstract Map<DateTime, String> audit(long record);

	/**
	 * Audit {@code key} in {@code record} and return a log of revisions.
	 * 
	 * @param key
	 * @param record
	 * @return a mapping of timestamps to revision descriptions
	 */
	public abstract Map<DateTime, String> audit(String key, long record);

	/**
	 * Attempt to permanently commit all the changes that are currently staged.
	 * This function returns {@code true} if and only if all the changes can be
	 * successfully applied to the database.Otherwise, this function returns
	 * {@code false} and all the changes are aborted.
	 * <p>
	 * After this function returns, Concourse will return to {@code autocommit}
	 * mode and all subsequent changes will be immediately written to the
	 * database.
	 * </p>
	 */
	public abstract boolean commit();

	/**
	 * Describe {@code record} and return the keys for fields that currently
	 * contain at least one value. If there are no such fields, an empty Set is
	 * returned.
	 * 
	 * @param record
	 * @return the keys for populated fields
	 */
	public Set<String> describe(long record) {
		return describe(record, now);
	}

	/**
	 * Describe {@code record} at {@code timestamp} and return the keys foe
	 * fields that contained at least one value. If there were no such fields,
	 * an empty Set is returned.
	 * 
	 * @param record
	 * @param timestamp
	 * @return the keys for populated fields
	 */
	public abstract Set<String> describe(long record, DateTime timestamp);

	/**
	 * Fetch {@code key} from {@code record} and return the values currently
	 * contained in the mapped field. If the field is empty or does not exist,
	 * an empty Set is returned.
	 * 
	 * @param key
	 * @param record
	 * @return the contained values
	 */
	public Set<Object> fetch(String key, long record) {
		return fetch(key, record, now);
	}

	/**
	 * Fetch {@code key} from {@code record} at {@code timestamp} and return the
	 * values that were contained in the mapped field. If the field was empty of
	 * did not exist, an empty Set is returned.
	 * 
	 * @param key
	 * @param record
	 * @param timestamp
	 * @return the contained values
	 */
	public abstract Set<Object> fetch(String key, long record,
			DateTime timestamp);

	/**
	 * Find {@code key} {@code operator} {@code values} at {@code timestamp} and
	 * return the records that satisfied the criteria. This is analogous to a
	 * SELECT query in a RDBMS. If there were no records that matched the
	 * criteria, an empty Set is returned.
	 * 
	 * @param timestamp
	 * @param key
	 * @param operator
	 * @param values
	 * @return the records that match the criteria
	 */
	public abstract Set<Long> find(DateTime timestamp, String key,
			Operator operator, Object... values);

	/**
	 * Find {@code key} {@code operator} {@code values} and return the records
	 * that satisfy the criteria. This is analogous to a SELECT query in a
	 * RDBMS. If there are now records that match the criteria, an empty Set is
	 * returned.
	 * 
	 * @param key
	 * @param operator
	 * @param values
	 * @return the records that match the criteria
	 */
	public Set<Long> find(String key, Operator operator, Object... values) {
		return find(now, key, operator, values);
	}

	/**
	 * Ping {@code record} and return {@code true} if there is
	 * <em>currently</em> at least one populated field.
	 * 
	 * @param record
	 * @return {@code true} if {@code record} currently contains data
	 */
	public abstract boolean ping(long record);

	/**
	 * Remove {@code key} as {@code value} from {@code record} if the mapping
	 * currently exists. No other mappings are affected.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is removed
	 */
	public abstract boolean remove(String key, Object value, long record);

	/**
	 * Revert {@code key} in {@code record} to {@code timestamp}. This method
	 * restores the field to its state at {@code timestamp} by reversing all
	 * revisions that have occurred since.
	 * <p>
	 * Please note that this method <strong>does not</strong> {@code rollback}
	 * any revisions, but creates <em>new</em> revisions that are the inverse of
	 * all revisions since {@code timestamp} in reverse order.
	 * </p>
	 * 
	 * @param key
	 * @param record
	 * @param timestamp
	 */
	public abstract void revert(String key, long record, DateTime timestamp);

	/**
	 * Search {@code key} for {@code query} and return the records that match
	 * the fulltext query. If there are no such records, an empty Set is
	 * returned.
	 * 
	 * @param key
	 * @param query
	 * @return the records that match the query
	 */
	public abstract Set<Long> search(String key, String query);

	/**
	 * Set {@code key} as {@code value} in {@code record}. This is a convenience
	 * method that removes the values currently contained in the field and adds
	 * {@code value}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the old mappings are removed and the new one is
	 *         added
	 */
	public boolean set(String key, Object value, long record) {
		Set<Object> values = fetch(key, record);
		for (Object v : values) {
			remove(key, v, record);
		}
		return add(key, value, record);
	}

	/**
	 * Turn on {@code staging} mode so that all subsequent changes are
	 * collected
	 * in a staging area before possibly being committed to the database. Staged
	 * operations are guaranteed to be reliable, all or nothing units of work
	 * that allow correct recovery from failures and provide isolation between
	 * clients so the database is always in a consistent state (i.e. a
	 * transaction).
	 * <p>
	 * After this method returns, all subsequent operations will be done in
	 * {@code staging} mode until either {@link #abort()} or {@link #commit()}
	 * is invoked.
	 * </p>
	 */
	public abstract void stage();

	/**
	 * Verify {@code key} equals {@code value} in {@code record} and return
	 * {@code true} if {@code value} is currently contained in the field.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping exists
	 */
	public boolean verify(String key, Object value, long record) {
		return verify(key, value, record, now);
	}

	/**
	 * Verify {@code key} equaled {@code value} in {@code record} at
	 * {@code timestamp} and return {@code true} if {@code value} was contained
	 * in the field.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @param timestamp
	 * @return {@code true} if the mapping existed
	 */
	public abstract boolean verify(String key, Object value, long record,
			DateTime timestamp);

}
