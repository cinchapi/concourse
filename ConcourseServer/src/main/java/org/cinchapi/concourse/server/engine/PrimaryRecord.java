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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.multithread.Lock;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A logical grouping of data for a single entity.
 * <p>
 * This is the primary view of stored data within Concourse, similar to a Row in
 * a traditional database. PrimaryRecords are designed to efficiently handle
 * direct/non-query reads.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
final class PrimaryRecord extends Record<PrimaryKey, Text, Value> {

	/**
	 * Return the PrimaryRecord that is identified by {@code key}.
	 * 
	 * @param key
	 * @return the PrimaryRecord
	 */
	public static PrimaryRecord loadPrimaryRecord(PrimaryKey key) {
		return Records.open(PrimaryRecord.class, PrimaryKey.class, key);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 */
	@DoNotInvoke
	public PrimaryRecord(PrimaryKey key) {
		super(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T extends Field<Text, Value>> Class<T> fieldImplClass() {
		return (Class<T>) PrimaryField.class;
	}

	@Override
	protected String fileNameExt() {
		return "cpr";
	}

	@Override
	protected Map<Text, Field<Text, Value>> init() {
		return Maps.newHashMap();
	}

	@Override
	protected Class<Text> keyClass() {
		return Text.class;
	}

	/**
	 * Return a log of revision to the entire Record.
	 * 
	 * @return the revision log
	 */
	@GuardedBy("this.readLock")
	@PackagePrivate
	Map<Long, String> audit() {
		Lock lock = readLock();
		try {
			Map<Long, String> audit = Maps.newTreeMap();
			for (Text field : fields().keySet()) {
				audit.putAll(get(field).audit());
			}
			return audit;
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return a log of revisions to the field mapped from {@code key}.
	 * 
	 * @param key
	 * @return the revision log
	 */
	@GuardedBy("Field.readLock")
	@PackagePrivate
	Map<Long, String> audit(Text key) {
		return get(key).audit();
	}

	/**
	 * Return the Set of {@code keys} that map to fields which
	 * <em>currently</em> contain values.
	 * 
	 * @return the Set of non-empty field keys
	 */
	@PackagePrivate
	Set<Text> describe() {
		return describe(false, Storable.NIL);
	}

	/**
	 * Return the Set of {@code keys} that mapped to fields which contained
	 * values at {@code timestamp}.
	 * 
	 * @param timestamp
	 * @return the Set of non-empty field keys
	 */
	@PackagePrivate
	Set<Text> describe(long timestamp) {
		return describe(true, timestamp);
	}

	/**
	 * Return the Set of values <em>currently</em> contained in the field mapped
	 * from {@code key}.
	 * 
	 * @param key
	 * @return the Set of contained values
	 */
	@PackagePrivate
	Set<Value> fetch(Text key) {
		return fetch(key, false, Storable.NIL);
	}

	/**
	 * Return {@code true} if the Record <em>currently</em> contains data.
	 * 
	 * @return {@code true} if {@link #describe()} is not an empty Set
	 */
	@PackagePrivate
	boolean ping() {
		return !describe().isEmpty();
	}

	/**
	 * Return the Set of values contained in the field mapped from {@code key}
	 * at {@code timestamp}.
	 * 
	 * @param key
	 * @param timestamp
	 * @return the Set of contained values
	 */
	@PackagePrivate
	Set<Value> fetch(Text key, long timestamp) {
		return fetch(key, true, timestamp);
	}

	/**
	 * Return {@code true} if {@code value} <em>currently</em> exists in the
	 * field mapped from {@code key}.
	 * 
	 * @param key
	 * @param value
	 * @return {@code true} if {@code key} as {@code value} is a valid mapping
	 */
	@PackagePrivate
	boolean verify(Text key, Value value) {
		return verify(key, value, false, Storable.NIL);
	}

	/**
	 * Return {@code true} if {@code value} existed in the field mapped from
	 * {@code key} at {@code timestamp}
	 * 
	 * @param key
	 * @param value
	 * @param timestamp
	 * @return {@code true} if {@code key} as {@code value} is a valid mapping
	 */
	@PackagePrivate
	boolean verify(Text key, Value value, long timestamp) {
		return verify(key, value, true, timestamp);
	}

	/**
	 * Return the Set of {@code keys} that map to fields which
	 * <em>currently</em> contain values or contained values at
	 * {@code timestamp} if {@code historical} is {@code true}.
	 * 
	 * @param historical - if {@code true}, read from the history, otherwise
	 *            read from the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to read
	 * @return the Set of non-empty field keys
	 */
	@GuardedBy("this.readLock")
	private Set<Text> describe(boolean historical, long timestamp) {
		Lock lock = readLock();
		try {
			Map<Text, Field<Text, Value>> fields = fields();
			Set<Text> description = Sets.newHashSetWithExpectedSize(fields
					.size());
			Iterator<Field<Text, Value>> it = fields.values().iterator();
			while (it.hasNext()) {
				Field<Text, Value> field = it.next();
				if(historical ? !field.getValues(timestamp).isEmpty() : !field
						.isEmpty()) {
					description.add(field.getKey());
				}
			}
			return description;
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return the Set of values <em>currently</em> contained in the field mapped
	 * from {@code key} or contained at {@code timestamp} if {@code historical}
	 * is {@code true}.
	 * 
	 * @param key
	 * @param historical - if {@code true}, read from the history, otherwise
	 *            read from the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to read
	 * @return the Set of contained values
	 */
	@GuardedBy("Field.readLock")
	private Set<Value> fetch(Text key, boolean historical, long timestamp) {
		return Sets.newLinkedHashSet(historical ? get(key).getValues(timestamp)
				: get(key).getValues());
	}

	/**
	 * Return {@code true} if {@code value} <em>currently</em> exists in the
	 * field mapped from {@code key} or existed in that field at
	 * {@code timestamp} if {@code historical} is {@code true}.
	 * 
	 * @param key
	 * @param value
	 * @param historical - if {@code true}, read from the history, otherwise
	 *            read from the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to read
	 * @return {@code true} if {@code key} as {@code value} is a valid mapping
	 */
	@GuardedBy("Field.readLock")
	private boolean verify(Text key, Value value, boolean historical,
			long timestamp) {
		return historical ? get(key).getValues(timestamp).contains(value)
				: get(key).getValues().contains(value);
	}

}
