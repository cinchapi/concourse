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
package org.cinchapi.concourse.server.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.util.Numbers;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A wrapper around a collection of Revisions that provides in-memory indices to
 * allow efficient reads. All the Revisions in the Record must have the same
 * locator. They must also have the same key if the Revision is partial.
 * 
 * @author jnelson
 * @param <L> - the locator type
 * @param <K> - the key type
 * @param <V> - value type
 */
@PackagePrivate
@ThreadSafe
@SuppressWarnings("unchecked")
abstract class Record<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> {

	/**
	 * Return a PrimaryRecord for {@code primaryKey}.
	 * 
	 * @param primaryKey
	 * @return the PrimaryRecord
	 */
	public static PrimaryRecord createPrimaryRecord(PrimaryKey record) {
		return new PrimaryRecord(record, null);
	}

	/**
	 * Return a partial PrimaryRecord for {@code key} in {@record}.
	 * 
	 * @param primaryKey
	 * @param key
	 * @return the PrimaryRecord.
	 */
	public static PrimaryRecord createPrimaryRecordPartial(PrimaryKey record,
			Text key) {
		return new PrimaryRecord(record, key);
	}

	/**
	 * Return a SearchRecord for {@code key}.
	 * 
	 * @param key
	 * @return the SearchRecord
	 */
	public static SearchRecord createSearchRecord(Text key) {
		return new SearchRecord(key, null);
	}

	/**
	 * Return a partial SearchRecord for {@code term} in {@code key}.
	 * 
	 * @param key
	 * @param term
	 * @return the partial SearchRecord
	 */
	public static SearchRecord createSearchRecordPartial(Text key, Text term) {
		return new SearchRecord(key, term);
	}

	/**
	 * Return a SeconaryRecord for {@code key}.
	 * 
	 * @param key
	 * @return the SecondaryRecord
	 */
	public static SecondaryRecord createSecondaryRecord(Text key) {
		return new SecondaryRecord(key, null);
	}

	/**
	 * Return a partial SecondaryRecord for {@code value} in {@code key}.
	 * 
	 * @param key
	 * @param value
	 * @return the SecondaryRecord
	 */
	public static SecondaryRecord createSecondaryRecordPartial(Text key,
			Value value) {
		return new SecondaryRecord(key, value);
	}
	
	/**
	 * Lock used to ensure the object is ThreadSafe. This lock provides access
	 * to a masterLock.readLock()() and masterLock.writeLock()().
	 */
	protected final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

	/**
	 * The index is used to efficiently determine the set of values currently
	 * mapped from a key. The subclass should specify the appropriate type of
	 * key sorting via the returned type for {@link #mapType()}.
	 */
	protected final transient Map<K, Set<V>> present = mapType();

	/**
	 * This index is used to efficiently handle historical reads. Given a
	 * revision (e.g key/value pair), and historical timestamp, we can count the
	 * number of times that the value appears <em>beforehand</em> at determine
	 * if the mapping existed or not.
	 */
	protected final transient HashMap<K, List<Revision<L, K, V>>> history = Maps
			.newHashMap();

	/**
	 * The index is used to efficiently see the number of times that a revision
	 * appears in the Record and therefore determine if the revision (e.g. the
	 * key/value pair) currently exists.
	 */
	private final transient HashMap<Revision<L, K, V>, Integer> counts = Maps
			.newHashMap();

	/**
	 * The version of the Record's most recently appended {@link Revision}.
	 */
	private transient long version = 0;

	/**
	 * The locator used to identify this Record.
	 */
	private final L locator;

	/**
	 * The key used to identify this Record. This value is {@code null} unless
	 * {@link #partial} equals {@code true}.
	 */
	@Nullable
	private final K key;

	/**
	 * Indicates that this Record is partial and only contains Revisions for a
	 * specific {@link #key}.
	 */
	private final boolean partial;

	/**
	 * This set is returned when a key does not map to any values so that the
	 * caller can transparently interact without performing checks or
	 * compromising data consisentcy. This is a member variable (as opposed to
	 * static constant) that is mocked in the constructor because it has a
	 * generic type argument.
	 */
	private final Set<V> emptyValues;

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 * @param key
	 */
	protected Record(L locator, @Nullable K key) {
		this.locator = locator;
		this.key = key;
		this.partial = key == null ? false : true;
		this.emptyValues = Mockito.mock(Set.class);
		Mockito.doReturn(false).when(emptyValues).add((V) Matchers.anyObject());
		Mockito.doReturn(false).when(emptyValues)
				.remove((V) Matchers.anyObject());
		Mockito.doReturn(false).when(emptyValues)
				.contains((V) Matchers.anyObject());
		Mockito.doReturn(Collections.<V> emptyListIterator()).when(emptyValues)
				.iterator();
	}

	/**
	 * Append {@code revision} to the record by updating the in-memory indices.
	 * The {@code revision} must have:
	 * <ul>
	 * <li>a higher version than that of this Record</li>
	 * <li>a locator equal to that of this Record</li>
	 * <li>a key equal to that of this Record if this Record is partial</li>
	 * </ul>
	 * 
	 * @param revision
	 */
	public void append(Revision<L, K, V> revision) {
		masterLock.writeLock().lock();
		try {
			Preconditions.checkArgument(revision.getVersion() > version,
					"Cannot append %s because its version is not higher "
							+ "than the Record's current version", revision);
			Preconditions.checkArgument(revision.getLocator().equals(locator),
					"Cannot append %s because it does not belong "
							+ "to this Record", revision);
			Preconditions.checkArgument(
					(partial && revision.getKey().equals(key)) || !partial,
					"Cannot append %s because it does not "
							+ "belong to This Record", revision);
			// Update revision count
			int count = count(revision) + 1;
			counts.put(revision, count);

			// Update present index
			Set<V> values = present.get(revision.getKey());
			if(values == null) {
				values = Sets.<V> newLinkedHashSet();
				present.put(revision.getKey(), values);
			}
			if(Numbers.isOdd(count)) {
				values.add(revision.getValue());
			}
			else {
				values.remove(revision.getValue());
				if(values.isEmpty()) {
					present.remove(revision.getKey());
				}
			}

			// Update history index
			List<Revision<L, K, V>> revisions = history.get(revision.getKey());
			if(revisions == null) {
				revisions = Lists.newArrayList();
				history.put(revision.getKey(), revisions);
			}
			revisions.add(revision);

			// Update metadata
			version = revision.getVersion();

		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if(obj.getClass() == this.getClass()) {
			Record<L, K, V> other = (Record<L, K, V>) obj;
			return locator.equals(other.locator)
					&& (partial ? key.equals(other.key) : true);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return partial ? Objects.hash(locator, key) : locator.hashCode();
	}

	/**
	 * Return {@code true} if this record is partial.
	 * 
	 * @return {@link #partial}
	 */
	public boolean isPartial() {
		return partial;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " " + (partial ? key + " IN " : "")
				+ locator;
	}

	/**
	 * Lazily retrieve an unmodifiable view of the current set of values mapped
	 * from {@code key}.
	 * 
	 * @param key
	 * @return the set of mapped values for {@code key}
	 */
	protected Set<V> get(K key) {
		masterLock.readLock().lock();
		try {
			return present.containsKey(key) ? Collections
					.unmodifiableSet(present.get(key)) : emptyValues;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	/**
	 * Lazily retrieve the historical set of values for {@code key} at
	 * {@code timestamp}.
	 * 
	 * @param key
	 * @param timestamp
	 * @return the set of mapped values for {@code key} at {@code timestamp}.
	 */
	protected Set<V> get(K key, long timestamp) {
		masterLock.readLock().lock();
		try {
			Set<V> values = emptyValues;
			if(history.containsKey(key)) {
				values = Sets.newLinkedHashSet();
				Iterator<Revision<L, K, V>> it = history.get(key).iterator();
				while (it.hasNext()) {
					Revision<L, K, V> revision = it.next();
					if(revision.getVersion() <= timestamp) {
						if(values.contains(revision.getValue())) {
							values.remove(revision.getValue());
						}
						else {
							values.add(revision.getValue());
						}
					}
					else {
						break;
					}
				}
			}
			return values;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	/**
	 * Initialize the appropriate data structure for the {@link #present}.
	 * 
	 * @return the initialized mappings
	 */
	protected abstract Map<K, Set<V>> mapType();

	/**
	 * Count the number of times that {@code revision} appears in the Record.
	 * This method uses the {@link #counts} index for efficiency. <strong>If
	 * {@code revision} does not appear, this method will create a mapping from
	 * the nonexistent revision to the count of 0.</strong> Therefore, this
	 * method should only be called if it will follow an operation that appends
	 * {@code revision} to the Record.
	 * 
	 * @param revision
	 * @return the number of times {@code revision} appears
	 */
	private int count(Revision<L, K, V> revision) {
		Integer count = counts.get(revision);
		if(count == null) {
			count = 0;
			counts.put(revision, count);
		}
		return count;
	}

}
