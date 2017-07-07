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
package com.cinchapi.concourse.server.storage.db;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A wrapper around a collection of Revisions that provides in-memory indices to
 * allow efficient reads. All the Revisions in the Record must have the same
 * locator. They must also have the same key if the Revision is partial.
 * 
 * @author Jeff Nelson
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
     * The master lock for {@link #write} and {@link #read}. DO NOT use this
     * lock directly.
     */
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();

    /**
     * An exclusive lock that permits only one writer and no reader. Use this
     * lock to ensure that no read occurs while data is being appended to the
     * Record.
     */
    private final WriteLock write = master.writeLock();

    /**
     * A shared lock that permits many readers and no writer. Use this lock to
     * ensure that no data append occurs while a read is happening within the
     * Record.
     */
    protected final ReadLock read = master.readLock();

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
    protected final transient HashMap<K, List<CompactRevision<V>>> history = Maps
            .newHashMap();

    /**
     * The version of the Record's most recently appended {@link Revision}.
     */
    private transient long version = 0;

    /**
     * The locator used to identify this Record.
     */
    protected final L locator;

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
    private final Set<V> emptyValues = new EmptyValueSet();

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
        write.lock();
        try {
            // NOTE: We only need to enforce the monotonic increasing constraint
            // for PrimaryRecords because Secondary and Search records will be
            // populated from Blocks that were sorted based primarily on
            // non-version factors.
            Preconditions
                    .checkArgument((this instanceof PrimaryRecord && revision
                            .getVersion() >= version) || true, "Cannot "
                            + "append %s because its version(%s) is lower "
                            + "than the Record's current version(%s). The",
                            revision, revision.getVersion(), version);
            Preconditions.checkArgument(revision.getLocator().equals(locator),
                    "Cannot append %s because it does not belong to %s",
                    revision, this);
            // NOTE: The check below is ignored for a partial SearchRecord
            // instance because they 'key' is the entire search query, but we
            // append Revisions for each term in the query
            Preconditions.checkArgument(
                    (partial && revision.getKey().equals(key)) || !partial
                            || this instanceof SearchRecord,
                    "Cannot append %s because it does not belong to %s",
                    revision, this);
            // NOTE: The check below is ignored for a SearchRecord instance
            // because it will legitimately appear that "duplicate" data has
            // been added if similar data is added to the same key in a record
            // at different times (i.e. adding John Doe and Johnny Doe to the
            // "name")
            Preconditions.checkArgument(this instanceof SearchRecord
                    || isOffset(revision), "Cannot append "
                    + "%s because it represents an action "
                    + "involving a key, value and locator that has not "
                    + "been offset.", revision);

            // Update present index
            Set<V> values = present.get(revision.getKey());
            if(values == null) {
                values = Sets.<V> newLinkedHashSet();
                present.put(revision.getKey(), values);
            }
            if(revision.getType() == Action.ADD) {
                values.add(revision.getValue());
            }
            else {
                values.remove(revision.getValue());
                if(values.isEmpty()) {
                    present.remove(revision.getKey());
                }
            }

            // Update history index
            List<CompactRevision<V>> revisions = history.get(revision.getKey());
            if(revisions == null) {
                revisions = Lists.newArrayList();
                history.put(revision.getKey(), revisions);
            }
            revisions.add(revision.compact());

            // Update metadata
            version = Math.max(version, revision.getVersion());

            // Make revision eligible for GC
            revision = null;

        }
        finally {
            write.unlock();
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

    /**
     * Return the Record's version, which is equal to the largest version of an
     * appended Revision.
     * 
     * @return the version
     */
    public long getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return partial ? Objects.hash(locator, key) : locator.hashCode();
    }

    /**
     * Return {@code true} if this record is empty and contains no present or
     * historical data.
     * 
     * @return {@code true} if the record is empty
     */
    public boolean isEmpty() {
        read.lock();
        try {
            return present.isEmpty() && history.isEmpty();
        }
        finally {
            read.unlock();
        }
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
     * Return the Set of {@code keys} that map to fields which
     * <em>currently</em> contain values.
     * 
     * @return the Set of non-empty field keys
     */
    protected Set<K> describe() {
        read.lock();
        try {
            return Collections.unmodifiableSet(present.keySet()); /* Authorized */
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return the Set of {@code keys} that mapped to fields which contained
     * values at {@code timestamp}.
     * 
     * @param timestamp
     * @return the Set of non-empty field keys
     */
    protected Set<K> describe(long timestamp) {
        read.lock();
        try {
            Set<K> description = Sets.newLinkedHashSet();
            Iterator<K> it = history.keySet().iterator(); /* Authorized */
            while (it.hasNext()) {
                K key = it.next();
                if(!get(key, timestamp).isEmpty()) {
                    description.add(key);
                }
            }
            return description;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Lazily retrieve an unmodifiable view of the current set of values mapped
     * from {@code key}.
     * 
     * @param key
     * @return the set of mapped values for {@code key}
     */
    protected Set<V> get(K key) {
        read.lock();
        try {
            Set<V> values = present.get(key);
            return values != null ? values : emptyValues;
        }
        finally {
            read.unlock();
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
        read.lock();
        try {
            Set<V> values = emptyValues;
            List<CompactRevision<V>> stored = history.get(key);
            if(stored != null) {
                values = Sets.newLinkedHashSet();
                Iterator<CompactRevision<V>> it = stored.iterator();
                while (it.hasNext()) {
                    CompactRevision<V> revision = it.next();
                    if(revision.getVersion() <= timestamp) {
                        if(revision.getType() == Action.ADD) {
                            values.add(revision.getValue());
                        }
                        else {
                            values.remove(revision.getValue());
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
            read.unlock();
        }
    }

    /**
     * Initialize the appropriate data structure for the {@link #present}.
     * 
     * @return the initialized mappings
     */
    protected abstract Map<K, Set<V>> mapType();

    /**
     * Return {@code true} if the action associated with {@code revision}
     * offsets the last action for an equal revision.
     * 
     * @param revision
     * @return {@code true} if the revision if offset.
     */
    private boolean isOffset(Revision<L, K, V> revision) {
        boolean contained = get(revision.getKey())
                .contains(revision.getValue());
        return ((revision.getType() == Action.ADD && !contained) || (revision
                .getType() == Action.REMOVE && contained)) ? true : false;
    }

    /**
     * An empty Set of type V that cannot be modified, but won't throw
     * exceptions. This returned in instances when a key does not map to any
     * values so that the caller can interact with the Set normally without
     * performing validity checks and while preserving data consistency.
     * 
     * @author Jeff Nelson
     */
    private final class EmptyValueSet implements Set<V> {

        @Override
        public boolean add(V e) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            return false;
        }

        @Override
        public void clear() {}

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Iterator<V> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Object[] toArray() {
            return null;
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return null;
        }

    }

}
