/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
import java.util.Iterator;
import java.util.LinkedHashSet;
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
public abstract class Record<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> {

    /**
     * This index is used to efficiently handle historical reads. Given a
     * revision (e.g key/value pair), and historical timestamp, we can count the
     * number of times that the value appears <em>beforehand</em> at determine
     * if the mapping existed or not.
     */
    protected final transient Map<K, List<CompactRevision<V>>> history = $createHistoryMap();

    /**
     * The locator used to identify this Record.
     */
    protected final L locator;

    /**
     * The index is used to efficiently determine the set of values currently
     * mapped from a key. The subclass should specify the appropriate type of
     * key sorting via the returned type for {@link #$createDataMap()}.
     */
    protected final transient Map<K, Set<V>> present = $createDataMap();

    /**
     * The master lock for {@link #write} and {@link #read}. DO NOT use this
     * lock directly.
     */
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();

    /**
     * A shared lock that permits many readers and no writer. Use this lock to
     * ensure that no data append occurs while a read is happening within the
     * Record.
     */
    protected final ReadLock read = master.readLock();

    /**
     * This set is returned when a key does not map to any values so that the
     * caller can transparently interact without performing checks or
     * compromising data consisentcy. This is a member variable (as opposed to
     * static constant) that is mocked in the constructor because it has a
     * generic type argument.
     */
    private final Set<V> emptyValues = new EmptyValueSet();

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
     * An exclusive lock that permits only one writer and no reader. Use this
     * lock to ensure that no read occurs while data is being appended to the
     * Record.
     */
    private final WriteLock write = master.writeLock();

    /**
     * Construct a new instance.
     * 
     * @param locator
     * @param key
     */
    protected Record(L locator, @Nullable K key) {
        this.locator = locator;
        this.key = key;
        this.partial = key != null;
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
            checkIsRelevantRevision(revision);
            checkIsOffsetRevision(revision);

            K key = revision.getKey();
            // Update present index
            Set<V> values = present.get(key);
            if(values == null) {
                values = setType();
                present.put(key, values);
            }
            if(revision.getType() == Action.ADD) {
                values.add(revision.getValue());
            }
            else {
                values.remove(revision.getValue());
                if(values.isEmpty()) {
                    present.remove(key);
                }
            }

            // Update history index
            List<CompactRevision<V>> revisions = history.get(key);
            if(revisions == null) {
                revisions = Lists.newArrayList();
                history.put(key, revisions);
            }
            revisions.add(revision.compact());

            // Run post-append hook
            onAppend(revision);

            // Make revision eligible for GC
            revision = null;
        }
        finally {
            write.unlock();
        }
    }

    /**
     * Return the number of unique keys in this {@link Record}'s history.
     * 
     * @return the {@link Record} cardinality
     */
    public int cardinality() {
        return history.size();
    }

    /**
     * Return {@code true} if {@code value} <em>currently</em> exists in the
     * field mapped from {@code key}.
     * 
     * @param key
     * @param value
     * @return {@code true} if {@code key} as {@code value} is a valid mapping
     */
    public boolean contains(K key, V value) {
        read.lock();
        try {
            return $get(key).contains(value);
        }
        finally {
            read.unlock();
        }
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
    public boolean contains(K key, V value, long timestamp) {
        read.lock();
        try {
            return get(key, timestamp).contains(value);
        }
        finally {
            read.unlock();
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
     * Return a live view of the current set of values mapped from
     * {@code key}.
     * 
     * @param key
     * @return the set of mapped values for {@code key}
     */
    public Set<V> get(K key) {
        read.lock();
        try {
            Set<V> values = $get(key);
            return values == emptyValues ? emptyValues
                    : Collections.unmodifiableSet(values);
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return the historical set of values for {@code key} at {@code timestamp}.
     * 
     * @param key
     * @param timestamp
     * @return the set of mapped values for {@code key} at {@code timestamp}.
     */
    public Set<V> get(K key, long timestamp) {
        read.lock();
        try {
            List<CompactRevision<V>> stored = history.get(key);
            return extractHistoricalValues(stored, timestamp);
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return a live view of all the data that is presently contained in this
     * record.
     * 
     * @return the data
     */
    public Map<K, Set<V>> getAll() {
        read.lock();
        try {
            return Collections.unmodifiableMap(present);
        }
        finally {
            read.unlock();
        }

    }

    /**
     * Return a view of all the data that was contained in this record at
     * {@code timestamp}.
     * 
     * @param timestamp
     * @return the data
     */
    public Map<K, Set<V>> getAll(long timestamp) {
        read.lock();
        try {
            Map<K, Set<V>> data = Maps.newLinkedHashMap();
            for (K key : keys(timestamp)) {
                data.put(key, get(key, timestamp));
            }
            return data;
        }
        finally {
            read.unlock();
        }
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

    /**
     * Return a live view of the keys that map to fields which
     * <em>presently</em> contain values.
     * 
     * @return the Set of non-empty field keys
     */
    public Set<K> keys() {
        read.lock();
        try {
            return Collections.unmodifiableSet(present.keySet());
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
    public Set<K> keys(long timestamp) {
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + " " + (partial ? key + " IN " : "")
                + locator;
    }

    /**
     * Initialize the appropriate data structure for the {@link #present}.
     * 
     * @return the initialized mappings
     */
    protected abstract Map<K, Set<V>> $createDataMap();

    /**
     * Initialize the appropriate data structure for the {@link #history}.
     * 
     * @return the initialized mappings
     */
    protected Map<K, List<CompactRevision<V>>> $createHistoryMap() {
        return Maps.newHashMap();
    }

    /**
     * Check that {@code revision} is {@link #isOffset(Revision) offset} within
     * this {@link Record} and throw and {@link IllegalArgumentException} if
     * that is not the case.
     * <p>
     * NOTE: Specific {@link Record} subtypes may override this method to ignore
     * the check if it is not valid for their operations.
     * </p>
     * 
     * @param revision
     * @throws IllegalArgumentException
     */
    protected void checkIsOffsetRevision(Revision<L, K, V> revision)
            throws IllegalArgumentException {
        Preconditions.checkArgument(isOffset(revision),
                "Cannot append %s because it represents an action "
                        + "involving a key, value and locator that has not "
                        + "been offset.",
                revision);
    }

    /**
     * Check that {@code revision} is relevant to this {@link Record} and throw
     * and {@link IllegalArgumentException} if that is not the case.
     * <p>
     * NOTE: Specific {@link Record} subtypes may override this method to ignore
     * the check if it is not valid for their operations.
     * </p>
     * 
     * @param revision
     * @throws IllegalArgumentException
     */
    protected void checkIsRelevantRevision(Revision<L, K, V> revision)
            throws IllegalArgumentException {
        Preconditions.checkArgument(revision.getLocator().equals(locator),
                "Cannot append %s because it does not belong to %s", revision,
                this);
        Preconditions.checkArgument(
                (partial && revision.getKey().equals(key)) || !partial,
                "Cannot append %s because it does not belong to %s", revision,
                this);
    }

    /**
     * Extract the {@code values} from the list of {@link CompactRevision
     * revisions} that occurred in or before {@code timestamp}.
     * 
     * @param revisions
     * @param timestamp
     * @return the set of values at {@code timestamp}.
     */
    protected Set<V> extractHistoricalValues(List<CompactRevision<V>> revisions,
            long timestamp) {
        Set<V> values = emptyValues;
        if(revisions != null) {
            values = Sets.newLinkedHashSet();
            Iterator<CompactRevision<V>> it = revisions.iterator();
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

    /**
     * Logic that the subclass can run after the {@code revision} is
     * {@link #append(Revision) appended}.
     */
    protected void onAppend(Revision<L, K, V> revision) {/* no-op */}

    /**
     * Initialized the appropriate data structure for the {@link Set} that is
     * mapped from each {@code K key} in the {@link #present} collection to
     * contain all associated values.
     * 
     * @return an initialized value set
     */
    protected Set<V> setType() {
        return new LinkedHashSet<>();
    }

    /**
     * Implementation of {@link #get(Byteable)} for internal use.
     * <p>
     * Does not grab the {@link #read read lock} or create an unmodifiable view
     * of the returned values.
     * </p>
     * 
     * @param key
     * @return the set of mapped values for {@code key}
     */
    private Set<V> $get(K key) {
        return present.getOrDefault(key, emptyValues);
    }

    /**
     * Return {@code true} if the action associated with {@code revision}
     * offsets the last action for an equal revision.
     * 
     * @param revision
     * @return {@code true} if the revision if offset.
     */
    private boolean isOffset(Revision<L, K, V> revision) {
        boolean contained = $get(revision.getKey())
                .contains(revision.getValue());
        return ((revision.getType() == Action.ADD && !contained)
                || (revision.getType() == Action.REMOVE && contained));
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
