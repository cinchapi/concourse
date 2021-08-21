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
package com.cinchapi.concourse.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Multiset;
import com.google.common.collect.SortedMultiset;
import com.google.common.primitives.Ints;

/**
 * Similar to a {@link TreeMultiset} but internally uses a
 * {@link ConcurrentSkipListMap} to provide thread safe access to many
 * concurrent threads.
 * <p>
 * <em>Be warned that several operations are not implemented and will throw a
 * {@link UnsupportedOperationException}</em>.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class ConcurrentSkipListMultiset<T> implements SortedMultiset<T> {

    /**
     * Create a {@link ConcurrentSkipListMultiset} that sorts elements using
     * {@code comparator}.
     * 
     * @param comparator
     * @return the {@link ConcurrentSkipListMultiset}
     */
    public static <T> ConcurrentSkipListMultiset<T> create(
            Comparator<? super T> comparator) {
        return new ConcurrentSkipListMultiset<T>(comparator);
    }

    /**
     * The backing store that holds the data. Each element is mapped to a
     * {@link SkipListEntry} that information about the number of occurrences
     * for the element.
     */
    private final ConcurrentSkipListMap<T, AtomicInteger> backing;

    /**
     * Construct a new instance.
     * 
     * @param comparator
     */
    private ConcurrentSkipListMultiset(Comparator<? super T> comparator) {
        this.backing = new ConcurrentSkipListMap<T, AtomicInteger>(comparator);
    }

    @Override
    public boolean add(T element) {
        return add(element, 1) >= 0;
    }

    @Override
    public int add(T element, int occurrences) {
        AtomicInteger created = new AtomicInteger(0);
        AtomicInteger entry = backing.putIfAbsent(element, created);
        entry = MoreObjects.firstNonNull(entry, created);
        return entry.getAndAdd(occurrences);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        backing.clear();
    }

    @Override
    public Comparator<? super T> comparator() {
        return backing.comparator();
    }

    @Override
    public boolean contains(Object element) {
        return backing.containsKey(element);
    }

    @Override
    public boolean containsAll(Collection<?> elements) {
        for (Object element : elements) {
            if(!backing.containsKey(element)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int count(Object element) {
        AtomicInteger entry = backing.get(element);
        if(entry != null) {
            return entry.get();
        }
        else {
            return 0;
        }
    }

    @Override
    public SortedMultiset<T> descendingMultiset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<T> elementSet() {
        return backing.keySet();
    }

    @Override
    public Set<Entry<T>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Multiset) {
            Multiset<?> other = (Multiset<?>) obj;
            return toString().equals(other.toString());
        }
        else {
            return false;
        }
    }

    @Override
    public Entry<T> firstEntry() {
        Map.Entry<T, AtomicInteger> entry = backing.firstEntry();
        return new SkipListEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public int hashCode() {
        return backing.values().hashCode();
    }

    @Override
    public SortedMultiset<T> headMultiset(T upperBound, BoundType boundType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return backing.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            // This implementation is adapted from that in Guava's
            // MultisetIteratorImpl class

            private boolean canRemove;
            private Map.Entry<T, AtomicInteger> currentEntry;

            private final Iterator<Map.Entry<T, AtomicInteger>> entryIterator = ConcurrentSkipListMultiset.this.backing
                    .entrySet().iterator();
            /** Count of subsequent elements equal to current element */
            private int laterCount;
            private final Multiset<T> multiset = ConcurrentSkipListMultiset.this;
            /** Count of all elements equal to current element */
            private int totalCount;

            @Override
            public boolean hasNext() {
                return laterCount > 0 || entryIterator.hasNext();
            }

            @Override
            public T next() {
                if(!hasNext()) {
                    throw new NoSuchElementException();
                }
                if(laterCount == 0) {
                    currentEntry = entryIterator.next();
                    totalCount = laterCount = currentEntry.getValue().get();
                }
                laterCount--;
                canRemove = true;
                return currentEntry.getKey();
            }

            @Override
            public void remove() {
                Preconditions.checkState(canRemove,
                        "no calls to next() since the last call to remove()");
                if(totalCount == 1) {
                    entryIterator.remove();
                }
                else {
                    multiset.remove(currentEntry.getKey());
                }
                totalCount--;
                canRemove = false;
            }
        };
    }

    @Override
    public Entry<T> lastEntry() {
        Map.Entry<T, AtomicInteger> entry = backing.lastEntry();
        return new SkipListEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public Entry<T> pollFirstEntry() {
        Map.Entry<T, AtomicInteger> entry = backing.pollFirstEntry();
        return new SkipListEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public Multiset.Entry<T> pollLastEntry() {
        Map.Entry<T, AtomicInteger> entry = backing.pollLastEntry();
        return new SkipListEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public boolean remove(Object element) {
        return remove(element, 1) > 0;
    }

    @Override
    public int remove(Object element, int occurrences) {
        AtomicInteger entry = backing.get(element);
        if(entry != null) {
            int count = entry.get();
            entry.addAndGet(-1 * occurrences);
            if(entry.get() <= 0) {
                backing.remove(element);
            }
            return count;
        }
        else {
            return 0;
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setCount(T element, int count) {
        AtomicInteger created = new AtomicInteger(0);
        AtomicInteger entry = backing.putIfAbsent(element, created);
        entry = MoreObjects.firstNonNull(entry, created);
        return entry.getAndSet(count);
    }

    @Override
    public boolean setCount(T element, int oldCount, int newCount) {
        if(oldCount < 1) {
            setCount(element, 0, newCount);
            return true;
        }
        else {
            AtomicInteger entry = backing.get(element);
            if(entry != null) {
                return entry.compareAndSet(oldCount, newCount);
            }
            else {
                return false;
            }
        }
    }

    @Override
    public int size() {
        long size = 0;
        for (Entry<?> entry : entrySet()) {
            size += entry.getCount();
        }
        return Ints.saturatedCast(size);
    }

    @Override
    public SortedMultiset<T> subMultiset(T lowerBound, BoundType lowerBoundType,
            T upperBound, BoundType upperBoundType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedMultiset<T> tailMultiset(T lowerBound, BoundType boundType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("hiding")
    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return backing.values().toString();
    }

    /**
     * An {@link Entry} that is dynamically created and returned from probing
     * methods of this collection.
     *
     */
    @Immutable
    private final class SkipListEntry implements Entry<T> {

        private AtomicInteger count;
        private T element;

        /**
         * Construct a new instance.
         * 
         * @param element
         * @param count
         */
        public SkipListEntry(T element, AtomicInteger count) {
            this.element = element;
            this.count = count;
        }

        /**
         * <p>
         * Copied for {@link Multiset.AbstractEntry#equals(Object)}
         * </p>
         * Indicates whether an object equals this entry, following the behavior
         * specified in {@link Multiset.Entry#equals}.
         */
        @Override
        public boolean equals(@Nullable Object object) {
            if(object instanceof Multiset.Entry) {
                Multiset.Entry<?> that = (Multiset.Entry<?>) object;
                return this.getCount() == that.getCount()
                        && Objects.equal(this.getElement(), that.getElement());
            }
            return false;
        }

        @Override
        public int getCount() {
            return count.get();
        }

        @Override
        public T getElement() {
            return element;
        }

        /**
         * <p>
         * Copied for {@link Multiset.AbstractEntry#hashCode()}
         * </p>
         * Return this entry's hash code, following the behavior specified in
         * {@link Multiset.Entry#hashCode}.
         */
        @Override
        public int hashCode() {
            T e = getElement();
            return ((e == null) ? 0 : e.hashCode()) ^ getCount();
        }

        /**
         * <p>
         * Copied for {@link Multiset.AbstractEntry#toString()}
         * </p>
         * Returns a string representation of this multiset entry. The string
         * representation consists of the associated element if the associated
         * count is one, and otherwise the associated element followed by the
         * characters " x " (space, x and space) followed by the count. Elements
         * and counts are converted to strings as by {@code String.valueOf}.
         */
        @Override
        public String toString() {
            String text = String.valueOf(getElement());
            int n = getCount();
            return (n == 1) ? text : (text + " x " + n);
        }

    }

}
