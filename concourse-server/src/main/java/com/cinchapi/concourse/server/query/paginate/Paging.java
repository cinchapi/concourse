/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query.paginate;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.cinchapi.concourse.lang.paginate.Page;
import com.google.common.collect.Iterables;

/**
 * A utility for applying {@link Page Pages}.
 *
 * @author Jeff Nelson
 */
public final class Paging {

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param collection
     * @param page
     * @param supplier
     * @return the paginated data
     */
    public static <T, C extends Collection<T>> C paginate(C collection,
            Page page, Supplier<C> supplier) {
        return paginate(collection, page, supplier, (s, i) -> s.add(i));
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param collection
     * @param page
     * @param supplier
     * @param accumulator
     * @return the paginated data
     */
    public static <T, C extends Collection<T>> C paginate(C collection,
            Page page, Supplier<C> supplier, BiConsumer<C, T> accumulator) {
        Iterable<T> stream = paginate(collection, page);
        C $set = supplier.get();
        for (T item : stream) {
            accumulator.accept($set, item);
        }
        return $set;
    }

    /**
     * Return a copy of the original {@code iterable} that only contains that
     * that appears on the provided {@code page}.
     * <p>
     * This method is similar to calling
     * {@link java.util.stream.Stream#limit(long)} and
     * {@link java.util.stream.Stream#skip(long)} on a {@link Stream} except it
     * may have better performance; especially for {@link List lists}.
     * </p>
     * 
     * @param iterable
     * @param page
     * @return the paginated {@link Iterable}
     */
    public static <T> Iterable<T> paginate(Iterable<T> iterable, Page page) {
        if(page.skip() == 0 && page.limit() == Integer.MAX_VALUE) {
            return iterable;
        }
        else {
            // Return a classic iterable instead of a stream based one for
            // performance reasons. For best results, the returned Iterable
            // should be used with a for loop.
            return new Iterable<T>() {

                @Override
                public Iterator<T> iterator() {
                    return iterable instanceof List
                            ? new ListSkipLimitIterator<>((List<T>) iterable,
                                    page.skip(), page.limit())
                            : new DelegateSkipLimitIterator<>(iterable,
                                    page.skip(), page.limit());

                }

                @Override
                public String toString() {
                    return Iterables.toString(this);
                }

            };
        }
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param map
     * @param page
     * @return the paginated data
     */
    public static <K, V> Map<K, V> paginate(Map<K, V> map, Page page) {
        return paginate(map, page, LinkedHashMap::new);
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param map
     * @param page
     * @param supplier
     * @return the paginated data
     */
    public static <K, V, M extends Map<K, V>> M paginate(Map<K, V> map,
            Page page, Supplier<M> supplier) {
        return paginate(map, page, supplier,
                (m, e) -> m.put(e.getKey(), e.getValue()));
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param map
     * @param page
     * @param supplier
     * @param accumulator
     * @return the paginated data
     */
    public static <K, V, M extends Map<K, V>> M paginate(Map<K, V> map,
            Page page, Supplier<M> supplier,
            BiConsumer<Map<K, V>, Entry<K, V>> accumulator) {
        Iterable<Entry<K, V>> stream = paginate(map.entrySet(), page);
        M $map = supplier.get();
        for (Entry<K, V> entry : stream) {
            accumulator.accept($map, entry);
        }
        return $map;
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param set
     * @param page
     * @return the paginated data
     */
    public static <T> Set<T> paginate(Set<T> set, Page page) {
        return paginate(set, page, LinkedHashSet::new);
    }

    private Paging() {/* no-init */}

    /**
     * {@link SkipLimitIterator} that delegates to another {@link Iterator}.
     *
     * @author Jeff Nelson
     */
    private static class DelegateSkipLimitIterator<T>
            extends SkipLimitIterator<T> {

        private Iterator<T> delegate;

        /**
         * Construct a new instance.
         * 
         * @param iterable
         * @param skip
         * @param limit
         */
        public DelegateSkipLimitIterator(Iterable<T> iterable, int skip,
                int limit) {
            super(iterable, skip, limit);
        }

        @Override
        protected boolean $hasNext() {
            if(delegate == null) {
                delegate = iterable.iterator();
            }
            return delegate.hasNext();
        }

        @Override
        protected T $next() {
            if(delegate == null) {
                delegate = iterable.iterator();
            }
            return delegate.next();
        }

    }

    /**
     * {@link SkipLimitIterator} that provides random access to a list.
     *
     * @author Jeff Nelson
     */
    private static class ListSkipLimitIterator<T> extends SkipLimitIterator<T> {

        /**
         * Construct a new instance.
         * 
         * @param iterable
         * @param skip
         * @param limit
         */
        public ListSkipLimitIterator(List<T> iterable, int skip, int limit) {
            super(iterable, skip, limit);
            max = Math.min(max, iterable.size());
        }

        @Override
        protected boolean $hasNext() {
            return index < max;
        }

        @Override
        protected T $next() {
            if(index < max) {
                return ((List<T>) iterable).get(index);
            }
            else {
                throw new NoSuchElementException();
            }
        }

        @Override
        protected void advanceIndex(int min) {
            index = min;
        }

    }

    /**
     * A {@link Iterable} that displays data between bounds defined by a skip
     * and limit.
     *
     * @author Jeff Nelson
     */
    private static abstract class SkipLimitIterator<T> implements Iterator<T> {

        protected int index;
        protected int min;
        protected int max;
        private boolean exhausted = false;

        /**
         * The {@link Iterable} to which the skip/limit is applied.
         */
        protected Iterable<T> iterable;

        /**
         * Construct a new instance.
         * 
         * @param iterable
         * @param skip
         * @param limit
         */
        public SkipLimitIterator(Iterable<T> iterable, int skip, int limit) {
            this.iterable = iterable;
            this.index = 0;
            this.min = skip;
            this.max = limit + skip;
            advanceIndex(min);
            if(max - min == 0) {
                this.exhausted = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !exhausted && $hasNext();
        }

        @Override
        public T next() {
            if(index < max) {
                T next = $next();
                ++index;
                exhausted = index >= max;
                return next;
            }
            else {
                throw new NoSuchElementException();
            }
        }

        /**
         * Return {@true} if the this iterator has a next element within the
         * skip/limit confines.
         * 
         * @return {@code} true if another element remains
         */
        protected abstract boolean $hasNext();

        /**
         * Return the {@code next} element with the skip/limit confines.
         * 
         * @return the next element
         */
        protected abstract T $next();

        /**
         * Advance the {@link #index} to the specific {@code min}.
         * 
         * @param min
         */
        protected void advanceIndex(int min) {
            while (index < min) {
                try {
                    $next();
                    ++index;
                }
                catch (NoSuchElementException e) {
                    exhausted = true;
                    break;
                }
            }
        }

    }

}
