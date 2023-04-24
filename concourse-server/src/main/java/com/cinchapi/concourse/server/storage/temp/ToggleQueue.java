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
package com.cinchapi.concourse.server.storage.temp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

/**
 * A {@link Queue} that assumes equal {@link Write Writes} are
 * {@link #insert(Write) inserted} in {@link Write#inverse() offsetting} order.
 * As a result, this {@link Queue} automatically eliminates extraneous
 * {@link Writes} while preserving the intended state.
 * <p>
 * For example, assume {@code A} defines the
 * {@code key}/{@code value}/{@code record}/{@code timestamp} components of a
 * {@link Write} and the following {@link #insert(Write) insertions} are made:
 * 
 * <pre>
 * REMOVE A
 * ...
 * ADD A
 * </pre>
 * 
 * The first {@link #insert(Write) insertion} intends to change the state by
 * removing {@code A}. Because it is the first {@link Write} that references
 * {@code A} in the {@link ToggleQueue queue} it is initially preserved.
 * However, when second {@link Write} referencing {@code A} is
 * {@link #insert(Write) inserted}, the intended state is that A exists (which
 * is the state prior to {@link #insert(Write) inserting} any {@link Write
 * Writes} into the {@link ToggleQueue queue}. As a result, the
 * {@link ToggleQueue queue} can eliminate both insertions for {@code A} since
 * they are rendered extraneous.
 * </p>
 * <p>
 * As such, each time after the first time an {@link Write#equals(Object) equal}
 * {@link Write} is {@link #insert(Write) inserted} into this {@link ToggleQueue
 * queue}, it causes the removal of all equal instances of the {@link Write}.
 * This ensures that each {@link Write} in the {@link ToggleQueue queue} isn't
 * duplicated while the intended state is preserved.
 * </p>
 *
 * @author Jeff Nelson
 */
public class ToggleQueue extends Queue {

    /**
     * The history of everything that has ever been
     * {@link #insert(Write, boolean) inserted} into this {@link Queue} to
     * facilitate accurate historical lookups via the {@link #iterator()}.
     * <p>
     * NOTE: The {@link ToggleWriteList} is used to access the consolidated
     * state.
     * </p>
     */
    private final List<Write> history;

    /**
     * Construct a new instance.
     * 
     * @param initialSize
     */
    public ToggleQueue(int initialSize) {
        super(new ToggleWriteList(initialSize));
        history = new ArrayList<>();
    }

    @Override
    public boolean insert(Write write, boolean sync) {
        if(super.insert(write, sync)) {
            history.add(write);
            return true;
        }
        else {
            return false;
        }

    }

    @Override
    public Iterator<Write> iterator() {
        // Limbo read methods rely on the iterators to perform historical
        // lookups, so ensure that the #history is used instead of the
        // ToggleWriteList, for accuracy.
        return history.iterator();
    }

    @Override
    public void transform(Function<Write, Write> transformer) {
        ToggleWriteList twl = (ToggleWriteList) getWrites();
        for (Entry<ToggleWriteList.Wrapper, Boolean> entry : twl.items
                .entrySet()) {
            if(entry.getValue()) {
                entry.getKey().write = transformer.apply(entry.getKey().write);
                twl.filtered = null;
            }
        }
    }

    @Override
    protected long getOldestWriteTimestamp() {
        return history.size() > 0 ? history.get(0).getVersion()
                : super.getOldestWriteTimestamp();

    }

    /**
     * A {@link List} that doesn't allow duplicates. Every attempt to
     * {@link #add(Object)} an element will toggle whether it is
     * {@link #contains(Object) contained}.
     *
     *
     * @author Jeff Nelson
     */
    private static class ToggleWriteList implements List<Write> {

        /**
         * A cached version of the {@link #filter() filtered} items.
         */
        private List<Write> filtered;

        /**
         * The items in the list.
         */
        private final LinkedHashMap<Wrapper, Boolean> items;

        /**
         * Construct a new instance.
         * 
         * @param capacity
         */
        private ToggleWriteList(int capacity) {
            items = Maps.newLinkedHashMapWithExpectedSize(capacity);
            filtered = null;
        }

        @Override
        public void add(int index, Write element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(Write e) {
            Wrapper wrapper = new Wrapper(e);
            boolean present = items.getOrDefault(wrapper, false);
            present ^= true;
            items.put(wrapper, present);
            filtered = null;
            return true;
        }

        @Override
        public boolean addAll(Collection<? extends Write> c) {
            for (Write item : c) {
                add(item);
            }
            return true;
        }

        @Override
        public boolean addAll(int index, Collection<? extends Write> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(Object o) {
            return items.getOrDefault(o, false);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            for (Object item : c) {
                if(!contains(item)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Write get(int index) {
            return filter().get(index);
        }

        @Override
        public int indexOf(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public Iterator<Write> iterator() {
            return filter().iterator();
        }

        @Override
        public int lastIndexOf(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListIterator<Write> listIterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListIterator<Write> listIterator(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Write remove(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
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
        public Write set(int index, Write element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return Iterators.size(iterator());
        }

        @Override
        public List<Write> subList(int fromIndex, int toIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray() {
            return filter().toArray();
        }

        @Override
        public <A> A[] toArray(A[] a) {
            return filter().toArray(a);
        }

        /**
         * Return the filtered items.
         * 
         * @return the filtered items
         */
        private List<Write> filter() {
            if(filtered == null) {
                filtered = items.entrySet().stream()
                        .filter(entry -> entry.getValue()).map(Entry::getKey)
                        .map(wrapper -> wrapper.write)
                        .collect(Collectors.toList());
            }
            return filtered;
        }

        /**
         * A wrapper around inserted {@link Write Writes} that takes the
         * {@link Write#getVersion() version} into account for
         * {@link Write#hashCode() hashCode} and {@link Write#equals(Object)
         * equals}.
         *
         * @author Jeff Nelson
         */
        private class Wrapper {

            /**
             * The wrapped {@link Write}.
             */
            private Write write;

            /**
             * Construct a new instance.
             * 
             * @param write
             */
            Wrapper(Write write) {
                this.write = write;
            }

            @Override
            public boolean equals(Object obj) {
                if(obj instanceof Wrapper) {
                    Wrapper other = (Wrapper) obj;
                    return write.getValue().equals(other.write.getValue())
                            && write.getRecord().equals(other.write.getRecord())
                            && write.getKey().equals(other.write.getKey());
                }
                else {
                    return false;
                }
            }

            @Override
            public int hashCode() {
                return Objects.hash(write.getKey(), write.getValue(),
                        write.getRecord());
            }

        }

    }

}
