/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;

/**
 * A collection of n-gram indexes that enable fulltext infix searching. For
 * every word in a {@link Value}, each substring index is mapped to a
 * {@link Position}. The entire SearchIndex contains a collection of these
 * mappings.
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
public final class CorpusRecord extends AmnesiaRecord<Text, Text, Position> {

    /**
     * Return a {@link CorpusRecord} that holds data for {@code locator}.
     * 
     * @param locator
     * @return the {@link CorpusRecord}
     */
    public static CorpusRecord create(Text locator) {
        return new CorpusRecord(locator, null);
    }

    /**
     * Return a {@link CorpusRecord} that holds data for {@code key} in
     * {@code locator}.
     * 
     * @param locator
     * @param key
     * @return the {@link CorpusRecord}
     */
    public static CorpusRecord createPartial(Text locator, Text key) {
        return new CorpusRecord(locator, key);
    }

    /**
     * DO NOT INVOKE. Use {@link Record#createSearchRecord(Text)} or
     * {@link Record#createSearchRecordPartial(Text, Text)} instead.
     * 
     * @param locator
     * @param key
     */
    @PackagePrivate
    @DoNotInvoke
    CorpusRecord(Text locator, @Nullable Text key) {
        super(locator, key);
    }

    @Override
    protected void checkIsOffsetRevision(
            Revision<Text, Text, Position> revision) { /* no-op */
        // NOTE: The check is ignored for a CorpusRecord instance
        // because it will legitimately appear that "duplicate" data has
        // been added if similar data is added to the same key in a record
        // at different times (i.e. adding John Doe and Johnny Doe to the
        // "name")
    }

    @Override
    protected Map<Text, Set<Position>> $createDataMap() {
        return Maps.newHashMap();
    }

    @Override
    protected Set<Position> setType() {
        // The Record interface mandates that values be contained in a Set to
        // enforce the fact that duplicates cannot exist. CorpusRecords, on the
        // other hand, must legitimately violate this rule because it is
        // possible that a Concourse Record may contain multiple Values that
        // generate duplicate indexes (e.g. "jeff" and "jeffery" would both
        // generate an index at Position 0 in the same record) and the removal
        // of one of those values would erroneously remove infix entries for the
        // other contain values when using a standard Set.
        return new ImpersonatingLinkedHashMulitset<>();
    }

    /**
     * Similar to a {@link Multiset} but conforms to the {@link Set} interface
     * and is therefore considered equal to a {@link Set} where the distinct
     * elements in each are the same.
     *
     * @author Jeff Nelson
     */
    private static class ImpersonatingLinkedHashMulitset<V> implements Set<V> {

        /**
         * The wrapped {@link Multiset}.
         */
        private final Multiset<V> backing = LinkedHashMultiset.create();

        @Override
        public boolean add(V e) {
            return backing.add(e);
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            return backing.addAll(c);
        }

        @Override
        public void clear() {
            backing.clear();
        }

        @Override
        public boolean contains(Object o) {
            return backing.contains(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return backing.containsAll(c);
        }

        @Override
        public boolean equals(Object obj) {
            return backing.elementSet().equals(obj);
        }

        @Override
        public int hashCode() {
            return backing.elementSet().hashCode();
        }

        @Override
        public boolean isEmpty() {
            return backing.isEmpty();
        }

        @Override
        public Iterator<V> iterator() {
            return backing.iterator();
        }

        @Override
        public boolean remove(Object o) {
            return backing.remove(o);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return backing.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return backing.retainAll(c);
        }

        @Override
        public int size() {
            return backing.size();
        }

        @Override
        public Object[] toArray() {
            return backing.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return backing.toArray(a);
        }

        @Override
        public String toString() {
            return backing.elementSet().toString();
        }

    }

}
