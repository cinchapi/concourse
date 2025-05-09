/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.collect.CoalescableTreeMap;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.search.CompiledInfingram;
import com.cinchapi.concourse.search.Infingram;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.Versioned;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.MultimapViews;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A grouping of data for efficient indirect queries.
 * <p>
 * Each SecondaryRecord maps a value to a set of PrimaryKeys and provides an
 * interface for querying.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public final class IndexRecord extends Record<Text, Value, Identifier> {

    /**
     * Return an {@link IndexRecord} that holds data for {@code locator}.
     * 
     * @param locator
     * @return the {@link IndexRecord}
     */
    public static IndexRecord create(Text locator) {
        return new IndexRecord(locator, null);
    }

    /**
     * Return an {@link IndexRecord} that holds data for {@code key} in
     * {@code locator}.
     * 
     * @param locator
     * @param key
     * @return the {@link IndexRecord}
     */
    public static IndexRecord createPartial(Text locator, Value key) {
        return new IndexRecord(locator, key);
    }

    /**
     * Determines whether a nearby key should be coalesced with another one.
     * <p>
     * To support case insensitive matching, we coalesce character sequences
     * that express the same sentiment, even if they have different case
     * formats.
     * </p>
     */
    private static BiPredicate<Value, Value> CASE_INSENSITIVE_COALESCER = (v1,
            v2) -> v1.getObject().toString()
                    .equalsIgnoreCase(v2.getObject().toString());

    /**
     * A {@link Cube} that supports efficient {@link #gather(Identifier)
     * gathering}.
     */
    private final transient Cube cube = new Cube();

    /**
     * DO NOT INVOKE. Use {@link Record#createSearchRecord(Text)} or
     * {@link Record#createSecondaryRecordPartial(Text, Value)} instead.
     * 
     * @param locator
     * @param key
     */
    @DoNotInvoke
    @PackagePrivate
    IndexRecord(Text locator, @Nullable Value key) {
        super(locator, key);
    }

    /**
     * Explore this record and return a mapping from PrimaryKey to the Values
     * that cause the corresponding records to satisfy {@code operator} in
     * relation to the specified {@code values} at {@code timestamp}.
     * 
     * @param timestamp
     * @param operator
     * @param values
     * @return the relevant data that causes the matching records to satisfy the
     *         criteria
     */
    public Map<Identifier, Set<Value>> findAndGet(long timestamp,
            Operator operator, Value... values) {
        return findAndGet(true, timestamp, operator, values);
    }

    /**
     * Explore this record and return a mapping from PrimaryKey to the
     * Values that cause the corresponding records to satisfy {@code operator}
     * in relation to the specified {@code values}.
     * 
     * @param operator
     * @param values
     * @return the relevant data that causes the matching records to satisfy the
     *         criteria
     */
    public Map<Identifier, Set<Value>> findAndGet(Operator operator,
            Value... values) {
        return findAndGet(false, Versioned.NO_VERSION, operator, values);
    }

    /**
     * Return the PrimaryKeys that satisfied {@code operator} in relation to the
     * specified {@code values} at {@code timestamp}.
     * 
     * @param timestamp
     * @param operator
     * @param values
     * @return the Set of PrimaryKeys that match the query
     */
    public Set<Identifier> find(long timestamp, Operator operator,
            Value... values) {
        return findAndGet(true, timestamp, operator, values).keySet();
    }

    /**
     * Return the PrimaryKeys that <em>currently</em> satisfy {@code operator}
     * in relation to the specified {@code values}.
     * 
     * @param operator
     * @param values
     * @return they Set of PrimaryKeys that match the query
     */
    public Set<Identifier> find(Operator operator, Value... values) {
        return findAndGet(false, 0, operator, values).keySet();
    }

    /**
     * Return all the keys that map to {@code value}.
     * <p>
     * In the broader {@link Database} sense, this method can be used to return
     * all the data "values" that are stored within a data "record" under a data
     * "key" that is equivalent to this {@link IndexRecord IndexRecords's}
     * locator (similar to {@link Database#select(long)}).
     * </p>
     * <p>
     * NOTE: The order of the items in the returned {@link Set} are not
     * necessarily reflective of the order in which they were inserted into the
     * {@link IndexRecord}.
     * </p>
     * 
     * @param record
     * @return a {@link Set} containing all the keys that map to the
     *         {@code value}
     */
    public Set<Value> gather(Identifier record) {
        read.lock();
        try {
            return gather(record, Time.NONE);
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return all the keys that mapped to the {@code record} at
     * {@code timestamp}.
     * <p>
     * In the broader {@link Database} sense, this method can be used to return
     * all the data "values" that were stored within a data "record" under a
     * data "key" that is equivalent to this {@link IndexRecord
     * IndexRecord's} locator at {@code timestamp} (similar to
     * {@link Database#select(long, long)}).
     * </p>
     * <p>
     * NOTE: The order of the items in the returned {@link Set} are not
     * necessarily reflective of the order in which they were inserted into the
     * {@link IndexRecord}.
     * </p>
     * 
     * @param record
     * @return a {@link Set} containing all the keys that map to the
     *         {@code value}
     */
    public Set<Value> gather(Identifier record, long timestamp) {
        Preconditions.checkState(!isPartial(),
                "Cannot gather from a partial Secondary Record.");
        read.lock();
        try {
            Map<Identifier, Set<Value>> slice = cube.slice(timestamp);
            if(slice == null) {
                Set<Value> values = Sets.newHashSet();
                Set<Entry<Value, Set<Identifier>>> entries = timestamp != Time.NONE
                        ? LazyTransformSet.of(history.entrySet(),
                                entry -> new AbstractMap.SimpleImmutableEntry<>(
                                        entry.getKey(),
                                        extractHistoricalValues(
                                                entry.getValue(), timestamp)))
                        : present.entrySet();
                for (Entry<Value, Set<Identifier>> entry : entries) {
                    Value value = entry.getKey();
                    Set<Identifier> records = entry.getValue();
                    if(records.contains(record)) {
                        values.add(value);
                    }
                    for (Identifier $record : records) {
                        if($record.equals(record)) {
                            values.add(value);
                        }
                        cube.put($record, value, timestamp);
                    }
                }
                return values;
            }
            else {
                return slice.getOrDefault(record, ImmutableSet.of());
            }
        }
        finally {
            read.unlock();
        }
    }

    @Override
    protected Map<Value, List<CompactRevision<Identifier>>> $createHistoryMap() {
        return new CoalescableTreeMap<>();
    }

    @Override
    protected Map<Value, Set<Identifier>> $createDataMap() {
        return new CoalescableTreeMap<>();
    }

    @Override
    protected void onAppend(Revision<Text, Value, Identifier> revision) {
        cube.clear();
    }

    /**
     * Return all the key/value mappings of keys that match {@code key} in a
     * case insensitive manner.
     * 
     * @param key
     * @return the matching entries
     */
    private final Map<Value, Set<Identifier>> coalesce(Value key) {
        read.lock();
        try {
            if(key.isCharSequenceType()) {
                return ((CoalescableTreeMap<Value, Set<Identifier>>) present)
                        .coalesce(key, CASE_INSENSITIVE_COALESCER);
            }
            else {
                return ImmutableMap.of(key, super.get(key));
            }
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return all the key/value mappings of keys that matched {@code key} at
     * {@code timestamp} in a case insensitive manner.
     * 
     * @param key
     * @param timestamp
     * @return the matching entries
     */
    private Map<Value, Set<Identifier>> coalesce(Value key, long timestamp) {
        read.lock();
        try {
            Map<Value, Set<Identifier>> data = Maps.newLinkedHashMap();
            Map<Value, List<CompactRevision<Identifier>>> coalesced = ((CoalescableTreeMap<Value, List<CompactRevision<Identifier>>>) history)
                    .coalesce(key, CASE_INSENSITIVE_COALESCER);
            for (Entry<Value, List<CompactRevision<Identifier>>> entry : coalesced
                    .entrySet()) {
                Value stored = entry.getKey();
                List<CompactRevision<Identifier>> revisions = entry.getValue();
                Set<Identifier> values = Sets.newLinkedHashSet();
                Iterator<CompactRevision<Identifier>> it = revisions.iterator();
                while (it.hasNext()) {
                    CompactRevision<Identifier> revision = it.next();
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
                data.put(stored, values);
            }
            return data;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Explore this record and return a mapping from PrimaryKey to the Values
     * that cause the corresponding records to satisfy {@code operator} in
     * relation to the specified {@code values} (and at the specified
     * {@code timestamp} if {@code historical} is {@code true}).
     * 
     * @param historical - if {@code true} query the history, otherwise query
     *            the current state
     * @param timestamp - this value is ignored if {@code historical} is
     *            {@code false}, otherwise this value is the historical
     *            timestamp at which to query the field
     * @param operator
     * @param values
     * @return the relevant data that causes the matching records to satisfy the
     *         criteria
     */
    private Map<Identifier, Set<Value>> findAndGet(boolean historical,
            long timestamp, Operator operator,
            Value... values) { /* Authorized */
        // CON-667: Value ordering for Strings is such that uppercase characters
        // are "smaller" than lowercase ones. Concourse uses case insensitive
        // matching, so we sometimes must modify the input #values in order to
        // ensure that all equal forms of a value are captured or excluded as
        // necessary.
        read.lock();
        try {
            Map<Identifier, Set<Value>> data = Maps.newHashMap();
            Value value = values[0];
            if(operator == Operator.EQUALS) {
                for (Entry<Value, Set<Identifier>> entry : (historical
                        ? coalesce(value, timestamp)
                        : coalesce(value)).entrySet()) {
                    Value stored = entry.getKey();
                    for (Identifier record : entry.getValue()) {
                        MultimapViews.put(data, record, stored);
                    }
                }
            }
            else if(operator == Operator.NOT_EQUALS) {
                for (Value stored : historical ? history.keySet()
                        : present.keySet()) {
                    if(!value.equalsIgnoreCase(stored)) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.GREATER_THAN) {
                value = value.toLowerCase(); // CON-667
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet())
                                .tailSet(value, false)) {
                    if(!historical || stored.compareToIgnoreCase(value) > 0) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
                value = value.toUpperCase(); // CON-667
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet())
                                .tailSet(value, true)) {
                    if(!historical || stored.compareToIgnoreCase(value) >= 0) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.LESS_THAN) {
                value = value.toUpperCase(); // CON-667
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet())
                                .headSet(value, false)) {
                    if(!historical || stored.compareToIgnoreCase(value) < 0) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.LESS_THAN_OR_EQUALS) {
                value = value.toLowerCase(); // CON-667
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet())
                                .headSet(value, true)) {
                    if(!historical || stored.compareToIgnoreCase(value) <= 0) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.BETWEEN) {
                Preconditions.checkArgument(values.length > 1);
                Value value2 = values[1];
                value = value.toUpperCase(); // CON-667
                value2 = value2.toUpperCase(); // CON-667
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet()).subSet(value,
                                true, value2, false)) {
                    if(!historical || (stored.compareTo(value) >= 0
                            && stored.compareTo(value2) < 0)) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.REGEX) {
                Pattern p = Pattern.compile(value.getObject().toString());
                for (Value stored : historical ? history.keySet()
                        : present.keySet()) {
                    Matcher m = p.matcher(stored.getObject().toString());
                    if(m.matches()) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.NOT_REGEX) {
                Pattern p = Pattern.compile(value.getObject().toString());
                for (Value stored : historical ? history.keySet()
                        : present.keySet()) {
                    Matcher m = p.matcher(stored.getObject().toString());
                    if(!m.matches()) {
                        for (Identifier record : historical
                                ? get(stored, timestamp)
                                : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.CONTAINS
                    || operator == Operator.NOT_CONTAINS) {
                Infingram needle = new CompiledInfingram(
                        value.getObject().toString());
                for (Value stored : historical ? history.keySet()
                        : present.keySet()) {
                    if(stored.getType() == Type.STRING) {
                        boolean in = needle.in(stored.getObject().toString());
                        if(in && operator == Operator.CONTAINS || !in) {
                            for (Identifier record : historical
                                    ? get(stored, timestamp)
                                    : get(stored)) {
                                MultimapViews.put(data, record, stored);
                            }
                        }
                    }
                }
            }
            else {
                throw new UnsupportedOperationException();
            }
            return data;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * A {@link Cube} maps a {@link Identifier value} to the set of {@link Value
     * keys} that contain it at a specific timestamp. A {@link Cube} is used to
     * facilitate efficient {@link #gather(Identifier) gathering} of keys that
     * contain values.
     * <p>
     * The {@link #slice(long)} method can be used to get a partition of the
     * cube that contains a view of the gatherable data at a specific timestamp.
     * </p>
     *
     * @author Jeff Nelson
     */
    private static class Cube {

        // NOTE: At the moment, slicing is only supported for the present data.
        // In the future, we may extend the Cube to hold slices for hot
        // historical timestamps.

        /**
         * The slice of present data.
         */
        private transient SoftReference<Map<Identifier, Set<Value>>> slice = new SoftReference<>(
                null);

        /**
         * Remove all the data in the cube.
         */
        public void clear() {
            slice.clear();
        }

        /**
         * Add the {@code value} in {@code record} at {@code timestamp} to the
         * cube.
         * 
         * @param record
         * @param value
         * @param timestamp
         */
        public void put(Identifier record, Value value, long timestamp) {
            if(timestamp == Time.NONE) {
                if(slice.get() == null) {
                    slice = new SoftReference<>(Maps.newHashMap());
                }
                MultimapViews.put(slice.get(), record, value);
            }
        }

        /**
         * Return the cubed data at {@code timestamp}.
         * 
         * @param timestamp
         * @return the cubed data, if it exists, otherwise {@code null}.
         */
        @Nullable
        public Map<Identifier, Set<Value>> slice(long timestamp) {
            if(timestamp == Time.NONE) {
                return slice.get();
            }
            else {
                return null;
            }
        }

    }

}
