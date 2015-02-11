/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.temp;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.cinchapi.concourse.util.MultimapViews;
import org.cinchapi.concourse.util.TMaps;
import org.cinchapi.concourse.util.TStrings;
import org.cinchapi.concourse.server.model.TObjectSorter;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.BaseStore;
import org.cinchapi.concourse.server.storage.PermanentStore;
import org.cinchapi.concourse.server.storage.VersionGetter;
import org.cinchapi.concourse.server.storage.Versioned;
import org.cinchapi.concourse.server.storage.db.Database;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;
import org.cinchapi.concourse.time.Time;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.google.common.collect.Maps.newLinkedHashMap;

/**
 * {@link Limbo} is a lightweight in-memory proxy store that is a suitable cache
 * or fast, albeit temporary, store for data that will eventually be persisted
 * to a {@link PermanentStore}.
 * <p>
 * The store is designed to write data very quickly <strong>
 * <em>at the expense of much slower read time.</em></strong> {@code Limbo} does
 * not index<sup>1</sup> any of the data it stores, so reads are not as
 * efficient as they would normally be in the {@link Database}.
 * </p>
 * <p>
 * This class provides naive read implementations for the methods specified in
 * the {@link WritableStore} interface, but the subclass is free to override
 * those methods to provide smarter implementations of introduce concurrency
 * controls.
 * </p>
 * <sup>1</sup> - All reads are O(n) because {@code Limbo} uses an
 * {@link #iterator()} to traverse the {@link Write} objects that it stores.
 * 
 * @author jnelson
 */
@NotThreadSafe
public abstract class Limbo extends BaseStore implements
        Iterable<Write>,
        VersionGetter {

    /**
     * Return {@code true} if {@code input} matches {@code operator} in relation
     * to {@code values}.
     * 
     * @param input
     * @param operator
     * @param values
     * @return {@code true} if {@code input} matches
     */
    private static boolean matches(Value input, Operator operator,
            TObject... values) {
        Value v1 = Value.wrap(values[0]);
        switch (operator) {
        case EQUALS:
            return v1.equals(input);
        case NOT_EQUALS:
            return !v1.equals(input);
        case GREATER_THAN:
            return v1.compareTo(input) < 0;
        case GREATER_THAN_OR_EQUALS:
            return v1.compareTo(input) <= 0;
        case LESS_THAN:
            return v1.compareTo(input) > 0;
        case LESS_THAN_OR_EQUALS:
            return v1.compareTo(input) >= 0;
        case BETWEEN:
            Preconditions.checkArgument(values.length > 1);
            Value v2 = Value.wrap(values[1]);
            return v1.compareTo(input) <= 0 && v2.compareTo(input) > 0;
        case REGEX:
            return input.getObject().toString()
                    .matches(v1.getObject().toString());
        case NOT_REGEX:
            return !input.getObject().toString()
                    .matches(v1.getObject().toString());
        default:
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A Predicate that is used to filter out empty sets.
     */
    private static final Predicate<Set<? extends Object>> emptySetFilter = new Predicate<Set<? extends Object>>() {

        @Override
        public boolean apply(@Nullable Set<? extends Object> input) {
            return !input.isEmpty();
        }

    };

    /**
     * The writeLock ensures that only a single writer can modify the state of
     * the store, without affecting any readers. The subclass should, at a
     * minimum, use this lock in the {@link #insert(Write)} method.
     */
    protected final ReentrantLock writeLock = new ReentrantLock();

    @Override
    public Map<Long, String> audit(long record) {
        Map<Long, String> audit = Maps.newTreeMap();
        Iterator<Write> it = iterator();
        while (it.hasNext()) {
            Write write = it.next();
            if(write.getRecord().longValue() == record) {
                audit.put(write.getVersion(), write.toString());
            }
        }
        return audit;

    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        Map<Long, String> audit = Maps.newTreeMap();
        Iterator<Write> it = iterator();
        while (it.hasNext()) {
            Write write = it.next();
            if(write.getKey().toString().equals(key)
                    && write.getRecord().longValue() == record) {
                audit.put(write.getVersion(), write.toString());
            }
        }
        return audit;

    }

    @Override
    public Map<String, Set<TObject>> browse(long record) {
        return browse(record, Time.now());
    }

    @Override
    public Map<String, Set<TObject>> browse(long record, long timestamp) {
        Map<String, Set<TObject>> context = Maps
                .newTreeMap(new Comparator<String>() {

                    @Override
                    public int compare(String s1, String s2) {
                        return s1.compareToIgnoreCase(s2);
                    }

                });
        return browse(record, timestamp, context);
    }

    /**
     * Calculate the browsable view of {@code record} at {@code timestamp} using
     * prior {@code context} as if it were also a part of the Buffer.
     * 
     * @param key
     * @param timestamp
     * @param context
     * @return a possibly empty Map of data
     */
    public Map<String, Set<TObject>> browse(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        if(timestamp >= getOldestWriteTimstamp()) {
            Iterator<Write> it = iterator();
            while (it.hasNext()) {
                Write write = it.next();
                if(write.getRecord().longValue() == record
                        && write.getVersion() <= timestamp) {
                    Set<TObject> values;
                    values = context.get(write.getKey().toString());
                    if(values == null) {
                        values = Sets.newHashSet();
                        context.put(write.getKey().toString(), values);
                    }
                    if(write.getType() == Action.ADD) {
                        values.add(write.getValue().getTObject());
                    }
                    else {
                        values.remove(write.getValue().getTObject());
                    }
                }
                else if(write.getVersion() > timestamp) {
                    break;
                }
                else {
                    continue;
                }
            }
        }
        return Maps.newTreeMap((SortedMap<String, Set<TObject>>) Maps
                .filterValues(context, emptySetFilter));
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        return browse(key, Time.now());
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Map<TObject, Set<Long>> context = Maps
                .newTreeMap(TObjectSorter.INSTANCE);
        return browse(key, timestamp, context);
    }

    /**
     * Calculate the browsable view of {@code key} at {@code timestamp} using
     * prior {@code context} as if it were also a part of the Buffer.
     * 
     * @param key
     * @param timestamp
     * @param context
     * @return a possibly empty Map of data
     */
    public Map<TObject, Set<Long>> browse(String key, long timestamp,
            Map<TObject, Set<Long>> context) {
        if(timestamp >= getOldestWriteTimstamp()) {
            Iterator<Write> it = iterator();
            while (it.hasNext()) {
                Write write = it.next();
                if(write.getKey().toString().equals(key)
                        && write.getVersion() <= timestamp) {
                    Set<Long> records = context.get(write.getValue()
                            .getTObject());
                    if(records == null) {
                        records = Sets.newLinkedHashSet();
                        context.put(write.getValue().getTObject(), records);
                    }
                    if(write.getType() == Action.ADD) {
                        records.add(write.getRecord().longValue());
                    }
                    else {
                        records.remove(write.getRecord().longValue());
                    }
                }
                else if(write.getVersion() > timestamp) {
                    break;
                }
                else {
                    continue;
                }
            }
        }
        return Maps.newTreeMap((SortedMap<TObject, Set<Long>>) Maps
                .filterValues(context, emptySetFilter));
    }

    /**
     * Calculate the description for {@code record} using prior {@code context}
     * as if it were also a part of the Buffer.
     * 
     * @param record
     * @param timestamp
     * @param context
     * @return a possibly empty Set of keys
     */
    public Set<String> describe(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        if(timestamp >= getOldestWriteTimstamp()) {
            Iterator<Write> it = iterator();
            while (it.hasNext()) {
                Write write = it.next();
                if(write.getRecord().longValue() == record
                        && write.getVersion() <= timestamp) {
                    Set<TObject> values;
                    values = context.get(write.getKey().toString());
                    if(values == null) {
                        values = Sets.newHashSet();
                        context.put(write.getKey().toString(), values);
                    }
                    if(write.getType() == Action.ADD) {
                        values.add(write.getValue().getTObject());
                    }
                    else {
                        values.remove(write.getValue().getTObject());
                    }
                }
                else if(write.getVersion() > timestamp) {
                    break;
                }
                else {
                    continue;
                }
            }
        }
        return newLinkedHashMap(Maps.filterValues(context, emptySetFilter))
                .keySet();
    }

    /**
     * This is an implementation of the {@code findAndBrowse} routine that takes
     * in a prior {@code context}. Find and browse will return a mapping from
     * records that match a criteria (expressed as {@code key} filtered by
     * {@code operator} in relation to one or more {@code values}) to the set of
     * values that cause that record to match the criteria.
     * 
     * @param context
     * @param timestamp
     * @param key
     * @param operator
     * @param values
     * @return the relevant data for the records that satisfy the find query
     */
    public Map<Long, Set<TObject>> explore(Map<Long, Set<TObject>> context,
            long timestamp, String key, Operator operator, TObject... values) {
        if(timestamp >= getOldestWriteTimstamp()) {
            Iterator<Write> it = iterator();
            while (it.hasNext()) {
                Write write = it.next();
                long record = write.getRecord().longValue();
                if(write.getVersion() <= timestamp) {
                    if(write.getKey().toString().equals(key)
                            && matches(write.getValue(), operator, values)) {
                        if(write.getType() == Action.ADD) {
                            MultimapViews.put(context, record, write.getValue()
                                    .getTObject());
                        }
                        else {
                            MultimapViews.remove(context, record, write
                                    .getValue().getTObject());
                        }
                    }
                }
                else {
                    break;
                }
            }
        }
        return TMaps.asSortedMap(context);
    }

    @Override
    public Set<TObject> fetch(String key, long record) {
        return fetch(key, record, Time.now());
    }

    @Override
    public Set<TObject> fetch(String key, long record, long timestamp) {
        return fetch(key, record, timestamp, Sets.<TObject> newLinkedHashSet());
    }

    /**
     * Fetch the values mapped from {@code key} in {@code record} at
     * {@code timestamp} using prior {@code context} as if it were also a part
     * of the Buffer.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @param context
     * @return the values
     */
    public Set<TObject> fetch(String key, long record, long timestamp,
            Set<TObject> context) {
        if(timestamp >= getOldestWriteTimstamp()) {
            Iterator<Write> it = iterator();
            while (it.hasNext()) {
                Write write = it.next();
                if(write.getVersion() <= timestamp) {
                    if(key.equals(write.getKey().toString())
                            && record == write.getRecord().longValue()) {
                        if(write.getType() == Action.ADD) {
                            context.add(write.getValue().getTObject());
                        }
                        else {
                            context.remove(write.getValue().getTObject());
                        }
                    }
                }
                else {
                    break;
                }
            }
        }
        return context;
    }

    @Override
    public long getVersion(long record) {
        return getVersion(null, record);
    }

    @Override
    public long getVersion(String key) {
        Iterator<Write> it = reverseIterator();
        while (it.hasNext()) {
            Write write = it.next();
            if(write.getKey().equals(Text.wrapCached(key))) {
                return write.getVersion();
            }
        }
        return Versioned.NO_VERSION;
    }

    @Override
    public long getVersion(String key, long record) {
        key = Strings.nullToEmpty(key);
        Iterator<Write> it = reverseIterator();
        while (it.hasNext()) {
            Write write = it.next();
            if(record == write.getRecord().longValue()
                    && (Strings.isNullOrEmpty(key) || write.getKey().equals(
                            Text.wrapCached(key)))) {
                return write.getVersion();
            }
        }
        return Versioned.NO_VERSION;
    }

    /**
     * Return the timestamp for the oldest write available.
     * 
     * @return {@code timestamp}
     */
    protected abstract long getOldestWriteTimstamp();

    /**
     * Insert {@code write} into the store <strong>without performing any
     * validity checks</strong>.
     * <p>
     * This method is <em>only</em> safe to call from a context that performs
     * its own validity checks (i.e. a {@link BufferedStore}).
     * 
     * @param write
     * @return {@code true}
     */
    public abstract boolean insert(Write write);

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE:</strong> The subclass <em>may</em> override this method to
     * provide an iterator with granular locking functionality for increased
     * throughput.
     * </p>
     */
    @Override
    public abstract Iterator<Write> iterator();

    /**
     * Return an iterator that traverses the Writes in the store in reverse
     * order.
     * 
     * @return the iterator
     */
    public abstract Iterator<Write> reverseIterator();

    @Override
    public Set<Long> search(String key, String query) {
        Map<Long, Set<Value>> rtv = Maps.newHashMap();
        Iterator<Write> it = iterator();
        while (it.hasNext()) {
            Write write = it.next();
            Value value = write.getValue();
            long record = write.getRecord().longValue();
            if(write.getKey().toString().equals(key)
                    && value.getType() == Type.STRING) {
                /*
                 * NOTE: It is not enough to merely check if the stored text
                 * contains the query because the Database does infix
                 * indexing/searching, which has some subtleties:
                 * 1. Stop words are removed from the both stored indices and 
                 * the search query
                 * 2. A query and document are considered to match if the
                 * document contains a sequence of terms where each term or a
                 * substring of the term matches the term in the same relative
                 * position of the query.
                 */
                // CON-10: compare lowercase for case insensitive search
                String stored = TStrings.stripStopWords(((String) (value
                        .getObject())).toLowerCase());
                query = TStrings.stripStopWords(query.toLowerCase());
                if(!Strings.isNullOrEmpty(stored)
                        && !Strings.isNullOrEmpty(query)
                        && TStrings.isInfixSearchMatch(query, stored)) {
                    Set<Value> values = rtv.get(record);
                    if(values == null) {
                        values = Sets.newHashSet();
                        rtv.put(record, values);
                    }
                    if(values.contains(value)) {
                        values.remove(value);
                    }
                    else {
                        values.add(value);
                    }

                }
            }
        }
        // FIXME sort search results based on frequency (see
        // SearchRecord#search())
        return newLinkedHashMap(Maps.filterValues(rtv, emptySetFilter))
                .keySet();
    }

    /**
     * Transport the content of this store to {@code destination}.
     * 
     * @param destination
     */
    public void transport(PermanentStore destination) {
        Iterator<Write> it = iterator();
        while (it.hasNext()) {
            destination.accept(it.next());
            it.remove();
        }

    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        return verify(key, value, record, Time.now());
    }

    @Override
    public boolean verify(String key, TObject value, long record, long timestamp) {
        return verify(Write.notStorable(key, value, record), timestamp);
    }

    /**
     * Return {@code true} if {@code write} represents a data mapping that
     * currently exists using {@code exists} as prior context.
     * <p>
     * <strong>This method is called from
     * {@link BufferedStore#verify(String, TObject, long)}.</strong>
     * </p>
     * 
     * @param write
     * @return {@code true} if {@code write} currently appears an odd number of
     *         times
     */
    public boolean verify(Write write, boolean exists) {
        return verify(write, Time.now(), exists);
    }

    /**
     * Return {@code true} if {@code write} represents a data mapping that
     * exists at {@code timestamp}.
     * <p>
     * <strong>This method is called from
     * {@link BufferedStore#verify(String, TObject, long, long)}.</strong>
     * </p>
     * 
     * @param write
     * @param timestamp
     * @return {@code true} if {@code write} appears an odd number of times at
     *         {@code timestamp}
     */
    public boolean verify(Write write, long timestamp) {
        return verify(write, timestamp, false);
    }

    /**
     * Return {@code true} if {@code write} represents a data mapping that
     * exists at {@code timestamp}, using {@code exists} as prior context.
     * <p>
     * <strong>NOTE: ALL OTHER VERIFY METHODS DEFER TO THIS ONE.</strong>
     * </p>
     * 
     * @param write
     * @param timestamp
     * @param exists
     * @return {@code true} if {@code write} appears an odd number of times at
     *         {@code timestamp}
     */
    public boolean verify(Write write, long timestamp, boolean exists) {
        if(timestamp >= getOldestWriteTimstamp()) {
            Iterator<Write> it = iterator();
            while (it.hasNext()) {
                Write stored = it.next();
                if(stored.getVersion() <= timestamp) {
                    if(stored.equals(write)) {
                        exists ^= true; // toggle boolean
                    }
                }
                else {
                    break;
                }
            }
        }
        return exists;
    }

    /**
     * Wait (block) until the Buffer has enough data to complete a transport.
     * This method should be called from the external service to avoid busy
     * waiting if continuously transporting data in the background.
     */
    public void waitUntilTransportable() {
        return; // do nothing because Limbo is assumed to always be
                // transportable. But the Buffer will override this method with
                // the appropriate conditions.
    }

    /**
     * Return the number of milliseconds that this store desires any back to
     * back transport requests to pause in between.
     * 
     * @return the pause time
     */
    public int getDesiredTransportSleepTimeInMs() {
        return 0;
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        return explore(Maps.<Long, Set<TObject>> newLinkedHashMap(), timestamp,
                key, operator, values);
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        return explore(Time.now(), key, operator, values);
    }

}
