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
package com.cinchapi.concourse.server.storage.temp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.TernaryTruth;
import com.cinchapi.concourse.collect.Iterators;
import com.cinchapi.concourse.search.CompiledInfingram;
import com.cinchapi.concourse.search.Infingram;
import com.cinchapi.concourse.server.model.TObjectSorter;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.DurableStore;
import com.cinchapi.concourse.server.storage.Memory;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Comparators;
import com.cinchapi.concourse.util.MultimapViews;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;

/**
 * {@link Limbo} is a lightweight in-memory proxy store that is a suitable cache
 * or fast, albeit temporary, store for data that will eventually be persisted
 * to a {@link DurableStore}.
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
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class Limbo implements Store, Iterable<Write> {

    /**
     * Return {@code true} if {@code input} matches {@code operator} in relation
     * to {@code values}.
     * 
     * @param input
     * @param operator
     * @param values
     * @return {@code true} if {@code input} matches
     */
    protected static boolean matches(Value input, Operator operator,
            TObject... values) {
        TObject $needle;
        if((operator == Operator.CONTAINS || operator == Operator.NOT_CONTAINS)
                && ($needle = values[0]).isCharSequenceType()
                && input.isCharSequenceType()) {
            // This method is called from #explore as the Writes are iterated
            // and each one is compared against the values. So, if the
            // exploration is in service of a query-based search, optimize
            // it by constructing and caching a CompiledInfingram instead of
            // relying on TObject#is which creates a new Infingram for the same
            // needle (values[0]) each time.
            Infingram needle;
            if($needle instanceof TInfingram) {
                needle = ((TInfingram) $needle).get();
            }
            else {
                needle = new CompiledInfingram(
                        $needle.getJavaFormat().toString());
                $needle = new TInfingram(needle);
                values[0] = $needle;
            }
            String haystack = input.getObject().toString();
            return needle.in(haystack);
        }
        else {
            return input.getTObject().isIgnoreCase(operator, values);
        }
    }

    /**
     * Constant {@link Memory} that is always returned by a {@link Limbo} store.
     */
    private final Memory memory = new Memory() {

        @Override
        public boolean contains(long record) {
            return true;
        }

        @Override
        public boolean contains(String key) {
            return true;
        }

        @Override
        public boolean contains(String key, long record) {
            return true;
        }

    };

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        return browse(key, Time.NONE);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Map<TObject, Set<Long>> context = new TreeMap<>(TObjectSorter.INSTANCE);
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
        if(timestamp >= getOldestWriteTimestamp()) {
            for (Iterator<Write> it = iterator(); it.hasNext();) {
                Write write = it.next();
                TObject value = write.getValue().getTObject();
                long version = write.getVersion();
                if(write.getKey().toString().toString().equals(key)
                        && version <= timestamp) {
                    long record = write.getRecord().longValue();
                    Action action = write.getType();
                    Set<Long> records = context.computeIfAbsent(value,
                            $ -> new LinkedHashSet<>());
                    if(action == Action.ADD) {
                        records.add(record);
                    }
                    else if(action == Action.REMOVE) {
                        records.remove(record);
                        if(records.isEmpty()) {
                            context.remove(value);
                        }
                    }
                }
                else if(version > timestamp) {
                    break;
                }
                else {
                    continue;
                }
            }
        }
        return context;
    }

    /**
     * Calculate the browsable view of {@code key} at {@code timestamp} using
     * prior {@code context} as if it were also a part of the Buffer.
     * 
     * @param key
     * @param context
     * @return a possibly empty Map of data
     */
    public final Map<TObject, Set<Long>> browse(String key,
            Map<TObject, Set<Long>> context) {
        return browse(key, Time.NONE, context);
    }

    @Override
    public final Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        Map<Long, Set<TObject>> context = new LinkedHashMap<>();
        return chronologize(key, record, start, end, context);
    }

    /**
     * Return a time series that contains the values stored for {@code key} in
     * {@code record} at each modification timestamp between {@code start}
     * (inclusive) and {@code end} (exclusive).
     * 
     * @param key the field name
     * @param record the record id
     * @param start the start timestamp (inclusive)
     * @param end the end timestamp (exclusive)
     * @param context the prior context
     * @return a {@link Map mapping} from modification timestamp to a non-empty
     *         {@link Set} of values that were contained at that timestamp
     */
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end, Map<Long, Set<TObject>> context) {
        Set<TObject> snapshot = Iterables.getLast(context.values(),
                new LinkedHashSet<>());
        if(snapshot.isEmpty() && !context.isEmpty()) {
            // CON-474: Empty set is placed in the context if it was the last
            // snapshot known to the database
            context.remove(Time.NONE);
        }
        for (Iterator<Write> it = iterator(); it.hasNext();) {
            Write write = it.next();
            long timestamp = write.getVersion();
            if(timestamp >= end) {
                break;
            }
            else {
                Action action = write.getType();
                if(write.getKey().toString().equals(key)
                        && write.getRecord().longValue() == record) {
                    snapshot = new LinkedHashSet<>(snapshot);
                    TObject value = write.getValue().getTObject();
                    if(action == Action.ADD) {
                        snapshot.add(value);
                    }
                    else if(action == Action.REMOVE) {
                        snapshot.remove(value);
                    }
                    if(timestamp >= start) {
                        context.put(timestamp, snapshot);
                    }
                }
            }
        }
        // NOTE: Empty snapshots couldn't be removed while processing because
        // that state needed to be preserved to calculate subsequent diffs.
        context.values().removeIf(v -> v.isEmpty());
        return context;
    }

    @Override
    public boolean contains(long record) {
        for (Iterator<Write> it = iterator(); it.hasNext();) {
            Write write = it.next();
            if(write.getRecord().longValue() == record) {
                return true;
            }
        }
        return false;
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
        if(timestamp >= getOldestWriteTimestamp()) {
            for (Iterator<Write> it = iterator(); it.hasNext();) {
                Write write = it.next();
                String key = write.getKey().toString();
                Action action = write.getType();
                TObject value = write.getValue().getTObject();
                if(write.getRecord().longValue() == record
                        && write.getVersion() <= timestamp) {
                    Set<TObject> values = context.computeIfAbsent(key,
                            $ -> new HashSet<>());
                    if(action == Action.ADD) {
                        values.add(value);
                    }
                    else if(action == Action.REMOVE) {
                        values.remove(value);
                        if(values.isEmpty()) {
                            context.remove(key);
                        }
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
        return context.keySet();
    }

    /**
     * This is an implementation of the
     * {@link #explore(String, Operator, TObject...)} routine that takes
     * in a prior {@code context} and will return a mapping from
     * records that match a criteria (expressed as {@code key} filtered by
     * {@code operator} in relation to one or more {@code values}) to the set of
     * values that cause that record to match the criteria.
     * 
     * @param context
     * @param key
     * @param operator
     * @param values
     * @return the relevant data for the records that satisfy the find query
     */
    public final Map<Long, Set<TObject>> explore(
            Map<Long, Set<TObject>> context, String key, Aliases aliases) {
        return explore(context, key, aliases, Time.NONE);
    }

    /**
     * This is an implementation of the
     * {@link #explore(long, String, Operator, TObject...)} routine that takes
     * in a prior {@code context} and will return a mapping from
     * records that match a criteria (expressed as {@code key} filtered by
     * {@code operator} in relation to one or more {@code values}) to the set of
     * values that caused that record to match the criteria {@code timestamp}.
     * 
     * @param context
     * @param timestamp
     * @param key
     * @param operator
     * @param values
     * @return the relevant data for the records that satisfy the find query
     */
    public Map<Long, Set<TObject>> explore(Map<Long, Set<TObject>> context,
            String key, Aliases aliases, long timestamp) {
        if(timestamp >= getOldestWriteTimestamp()) {
            Operator operator = aliases.operator();
            TObject[] values = aliases.values();
            for (Iterator<Write> it = iterator(); it.hasNext();) {
                Write write = it.next();
                long record = write.getRecord().longValue();
                if(write.getVersion() <= timestamp) {
                    if(write.getKey().toString().equals(key)
                            && matches(write.getValue(), operator, values)) {
                        if(write.getType() == Action.ADD) {
                            MultimapViews.put(context, record,
                                    write.getValue().getTObject());
                        }
                        else {
                            MultimapViews.remove(context, record,
                                    write.getValue().getTObject());
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
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases) {
        return explore(key, aliases, Time.NONE);
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases,
            long timestamp) {
        return explore(new TreeMap<>(), key, aliases, timestamp);
    }

    /**
     * Gather the values mapped from {@code key} in {@code record} at
     * {@code timestamp} using prior {@code context} as if it were also a part
     * of the Buffer.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @param context
     * @return the values
     */
    public final Set<TObject> gather(String key, long record, long timestamp,
            Set<TObject> context) {
        return select(key, record, timestamp, context);
    }

    /**
     * Gather the values mapped from {@code key} in {@code record} at
     * {@code timestamp} using prior {@code context} as if it were also a part
     * of the Buffer.
     * 
     * @param key
     * @param record
     * @param context
     * @return the values
     */
    public final Set<TObject> gather(String key, long record,
            Set<TObject> context) {
        return select(key, record, context);
    }

    @Override
    public Set<Long> getAllRecords() {
        Set<Long> records = new HashSet<>();
        for (Iterator<Write> it = iterator(); it.hasNext();) {
            Write write = it.next();
            records.add(write.getRecord().longValue());
        }
        return records;
    }

    /**
     * Return a snapshot {@link Iterable} that contains the
     * {@link Write#hash() hash} of every {@link Write} in the
     * {@link Store}.
     * 
     * @return all the contained {@link Write} {@link Write#hash() hashes}
     */
    public Set<HashCode> hashes() {
        Set<HashCode> hashes = new HashSet<>();
        Iterator<Write> it = iterator();
        try {
            while (it.hasNext()) {
                hashes.add(it.next().hash());
            }
        }
        finally {
            Iterators.close(it);
        }
        return hashes;
    }

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
    public final boolean insert(Write write) {
        return insert(write, true);
    }

    /**
     * Insert {@code write} into the store <strong>without performing any
     * validity checks</strong> and specify whether a {@code sync} should occur
     * or not. By default, syncs are meaningless in {@link Limbo}, but some
     * implementations may wish to provide guarantees that the write will be
     * durably stored.
     * <p>
     * This method is <em>only</em> safe to call from a context that performs
     * its own validity checks (i.e. a {@link BufferedStore}).
     * 
     * @param write - The write to append
     * @param sync - a flag that controls whether this instance will make an
     *            attempt to durably persist the data to some backing store.
     *            Simply ignore this flag if the implementation does not support
     *            durability
     * @return {@code true}
     */
    public abstract boolean insert(Write write, boolean sync);

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

    @Override
    public Memory memory() {
        return memory;
    }

    @Override
    public Map<Long, List<String>> review(long record) {
        Map<Long, List<String>> review = new LinkedHashMap<>();
        for (Iterator<Write> it = iterator(); it.hasNext();) {
            Write write = it.next();
            if(write.getRecord().longValue() == record) {
                review.computeIfAbsent(write.getVersion(),
                        $ -> new ArrayList<>()).add(write.toString());
            }
        }
        return review;

    }

    @Override
    public Map<Long, List<String>> review(String key, long record) {
        Map<Long, List<String>> review = new LinkedHashMap<>();
        for (Iterator<Write> it = iterator(); it.hasNext();) {
            Write write = it.next();
            if(write.getKey().toString().equals(key)
                    && write.getRecord().longValue() == record) {
                review.computeIfAbsent(write.getVersion(),
                        $ -> new ArrayList<>()).add(write.toString());
            }
        }
        return review;

    }

    @Override
    public Set<Long> search(String key, String query) {
        Map<Long, Set<Value>> matches = new HashMap<>();
        Infingram needle = new CompiledInfingram(query);
        if(needle.numTokens() > 0) {
            for (Iterator<Write> it = getSearchIterator(key); it.hasNext();) {
                Write write = it.next();
                Value value = write.getValue();
                long record = write.getRecord().longValue();
                Action action = write.getType();
                if(isPossibleSearchMatch(key, write, value)) {
                    /*
                     * NOTE: It is not enough to merely check if the stored text
                     * contains the query because the Database does infix
                     * indexing/searching, which has some subtleties:
                     * 1. Stop words are removed from the both stored indices
                     * and the search query
                     * 2. A query and document are considered to match if the
                     * document contains a sequence of terms where each term or
                     * a substring of the term matches the term in the same
                     * relative position of the query.
                     */
                    // CON-10: compare lowercase for case insensitive search
                    String haystack = ((String) value.getObject());
                    if(needle.in(haystack)) {
                        Set<Value> values = matches.computeIfAbsent(record,
                                $ -> new HashSet<>());
                        if(action == Action.REMOVE) {
                            values.remove(value);
                            if(values.isEmpty()) {
                                matches.remove(record);
                            }
                        }
                        else if(action == Action.ADD) {
                            values.add(value);
                        }
                    }
                }
            }
        }
        // FIXME sort search results based on frequency (see
        // CorpusRecord#search())
        return matches.keySet();
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        return select(record, Time.NONE);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        Map<String, Set<TObject>> context = new TreeMap<>(
                Comparators.CASE_INSENSITIVE_STRING_COMPARATOR);
        return select(record, timestamp, context);
    }

    /**
     * Select all the data from {@code record} using prior {@code context} as if
     * it were also contained in {@link Limbo}.
     * 
     * @param record
     * @param timestamp
     * @param context
     * @return a mapping from each key to contained values in {@code record} at
     *         {@code timestamp}
     */
    public Map<String, Set<TObject>> select(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        if(timestamp >= getOldestWriteTimestamp()) {
            for (Iterator<Write> it = iterator(); it.hasNext();) {
                Write write = it.next();
                String key = write.getKey().toString();
                TObject value = write.getValue().getTObject();
                Action action = write.getType();
                long version = write.getVersion();
                if(write.getRecord().longValue() == record
                        && version <= timestamp) {
                    Set<TObject> values = context.computeIfAbsent(key,
                            $ -> new LinkedHashSet<>());
                    if(action == Action.ADD) {
                        values.add(value);
                    }
                    else if(action == Action.REMOVE) {
                        values.remove(value);
                        if(values.isEmpty()) {
                            context.remove(key);
                        }
                    }
                }
                else if(version > timestamp) {
                    break;
                }
                else {
                    continue;
                }
            }
        }
        return context;
    }

    /**
     * Select all the data from {@code record} at {@code timestamp} using
     * prior {@code context} as if it were also contained in {@link Limbo}.
     * 
     * @param record
     * @param context
     * @return a mapping from each key to contained values in {@code record} at
     *         {@code timestamp}
     */
    public final Map<String, Set<TObject>> select(long record,
            Map<String, Set<TObject>> context) {
        return select(record, Time.NONE, context);
    }

    @Override
    public Set<TObject> select(String key, long record) {
        return select(key, record, Time.NONE);
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp) {
        return select(key, record, timestamp, new LinkedHashSet<>());
    }

    /**
     * Select the values mapped from {@code key} in {@code record} at
     * {@code timestamp} using prior {@code context} as if it were also
     * contained in {@link Limbo}.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @param context
     * @return the values
     */
    public Set<TObject> select(String key, long record, long timestamp,
            Set<TObject> context) {
        if(timestamp >= getOldestWriteTimestamp()) {
            for (Iterator<Write> it = iterator(); it.hasNext();) {
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

    /**
     * Select the values mapped from {@code key} in {@code record} using prior
     * {@code context} as if it were also contained in {@link Limbo}.
     * 
     * @param key
     * @param record
     * @param context
     * @return the values
     */
    public final Set<TObject> select(String key, long record,
            Set<TObject> context) {
        return select(key, record, Time.NONE, context);
    }

    /**
     * If the implementation supports durable storage, this method guarantees
     * that all the data contained here-within is durably persisted. Otherwise,
     * this method is meaningless and returns immediately.
     */
    public void sync() {/* noop */}

    /**
     * If supported, eagerly apply a transformation {@link Function} to every
     * contained {@link Write}.
     * 
     * @param transformer
     */
    public void transform(Function<Write, Write> transformer) {
        throw new UnsupportedOperationException();
    }

    /**
     * Transport the content of this store to {@code destination}.
     * 
     * @param destination
     */
    public final void transport(DurableStore destination) {
        transport(destination, true);
    }

    /**
     * Transport the content of this store to {@code destination} with the
     * directive to {@code sync} or not. A sync guarantees that the transported
     * data is durably persisted within the {@link DurableStore}.
     * 
     * @param destination - the recipient store for the data
     * @param syncAfterEach - a flag that controls whether a call is always made
     *            to durably persist (i.e. fsync) in the {@code destination}
     *            after each write is transported
     */
    public void transport(DurableStore destination, boolean syncAfterEach) {
        for (Iterator<Write> it = iterator(); it.hasNext();) {
            destination.accept(it.next(), syncAfterEach);
            it.remove();
        }
    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        return verify(key, value, record, Time.NONE);
    }

    @Override
    public boolean verify(String key, TObject value, long record,
            long timestamp) {
        return verify(Write.notStorable(key, value, record), timestamp);
    }

    @Override
    public boolean verify(Write write) {
        return verify(write, Time.NONE);
    }

    @Override
    public boolean verify(Write write, long timestamp) {
        boolean exists = false;
        if(timestamp >= getOldestWriteTimestamp()) {
            for (Iterator<Write> it = iterator(); it.hasNext();) {
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
     * A specialized implementation to possibly verify the existence of
     * {@code write} using three-valued logic. This routine allows the caller to
     * get a potentially definitive answer by only consulting this store instead
     * of having to gather prior context beforehand.
     * <p>
     * This method will respond in one of three ways when verifying the
     * existence of {@code write}:
     * <ul>
     * <li>Definitively {@link TernaryTruth#TRUE true} if the {@code write}
     * appears in this store at least once and the most recent appearance is the
     * result of an {@link Action#ADD add} operation.</li>
     * <li>Definitively {@link TernaryTruth#TRUE false} if the {@code write}
     * appears in the Buffer at least once and the most recent appearance is the
     * result of a {@link Action#REMOVE remove} operation.</li>
     * <li>{@link TernaryTruth#UNSURE} if the {@code write}'s
     * {@link Write#getRecord()} appears is in the inventory AND the
     * {@code write} does not appear in the Buffer.</li>
     * </ul>
     * </p>
     * 
     * @param write the {@link Write} to verify
     * @return the appropriate {@link TernaryTruth} value that corresponds to
     *         the Buffer's ability to verify the existence of {@code write}
     */
    public final TernaryTruth verifyFast(Write write) {
        return verifyFast(write, Time.NONE);
    }

    /**
     * A specialized implementation to possibly verify the existence of
     * {@code write} at {@code timestamp} using three-valued logic.
     * This routine allows the caller to get a potentially definitive answer by
     * only consulting the Buffer instead of having to gather prior context
     * beforehand.
     * <p>
     * This method will respond in one of three ways when verifying the
     * existence of {@code write} at {@code timestamp}:
     * <ul>
     * <li>Definitively {@link TernaryTruth#TRUE true} if the {@code write}'s
     * {@link Write#getRecord record} is in the {@link #inventory} AND the
     * {@code write} appears in the Buffer at least once on or before timestamp
     * and the appearance most recent to {@code timestamp} is the result of an
     * {@link Action#ADD add} operation.</li>
     * <li>Definitively {@link TernaryTruth#TRUE false} if the {@code write}'s
     * {@link Write#getRecord record} is NOT in the {@link #inventory} OR the
     * {@code write} appears in the Buffer at least once on or before timestamp
     * and the appearance most recent to {@code timestamp} is the result of a
     * {@link Action#REMOVE remove} operation.</li>
     * <li>{@link TernaryTruth#UNSURE} if the {@code write}'s
     * {@link Write#getRecord()} does not appear in this store at
     * {@code timestamp}</li>
     * </ul>
     * </p>
     * 
     * @param write the {@link Write} to verify
     * @param timestamp the timestamp at which the verification should happen
     * @return the appropriate {@link TernaryTruth} value that corresponds to
     *         the store's ability to verify the existence of {@code write} at
     *         {@code timestamp}
     */
    public TernaryTruth verifyFast(Write write, long timestamp) {
        Action action = getLastWriteAction(write, timestamp);
        if(action == Action.ADD) {
            return TernaryTruth.TRUE;
        }
        else if(action == Action.REMOVE) {
            return TernaryTruth.FALSE;
        }
        else {
            return TernaryTruth.UNSURE;
        }
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
     * Return the {@link Action} associated with the most recent instance of
     * {@code write} at {@code timestamp} in the the store. For example, if
     * {@code timestamp} {@code write} was most recently added, then this method
     * will return {@link Action#ADD}.
     * 
     * @param write the comparison {@link Write} whose most recent action is of
     *            interest
     * @param timestamp the latest timestamp to use when searching
     * @return the most recent write {@link Action action} or {@code null} if
     *         {@code write} was not present in the store at {@code timestamp}
     */
    @Nullable
    protected Action getLastWriteAction(Write write, long timestamp) {
        Action action = null;
        if(timestamp >= getOldestWriteTimestamp()) {
            Iterator<Write> it = iterator();
            while (it.hasNext()) {
                Write stored = it.next();
                if(stored.getVersion() <= timestamp) {
                    if(stored.equals(write)) {
                        action = stored.getType();
                    }
                }
                else {
                    break;
                }
            }
        }
        return action;
    }

    /**
     * Return the timestamp for the oldest write available.
     * 
     * @return {@code timestamp}
     */
    protected abstract long getOldestWriteTimestamp();

    /**
     * Return the iterator to use in the {@link #search(String, String)} method.
     * 
     * @param key
     * @return the appropriate iterator to use for searching
     */
    protected abstract Iterator<Write> getSearchIterator(String key);

    /**
     * Allows the subclass to define some criteria for the search logic to
     * determine if {@code write} with {@code value} is a possible search match
     * for {@code key}.
     * <p>
     * <strong>NOTE:</strong> This method should NOT check to see if
     * {@code write} is an true search match for {@code value} because that
     * logic is handled in the {@link #search(String, String)} method. The
     * purpose of this method is merely to help quickly eliminate writes that
     * can't possibly be a search match (i.e. because the write has a non-string
     * value or a different key).
     * </p>
     * <p>
     * <strong>NOTE:</strong> The {@link Buffer} uses this method to optimize
     * the check since the iterator it returns in
     * {@link #getSearchIterator(String)} already ensures that {@code write} has
     * the same key component as {@code key}.
     * </p>
     * 
     * @param key
     * @param write
     * @param value
     * @return {@code true} if the write is a possible search match
     */
    protected abstract boolean isPossibleSearchMatch(String key, Write write,
            Value value);

    /**
     * A specialized implementation of {@link TObject} that wraps an
     * {@link Infingram} value for efficient
     * {@link #matches(Value, Operator, TObject[]) matching} during
     * {@link Limbo#explore(String, Operator, TObject...) exploration}.
     */
    private static class TInfingram extends TObject {

        private static final long serialVersionUID = 1L;

        /**
         * The wrapped {@link Infingram}
         */
        private Infingram value;

        /**
         * Construct a new instance.
         * 
         * @param needle
         */
        TInfingram(Infingram needle) {
            this.value = needle;
        }

        /**
         * Return the wrapping {@link Infingram}.
         * 
         * @return
         */
        Infingram get() {
            return value;
        }

        @Override
        public boolean isCharSequenceType() {
            return true;
        }

    }

}
