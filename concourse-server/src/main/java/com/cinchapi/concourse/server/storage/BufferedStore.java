/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.TernaryTruth;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.storage.temp.Limbo;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TSets;
import com.cinchapi.concourse.validate.Keys;
import com.google.common.collect.Sets;

/**
 * A {@link BufferedStore} holds {@link Write Writes} in {@link Limbo} before
 * {@link Limbo#transport(DurableStore) transporting} them to a
 * {@link DurableStore}.
 * <p>
 * A {@link BufferedStore} is used when it is necessary (e.g. atomic operation
 * where {@link Write Writes} are not guaranteed to succeed) or convenient (e.g.
 * rich indexing that would slow down writes in real time, so must be done in
 * the background) to buffer {@link Write Writes} in {@link Limbo} before
 * incorporating them in a {@link DurableStore}.
 * </p>
 * <p>
 * In a {@link BufferedStore}, {@link Write Writes} are
 * {@link Limbo#insert(Write) inserted} in {@link Limbo} and later
 * {@link Limbo#transport(DurableStore) transported}. The rate of
 * {@link Limbo#transport(DurableStore) transport} is externally controlled and
 * the {@link Limbo} type defines how many {@link Write Writes} are included in
 * each {@link Limbo#transport(DurableStore) transport}. Furthermore,
 * {@link Limbo} determines how/when it wants the {@link DurableStore} to
 * {@link DurableStore#sync() sync} the {@link Writes} when
 * {@link DurableStore#accept(Write) accepting} the
 * {@link Limbo#transport(DurableStore) transported} {@link Write Writes}.
 * </p>
 * <p>
 * Reads are handled by taking the XOR (see
 * {@link Sets#symmetricDifference(Set, Set)}) or
 * XOR truth (see <a
 * href="http://en.wikipedia.org/wiki/Exclusive_or#Truth_table"
 * >http://en.wikipedia.org/wiki/Exclusive_or#Truth_table</a>) of the values
 * read from the {@link #limbo} and {@link #durable} {@link Store stores}.
 * </p>
 * <h2>Concurrency</h2>
 * <h3>BETWEEN the {@link #limbo} and {@link #durable} {@link Store stores}</h3>
 * <p>
 * The {@link BufferedStore} framework does not provide concurrency controls to
 * ensure that the state of the {@link Store stores} remains consistent in the
 * middle of an operation. If such coordination is required, the extending class
 * must implement them.
 * </p>
 * <h3>WITHIN each of the {@link #limbo} and {@link #durable} {@link Store
 * stores}</h3>
 * <p>
 * It is assumed that each {@link Store} defines its own local concurrency
 * controls (if needed), but the {@link BufferedStore} framework does recognize
 * the possibility that {@link #durable} may be a {@link LockFreeStore} and
 * the overall performance of the {@link BufferedStore} may depend on the
 * ability to control the timing of logical locking. So, methods are exposed to
 * subclasses (e.g. those prefixed with {@code $}) that allow it to instruct
 * buffered resolution logic on how to dispatch methods to the {@link #durable}
 * {@link Store sore}.
 * </p>
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class BufferedStore implements Store {

    // NOTE ON HISTORICAL READS
    // ========================
    // Buffered historical reads do not merely delegate to their timestamp
    // accepting counterparts because we expect the #destination to use
    // different code paths for present vs historical reads unlike Limbo.

    /**
     * Perform validation on a write that touches {@code key} as {@code value}
     * in {@code record} and throw an exception if necessary.
     * 
     * @param key
     * @param value
     * @param record
     */
    private static void ensureWriteIntegrity(String key, TObject value,
            long record) { // CON-21
        if(!Keys.isWritable(key)) {
            throw new IllegalArgumentException(
                    AnyStrings.joinWithSpace(key, "is not a valid key"));
        }
        else if(value.isBlank()) {
            throw new IllegalArgumentException(
                    "Cannot use a blank value for " + key);
        }
        else if(value.getType() == Type.FUNCTION) {
            throw new IllegalArgumentException(
                    "Cannot use a function for " + key);
        }
        else if(value.getType() == Type.LINK
                && ((Link) Convert.thriftToJava(value)).longValue() == record) {
            throw new ReferentialIntegrityException(
                    "A record cannot link to itself");
        }
    }

    /**
     * The {@link DurableStore store} where {@link Write writes} in
     * {@link #limbo} can be {@link Limbo#transport(DurableStore) transported}.
     */
    protected final DurableStore durable;

    /**
     * The {@link Limbo store} where {@link Write writes} are initially stored.
     */
    protected final Limbo limbo;

    /**
     * Construct a new instance.
     * 
     * @param limbo
     * @param durable
     */
    protected BufferedStore(Limbo limbo, DurableStore durable) {
        this.limbo = limbo;
        this.durable = durable;
    }

    /**
     * Add {@code key} as {@code value} to {@code record}.
     * <p>
     * This method maps {@code key} to {@code value} in {@code record}, if and
     * only if that mapping does not <em>currently</em> exist (i.e.
     * {@link #verify(String, Object, long)} is {@code false}). Adding
     * {@code value} to {@code key} does not replace any existing mappings from
     * {@code key} in {@code record} because a field may contain multiple
     * distinct values.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if the mapping is added
     */
    public boolean add(String key, TObject value, long record) {
        return add(Write.add(key, value, record), Sync.YES, Verify.YES);
    }

    @Override
    public ReadWriteLock advisoryLock() {
        return durable.advisoryLock();
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        Map<TObject, Set<Long>> context = $browse(key);
        return limbo.browse(key, context);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Map<TObject, Set<Long>> context = durable.browse(key, timestamp);
        return limbo.browse(key, timestamp, context);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        Map<Long, Set<TObject>> context = $chronologize(key, record, start,
                end);
        return limbo.chronologize(key, record, start, end, context);
    }

    @Override
    public boolean contains(long record) {
        return durable.contains(record) || limbo.contains(record);
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases) {
        Map<Long, Set<TObject>> context = $explore(key, aliases);
        return limbo.explore(context, key, aliases);
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases,
            long timestamp) {
        Map<Long, Set<TObject>> context = durable.explore(key, aliases,
                timestamp);
        return limbo.explore(context, key, aliases, timestamp);
    }

    @Override
    public Set<TObject> gather(String key, long record) {
        Set<TObject> context = $gather(key, record);
        return limbo.gather(key, record, context);
    }

    @Override
    public Set<TObject> gather(String key, long record, long timestamp) {
        Set<TObject> context = durable.gather(key, record, timestamp);
        return limbo.gather(key, record, timestamp, context);
    }

    @Override
    public Set<Long> getAllRecords() {
        return TSets.union(durable.getAllRecords(), limbo.getAllRecords());
    }

    @Override
    public Memory memory() {
        return new Memory() {

            @Override
            public boolean contains(long record) {
                return durable.memory().contains(record)
                        && limbo.memory().contains(record);
            }

            @Override
            public boolean contains(String key) {
                return durable.memory().contains(key)
                        && limbo.memory().contains(key);
            }

            @Override
            public boolean contains(String key, long record) {
                return durable.memory().contains(key, record)
                        && limbo.memory().contains(key, record);
            }

        };
    }

    /**
     * Remove {@code key} as {@code value} from {@code record}.
     * <p>
     * This method deletes the mapping from {@code key} to {@code value} in
     * {@code record}, if that mapping <em>currently</em> exists (i.e.
     * {@link #verify(String, Object, long)} is {@code true}. No other mappings
     * from {@code key} in {@code record} are affected.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if the mapping is removed
     */
    public boolean remove(String key, TObject value, long record) {
        return remove(Write.remove(key, value, record), Sync.YES, Verify.YES);
    }

    @Override
    public void repair() {
        durable.repair();
        limbo.repair();
    }

    @Override
    public Map<Long, List<String>> review(long record) {
        Map<Long, List<String>> context = $review(record);
        for (Entry<Long, List<String>> entry : limbo.review(record)
                .entrySet()) {
            long version = entry.getKey();
            List<String> descriptions = entry.getValue();
            context.merge(version, descriptions, (existing, latest) -> {
                existing.addAll(latest);
                return existing;
            });
        }
        return context;
    }

    @Override
    public Map<Long, List<String>> review(String key, long record) {
        Map<Long, List<String>> context = $review(key, record);
        for (Entry<Long, List<String>> entry : limbo.review(key, record)
                .entrySet()) {
            long version = entry.getKey();
            List<String> descriptions = entry.getValue();
            context.merge(version, descriptions, (existing, latest) -> {
                existing.addAll(latest);
                return existing;
            });
        }
        return context;
    }

    @Override
    public Set<Long> search(String key, String query) {
        // FIXME: should this be implemented using a context instead?
        return Sets.symmetricDifference(limbo.search(key, query),
                durable.search(key, query));
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        Map<String, Set<TObject>> context = $select(record);
        return limbo.select(record, context);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        Map<String, Set<TObject>> context = durable.select(record, timestamp);
        return limbo.select(record, timestamp, context);
    }

    @Override
    public Set<TObject> select(String key, long record) {
        Set<TObject> context = $select(key, record);
        return limbo.select(key, record, context);
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp) {
        Set<TObject> context = durable.select(key, record, timestamp);
        return limbo.select(key, record, timestamp, context);
    }

    /**
     * Set {@code key} as {@code value} in {@code record}.
     * <p>
     * This method will remove all the values currently mapped from {@code key}
     * in {@code record} and add {@code value} without performing any validity
     * checks.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     */
    public void set(String key, TObject value, long record) {
        try {
            ensureWriteIntegrity(key, value, record);
            Set<TObject> values = select(key, record);
            for (TObject stored : values) {
                limbo.insert(Write.remove(key, stored, record));
            }
            limbo.insert(Write.add(key, value, record));
        }
        catch (ReferentialIntegrityException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public boolean verify(Write write) {
        TernaryTruth truth = limbo.verifyFast(write);
        if(truth == TernaryTruth.UNSURE) {
            return $verify(write);
        }
        else {
            return truth.boolValue();
        }
    }

    @Override
    public boolean verify(Write write, long timestamp) {
        TernaryTruth truth = limbo.verifyFast(write, timestamp);
        if(truth == TernaryTruth.UNSURE) {
            return durable.verify(write, timestamp);
        }
        else {
            return truth.boolValue();
        }
    }

    /**
     * Browse {@code key} in the {@link #durable} store.
     * <p>
     * By default, a call is made to {@link DurableStore#browse(String)},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param record
     * @return a {@link Map} of each value stored from {@code key} mapped to
     *         every record where that value is contained for {@code key} within
     *         the {@link #durable} {@link Store store}
     */
    protected Map<TObject, Set<Long>> $browse(String key) {
        return durable.browse(key);
    }

    /**
     * Chronologize {@code key} in {@code record} within the {@link #durable}
     * store.
     * <p>
     * By default, a call is made to
     * {@link DurableStore#chronologize(String, long, long, long)},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param record
     * @return a {@link Map} from modification timestamp to a non-empty Set of
     *         values that were contained at that timestamp
     */
    protected Map<Long, Set<TObject>> $chronologize(String key, long record,
            long start, long end) {
        return durable.chronologize(key, record, start, end);
    }

    /**
     * Return a {@link Map} of all the records that satisfy {@code operator}
     * in relation to the {@code values} stored for {@code key} mapped to each
     * of their values that cause it to satisfy
     * 
     * <p>
     * By default, a call is made to
     * {@link DurableStore#doExplore(String, Operator, TObject[])},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param aliases
     * @return a {@link Map} of all the records that satisfy {@code operator}
     *         in relation to the {@code values} stored for {@code key} mapped
     *         to each
     *         of their values that cause it to satisfy
     */
    protected Map<Long, Set<TObject>> $explore(String key, Aliases aliases) {
        return durable.explore(key, aliases);
    }

    /**
     * Gather {@code key} as {@code value} from {@code record} in the
     * {@link #durable} store.
     * <p>
     * By default, a call is made to {@link DurableStore#gather(String, long)},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param record
     * @return the values currently stored from {@code key} in {@code record}
     *         within the {@link #durable} {@link Store store}
     */
    protected Set<TObject> $gather(String key, long record) {
        return durable.gather(key, record);
    }

    /**
     * Audit {@code record} within the {@link #durable} store.
     * <p>
     * By default, a call is made to {@link DurableStore#review(long)},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param record
     * @return the log of changes
     */
    protected Map<Long, List<String>> $review(long record) {
        return durable.review(record);
    }

    /**
     * Audit {@code key} in {@code record} within the {@link #durable} store.
     * <p>
     * By default, a call is made to {@link DurableStore#audit(String, long)},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param record
     * @return the log of changes
     */
    protected Map<Long, List<String>> $review(String key, long record) {
        return durable.review(key, record);
    }

    /**
     * Select all the data from {@code record} in the {@link #durable} store.
     * <p>
     * By default, a call is made to {@link DurableStore#select(long)},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param record
     * @return a {@link Map} from each key to its contained values that are
     *         currently stored in {@code record} within the {@link #durable}
     *         {@link Store store}
     */
    protected Map<String, Set<TObject>> $select(long record) {
        return durable.select(record);
    }

    /**
     * Select {@code key} as {@code value} from {@code record} in the
     * {@link #durable} store.
     * <p>
     * By default, a call is made to {@link DurableStore#select(String, long)},
     * but the subclass can override this method to route differently if
     * necessary (e.g. an {@link AtomicOperation} routing to lock free
     * implementations).
     * </p>
     * 
     * @param key
     * @param record
     * @return the values currently stored from {@code key} in {@code record}
     *         within the {@link #durable} {@link Store store}
     */
    protected Set<TObject> $select(String key, long record) {
        return durable.select(key, record);
    }

    /**
     * Verify the {@code write} in the {@link #durable} store.
     * <p>
     * By default, a call is made to {@link DurableStore#verify(Write)}, but the
     * subclass can override this method to route differently if necessary (e.g.
     * an {@link AtomicOperation} routing to lock free implementations).
     * </p>
     * 
     * @param write
     * @return {@code} true if the {@link Write Write's} element currently
     *         exists in the {@link #durable} {@link Store store}.
     */
    protected boolean $verify(Write write) {
        return durable.verify(write);
    }

    /**
     * Add {@code key} as {@code value} to {@code record} with the directive to
     * {@code sync} the data or not. Depending upon the implementation of the
     * {@link #limbo}, a sync may guarantee that the data is durably stored.
     * <p>
     * This method maps {@code key} to {@code value} in {@code record}, if and
     * only if that mapping does not <em>currently</em> exist (i.e.
     * {@link #verify(String, Object, long)} is {@code false}). Adding
     * {@code value} to {@code key} does not replace any existing mappings from
     * {@code key} in {@code record} because a field may contain multiple
     * distinct values.
     * </p>
     * 
     * @param write
     * @param sync
     * @param verify
     * @return {@code true} if the mapping is added
     */
    protected boolean add(Write write, Sync sync, Verify verify) {
        try {
            String key = write.getKey().toString();
            TObject value = write.getValue().getTObject();
            long record = write.getRecord().longValue();
            ensureWriteIntegrity(key, value, record);
            // NOTE: #verify ends up being NO when the Engine accepts Writes
            // that are transported from a committing AtomicOperation or
            // Transaction
            if(verify == Verify.NO || !verifyWithReentrancy(write)) {
                // NOTE: #sync ends up being NO when the Engine accepts
                // Writes that are transported from a committing AtomicOperation
                // or Transaction, in which case passing this boolean along to
                // the Buffer allows group sync to happen
                return limbo.insert(write, sync == Sync.YES);
            }
            else {
                return false;
            }
        }
        catch (ReferentialIntegrityException e) {
            return false;
        }
    }

    /**
     * Remove {@code key} as {@code value} from {@code record} with the
     * directive to {@code sync} the data or not. Depending upon the
     * implementation of the {@link #limbo}, a sync may guarantee that the data
     * is durably stored.
     * <p>
     * This method deletes the mapping from {@code key} to {@code value} in
     * {@code record}, if that mapping <em>currently</em> exists (i.e.
     * {@link #verify(String, Object, long)} is {@code true}. No other mappings
     * from {@code key} in {@code record} are affected.
     * </p>
     * 
     * @param write
     * @param sync
     * @param verify
     * @return {@code true} if the mapping is removed
     */
    protected boolean remove(Write write, Sync sync, Verify verify) {
        try {
            String key = write.getKey().toString();
            TObject value = write.getValue().getTObject();
            long record = write.getRecord().longValue();
            ensureWriteIntegrity(key, value, record);
            // NOTE: #verify ends up being NO when the Engine accepts Writes
            // that are transported from a committing AtomicOperation or
            // Transaction
            if(verify == Verify.NO || verifyWithReentrancy(write)) {
                // NOTE: #sync ends up being NO when the Engine accepts
                // Writes that are transported from a committing AtomicOperation
                // or Transaction, in which case passing this boolean along to
                // the Buffer allows group sync to happen
                return limbo.insert(write, sync == Sync.YES);
            }
            else {
                return false;
            }
        }
        catch (ReferentialIntegrityException e) {
            return false;
        }
    }

    /**
     * {@link #verify(Write) Verify} the {@link Write} in a manner that assumes
     * any related locks have already been grabbed and don't need to be grabbed
     * again.
     * <p>
     * Internally, this method is called when {@link #add(Write, Sync, Verify)
     * adding} or {@link #remove(Write, Sync, Verify) removing} a {@link Write},
     * so any locking procedures related to {@link Write} will have already run
     * as part of those routines.
     * </p>
     * <p>
     * In most cases, it is acceptable to call {@code super.verify(write)} from
     * this method as that routine defined in this class does not perform any
     * locking.
     * </p>
     * 
     * @param write
     * @return {@code} true if the {@link Write Write's} element currently
     *         exists in the this {@link BufferedStore store}.
     */
    protected abstract boolean verifyWithReentrancy(Write write);

    /**
     * A semantic enum to track whether the {@link Write} from an add or remove
     * should be immediately synced.
     */
    protected enum Sync {

        /**
         * Instruct {@link #limbo} NOT to sync the write.
         */
        NO,

        /**
         * Instruct {@link #limbo} to sync the write.
         */
        YES
    }

    /**
     * A semantic enum to track whether the {@link Write} from an add or remove
     * should be verified.
     *
     */
    protected enum Verify {

        /**
         * Skip the verify.
         */
        NO,

        /**
         * Do the verify.
         */
        YES
    }

}
