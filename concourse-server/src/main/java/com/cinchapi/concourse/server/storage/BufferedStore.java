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
package com.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.TernaryTruth;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.storage.temp.Limbo;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.time.Time;
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
 * <p>
 * <h2>Concurrency</h2>
 * The {@link BufferedStore} framework does not perform any locking within or
 * across the {@link #limbo} or {@link #durable} {@link Store stores}
 * because it is assumed that each {@link Store} defines its own local
 * concurrency controls and that concurrency across the stores is highly
 * variable among implementations (e.g. An {@link AtomicOperation} does not grab
 * any coordinating locks until it is {@link AtomicOperation#commit() committed}
 * so that it does not do any unnecessary blocking. So, each extending class
 * should ensure that the necessary concurrency controls are in place if thread
 * safety must be guaranteed.
 * </p>
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class BufferedStore extends BaseStore {

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
     * The {@link Limbo store} where {@link Write writes} are initially stored.
     */
    protected final Limbo limbo;

    /**
     * The {@link DurableStore store} where {@link Write writes} in
     * {@link #limbo} can be {@link Limbo#transport(DurableStore) transported}.
     */
    protected final DurableStore durable;

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
        return add(key, value, record, true, true, true);
    }

    @Override
    public Map<Long, String> audit(long record) {
        return audit(record, false);
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        return audit(key, record, false);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        return browse(key, false);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Map<TObject, Set<Long>> context = durable.browse(key, timestamp);
        return limbo.browse(key, timestamp, context);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        return chronologize(key, record, start, end, false);
    }

    @Override
    public boolean contains(long record) {
        return durable.contains(record) || limbo.contains(record);
    }

    @Override
    public Set<TObject> gather(String key, long record) {
        return gather(key, record, false);
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
        return remove(key, value, record, true, true, true);
    }

    @Override
    public Set<Long> search(String key, String query) {
        // FIXME: should this be implemented using a context instead?
        return Sets.symmetricDifference(limbo.search(key, query),
                durable.search(key, query));
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        return select(record, false);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        Map<String, Set<TObject>> context = durable.select(record, timestamp);
        return limbo.select(record, timestamp, context);
    }

    @Override
    public Set<TObject> select(String key, long record) {
        return select(key, record, false);
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
            for (TObject val : values) {
                limbo.insert(Write.remove(key, val, record)); /* Authorized */
            }
            limbo.insert(Write.add(key, value, record)); /* Authorized */
        }
        catch (ReferentialIntegrityException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        return verify(key, value, record, false);

    }

    @Override
    public boolean verify(String key, TObject value, long record,
            long timestamp) {
        return limbo.verify(Write.notStorable(key, value, record), timestamp,
                durable.verify(key, value, record, timestamp));
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
     * @param key
     * @param value
     * @param record
     * @param sync - a flag that controls whether the data is durably persisted,
     *            if possible (i.e. fsynced) when it is inserted into the
     *            {@link #limbo}
     * @param doVerify - a flag that controls whether an attempt is made to
     *            verify that removing the data is legal. This should always be
     *            set to {@code true} unless it is being called from a context
     *            where the operation has been previously verified (i.e.
     *            committing writes from an atomic operation or transaction)
     * @param lockOnVerify - a flag that controls whether a lock is grabbed when
     *            verifying the write (if {@code doVerify} is {@code true} and
     *            its possible to verify without locking (which is only possible
     *            in a {@link AtomicSupport} store)). This should generally be
     *            set to {@code true} unless its being called from the context
     *            of an atomic operation or transaction that uses Just-In-Time
     *            locking
     * @return {@code true} if the mapping is added
     */
    protected boolean add(String key, TObject value, long record, boolean sync,
            boolean doVerify, boolean lockOnVerify) {
        try {
            ensureWriteIntegrity(key, value, record);
            Write write = Write.add(key, value, record);
            if(!doVerify || !verify(write, lockOnVerify)) {
                return limbo.insert(write, sync); /* Authorized */
            }
            return false;
        }
        catch (ReferentialIntegrityException e) {
            return false;
        }
    }

    /**
     * Audit {@code record} either using safe methods or unsafe methods..
     * <p>
     * This method returns a log of revisions in {@code record} as a Map
     * associating timestamps (in milliseconds) to CAL statements:
     * </p>
     * 
     * <pre>
     * {
     *    "13703523370000" : "ADD 'foo' AS 'bar bang' TO 1", 
     *    "13703524350000" : "ADD 'baz' AS '10' TO 1",
     *    "13703526310000" : "REMOVE 'foo' AS 'bar bang' FROM 1"
     * }
     * </pre>
     * 
     * @param record
     * @param boolean
     * @return the the revision log
     */
    protected Map<Long, String> audit(long record, boolean unsafe) {
        Map<Long, String> result;
        if(unsafe && durable instanceof AtomicSupport) {
            result = ((AtomicSupport) (durable)).auditUnsafe(record);
        }
        else {
            result = durable.audit(record);
        }
        result.putAll(limbo.audit(record));
        return result;
    }

    /**
     * Audit {@code key} in {@code record} either using safe methods or unsafe
     * methods.
     * <p>
     * This method returns a log of revisions in {@code record} as a Map
     * associating timestamps (in milliseconds) to CAL statements:
     * </p>
     * 
     * <pre>
     * {
     *    "13703523370000" : "ADD 'foo' AS 'bar bang' TO 1", 
     *    "13703524350000" : "ADD 'baz' AS '10' TO 1",
     *    "13703526310000" : "REMOVE 'foo' AS 'bar bang' FROM 1"
     * }
     * </pre>
     * 
     * @param key
     * @param record
     * @param unsafe
     * @return the revision log
     */
    protected Map<Long, String> audit(String key, long record, boolean unsafe) {
        Map<Long, String> result;
        if(unsafe && durable instanceof AtomicSupport) {
            result = ((AtomicSupport) (durable)).auditUnsafe(key, record);
        }
        else {
            result = durable.audit(key, record);
        }
        result.putAll(limbo.audit(key, record));
        return result;
    }

    /**
     * Browse {@code key} either using safe or unsafe methods.
     * <p>
     * This method returns a mapping from each of the values that is currently
     * indexed to {@code key} to a Set the records that contain {@code key} as
     * the associated value. If there are no such values, an empty Map is
     * returned.
     * </p>
     * 
     * @param key
     * @param unsafe
     * @return a possibly empty Map of data
     */
    protected Map<TObject, Set<Long>> browse(String key, boolean unsafe) {
        Map<TObject, Set<Long>> context;
        if(unsafe && durable instanceof AtomicSupport) {
            context = ((AtomicSupport) (durable)).browseUnsafe(key);
        }
        else {
            context = durable.browse(key);
        }
        return limbo.browse(key, Time.now(), context);
    }

    /**
     * Execute the {@code chronologize} function for {@code key} in
     * {@code record} between {@code start} and {@code end} with the option to
     * perform an {@code unsafe} read.
     * 
     * @param key the field name
     * @param record the record id
     * @param start the start timestamp
     * @param end the end timestamp
     * @param unsafe a flag that indicates whether to use the
     *            {@link AtomicSupport#chronologizeUnsafe(String, long, long, long)
     *            unsafe chronologize} read in the {@link #durable}; this
     *            should be {@code true} doing an atomic operation
     * @return a possibly empty Map from each revision timestamp to the Set of
     *         objects that were contained in the field at the time of the
     *         revision
     */
    protected Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end, boolean unsafe) {
        Map<Long, Set<TObject>> context;
        if(unsafe && durable instanceof AtomicSupport) {
            context = ((AtomicSupport) (durable)).chronologizeUnsafe(key,
                    record, start, end);
        }
        else {
            context = durable.chronologize(key, record, start, end);
        }
        return limbo.chronologize(key, record, start, end, context);
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        Map<Long, Set<TObject>> context = durable.explore(timestamp, key,
                operator, values);
        return limbo.explore(context, timestamp, key, operator, values);
    }

    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        return doExplore(key, operator, values, false);
    }

    /**
     * Do the work to explore {@code key} {@code operator} {@code values}
     * without worry about normalizing the {@code operator} or {@code values}
     * either using safe or unsafe methods.
     * 
     * @param key
     * @param operator
     * @param values
     * @return {@code Map}
     */
    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject[] values, boolean unsafe) {
        Map<Long, Set<TObject>> context;
        if(unsafe && durable instanceof AtomicSupport) {
            context = ((AtomicSupport) (durable)).doExploreUnsafe(key, operator,
                    values);
        }
        else {
            context = durable.explore(key, operator, values);
        }
        return limbo.explore(context, Time.now(), key, operator, values);
    }

    /**
     * Gather {@code key} from {@code record} either using safe or unsafe
     * methods.
     * <p>
     * This method returns the values currently mapped from {@code key} in
     * {@code record}. The returned Set is nonempty if and only if {@code key}
     * is a member of the Set returned from {@link #describe(long)}.
     * </p>
     * 
     * @param key
     * @param record
     * @param lock
     * @return a possibly empty Set of values
     */
    protected Set<TObject> gather(String key, long record, boolean lock) {
        Set<TObject> context;
        if(!lock && durable instanceof AtomicSupport) {
            context = ((AtomicSupport) (durable)).gatherUnsafe(key, record);
        }
        else {
            context = durable.gather(key, record);
        }
        return limbo.gather(key, record, Time.now(), context);
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
     * @param key
     * @param value
     * @param record
     * @param sync - a flag that controls whether the data is durably persisted,
     *            if possible (i.e. fsynced) when it is inserted into the
     *            {@link #limbo}
     * @param doVerify - a flag that controls whether an attempt is made to
     *            verify that removing the data is legal. This should always be
     *            set to {@code true} unless it is being called from a context
     *            where the operation has been previously verified (i.e.
     *            committing writes from an atomic operation or transaction)
     * @param lockOnVerify - a flag that controls whether a lock is grabbed when
     *            verifying the write (if {@code doVerify} is {@code true} and
     *            its possible to verify without locking (which is only possible
     *            in a {@link AtomicSupport} store)). This should generally be
     *            set to {@code true} unless its being called from the context
     *            of an atomic operation or transaction that uses Just-In-Time
     *            locking
     * @return {@code true} if the mapping is removed
     */
    protected boolean remove(String key, TObject value, long record,
            boolean sync, boolean doVerify, boolean lockOnVerify) {
        try {
            ensureWriteIntegrity(key, value, record);
            Write write = Write.remove(key, value, record);
            if(!doVerify || verify(write, lockOnVerify)) {
                return limbo.insert(write, sync); /* Authorized */
            }
            return false;
        }
        catch (ReferentialIntegrityException e) {
            return false;
        }
    }

    /**
     * Select {@code record} either using safe or unsafe methods.
     * <p>
     * This method returns a mapping from each of the nonempty keys in
     * {@code record} to a Set of associated values. If there are no such keys,
     * an empty Map is returned.
     * </p>
     * 
     * @param record
     * @param unsafe
     * @return a possibly empty Map of data.
     */
    protected Map<String, Set<TObject>> select(long record, boolean unsafe) {
        Map<String, Set<TObject>> context;
        if(unsafe && durable instanceof AtomicSupport) {
            context = ((AtomicSupport) (durable)).selectUnsafe(record);
        }
        else {
            context = durable.select(record);
        }
        return limbo.select(record, Time.now(), context);
    }

    /**
     * Select {@code key} from {@code record} either using safe or unsafe
     * methods.
     * <p>
     * This method returns the values currently mapped from {@code key} in
     * {@code record}. The returned Set is nonempty if and only if {@code key}
     * is a member of the Set returned from {@link #describe(long)}.
     * </p>
     * 
     * @param key
     * @param record
     * @param lock
     * @return a possibly empty Set of values
     */
    protected Set<TObject> select(String key, long record, boolean lock) {
        Set<TObject> context;
        if(!lock && durable instanceof AtomicSupport) {
            context = ((AtomicSupport) (durable)).selectUnsafe(key, record);
        }
        else {
            context = durable.select(key, record);
        }
        return limbo.select(key, record, Time.now(), context);
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
     * @param lockOnRead
     */
    protected void set(String key, TObject value, long record,
            boolean lockOnRead) {
        try {
            ensureWriteIntegrity(key, value, record);
            Set<TObject> values = select(key, record, lockOnRead);
            for (TObject val : values) {
                limbo.insert(Write.remove(key, val, record)); /* Authorized */
            }
            limbo.insert(Write.add(key, value, record)); /* Authorized */
        }
        catch (ReferentialIntegrityException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * Verify {@code key} equals {@code value} in {@code record} either using
     * safe or unsafe method.
     * <p>
     * This method checks that there is <em>currently</em> a mapping from
     * {@code key} to {@code value} in {@code record}. This method has the same
     * affect as calling {@link #select(String, long)}
     * {@link Set#contains(Object)}.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if there is a an association from {@code key} to
     *         {@code value} in {@code record}
     */
    protected boolean verify(String key, TObject value, long record,
            boolean unsafe) {
        boolean destResult;
        if(unsafe && durable instanceof AtomicSupport) {
            destResult = ((AtomicSupport) (durable)).verifyUnsafe(key, value,
                    record);
        }
        else {
            destResult = durable.verify(key, value, record);
        }
        return limbo.verify(Write.notStorable(key, value, record), destResult);
    }

    /**
     * Shortcut method to verify {@code write}. This method is called from
     * {@link #add(String, TObject, long)} and
     * {@link #remove(String, TObject, long)} so that we can avoid creating a
     * duplicate Write.
     * 
     * @param write
     * @return {@code true} if {@code write} currently exists
     */
    protected final boolean verify(Write write) {
        return verify(write, true);
    }

    /**
     * Shortcut method to verify {@code write}. This method is called from
     * {@link #add(String, TObject, long)} and
     * {@link #remove(String, TObject, long)} so that we can avoid creating a
     * duplicate Write.
     * 
     * @param write the comparison {@link Write} to verify
     * @param lock a flag that controls whether an {@link AtomicSupport} store
     *            should or should not grab a lock when performing this
     *            operation
     * @return {@code true} if {@code write} currently exists
     */
    protected boolean verify(Write write, boolean lock) {
        String key = write.getKey().toString();
        TObject value = write.getValue().getTObject();
        long record = write.getRecord().longValue();
        TernaryTruth exists = limbo.verifyFast(write);
        if(exists != TernaryTruth.UNSURE) {
            return exists.boolValue();
        }
        else {
            if((!(limbo instanceof InventoryTracker)
                    && durable instanceof InventoryTracker)
                    && !((InventoryTracker) durable).getInventory()
                            .contains(write.getRecord().longValue())) {
                return false; // This is basically a special case for atomic
                              // operations

            }
            else if(!lock && durable instanceof AtomicSupport) {
                return ((AtomicSupport) durable).verifyUnsafe(key, value,
                        record);
            }
            else {
                return durable.verify(key, value, record);
            }
        }
    }

}
