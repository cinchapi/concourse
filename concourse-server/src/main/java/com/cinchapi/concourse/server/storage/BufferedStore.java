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
 * the possibility that {@link #durable} may be a {@link LockAvoidableStore} and
 * the overall performance of the {@link BufferedStore} may depend on the
 * ability to internal locking. So, methods are exposed to subclasses that
 * accept a {@link LockingAdvisory} parameter that can instruct the
 * buffered resolution logic to use the unlocked versions of a
 * {@link LockAvoidableStore} if they are available.
 * </p>
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class BufferedStore extends AbstractStore {

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
        return add(key, value, record, true, true, LockingAdvisory.DEFAULT);
    }

    @Override
    public Map<Long, String> audit(long record) {
        return audit(record, LockingAdvisory.DEFAULT);
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        return audit(key, record, LockingAdvisory.DEFAULT);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        return browse(key, LockingAdvisory.DEFAULT);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Map<TObject, Set<Long>> context = durable.browse(key, timestamp);
        return limbo.browse(key, timestamp, context);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        return chronologize(key, record, start, end, LockingAdvisory.DEFAULT);
    }

    @Override
    public boolean contains(long record) {
        return durable.contains(record) || limbo.contains(record);
    }

    @Override
    public Set<TObject> gather(String key, long record) {
        return gather(key, record, LockingAdvisory.DEFAULT);
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
        return remove(key, value, record, true, true, LockingAdvisory.DEFAULT);
    }

    @Override
    public Set<Long> search(String key, String query) {
        // FIXME: should this be implemented using a context instead?
        return Sets.symmetricDifference(limbo.search(key, query),
                durable.search(key, query));
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        return select(record, LockingAdvisory.DEFAULT);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        Map<String, Set<TObject>> context = durable.select(record, timestamp);
        return limbo.select(record, timestamp, context);
    }

    @Override
    public Set<TObject> select(String key, long record) {
        return select(key, record, LockingAdvisory.DEFAULT);
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
        set(key, value, record, LockingAdvisory.DEFAULT);
    }

    @Override
    public boolean verify(Write write) {
        return verify(write, LockingAdvisory.DEFAULT);
    }

    @Override
    public boolean verify(Write write, long timestamp) {
        TernaryTruth truth = limbo.verifyFast(write, timestamp);
        if(truth == TernaryTruth.UNSURE) {
            boolean context = durable.verify(write, timestamp);
            return limbo.verify(write, timestamp, context);
        }
        else {
            return truth.boolValue();
        }
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
     * @param verifyLockingAdvisory
     * @return {@code true} if the mapping is added
     */
    protected final boolean add(String key, TObject value, long record,
            boolean sync, boolean doVerify,
            LockingAdvisory verifyLockingAdvisory) {
        try {
            ensureWriteIntegrity(key, value, record);
            Write write = Write.add(key, value, record);
            if(!doVerify || !verify(write, verifyLockingAdvisory)) {
                return limbo.insert(write, sync); /* Authorized */
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
     * @param advisory
     * @return the the revision log
     */
    protected final Map<Long, String> audit(long record,
            LockingAdvisory advisory) {
        Map<Long, String> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).auditUnlocked(record);
        }
        else {
            context = durable.audit(record);
        }
        context.putAll(limbo.audit(record));
        return context;
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
     * @param advisrory
     * @return the revision log
     */
    protected final Map<Long, String> audit(String key, long record,
            LockingAdvisory advisory) {
        Map<Long, String> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).auditUnlocked(key,
                    record);
        }
        else {
            context = durable.audit(key, record);
        }
        context.putAll(limbo.audit(key, record));
        return context;
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
     * @param advisory
     * @return a possibly empty Map of data
     */
    protected final Map<TObject, Set<Long>> browse(String key,
            LockingAdvisory advisory) {
        Map<TObject, Set<Long>> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).browseUnlocked(key);
        }
        else {
            context = durable.browse(key);
        }
        return limbo.browse(key, context);
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
     * @param advisory
     * @return a possibly empty Map from each revision timestamp to the Set of
     *         objects that were contained in the field at the time of the
     *         revision
     */
    protected final Map<Long, Set<TObject>> chronologize(String key,
            long record, long start, long end, LockingAdvisory advisory) {
        Map<Long, Set<TObject>> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).chronologizeUnlocked(key,
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

    @Override
    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        return doExplore(key, operator, values, LockingAdvisory.DEFAULT);
    }

    /**
     * Do the work to explore {@code key} {@code operator} {@code values}
     * without worry about normalizing the {@code operator} or {@code values}
     * either using safe or unsafe methods.
     * 
     * @param key
     * @param operator
     * @param values
     * @param advisory
     * @return {@code Map}
     */
    protected final Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject[] values, LockingAdvisory advisory) {
        Map<Long, Set<TObject>> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).doExploreUnlocked(key,
                    operator, values);
        }
        else {
            context = durable.explore(key, operator, values);
        }
        return limbo.explore(context, key, operator, values);
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
    protected final Set<TObject> gather(String key, long record,
            LockingAdvisory advisory) {
        Set<TObject> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).gatherUnlocked(key,
                    record);
        }
        else {
            context = durable.gather(key, record);
        }
        return limbo.gather(key, record, context);
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
     * @param verifyLockingAdvisory
     * @return {@code true} if the mapping is removed
     */
    protected final boolean remove(String key, TObject value, long record,
            boolean sync, boolean doVerify,
            LockingAdvisory verifyLockingAdvisory) {
        try {
            ensureWriteIntegrity(key, value, record);
            Write write = Write.remove(key, value, record);
            if(!doVerify || verify(write, verifyLockingAdvisory)) {
                return limbo.insert(write, sync); /* Authorized */
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
     * Buffered {@link #select(long)} with configurable {@link LockingAdvisory}.
     * 
     * @param record
     * @param advisory
     * @return the buffered read result
     */
    protected final Map<String, Set<TObject>> select(long record,
            LockingAdvisory advisory) {
        Map<String, Set<TObject>> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).selectUnlocked(record);
        }
        else {
            context = durable.select(record);
        }
        return limbo.select(record, context);
    }

    /**
     * Buffered {@link #select(String, long)} with configurable
     * {@link LockingAdvisory}.
     * 
     * @param key
     * @param record
     * @param advisory
     * @return the buffered read result
     */
    protected final Set<TObject> select(String key, long record,
            LockingAdvisory advisory) {
        Set<TObject> context;
        if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            context = ((LockAvoidableStore) (durable)).selectUnlocked(key,
                    record);
        }
        else {
            context = durable.select(key, record);
        }
        return limbo.select(key, record, context);
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
     * @param advisory
     */
    protected final void set(String key, TObject value, long record,
            LockingAdvisory advisory) {
        try {
            ensureWriteIntegrity(key, value, record);
            Set<TObject> values = select(key, record, advisory);
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
     * Shortcut method to verify {@code write}. This method is called from
     * {@link #add(String, TObject, long)} and
     * {@link #remove(String, TObject, long)} so that we can avoid creating a
     * duplicate Write.
     * 
     * @param write the comparison {@link Write} to verify
     * @param advisory
     * @return {@code true} if {@code write} currently exists
     */
    protected final boolean verify(Write write, LockingAdvisory advisory) {
        // TODO: look in the memory to determine whether it would be faster to
        // look in #limbo or #durable first...
        TernaryTruth truth = limbo.verifyFast(write);
        if(truth != TernaryTruth.UNSURE) {
            return truth.boolValue();
        }
        else if(advisory == LockingAdvisory.SKIP
                && durable instanceof LockAvoidableStore) {
            return ((LockAvoidableStore) durable).verifyUnlocked(write);
        }
        else {
            return durable.verify(write);
        }
    }

    /**
     * If {@link #durable} is a {@link LockAvoidableStore}, describes the
     * appropriate internal methods to use in the course of buffered read logic.
     *
     * @author Jeff Nelson
     */
    protected enum LockingAdvisory {
        DEFAULT, SKIP
    }

}
