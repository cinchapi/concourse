/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.server.concurrent.LockService;
import org.cinchapi.concourse.server.concurrent.RangeLockService;
import org.cinchapi.concourse.server.storage.temp.Limbo;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;

import com.google.common.collect.Sets;

/**
 * A {@link BufferedStore} holds data in a {@link ProxyStore} buffer before
 * making batch commits to some other {@link PermanentStore}.
 * <p>
 * Data is written to the buffer until the buffer is full, at which point the
 * BufferingStore will flush the data to the destination store. Reads are
 * handled by taking the XOR (see {@link Sets#symmetricDifference(Set, Set)} or
 * XOR truth (see <a
 * href="http://en.wikipedia.org/wiki/Exclusive_or#Truth_table"
 * >http://en.wikipedia.org/wiki/Exclusive_or#Truth_table</a>) of the values
 * read from the buffer and the destination.
 * </p>
 * 
 * @author jnelson
 */
public abstract class BufferedStore extends BaseStore {

    // NOTE ON LOCKING:
    // ================
    // We can't do any global locking to coordinate the #buffer and #destination
    // in this class because we cannot make assumptions about how the
    // implementing class actually wants to handle concurrency (i.e. an
    // AtomicOperation does not grab any coordinating locks until it goes to
    // commit so that it doesn't do any unnecessary blocking).

    // NOTE ON HISTORICAL READS
    // ========================
    // Buffered historical reads do not merely delegate to their timestamp
    // accepting counterparts because we expect the #destination to use
    // different code paths for present vs historical reads unlike Limbo.

    /**
     * The {@code buffer} is the place where data is initially stored. The
     * contained data is eventually moved to the {@link #destination} when the
     * {@link Limbo#transport(PermanentStore)} method is called.
     */
    protected final Limbo buffer;

    /**
     * The {@code destination} is the place where data is stored when it is
     * transferred from the {@link #buffer}. The {@code destination} defines its
     * protocol for accepting data in the {@link PermanentStore#accept(Write)}
     * method.
     */
    protected final PermanentStore destination;

    /**
     * The {@link LockService} that is used to coordinate concurrent operations.
     */
    protected LockService lockService;

    /**
     * The {@link RangeLockService} that is used to coordinate concurrent
     * operations.
     */
    protected RangeLockService rangeLockService;

    /**
     * Construct a new instance.
     * 
     * @param transportable
     * @param destination
     * @param lockService
     * @param rangeLockService
     */
    protected BufferedStore(Limbo transportable, PermanentStore destination,
            LockService lockService, RangeLockService rangeLockService) {
        this.buffer = transportable;
        this.destination = destination;
        this.lockService = lockService;
        this.rangeLockService = rangeLockService;
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
        return add(key, value, record, true, true);
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
    public Map<String, Set<TObject>> browse(long record) {
        return browse(record, false);
    }

    @Override
    public Map<String, Set<TObject>> browse(long record, long timestamp) {
        Map<String, Set<TObject>> context = destination.browse(record,
                timestamp);
        return buffer.browse(record, timestamp, context);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        Map<TObject, Set<Long>> context = destination.browse(key);
        return buffer.browse(key, Time.now(), context);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Map<TObject, Set<Long>> context = destination.browse(key, timestamp);
        return buffer.browse(key, timestamp, context);
    }

    @Override
    public Set<TObject> fetch(String key, long record) {
        return fetch(key, record, false);
    }

    @Override
    public Set<TObject> fetch(String key, long record, long timestamp) {
        Set<TObject> context = destination.fetch(key, record, timestamp);
        return buffer.fetch(key, record, timestamp, context);
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
        return remove(key, value, record, true, true);
    }

    @Override
    public Set<Long> search(String key, String query) {
        // FIXME: should this be implemented using a context instead?
        return Sets.symmetricDifference(buffer.search(key, query),
                destination.search(key, query));
    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        return verify(key, value, record, false);

    }

    @Override
    public boolean verify(String key, TObject value, long record, long timestamp) {
        return buffer.verify(Write.notStorable(key, value, record), timestamp,
                destination.verify(key, value, record, timestamp));
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
    private boolean verify(Write write) {
        return buffer.verify(write, destination.verify(write.getKey()
                .toString(), write.getValue().getTObject(), write.getRecord()
                .longValue()));
    }

    /**
     * Add {@code key} as {@code value} to {@code record} with the directive to
     * {@code sync} the data or not. Depending upon the implementation of the
     * {@link #buffer}, a sync may guarantee that the data is durably stored.
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
     * @param sync
     * @param validate
     * @return {@code true} if the mapping is added
     */
    protected boolean add(String key, TObject value, long record, boolean sync,
            boolean validate) {
        Write write = Write.add(key, value, record);
        if(!validate || !verify(write)) {
            return buffer.insert(write, sync); /* Authorized */
        }
        return false;
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
        if(unsafe && destination instanceof Compoundable) {
            result = ((Compoundable) (destination)).auditUnsafe(record);
        }
        else {
            result = destination.audit(record);
        }
        result.putAll(buffer.audit(record));
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
        if(unsafe && destination instanceof Compoundable) {
            result = ((Compoundable) (destination)).auditUnsafe(key, record);
        }
        else {
            result = destination.audit(key, record);
        }
        result.putAll(buffer.audit(key, record));
        return result;
    }

    /**
     * Browse {@code record} either using safe or unsafe methods.
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
    protected Map<String, Set<TObject>> browse(long record, boolean unsafe) {
        Map<String, Set<TObject>> context;
        if(unsafe && destination instanceof Compoundable) {
            context = ((Compoundable) (destination)).browseUnsafe(record);
        }
        else {
            context = destination.browse(record);
        }
        return buffer.browse(record, Time.now(), context);
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
        if(unsafe && destination instanceof Compoundable) {
            context = ((Compoundable) (destination)).browseUnsafe(key);
        }
        else {
            context = destination.browse(key);
        }
        return buffer.browse(key, Time.now(), context);
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        Map<Long, Set<TObject>> context = destination.explore(timestamp, key,
                operator, values);
        return buffer.explore(context, timestamp, key, operator, values);
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
        if(unsafe && destination instanceof Compoundable) {
            context = ((Compoundable) (destination)).doExploreUnsafe(key,
                    operator, values);
        }
        else {
            context = destination.explore(key, operator, values);
        }
        return buffer.explore(context, Time.now(), key, operator, values);
    }

    /**
     * Fetch {@code key} from {@code record} either using safe or unsafe
     * methods.
     * <p>
     * This method returns the values currently mapped from {@code key} in
     * {@code record}. The returned Set is nonempty if and only if {@code key}
     * is a member of the Set returned from {@link #describe(long)}.
     * </p>
     * 
     * @param key
     * @param record
     * @param unsafe
     * @return a possibly empty Set of values
     */
    protected Set<TObject> fetch(String key, long record, boolean unsafe) {
        Set<TObject> context;
        if(unsafe && destination instanceof Compoundable) {
            context = ((Compoundable) (destination)).fetchUnsafe(key, record);
        }
        else {
            context = destination.fetch(key, record);
        }
        return buffer.fetch(key, record, Time.now(), context);
    }

    /**
     * Remove {@code key} as {@code value} from {@code record} with the
     * directive to {@code sync} the data or not. Depending upon the
     * implementation of the {@link #buffer}, a sync may guarantee that the data
     * is durably stored.
     * <p>
     * This method deletes the mapping from {@code key} to {@code value} in
     * {@code record}, if that mapping <em>currently</em> exists (i.e.
     * {@link #verify(String, Object, long)} is {@code true}. No other mappings
     * from {@code key} in {@code record} are affected.
     * 
     * @param key
     * @param value
     * @param record
     * @param sync
     * @param validate
     * @return {@code true} if the mapping is removed
     */
    protected boolean remove(String key, TObject value, long record,
            boolean sync, boolean validate) {
        Write write = Write.remove(key, value, record);
        if(!validate || verify(write)) {
            return buffer.insert(write, sync); /* Authorized */
        }
        return false;
    }

    /**
     * Verify {@code key} equals {@code value} in {@code record} either using
     * safe or unsafe method.
     * <p>
     * This method checks that there is <em>currently</em> a mapping from
     * {@code key} to {@code value} in {@code record}. This method has the same
     * affect as calling {@link #fetch(String, long)}
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
        if(unsafe && destination instanceof Compoundable) {
            destResult = ((Compoundable) (destination)).verifyUnsafe(key,
                    value, record);
        }
        else {
            destResult = destination.verify(key, value, record);
        }
        return buffer.verify(Write.notStorable(key, value, record), destResult);
    }

}
