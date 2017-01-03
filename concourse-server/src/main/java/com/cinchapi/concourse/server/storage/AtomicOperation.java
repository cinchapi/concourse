/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;

import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.concurrent.LockService;
import com.cinchapi.concourse.server.concurrent.LockType;
import com.cinchapi.concourse.server.concurrent.RangeLockService;
import com.cinchapi.concourse.server.concurrent.RangeToken;
import com.cinchapi.concourse.server.concurrent.RangeTokens;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.model.Ranges;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.temp.Queue;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

/**
 * A linearizable sequence of reads and writes that all succeed or fail
 * together. Each atomic operation is staged in an isolated buffer before being
 * committed to a destination store. For optimal concurrency, we use
 * <em>just in time locking</em> where destination resources are only locked
 * when its time to commit the operation.
 * 
 * @author Jeff Nelson
 */
public class AtomicOperation extends BufferedStore implements
        VersionChangeListener {
    // NOTE: This class does not need to do any locking on operations (until
    // commit time) because it is assumed to be isolated to one thread and the
    // destination is assumed to have its own concurrency control scheme in
    // place.

    /**
     * Start a new AtomicOperation that will commit to {@code store}.
     * <p>
     * Always use the {@link AtomicSupport#startAtomicOperation()} method over
     * this one because each store <em>may</em> may do some customization to the
     * AtomicOperation before it is returned to the caller.
     * </p>
     * 
     * @param store
     * @return the AtomicOperation
     */
    protected static AtomicOperation start(AtomicSupport store) {
        return new AtomicOperation(store);
    }

    /**
     * The initial capacity
     */
    protected static final int INITIAL_CAPACITY = 10;

    /**
     * The {@link RangeToken range read tokens} that represent any queries in
     * this operation that we must grab locks for at commit time.
     */
    private final RangeHolder rangeReads2Lock = new RangeHolder();

    /**
     * The read {@link Token tokens} that represent any record based reads in
     * this operation that we must grab locks for at commit time.
     */
    private final Set<Token> reads2Lock = Sets.newHashSet();

    /**
     * This map contains all the records in which a "wide read" (e.g. a read
     * that touches every field in the record) was performed. This data is used
     * to determine if lock coarsening can be performed at commit time.
     */
    private final Map<Long, Token> wideReads = Maps.newHashMap();

    /**
     * A casted pointer to the destination store, which is the source from which
     * this Atomic Operation stems.
     */
    private final AtomicSupport source;

    /**
     * The write {@link Token tokens} or {@link RangeToken range write tokens}
     * that represent the writes in this operation that we must grab locks for
     * at commit time. Both write and range write tokens are stored in the same
     * collection for efficiency reasons.
     */
    private final Set<Token> writes2Lock = Sets.newHashSet();

    /**
     * A flag that indicates this atomic operation has successfully grabbed all
     * required locks and is in the process of committing or has been notified
     * of a version change and is in the process of aborting. We use this flag
     * to protect the atomic operation from version change notifications that
     * happen while its committing (because the version change notifications
     * come from the transaction itself).
     */
    protected AtomicBoolean finalizing = new AtomicBoolean(false);

    /**
     * The collection of {@link LockDescription} objects that are grabbed in the
     * {@link #grabLocks()} method at commit time.
     */
    @Nullable
    protected Map<Token, LockDescription> locks = null;

    /**
     * The AtomicOperation is open until it is committed or aborted.
     */
    protected AtomicBoolean open = new AtomicBoolean(true);

    /**
     * A flag that is set when the atomic operation must fail because it is
     * notified about a version change. Each operation checks this flag so it
     * can know whether it needs to perform an abort (to clean up resources).
     */
    private boolean notifiedAboutVersionChange = false;

    /**
     * Construct a new instance.
     * 
     * @param destination - must be a {@link AtomicSupport}
     */
    protected AtomicOperation(AtomicSupport destination) {
        this(new Queue(INITIAL_CAPACITY), destination);
    }

    /**
     * Construct a new instance.
     * 
     * @param buffer
     * @param destination - must be a {@link AtomicSupport}
     */
    protected AtomicOperation(Queue buffer, AtomicSupport destination) {
        super(buffer, destination, ((BufferedStore) destination).lockService,
                ((BufferedStore) destination).rangeLockService);
        this.source = (AtomicSupport) this.destination;
    }

    /**
     * Close this operation and release all of the held locks without applying
     * any of the changes to the {@link #destination} store.
     */
    public void abort() {
        open.set(false);
        finalizing.set(true);
        if(locks != null && !locks.isEmpty()) {
            releaseLocks();
        }
    }

    @Override
    public boolean add(String key, TObject value, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrapCached(key),
                Value.wrap(value));
        Token wide = wideReads.get(record);
        if(wide != null) {
            wide.upgrade();
            writes2Lock.add(wide);
        }
        else {
            source.addVersionChangeListener(token, this);
            writes2Lock.add(token);
        }
        writes2Lock.add(rangeToken);
        return super.add(key, value, record, true, true, false);
    }

    @Override
    public Map<Long, String> audit(long record) throws AtomicStateException {
        checkState();
        Token token = Token.wrap(record);
        source.addVersionChangeListener(token, this);
        reads2Lock.add(token);
        wideReads.put(record, token);
        return super.audit(record, true);
    }

    @Override
    public Map<Long, String> audit(String key, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        source.addVersionChangeListener(token, this);
        reads2Lock.add(token);
        return super.audit(key, record, true);
    }

    @Override
    public boolean contains(long record) {
        checkState();
        Token token = Token.wrap(record);
        source.addVersionChangeListener(token, this);
        reads2Lock.add(token);
        wideReads.put(record, token);
        return super.contains(record);
    }

    @Override
    public Map<String, Set<TObject>> select(long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(record);
        source.addVersionChangeListener(token, this);
        reads2Lock.add(token);
        wideReads.put(record, token);
        return super.browse(record, true);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return select(record);
        }
        else {
            checkState();
            return super.select(record, timestamp);
        }
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key)
            throws AtomicStateException {
        checkState();
        Text key0 = Text.wrapCached(key);
        RangeToken rangeToken = RangeToken.forReading(key0, Operator.BETWEEN,
                Value.NEGATIVE_INFINITY, Value.POSITIVE_INFINITY);
        source.addVersionChangeListener(rangeToken, this);
        Iterable<Range<Value>> ranges = RangeTokens.convertToRange(rangeToken);
        for (Range<Value> range : ranges) {
            rangeReads2Lock.put(key0, range);
        }
        return super.browse(key, true);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return browse(key);
        }
        else {
            checkState();
            return super.browse(key, timestamp);
        }
    }

    /**
     * Commit the atomic operation to the destination store. The commit is only
     * successful if all the grouped operations can be successfully applied to
     * the destination. If the commit fails, the caller should retry the atomic
     * operation.
     * 
     * @return {@code true} if the atomic operation is completely applied
     */
    public final boolean commit() throws AtomicStateException {
        if(open.compareAndSet(true, false)) {
            if(grabLocks() && !notifiedAboutVersionChange
                    && finalizing.compareAndSet(false, true)) {
                doCommit();
                releaseLocks();
                if(destination instanceof Transaction) {
                    ((Transaction) destination).onCommit(this);
                }
                return true;
            }
            else {
                abort();
                return false;
            }
        }
        else {
            try {
                checkState();
            }
            catch (TransactionStateException e) { // the Transaction subclass
                                                  // overrides #checkState() to
                                                  // throw this exception to
                                                  // distinguish transaction
                                                  // failures
                throw e;
            }
            catch (AtomicStateException e) {/* ignore */}
            return false;
        }
    }

    @Override
    public Set<TObject> select(String key, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        source.addVersionChangeListener(token, this);
        reads2Lock.add(token);
        return super.select(key, record, true);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) throws AtomicStateException {
        checkState();
        long now = Time.now();
        if(start > now || end > now) {
            // Must perform a locking read to prevent a non-repeatable read if
            // writes occur between the present and the future timestamp(s)
            Token token = Token.wrap(key, record);
            source.addVersionChangeListener(token, this);
            reads2Lock.add(token);
            System.out.println("yo");
            return super.chronologize(key, record, start, end, true);
        }
        else {
            return super.chronologize(key, record, start, end);
        }
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return select(key, record);
        }
        else {
            checkState();
            return super.select(key, record, timestamp);
        }
    }

    @Override
    @Restricted
    public void onVersionChange(Token token) {
        notifiedAboutVersionChange = true;
        open.set(false);
    }

    @Override
    public boolean remove(String key, TObject value, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrapCached(key),
                Value.wrap(value));
        Token wide = wideReads.get(record);
        if(wide != null) {
            wide.upgrade();
            writes2Lock.add(wide);
        }
        else {
            source.addVersionChangeListener(token, this);
            writes2Lock.add(token);
        }
        writes2Lock.add(rangeToken);
        return super.remove(key, value, record, true, true, false);
    }

    @Override
    public Set<Long> search(String key, String query)
            throws AtomicStateException {
        checkState();
        return super.search(key, query);
    }

    @Override
    public void set(String key, TObject value, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrapCached(key),
                Value.wrap(value));
        Token wide = wideReads.get(record);
        if(wide != null) {
            wide.upgrade();
            writes2Lock.add(wide);
        }
        else {
            source.addVersionChangeListener(token, this);
            writes2Lock.add(token);
        }
        writes2Lock.add(rangeToken);
        super.set(key, value, record, false);
    }

    /**
     * Register interest in {@code record} so that this AtomicOperation can
     * listen for changes and grab a read lock at commit time.
     * 
     * @param record
     */
    public void touch(long record) {
        checkState();
        Token token = Token.wrap(record);
        source.addVersionChangeListener(token, this);
        reads2Lock.add(token);
        wideReads.put(record, token);
    }

    @Override
    public final void start() {}

    @Override
    public final void stop() {}

    @Override
    public boolean verify(String key, TObject value, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        source.addVersionChangeListener(token, this);
        reads2Lock.add(token);
        return super.verify(key, value, record, true);
    }

    @Override
    public boolean verify(String key, TObject value, long record, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return verify(key, value, record);
        }
        else {
            checkState();
            return super.verify(key, value, record, timestamp);
        }
    }

    /**
     * Check each one of the {@link #intentions} against the
     * {@link #destination} and grab the appropriate locks along the way. This
     * method will return {@code true} if all expectations are met and all
     * necessary locks are grabbed. Otherwise it will return {@code false}, in
     * which case this operation should be aborted immediately.
     * 
     * @return {@code true} if all expectations are met and all necessary locks
     *         are grabbed.
     */
    private boolean grabLocks() {
        if(isReadOnly()) {
            return true;
        }
        else {
            // NOTE: If we can't grab a lock immediately because it is held by
            // someone else, then we must fail immediately because the
            // AtomicOperation can't properly commit.
            locks = Maps.newHashMap();
            try {
                // Grab write locks and remove any covered read or range read
                // intentions
                for (Token token : writes2Lock) {
                    if(notifiedAboutVersionChange) {
                        return false;
                    }
                    LockType type;
                    if(token instanceof RangeToken) {
                        RangeToken rangeToken = (RangeToken) token;
                        if(!rangeReads2Lock.isEmpty(rangeToken.getKey())) {
                            Range<Value> containing = rangeReads2Lock.get(
                                    rangeToken.getKey(),
                                    rangeToken.getValues()[0]);
                            if(containing != null) {
                                rangeReads2Lock.remove(rangeToken.getKey(),
                                        containing);
                                Iterable<Range<Value>> xor = Ranges.xor(Range
                                        .singleton(rangeToken.getValues()[0]),
                                        containing);
                                for (Range<Value> range : xor) {
                                    rangeReads2Lock.put(rangeToken.getKey(),
                                            range);
                                }
                            }
                        }
                        type = LockType.RANGE_WRITE;
                    }
                    else {
                        reads2Lock.remove(token);
                        type = LockType.WRITE;
                    }
                    LockDescription lock = LockDescription.forToken(token,
                            lockService, rangeLockService, type);
                    if(lock.getLock().tryLock()) {
                        locks.put(lock.getToken(), lock);
                    }
                    else {
                        return false;
                    }
                }
                // Grab the read locks. We can be sure that any remaining
                // intentions are not covered by any of the write locks we
                // grabbed previously.
                for (Token token : reads2Lock) {
                    if(notifiedAboutVersionChange) {
                        return false;
                    }
                    LockDescription lock = LockDescription.forToken(token,
                            lockService, rangeLockService, LockType.READ);
                    if(lock.getLock().tryLock()) {
                        locks.put(lock.getToken(), lock);
                    }
                    else {
                        return false;
                    }
                }
                // Grab the range read locks. We can be sure that any remaining
                // intentions are not covered by any of the range write locks we
                // grabbed previously.
                for (Entry<Text, RangeSet<Value>> entry : rangeReads2Lock.ranges
                        .entrySet()) { /* (Authorized) */
                    if(notifiedAboutVersionChange) {
                        return false;
                    }
                    Text key = entry.getKey();
                    for (Range<Value> range : entry.getValue().asRanges()) {
                        RangeToken rangeToken = Ranges.convertToRangeToken(key,
                                range);
                        LockDescription lock = LockDescription.forToken(
                                rangeToken, lockService, rangeLockService,
                                LockType.RANGE_READ);
                        if(lock.getLock().tryLock()) {
                            locks.put(lock.getToken(), lock);
                        }
                        else {
                            return false;
                        }
                    }
                }
            }
            catch (NullPointerException e) {
                // If we are notified a version change while grabbing locks, we
                // abort immediately which means that #locks will become null.
                return false;
            }
            return true;
        }
    }

    /**
     * Release all of the locks that are held by this operation.
     */
    private void releaseLocks() {
        if(isReadOnly()) {
            return;
        }
        else if(locks != null) {
            Map<Token, LockDescription> _locks = locks;
            locks = null; // CON-172: Set the reference of the locks to null
                          // immediately to prevent a race condition where
                          // the #grabLocks method isn't notified of version
                          // change failure in time
            for (LockDescription lock : _locks.values()) {
                lock.getLock().unlock(); // We should never encounter an
                                         // IllegalMonitorStateException
                                         // here because a lock should only
                                         // go in #locks once it has been
                                         // locked.
            }
        }
    }

    /**
     * Check that this AtomicOperation is open and throw an
     * AtomicStateException if it is not.
     * 
     * @throws AtomicStateException
     */
    protected void checkState() throws AtomicStateException {
        if(notifiedAboutVersionChange) {
            abort();
        }
        if(!open.get()) {
            throw new AtomicStateException();
        }
    }

    /**
     * Transport the written data to the {@link #destination} store. The
     * subclass may override this method to do additional things (i.e. backup
     * the data, etc) if necessary.
     */
    protected void doCommit() {
        doCommit(false);
    }

    /**
     * Transport the written data to the {@link #destination} store. The
     * subclass may override this method to do additional things (i.e. backup
     * the data, etc) if necessary.
     * 
     * @param syncAndVerify a flag that controls whether this operation will
     *            cause the {@code destination} to always perform a sync and
     *            verify for each write that is transported. If this value is
     *            set to {@code false}, this operation will transport all the
     *            writes without instructing the destination to sync and verify.
     *            Once all the writes have been transported, the destination
     *            will be instructed to sync the writes as a group (GROUP SYNC),
     *            but no verification will occur for any of the writes (which is
     *            okay as long as this operation implicitly verifies each write
     *            prior to commit, see CON-246).
     *            <p>
     *            NOTE: This parameter is eventually passed from the
     *            {@code verify} parameter in a call to
     *            {@link BufferedStore#add(String, TObject, long, boolean, boolean, boolean)}
     *            or
     *            {@link BufferedStore#remove(String, TObject, long, boolean, boolean, boolean)}
     *            . Generally speaking, this coupling between optional syncing
     *            and optional verifying is okay because it doesn't make sense
     *            to sync but not verify or verify but not sync.
     *            </p>
     */
    protected void doCommit(boolean syncAndVerify) {
        // Since we don't take a backup, it is possible that we can end up
        // in a situation where the server crashes in the middle of the data
        // transport, which means that the atomic operation would be partially
        // committed on server restart, which would appear to violate the
        // "all or nothing" guarantee. We are willing to live with that risk
        // because the occurrence of that happening seems low and atomic
        // operations don't guarantee consistency or durability, so it is
        // technically not a violation of "all or nothing" if the entire
        // operation succeeds but isn't durable on crash and leaves the database
        // in an inconsistent state.
        buffer.transport(destination, syncAndVerify);
        if(!syncAndVerify) {
            destination.sync();
        }
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        if(timestamp > Time.now()) {
            return doExplore(key, operator, values);
        }
        else {
            checkState();
            return super.doExplore(timestamp, key, operator, values);
        }
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        checkState();
        Text key0 = Text.wrapCached(key);
        RangeToken rangeToken = RangeToken.forReading(key0, operator,
                Transformers.transformArray(values, Functions.TOBJECT_TO_VALUE,
                        Value.class));
        source.addVersionChangeListener(rangeToken, this);
        Iterable<Range<Value>> ranges = RangeTokens.convertToRange(rangeToken);
        for (Range<Value> range : ranges) {
            rangeReads2Lock.put(key0, range);
        }
        return super.doExplore(key, operator, values, true);
    }

    /**
     * Return {@code true} if this Atomic Operation has 0 writes.
     * 
     * @return {@code true} if this atomic operation is considered
     *         <em>read-only</em>
     */
    protected boolean isReadOnly() {
        return ((Queue) buffer).size() == 0;
    }

    /**
     * Encapsulates the logic to efficiently associate keys with ranges for the
     * purposes of JIT range locking.
     * 
     * @author Jeff Nelson
     */
    private class RangeHolder {

        /**
         * A mapping from each key to the ranges we need to lock for that key.
         */
        final Map<Text, RangeSet<Value>> ranges = Maps.newHashMap(); // accessible
                                                                     // to outer
                                                                     // class

        /**
         * Return the unique range for {@code key} that contains {@code value}
         * or {@code null} if it does not exist.
         * 
         * @param key
         * @param value
         * @return the range containing {@code value} for {@code key}
         */
        public Range<Value> get(Text key, Value value) {
            RangeSet<Value> set = ranges.get(key);
            if(set != null) {
                return set.rangeContaining(value);
            }
            else {
                return null;
            }
        }

        /**
         * Return {@code true} if there are no ranges for {@code key}.
         * 
         * @param key
         * @return {@code true} if the mapped range set is empty
         */
        public boolean isEmpty(Text key) {
            RangeSet<Value> set = ranges.get(key);
            return set == null || set.isEmpty();
        }

        /**
         * Add {@code range} for {@code key}.
         * 
         * @param key
         * @param range
         */
        public void put(Text key, Range<Value> range) {
            RangeSet<Value> set = ranges.get(key);
            if(set == null) {
                set = TreeRangeSet.create();
                ranges.put(key, set);
            }
            set.add(range);
        }

        /**
         * Remove the {@code range} from {@code key}.
         * 
         * @param key
         * @param range
         */
        public void remove(Text key, Range<Value> range) {
            RangeSet<Value> set = ranges.get(key);
            set.remove(range);
            if(set.isEmpty()) {
                ranges.remove(key);
            }
        }

    }

    /**
     * A LockDescription is a wrapper around a {@link Lock} that contains
     * metadata that can be serialized to disk. The AtomicOperation grabs a
     * collection of LockDescriptions when it goes to commit.
     * 
     * @author Jeff Nelson
     */
    protected static final class LockDescription implements Byteable {

        /**
         * Return the appropriate {@link LockDescription} that will provide
         * coverage for {@code token}
         * 
         * @param token
         * @param lockService
         * @param rangeLockService
         * @return the LockDescription
         */
        public static LockDescription forToken(Token token,
                LockService lockService, RangeLockService rangeLockService,
                LockType type) {
            switch (type) {
            case RANGE_READ:
                return new LockDescription(token,
                        rangeLockService.getReadLock((RangeToken) token), type);
            case RANGE_WRITE:
                return new LockDescription(token,
                        rangeLockService.getWriteLock((RangeToken) token), type);
            case READ:
                return new LockDescription(token,
                        lockService.getReadLock(token), type);
            case WRITE:
                return new LockDescription(token,
                        lockService.getWriteLock(token), type);
            default:
                return null;
            }
        }

        /**
         * Return the LockDescription encoded in {@code bytes} so long as those
         * bytes adhere to the format specified by the {@link #getBytes()}
         * method. This method assumes that all the bytes in the {@code bytes}
         * belong to the LockDescription. In general, it is necessary to get the
         * appropriate LockDescription slice from the parent ByteBuffer using
         * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
         * 
         * @param bytes
         * @param lockService
         * @param rangeLockService
         * @return the LockDescription
         */
        public static LockDescription fromByteBuffer(ByteBuffer bytes,
                LockService lockService, RangeLockService rangeLockService) {
            LockType type = LockType.values()[bytes.get()];
            Token token = null;
            Lock lock = null;
            switch (type) {
            case RANGE_READ:
                token = RangeToken.fromByteBuffer(bytes);
                lock = rangeLockService.getReadLock((RangeToken) token);
                break;
            case RANGE_WRITE:
                token = RangeToken.fromByteBuffer(bytes);
                lock = rangeLockService.getWriteLock((RangeToken) token);
                break;
            case READ:
                token = Token.fromByteBuffer(bytes);
                lock = lockService.getReadLock(token);
                break;
            case WRITE:
                token = Token.fromByteBuffer(bytes);
                lock = lockService.getWriteLock(token);
                break;

            }
            return new LockDescription(token, lock, type);
        }

        private final Lock lock;
        private final Token token;
        private final LockType type;

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param lock
         * @param type
         */
        private LockDescription(Token token, Lock lock, LockType type) {
            this.lock = lock;
            this.type = type;
            this.token = token;
        }

        @Override
        public void copyTo(ByteBuffer buffer) {
            buffer.put((byte) type.ordinal());
            token.copyTo(buffer);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof LockDescription) {
                return lock.equals(((LockDescription) obj).getLock())
                        && type == ((LockDescription) obj).type;
            }
            return false;
        }

        @Override
        public ByteBuffer getBytes() {
            // We do not create a cached copy for the entire class because we'll
            // only ever getBytes() for a lock description once and that only
            // happens if the AtomicOperation is not aborted before an attempt
            // to commit, so its best to not create a copy if we don't have to
            ByteBuffer bytes = ByteBuffer.allocate(size());
            copyTo(bytes);
            bytes.rewind();
            return bytes;
        }

        /**
         * Return the lock that is described by this LockDescription. This
         * method DOES NOT return a TLock, but will return a ReadLock or
         * WriteLock, depending on the LockType. The caller should immediately
         * lock/unlock on whatever is returned from this method.
         * 
         * @return the Read or Write lock.
         */
        public Lock getLock() {
            return lock;
        }

        /**
         * Return the Token.
         * 
         * @return the token
         */
        public Token getToken() {
            return token;
        }

        /**
         * Return the LockType
         * 
         * @return the LockType
         */
        public LockType getType() {
            return type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lock, type);
        }

        @Override
        public int size() {
            return token.size() + 1; // token + type(1)
        }

    }

}
