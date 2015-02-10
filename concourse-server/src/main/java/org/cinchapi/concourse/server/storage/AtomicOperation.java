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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;

import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.concurrent.LockService;
import org.cinchapi.concourse.server.concurrent.LockType;
import org.cinchapi.concourse.server.concurrent.RangeLockService;
import org.cinchapi.concourse.server.concurrent.RangeToken;
import org.cinchapi.concourse.server.concurrent.RangeTokenMap;
import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.temp.Queue;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Transformers;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A sequence of reads and writes that all succeed or fail together. Each
 * operation is staged in an isolated buffer before being committed to a
 * destination store. For optimal concurrency, we use
 * <em>just in time locking</em> where destination resources are only locked
 * when its time to commit the operation.
 * 
 * @author jnelson
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
     * Always use the {@link Compoundable#startAtomicOperation()} method over
     * this one because each store <em>may</em> may do some customization to the
     * AtomicOperation before it is returned to the caller.
     * </p>
     * 
     * @param store
     * @return the AtomicOperation
     */
    protected static AtomicOperation start(Compoundable store) {
        return new AtomicOperation(store);
    }

    /**
     * It is technically not possible to upgrade a READ lock to a WRITE lock, so
     * we must apply some logical rules to determine if we need to release one
     * of our read locks because the next lock will be an offsetting write lock.
     * 
     * @param expectation
     * @param locks
     */
    protected static void prepareLockForPossibleUpgrade(
            LockIntention expectation, Map<Token, LockDescription> locks,
            RangeTokenMap<LockDescription> rangeLocks) { // visible
                                                         // for
                                                         // testing
        LockType type = expectation.getLockType();
        Token token = expectation.getToken();
        if(token instanceof RangeToken && type == LockType.RANGE_WRITE) {
            RangeToken rt = (RangeToken) token;
            Value value = rt.getValues()[0];
            Iterator<Entry<RangeToken, LockDescription>> it = Iterables.concat(
                    rangeLocks.filter(rt.getKey(), Operator.EQUALS, value)
                            .entrySet(),
                    rangeLocks.filter(rt.getKey(), value, value).entrySet())
                    .iterator();
            while (it.hasNext()) {
                Entry<RangeToken, LockDescription> entry = it.next();
                RangeToken tok = entry.getKey();
                LockDescription lock = entry.getValue();
                if(lock.getType() == LockType.RANGE_READ) {
                    // NOTE: cannot use it.remove() here because the iterator
                    // does not read through to the underlying map
                    rangeLocks.remove(tok).getLock().unlock();
                }
            }
        }
        else if(token instanceof Token && type == LockType.WRITE
                && locks.containsKey(token)
                && locks.get(token).getType() == LockType.READ) {
            locks.remove(token).getLock().unlock();
        }
    }

    /**
     * The initial capacity
     */
    private static final int INITIAL_CAPACITY = 10;

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
     * The collection of range {@link LockDescription} objects that are grabbed
     * in the {@link #grabLocks()} method at commit time.
     */
    @Nullable
    protected RangeTokenMap<LockDescription> rangeLocks = null;

    /**
     * The AtomicOperation is open until it is committed or aborted.
     */
    protected AtomicBoolean open = new AtomicBoolean(true);

    /**
     * The sequence of LockIntentions that were generated from the sequence
     * of operations.
     */
    private final List<LockIntention> intentions = Lists
            .newArrayListWithExpectedSize(INITIAL_CAPACITY);

    /**
     * Construct a new instance.
     * 
     * @param destination - must be a {@link Compoundable}
     */
    protected AtomicOperation(Compoundable destination) {
        super(new Queue(INITIAL_CAPACITY), destination,
                ((BufferedStore) destination).lockService,
                ((BufferedStore) destination).rangeLockService);
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
        ((Compoundable) destination).addVersionChangeListener(
                Token.wrap(key, record), this);
        intentions
                .add(new KeyInRecordLockIntention(key, record, LockType.WRITE));
        intentions.add(new RangeLockIntention(Text.wrapCached(key), Value
                .wrap(value)));
        return super.add(key, value, record);
    }

    @Override
    public Map<Long, String> audit(long record) throws AtomicStateException {
        checkState();
        ((Compoundable) destination).addVersionChangeListener(
                Token.wrap(record), this);
        intentions.add(new RecordLockIntention(record));
        return super.audit(record, true);
    }

    @Override
    public Map<Long, String> audit(String key, long record)
            throws AtomicStateException {
        checkState();
        ((Compoundable) destination).addVersionChangeListener(
                Token.wrap(key, record), this);
        intentions
                .add(new KeyInRecordLockIntention(key, record, LockType.READ));
        return super.audit(key, record, true);
    }

    @Override
    public Map<String, Set<TObject>> browse(long record)
            throws AtomicStateException {
        checkState();
        ((Compoundable) destination).addVersionChangeListener(
                Token.wrap(record), this);
        intentions.add(new RecordLockIntention(record));
        return super.browse(record, true);
    }

    @Override
    public Map<String, Set<TObject>> browse(long record, long timestamp)
            throws AtomicStateException {
        checkState();
        return super.browse(record, timestamp);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key)
            throws AtomicStateException {
        checkState();
        ((Compoundable) destination)
                .addVersionChangeListener(RangeToken.forReading(
                        Text.wrapCached(key), Operator.BETWEEN,
                        Value.NEGATIVE_INFINITY, Value.POSITIVE_INFINITY), this);
        intentions.add(new KeyLockIntention(key));
        return super.browse(key, true);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp)
            throws AtomicStateException {
        checkState();
        return super.browse(key, timestamp);
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
            if(grabLocks() && finalizing.compareAndSet(false, true)) {
                doCommit();
                releaseLocks();
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
    public Set<TObject> fetch(String key, long record)
            throws AtomicStateException {
        checkState();
        ((Compoundable) destination).addVersionChangeListener(
                Token.wrap(key, record), this);
        intentions
                .add(new KeyInRecordLockIntention(key, record, LockType.READ));
        return super.fetch(key, record, true);
    }

    @Override
    public Set<TObject> fetch(String key, long record, long timestamp)
            throws AtomicStateException {
        checkState();
        return super.fetch(key, record, timestamp);
    }

    @Override
    @Restricted
    public void onVersionChange(Token token) {
        if(finalizing.compareAndSet(false, true)) {
            abort();
        }
    }

    @Override
    public boolean remove(String key, TObject value, long record)
            throws AtomicStateException {
        checkState();
        ((Compoundable) destination).addVersionChangeListener(
                Token.wrap(key, record), this);
        intentions
                .add(new KeyInRecordLockIntention(key, record, LockType.WRITE));
        intentions.add(new RangeLockIntention(Text.wrapCached(key), Value
                .wrap(value)));
        return super.remove(key, value, record);
    }

    @Override
    public Set<Long> search(String key, String query)
            throws AtomicStateException {
        checkState();
        return super.search(key, query);
    }

    public final void start() {}

    @Override
    public final void stop() {}

    @Override
    public boolean verify(String key, TObject value, long record)
            throws AtomicStateException {
        checkState();
        ((Compoundable) destination).addVersionChangeListener(
                Token.wrap(key, record), this);
        intentions
                .add(new KeyInRecordLockIntention(key, record, LockType.READ));
        return super.verify(key, value, record, true);
    }

    @Override
    public boolean verify(String key, TObject value, long record, long timestamp)
            throws AtomicStateException {
        checkState();
        return super.verify(key, value, record, timestamp);
    }

    /**
     * Check that this AtomicOperation is open and throw an
     * AtomicStateException if it is not.
     * 
     * @throws AtomicStateException
     */
    protected void checkState() throws AtomicStateException {
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
        buffer.transport(destination, false);
        destination.sync();
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        checkState();
        return super.doExplore(timestamp, key, operator, values);
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        checkState();
        ((Compoundable) destination).addVersionChangeListener(RangeToken
                .forReading(Text.wrapCached(key), operator, Transformers
                        .transformArray(values, Functions.TOBJECT_TO_VALUE,
                                Value.class)), this);
        intentions.add(new RangeLockIntention(Text.wrapCached(key), operator,
                Transformers.transformArray(values, Functions.TOBJECT_TO_VALUE,
                        Value.class)));
        return super.doExplore(key, operator, values, true);
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
        locks = Maps.newHashMap();
        rangeLocks = RangeTokenMap.create();
        try {
            search: for (LockIntention intention : intentions) {
                prepareLockForPossibleUpgrade(intention, locks, rangeLocks);
                if(intention.getLockType() == LockType.RANGE_READ) {
                    // CON-72: Check to see if we already have a RANGE_WRITE
                    // that covers one of the values in this RANGE_READ. If so
                    // skip this lock since the write will adequately block any
                    // conflicting reads
                    for (Value value : ((RangeToken) intention.getToken())
                            .getValues()) {
                        RangeToken rt = RangeToken.forWriting(
                                ((RangeToken) intention.getToken()).getKey(),
                                value);
                        if(rangeLocks.containsKey(rt)) {
                            continue search;
                        }
                    }
                }
                if((intention.getLockType() == LockType.RANGE_READ || intention
                        .getLockType() == LockType.RANGE_WRITE)
                        && !rangeLocks.containsKey(intention.getToken())) {
                    LockDescription description = LockDescription
                            .forVersionExpectation(intention, lockService,
                                    rangeLockService);
                    if(description.getLock().tryLock()) {
                        rangeLocks.put((RangeToken) intention.getToken(),
                                description);
                    }
                    else {
                        // If we can't grab the lock immediately because it
                        // is held by someone else, then we must fail
                        // immediately because the AtomicOperation can't
                        // properly commit.
                        return false;
                    }

                }
                else if(!locks.containsKey(intention.getToken())) {
                    LockDescription description = LockDescription
                            .forVersionExpectation(intention, lockService,
                                    rangeLockService);
                    if(description.getLock().tryLock()) {
                        locks.put(intention.getToken(), description);
                    }
                    else {
                        // If we can't grab the lock immediately because it
                        // is held by someone else, then we must fail
                        // immediately because the AtomicOperation can't
                        // properly commit.
                        return false;
                    }
                }
            }
            return true;
        }
        catch (NullPointerException e) {
            // If we are notified a version change while grabbing locks, we
            // abort immediately which means that #locks will become null.
            return false;
        }

    }

    /**
     * Release all of the locks that are held by this operation.
     */
    private void releaseLocks() {
        if(locks != null) {
            Map<Token, LockDescription> _locks = locks;
            RangeTokenMap<LockDescription> _rangeLocks = rangeLocks;
            rangeLocks = null;
            locks = null; // CON-172: Set the reference of the locks to null
                          // immediately to prevent a race condition where the
                          // #grabLocks method isn't notified of version change
                          // failure in time
            for (LockDescription lock : _locks.values()) {
                ((Compoundable) destination).removeVersionChangeListener(
                        lock.getToken(), this);
                lock.getLock().unlock(); // We should never encounter an
                                         // IllegalMonitorStateException here
                                         // because a lock should only go in
                                         // #locks once it has been locked.
            }
            for (LockDescription lock : _rangeLocks.values()) {
                ((Compoundable) destination).removeVersionChangeListener(
                        lock.getToken(), this);
                lock.getLock().unlock(); // We should never encounter an
                                         // IllegalMonitorStateException here
                                         // because a lock should only go in
                                         // #locks once it has been locked.
            }
        }
    }

    /**
     * A LockDescription is a wrapper around a {@link Lock} that contains
     * metadata that can be serialized to disk. The AtomicOperation grabs a
     * collection of LockDescriptions when it goes to commit.
     * 
     * @author jnelson
     */
    protected static final class LockDescription implements Byteable {

        /**
         * Return the LockDescription that corresponds to {@code expectation}.
         * 
         * @param expectation
         * @param lockService
         * @param rangeLockService
         * @return the LockDescription
         */
        public static LockDescription forVersionExpectation(
                LockIntention expectation, LockService lockService,
                RangeLockService rangeLockService) {
            switch (expectation.getLockType()) {
            case RANGE_READ:
                return new LockDescription(expectation.getToken(),
                        rangeLockService.getReadLock((RangeToken) expectation
                                .getToken()), expectation.getLockType());
            case RANGE_WRITE:
                return new LockDescription(expectation.getToken(),
                        rangeLockService.getWriteLock((RangeToken) expectation
                                .getToken()), expectation.getLockType());

            case READ:
                return new LockDescription(expectation.getToken(),
                        lockService.getReadLock(expectation.getToken()),
                        expectation.getLockType());

            case WRITE:
                return new LockDescription(expectation.getToken(),
                        lockService.getWriteLock(expectation.getToken()),
                        expectation.getLockType());
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

        @Override
        public void copyTo(ByteBuffer buffer) {
            buffer.put((byte) type.ordinal());
            token.copyTo(buffer);
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

    /**
     * A LockIntention for a read or write that touches a key IN a record
     * (i.e. fetch, verify, etc).
     * 
     * @author jnelson
     */
    private final class KeyInRecordLockIntention extends LockIntention {

        private final String key;

        private final LockType lockType;
        private final long record;

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param record
         * @param lockType
         */
        protected KeyInRecordLockIntention(String key, long record,
                LockType lockType) {
            super(Token.wrap(key, record));
            this.key = key;
            this.record = record;
            this.lockType = lockType;
        }

        @Override
        public String getKey() throws UnsupportedOperationException {
            return key;
        }

        @Override
        public LockType getLockType() {
            return lockType;
        }

        @Override
        public long getRecord() throws UnsupportedOperationException {
            return record;
        }

    }

    /**
     * A LockIntention for a read that touches an entire key (i.e.
     * browse(key)).
     * 
     * @author jnelson
     */
    private final class KeyLockIntention extends LockIntention {

        private final String key;

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param expectedVersion
         */
        protected KeyLockIntention(String key) {
            super(Token.wrap(key));
            this.key = key;
        }

        @Override
        public String getKey() throws UnsupportedOperationException {
            return key;
        }

        @Override
        public LockType getLockType() {
            return LockType.READ;
        }

        @Override
        public long getRecord() throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * A LockIntention for a range read/write. No version is actually
     * expected, but this is a placeholder so that we know to grab to
     * appropriate range lock.
     * 
     * @author jnelson
     */
    private final class RangeLockIntention extends LockIntention {

        private final Text key;

        private final Operator operator;

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param operator
         * @param values
         */
        protected RangeLockIntention(Text key, Operator operator,
                Value... values) {
            super(operator == null ? RangeToken.forWriting(key, values[0])
                    : RangeToken.forReading(key, operator, values));
            this.key = key;
            this.operator = operator;
        }

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param value
         */
        protected RangeLockIntention(Text key, Value value) {
            this(key, null, value);
        }

        @Override
        public String getKey() throws UnsupportedOperationException {
            return key.toString();
        }

        @Override
        public LockType getLockType() {
            return operator == null ? LockType.RANGE_WRITE
                    : LockType.RANGE_READ;
        }

        @Override
        public long getRecord() throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A LockIntention for a read that touches an entire record (i.e.
     * browse(record), audit, etc).
     * 
     * @author jnelson
     */
    private final class RecordLockIntention extends LockIntention {

        private final long record;

        /**
         * Construct a new instance.
         * 
         * @param record
         */
        public RecordLockIntention(long record) {
            super(Token.wrap(record));
            this.record = record;
        }

        @Override
        public String getKey() throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }

        @Override
        public LockType getLockType() {
            return LockType.READ;
        }

        @Override
        public long getRecord() throws UnsupportedOperationException {
            return record;
        }

    }

    /**
     * The base class for those that record an atomic operation's intention to
     * grab a particular lock identified by a certain token at commit time.
     * 
     * @author jnelson
     */
    private abstract class LockIntention {
        // NOTE: This class does not define hashCode() or equals() because the
        // defaults are the desired behaviour.

        /**
         * The Token that corresponds to the data components that were used to
         * generate this VersionExpectation.
         */
        private final Token token;

        /**
         * Construct a new instance.
         * 
         * @param token
         */
        protected LockIntention(Token token) {
            this.token = token;
        }

        /**
         * Return the key, if it exists.
         * 
         * @return the key
         * @throws UnsupportedOperationException
         */
        public abstract String getKey() throws UnsupportedOperationException;

        /**
         * Return the LockType that should be used based on this
         * VersionExpectation.
         * 
         * @return the LockType
         */
        public abstract LockType getLockType();

        /**
         * Return the record, if it exists.
         * 
         * @return the record
         * @throws UnsupportedOperationException
         */
        public abstract long getRecord() throws UnsupportedOperationException;

        /**
         * Return the token that can be used to grab the appropriate lock over
         * the data components held within.
         * 
         * @return the Token
         */
        public Token getToken() {
            return token;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean replaceInClause = false;
            sb.append("Intention to lock ");
            try {
                sb.append(getKey() + " IN ");
            }
            catch (UnsupportedOperationException e) {/* ignore */}
            try {
                sb.append(getRecord());
            }
            catch (UnsupportedOperationException e) {
                /* ignore exception */
                replaceInClause = true;
            }
            sb.append("'");
            String string = sb.toString();
            if(replaceInClause) {
                string.replace(" IN ", "");
            }
            return string;
        }
    }

}
