/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;

import org.cinchapi.concourse.server.concurrent.LockService;
import org.cinchapi.concourse.server.concurrent.LockType;
import org.cinchapi.concourse.server.concurrent.RangeLockService;
import org.cinchapi.concourse.server.concurrent.RangeToken;
import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.temp.Queue;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Transformers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A sequence of reads and writes that all succeed or fail together. Each
 * operation is staged in an isolated buffer before being committed to a
 * destination store. For optimal concurrency, we use
 * <em>just in time locking</em> where destination resources are only locked
 * when its time to commit the
 * operation.
 * 
 * @author jnelson
 */
public class AtomicOperation extends BufferedStore {
    // NOTE: This class does not need to do any locking on operations (until
    // commit time) because it is assumed to be isolated to one thread and the
    // destination is assumed to have its own concurrency control scheme in
    // place.

    /**
     * Start a new AtomicOperation that will commit to {@code store}.
     * 
     * @param store
     * @return the AtomicOperation
     */
    public static AtomicOperation start(Compoundable store) {
        return new AtomicOperation(store);
    }

    /**
     * The initial capacity
     */
    private static final int INITIAL_CAPACITY = 10;

    /**
     * A flag to distinguish the case where we should ignore the version when
     * checking that expectations are met (i.e when performing a historical
     * read).
     * <p>
     * --- We must use a value other than {@link Versioned#NO_VERSION} so that
     * we can distinguish the cases where we legitimately need to check that
     * there is no version for atomic safety.
     * </p>
     */
    private static final long IGNORE_VERSION = Versioned.NO_VERSION - 1;

    /**
     * The sequence of VersionExpectations that were generated from the sequence
     * of operations.
     */
    private final List<VersionExpectation> expectations = Lists
            .newArrayListWithExpectedSize(INITIAL_CAPACITY);

    @Nullable
    private Map<Token, LockDescription> locks = null;

    /**
     * The AtomicOperation is open until it is committed or aborted.
     */
    private boolean open = true;

    /**
     * Construct a new instance.
     * 
     * @param destination - must be a {@link VersionGetter}
     */
    protected AtomicOperation(Compoundable destination) {
        super(new Queue(INITIAL_CAPACITY), destination);
    }

    /**
     * Close this operation and release all of the held locks without applying
     * any of the changes to the {@link #destination} store.
     */
    public void abort() {
        open = false;
        releaseLocks();
    }

    @Override
    public boolean add(String key, TObject value, long record) {
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                LockType.WRITE));
        expectations.add(new RangeVersionExpectation(Text.wrap(key), Value
                .wrap(value)));
        if(super.add(key, value, record)) {
            return true;
        }
        else {
            abort();
            return false;
        }
    }

    @Override
    public Map<Long, String> audit(long record) {
        checkState();
        expectations.add(new RecordVersionExpectation(record));
        return super.audit(record);
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        checkState();
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                LockType.READ));
        return super.audit(key, record);
    }

    /**
     * Commit the atomic operation to the destination store. The commit is only
     * successful if all the grouped operations can be successfully applied to
     * the destination. If the commit fails, the caller should retry the atomic
     * operation.
     * 
     * @return {@code true} if the atomic operation is completely applied
     */
    public final boolean commit() {
        checkState();
        open = false;
        if(checkExpectationsAndGrabLocks()) {
            doCommit();
            releaseLocks();
            return true;
        }
        else {
            abort();
            return false;
        }
    }

    @Override
    public Set<String> describe(long record) {
        checkState();
        expectations.add(new RecordVersionExpectation(record));
        return super.describe(record);
    }

    @Override
    public Set<String> describe(long record, long timestamp) {
        checkState();
        return super.describe(record, timestamp);
    }

    @Override
    public Set<TObject> fetch(String key, long record) {
        checkState();
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                LockType.READ));
        return super.fetch(key, record);
    }

    @Override
    public Set<TObject> fetch(String key, long record, long timestamp) {
        checkState();
        return super.fetch(key, record, timestamp);
    }

    @Override
    public Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values) {
        checkState();
        return super.find(timestamp, key, operator, values);
    }

    @Override
    public Set<Long> find(String key, Operator operator, TObject... values) {
        checkState();
        expectations.add(new RangeVersionExpectation(Text.wrap(key), operator,
                Transformers.transformArray(values, Functions.TOBJECT_TO_VALUE,
                        Value.class)));
        return super.find(key, operator, values);
    }

    @Override
    public boolean remove(String key, TObject value, long record) {
        checkState();
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                LockType.WRITE));
        expectations.add(new RangeVersionExpectation(Text.wrap(key), Value
                .wrap(value)));
        if(super.remove(key, value, record)) {
            return true;
        }
        else {
            abort();
            return false;
        }
    }

    @Override
    public Set<Long> search(String key, String query) {
        checkState();
        return super.search(key, query);
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public boolean verify(String key, TObject value, long record) {
        checkState();
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                LockType.READ));
        return super.verify(key, value, record);
    }

    @Override
    public boolean verify(String key, TObject value, long record, long timestamp) {
        checkState();
        return super.verify(key, value, record, timestamp);
    }

    /**
     * Transport the written data to the {@link #destination} store. The
     * subclass may override this method to do additional things (i.e. backup
     * the data, etc) if necessary.
     */
    protected void doCommit() {
        buffer.transport(destination);
    }

    /**
     * Check each one of the {@code expectations} against the
     * {@link #destination} and grab the appropriate locks along the way. This
     * method will return {@code true} if all expectations are met and all
     * necessary locks are grabbed. Otherwise it will return {@code false}, in
     * which case this operation should be aborted immediately.
     * 
     * @return {@code true} if all expectations are met and all necessary locks
     *         are grabbed.
     */
    private boolean checkExpectationsAndGrabLocks() {
        locks = Maps.newHashMap();
        for (VersionExpectation expectation : expectations) {
            if(expectation.getVersion() != IGNORE_VERSION) {
                String key = null;
                Long record = null;
                try {
                    key = expectation.getKey();
                }
                catch (UnsupportedOperationException e) {/* ignore */}
                try {
                    record = expectation.getRecord();
                }
                catch (UnsupportedOperationException e) {/* ignore */}
                long actualVersion;
                if(key != null && record != null) {
                    actualVersion = ((VersionGetter) destination).getVersion(
                            key, record);
                }
                else if(key != null) {
                    actualVersion = ((VersionGetter) destination)
                            .getVersion(key);
                }
                else if(record != null) {
                    actualVersion = ((VersionGetter) destination)
                            .getVersion(record);
                }
                else {
                    throw new IllegalStateException("this should never happen");
                }
                if(expectation.getVersion() != actualVersion) {
                    return false;
                }
            }
            if(locks.containsKey(expectation.getToken())
                    && (locks.get(expectation.getToken()).getType() == LockType.READ || locks
                            .get(expectation.getToken()).getType() == LockType.RANGE_READ)
                    && (expectation.getLockType() == LockType.WRITE || expectation
                            .getLockType() == LockType.RANGE_WRITE)) {
                // This means we need to "upgrade" from a READ lock to a WRITE
                // lock. It is technically not possible to do lock upgrades, so
                // we must remove and unlock the existing READ lock and later
                // grab a WRITE lock.
                locks.remove(expectation.getToken()).getLock().unlock();
            }
            if(!locks.containsKey(expectation.getToken())) {
                LockDescription description = LockDescription
                        .forVersionExpectation(expectation);
                locks.put(expectation.getToken(), description);
                description.getLock().lock();
            }
        }
        return true;
    }

    /**
     * Check that this AtomicOperation is open and throw an
     * AtomicStateException if it is not.
     * 
     * @throws AtomicStateException
     */
    private void checkState() throws AtomicStateException {
        if(!open) {
            throw new AtomicStateException();
        }
    }

    /**
     * Release all of the locks that are held by this operation.
     */
    private void releaseLocks() {
        if(locks != null) {
            for (LockDescription lock : locks.values()) {
                lock.getLock().unlock(); // We should never encounter an
                                         // IllegalMonitorStateException here
                                         // because a lock should only go in
                                         // #locks once it has been locked.
            }
        }
        locks = null;
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
         * @return the LockDescription
         */
        public static LockDescription forVersionExpectation(
                VersionExpectation expectation) {
            switch (expectation.getLockType()) {
            case RANGE_READ:
                return new LockDescription(expectation.getToken(),
                        RangeLockService.getReadLock((RangeToken) expectation
                                .getToken()), expectation.getLockType());
            case RANGE_WRITE:
                return new LockDescription(expectation.getToken(),
                        RangeLockService.getWriteLock((RangeToken) expectation
                                .getToken()), expectation.getLockType());

            case READ:
                return new LockDescription(expectation.getToken(),
                        LockService.getReadLock(expectation.getToken()),
                        expectation.getLockType());

            case WRITE:
                return new LockDescription(expectation.getToken(),
                        LockService.getWriteLock(expectation.getToken()),
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
         * @return the LockDescription
         */
        public static LockDescription fromByteBuffer(ByteBuffer bytes) {
            LockType type = LockType.values()[bytes.get()];
            Token token = null;
            Lock lock = null;
            switch (type) {
            case RANGE_READ:
                token = RangeToken.fromByteBuffer(bytes);
                lock = RangeLockService.getReadLock((RangeToken) token); // authorized
                break;
            case RANGE_WRITE:
                token = RangeToken.fromByteBuffer(bytes);
                lock = RangeLockService.getWriteLock((RangeToken) token); // authorized
                break;
            case READ:
                token = Token.fromByteBuffer(bytes);
                lock = LockService.getReadLock(token);
                break;
            case WRITE:
                token = Token.fromByteBuffer(bytes);
                lock = LockService.getWriteLock(token);
                break;

            }
            return new LockDescription(token, lock, type);
        }

        /**
         * The size of each LockDescription
         */
        private static final int SIZE = Token.SIZE + 1; // token, type (1)

        private final Token token;
        private final Lock lock;
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
            ByteBuffer bytes = ByteBuffer.allocate(SIZE);
            bytes.put((byte) type.ordinal());
            bytes.put(token.getBytes());
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
            return SIZE;
        }

    }

    /**
     * A VersionExpectation for a read or write that touches a key IN a record
     * (i.e. fetch, verify, etc).
     * 
     * @author jnelson
     */
    private final class KeyInRecordVersionExpectation extends
            VersionExpectation {

        private final long record;
        private final String key;
        private final LockType lockType;

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param record
         * @param lockType
         */
        protected KeyInRecordVersionExpectation(String key, long record,
                LockType lockType) {
            super(Token.wrap(key, record), ((Compoundable) destination)
                    .getVersion(key, record));
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
     * A VersionExpectation for a range read/write. No version is actually
     * expected, but this is a placeholder so that we know to grab to
     * appropriate range lock.
     * 
     * @author jnelson
     */
    private final class RangeVersionExpectation extends VersionExpectation {

        private final Text key;
        private final Operator operator;

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param operator
         * @param values
         */
        protected RangeVersionExpectation(Text key, Operator operator,
                Value... values) {
            super(operator == null ? RangeToken.forWriting(key, values[0])
                    : RangeToken.forReading(key, operator, values),
                    IGNORE_VERSION);
            this.key = key;
            this.operator = operator;
        }

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param value
         */
        protected RangeVersionExpectation(Text key, Value value) {
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
     * A VersionExpectation for a read that touches an entire record (i.e.
     * describe, audit, etc).
     * 
     * @author jnelson
     */
    private final class RecordVersionExpectation extends VersionExpectation {

        private final long record;

        /**
         * Construct a new instance.
         * 
         * @param record
         */
        public RecordVersionExpectation(long record) {
            super(Token.wrap(record), ((Compoundable) destination)
                    .getVersion(record));
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
     * The base class for those that determine and stores the expected version
     * of a record and and/or key and/or timestamp in {@link #destination}. A
     * VersionExpectation should be stored whenever a read/write occurs in the
     * AtomicOperation, so that we can check to see if any versions have changed
     * when we go to commit.
     * 
     * @author jnelson
     */
    private abstract class VersionExpectation {
        // NOTE: This class does not define hashCode() or equals() because the
        // defaults are the desired behaviour.

        /**
         * The Token that corresponds to the data components that were used to
         * generate this VersionExpectation.
         */
        private final Token token;

        /**
         * OPTINAL parameter that exists iff {@link #timestamp} ==
         * {@link Versioned#NO_VERSION} since since data returned from a
         * historical read won't change with additional writes.
         */
        private final long expectedVersion;

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param expectedVersion
         */
        protected VersionExpectation(Token token, long expectedVersion) {
            this.token = token;
            this.expectedVersion = expectedVersion;
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

        /**
         * Return the expected version.
         * 
         * @return the expected version
         */
        public long getVersion() {
            return expectedVersion;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean replaceInClause = false;
            sb.append("Expecting version " + expectedVersion + " for '");
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
