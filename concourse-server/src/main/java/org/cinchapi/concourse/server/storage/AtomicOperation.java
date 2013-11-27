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

import org.cinchapi.concourse.server.concurrent.LockType;
import org.cinchapi.concourse.server.concurrent.TLock;
import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A sequence of reads and writes that all succeed or fail together. Each
 * operation is staged in an isolated buffer before being committed to a
 * destination store. For optimal concurrency, we use just in time locking
 * where destination resources are only locked when its time to commit the
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
        return super.add(key, value, record);
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

    public final boolean commit() {
        checkState();
        open = false;
        if(checkExpectationsAndGrabLocks()) {
            doCommit(); // --- the magic happens here
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
        expectations.add(new RecordVersionExpectation(record, timestamp));
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
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                timestamp));
        return super.fetch(key, record, timestamp);
    }

    @Override
    public Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values) {
        checkState();
        expectations.add(new KeyVersionExpectation(key, timestamp));
        return super.find(timestamp, key, operator, values);
    }

    @Override
    public Set<Long> find(String key, Operator operator, TObject... values) {
        checkState();
        expectations.add(new KeyVersionExpectation(key));
        return super.find(key, operator, values);
    }

    @Override
    public boolean ping(long record) {
        checkState();
        expectations.add(new RecordVersionExpectation(record));
        return super.ping(record);
    }

    @Override
    public boolean remove(String key, TObject value, long record) {
        checkState();
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                LockType.WRITE));
        return super.remove(key, value, record);
    }

    @Override
    public void revert(String key, long record, long timestamp) {
        checkState();
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                LockType.WRITE));
        super.revert(key, record, timestamp);
    }

    @Override
    public Set<Long> search(String key, String query) {
        checkState();
        expectations.add(new KeyVersionExpectation(key));
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
        expectations.add(new KeyInRecordVersionExpectation(key, record,
                timestamp));
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
                    && locks.get(expectation.getToken()).getType() == LockType.READ
                    && expectation.getLockType() == LockType.WRITE) {
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
            if(expectation instanceof KeyVersionExpectation) {
                //TODO grab a range lock
            }
        }
        return true;
    }

    /**
     * Check that this AtomicOperation is open and throw an
     * IllegalStateException if it is not.
     */
    private void checkState() {
        Preconditions.checkState(open,
                "Cannot modify an AtomicOperation that is closed");
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
            super(Token.wrap(key, record), Versioned.NO_VERSION,
                    ((Compoundable) destination).getVersion(key, record));
            this.key = key;
            this.record = record;
            this.lockType = lockType;
        }

        /**
         * Construct a new instance. NEVER use this constructor for a write
         * operation.
         * 
         * @param key
         * @param record
         * @param timestamp
         */
        protected KeyInRecordVersionExpectation(String key, long record,
                long timestamp) {
            super(Token.wrap(key, record), timestamp, IGNORE_VERSION);
            this.key = key;
            this.record = record;
            this.lockType = LockType.READ;
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
     * A VersionExpectation for a read that touches an entire key (i.e.
     * find, search, etc).
     * 
     * @author jnelson
     */
    private final class KeyVersionExpectation extends VersionExpectation {

        private final String key;

        /**
         * Construct a new instance.
         * 
         * @param key
         */
        public KeyVersionExpectation(String key) {
            super(Token.wrap(key), Versioned.NO_VERSION,
                    ((Compoundable) destination).getVersion(key));
            this.key = key;
        }

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param timestamp
         */
        public KeyVersionExpectation(String key, long timestamp) {
            super(Token.wrap(key), timestamp, IGNORE_VERSION);
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
     * A LockDescription is a wrapper around a {@link TLock} that contains
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
            return new LockDescription(TLock.grabWithToken(expectation
                    .getToken()), expectation.getLockType());
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
            TLock lock = TLock.grabWithToken(Token.fromByteBuffer(bytes));
            return new LockDescription(lock, type);
        }

        /**
         * The size of each LockDescription
         */
        private static final int SIZE = Token.SIZE + 1; // token, type (1)

        private final TLock lock;
        private final LockType type;

        /**
         * Construct a new instance.
         * 
         * @param lock
         * @param type
         */
        private LockDescription(TLock lock, LockType type) {
            this.lock = lock;
            this.type = type;
        }

        @Override
        public ByteBuffer getBytes() {
            // We do not create a cached copy for the entire class because we'll
            // only ever getBytes() for a lock description once and that only
            // happens if the AtomicOperation is not aborted before an attempt
            // to commit, so its best to not create a copy if we don't have to
            ByteBuffer bytes = ByteBuffer.allocate(SIZE);
            bytes.put((byte) type.ordinal());
            bytes.put(lock.getToken().getBytes());
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
            return type == LockType.WRITE ? lock.writeLock() : lock.readLock();
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
        public int size() {
            return SIZE;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lock, type);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof LockDescription) {
                return lock.equals(((LockDescription) obj).getLock())
                        && type == ((LockDescription) obj).type;
            }
            return false;
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
            super(Token.wrap(record), Versioned.NO_VERSION,
                    ((Compoundable) destination).getVersion(record));
            this.record = record;
        }

        /**
         * Construct a new instance.
         * 
         * @param record
         * @param timestamp
         */
        public RecordVersionExpectation(long record, long timestamp) {
            super(Token.wrap(record), timestamp, IGNORE_VERSION);
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
         * OPTIONAL parameter that exists if the VersionExpectation
         * was generated from a historical read.
         */
        private final long timestamp;

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
         * @param timestamp
         * @param expectedVersion
         */
        protected VersionExpectation(Token token, long timestamp,
                long expectedVersion) {
            Preconditions
                    .checkState((timestamp != Versioned.NO_VERSION && expectedVersion == IGNORE_VERSION) || true);
            this.token = token;
            this.timestamp = timestamp;
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
            if(timestamp != Versioned.NO_VERSION) {
                sb.append(" AT " + timestamp);
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
