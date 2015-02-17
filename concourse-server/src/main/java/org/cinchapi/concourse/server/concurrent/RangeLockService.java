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
package org.cinchapi.concourse.server.concurrent;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Functions;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Transformers;
import org.cinchapi.vendor.jsr166e.ConcurrentHashMapV8;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

/**
 * A global service that provides ReadLock and WriteLock instances for a given
 * {@link RangeToken}. The locks that are returned from this service can be used
 * to lock <em>notions of things</em> that aren't strictly defined in their own
 * right (i.e. a {@code key} in a {@code record})
 * <p>
 * </p>
 * RangeLocks are designed to protect concurrent access to secondary indices
 * (i.e. Reader A wants to find key between X and Z and Writer B wants to write
 * Z), so they are governed by static logic that determine if, given an action,
 * key, operator (optional) and value(s),it is acceptable to grab a lock
 * representing the range.
 * <p>
 * <strong>WARNING</strong>: If the caller requests a lock for a given token,
 * but does not attempt to grab it immediately, then it is possible that
 * subsequent requests for locks identified by the same token will return
 * different instances. This is unlikely to happen in practice, but it is
 * recommended that lock grabs happen immediately after lock requests just to be
 * safe (e.g
 * <code>RangeLockService.getReadLock(key, operator, value).lock()</code>).
 * </p>
 * 
 * @author jnelson
 */
public class RangeLockService extends
        AbstractLockService<RangeToken, RangeReadWriteLock> {

    /**
     * Create a new {@link RangeLockService}.
     * 
     * @return the RangeLockService
     */
    public static RangeLockService create() {
        return new RangeLockService(
                new ConcurrentHashMapV8<RangeToken, RangeReadWriteLock>());
    }

    /**
     * Return a {@link RangeLockService} that does not actually provide any
     * locks. This is used in situations where access is guaranteed (or at least
     * assumed) to be isolated (e.g. a Transaction) and we need to simulate
     * locking for polymorphic consistency.
     * 
     * @return the LockService
     */
    public static RangeLockService noOp() {
        return NOOP_INSTANCE;
    }

    /**
     * A {@link RangeLockService} that does not actually provide any locks. This
     * is used in situations where access is guaranteed (or at least assumed) to
     * be isolated (e.g. a Transaction) and we need to simulate locking for
     * polymorphic consistency.
     */
    private static final RangeLockService NOOP_INSTANCE = new RangeLockService() {

        @Override
        public ReadLock getReadLock(RangeToken token) {
            return Locks.noOpReadLock();
        }

        @Override
        public WriteLock getWriteLock(RangeToken token) {
            return Locks.noOpWriteLock();
        }
    };

    /**
     * The information used in the {@link #isRangeBlocked(LockType, RangeToken)}
     * method.
     */
    protected final RangeBlockingInfo info = new RangeBlockingInfo();

    private RangeLockService() {/* noop */}

    /**
     * Construct a new instance.
     * 
     * @param locks
     */
    private RangeLockService(ConcurrentMap<RangeToken, RangeReadWriteLock> locks) {
        super(locks);
    }

    /**
     * Return the ReadLock that is identified by {@code objects}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param objects
     * @return the ReadLock
     */
    public ReadLock getReadLock(String key, Operator operator,
            TObject... values) {
        return getReadLock(Text.wrapCached(key), operator,
                Transformers.transformArray(values, Functions.TOBJECT_TO_VALUE,
                        Value.class));
    }

    /**
     * Return the ReadLock that is identified by {@code objects}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param objects
     * @return the ReadLock
     */
    public ReadLock getReadLock(Text key, Operator operator, Value... values) {
        return getReadLock(RangeToken.forReading(key, operator, values));
    }

    /**
     * Return the WriteLock that is identified by {@code objects}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param objects
     * @return the WriteLock
     */
    public WriteLock getWriteLock(String key, TObject value) {
        return getWriteLock(Text.wrapCached(key), Value.wrap(value));
    }

    /**
     * Return the WriteLock that is identified by {@code objects}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param objects
     * @return the WriteLock
     */
    public WriteLock getWriteLock(Text key, Value value) {
        return getWriteLock(RangeToken.forWriting(key, value));
    }

    @Override
    protected RangeReadWriteLock createLock(RangeToken token) {
        return new RangeReadWriteLock(this, token);
    }

    /**
     * Return {@code true} if an attempt to used {@code token} for a
     * {@code type} lock is range blocked. Range blocking occurs when there is
     * another READ or WRITE happening such that allowing the proposed operation
     * to proceed could lead to inconsistent results (i.e. I want to write X but
     * there is a READ trying to find all values less than Y).
     * 
     * @param type
     * @param token
     * @return {@code true} if range blocked
     */
    protected final boolean isRangeBlocked(LockType type, RangeToken token) {
        Value value = token.getValues()[0];
        if(type == LockType.READ) {
            Preconditions.checkArgument(token.getOperator() != null);
            switch (token.getOperator()) {
            case EQUALS:
                return info.writes(token.getKey()).contains(value);
            case NOT_EQUALS:
                return info.writes(token.getKey()).size() > 1
                        || (info.writes(token.getKey()).size() == 1 && !info
                                .writes(token.getKey()).contains(value));
            default:
                Iterator<Value> it = info.writes(token.getKey()).iterator();
                while (it.hasNext()) {
                    Iterable<Range<Value>> ranges = RangeTokens
                            .convertToGuavaRange(token);
                    Value current = it.next();
                    Range<Value> point = Range.singleton(current);
                    for (Range<Value> range : ranges) {
                        RangeReadWriteLock lock = null;
                        if(range.isConnected(point)
                                && !range.intersection(point).isEmpty()
                                && (lock = locks.get(RangeToken.forWriting(
                                        token.getKey(), current))) != null
                                && !lock.isWriteLockedByCurrentThread()) {
                            return true;
                        }
                    }
                }
                return false;
            }
        }
        else {
            // If I want to WRITE X, I am blocked if there is a READ that
            // touches X (e.g. direct read for X or a range read that includes
            // X)
            return info.reads(token.getKey()).contains(value);

        }
    }

    /**
     * A class that holds information that is used to determine if a thread is
     * {@link RangeLockService#isRangeBlocked(LockType, RangeToken) range
     * blocked} for a given lock acquisition attempt. This state of this
     * information is updated externally in {@link RangeReadWriteLock} whenever
     * locks are acquired and released.
     * 
     * @author jnelson
     */
    protected class RangeBlockingInfo {

        /**
         * Info about range read locks.
         */
        private final ConcurrentMap<Text, RangeSet<Value>> reads = new ConcurrentHashMapV8<Text, RangeSet<Value>>();

        /**
         * Info about range write locks.
         */
        private final ConcurrentMap<Text, Set<Value>> writes = new ConcurrentHashMapV8<Text, Set<Value>>();

        /**
         * Add a RANGE_READ for {@code key} that covers all of the
         * {@code ranges}.
         * 
         * @param key
         * @param ranges
         */
        public void add(Text key, Iterable<Range<Value>> ranges) {
            RangeSet<Value> existing = reads.get(key);
            if(existing == null) {
                RangeSet<Value> created = TreeRangeSet.create();
                existing = reads.putIfAbsent(key, created);
                existing = Objects.firstNonNull(existing, created);
            }
            synchronized (existing) {
                for (Range<Value> range : ranges) {
                    existing.add(range);
                }
            }
        }

        /**
         * Add a RANGE_WRITE for {@code key} that covers the {@code value}.
         * 
         * @param key
         * @param value
         */
        public void add(Text key, Value value) {
            Set<Value> existing = writes.get(key);
            if(existing == null) {
                Set<Value> created = Sets.newConcurrentHashSet();
                existing = writes.putIfAbsent(key, created);
                existing = Objects.firstNonNull(existing, created);
            }
            existing.add(value);
        }

        /**
         * Return all the ranges that are RANGE_READ locked for {@code key}.
         * 
         * @param key
         * @return the locked reads
         */
        public RangeSet<Value> reads(Text key) {
            RangeSet<Value> existing = reads.get(key);
            if(existing == null) {
                RangeSet<Value> created = TreeRangeSet.create();
                existing = reads.putIfAbsent(key, created);
                existing = Objects.firstNonNull(existing, created);
            }
            synchronized (existing) {
                return existing;
            }
        }

        /**
         * Remove the RANGE_READ for {@code key} that covers the {@code ranges}.
         * 
         * @param key
         * @param ranges
         */
        public void remove(Text key, Iterable<Range<Value>> ranges) {
            RangeSet<Value> existing = reads.get(key);
            synchronized (existing) {
                for (Range<Value> range : ranges) {
                    existing.remove(range);
                }
            }
        }

        /**
         * Remove the RANGE_WRITE for {@code key} that covers the {@code value}.
         * 
         * @param key
         * @param value
         */
        public void remove(Text key, Value value) {
            Set<Value> existing = writes.get(key);
            existing.remove(value);
        }

        /**
         * Return all the values that are RANGE_WRITE locked for {@code key}.
         * 
         * @param key
         * @return the locked writes
         */
        public Set<Value> writes(Text key) {
            Set<Value> existing = writes.get(key);
            if(existing == null) {
                Set<Value> created = Sets.newConcurrentHashSet();
                existing = writes.putIfAbsent(key, created);
                existing = Objects.firstNonNull(existing, created);
            }
            return existing;
        }
    }
}
