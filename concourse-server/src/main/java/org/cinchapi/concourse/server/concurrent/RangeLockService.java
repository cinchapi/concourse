/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.concourse.server.concurrent.RangeTokenMap.Condition;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Functions;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Transformers;

import com.google.common.base.Preconditions;

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
        return new RangeLockService(RangeTokenMap.<RangeReadWriteLock> create());
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

    private static Condition<RangeToken, RangeReadWriteLock> READ_BLOCKING_CONDITION = new Condition<RangeToken, RangeReadWriteLock>() {

        @Override
        public boolean satisfiedBy(RangeToken key, RangeReadWriteLock value) {
            return value.isWriteLocked()
                    && !value.isWriteLockedByCurrentThread();
        }

    };

    private static Condition<RangeToken, RangeReadWriteLock> WRITE_BLOCKING_CONDITION = new Condition<RangeToken, RangeReadWriteLock>() {

        @Override
        public boolean satisfiedBy(RangeToken key, RangeReadWriteLock value) {
            return value.getReadLockCount() > 0;
        }

    };

    private static Condition<RangeToken, RangeReadWriteLock> readBlockingCondition() {
        return READ_BLOCKING_CONDITION;
    }

    private static Condition<RangeToken, RangeReadWriteLock> writeBlockingCondition() {
        return WRITE_BLOCKING_CONDITION;
    }

    private static Condition<RangeToken, RangeReadWriteLock> notRegexReadBlockingCondition(
            final Value proposed) {
        return new Condition<RangeToken, RangeReadWriteLock>() {

            @Override
            public boolean satisfiedBy(RangeToken key, RangeReadWriteLock value) {
                if(value.isWriteLocked()
                        && !value.isWriteLockedByCurrentThread()) {
                    for (Value myValue : key.getValues()) {
                        if(!myValue.getTObject().toString()
                                .matches(proposed.getTObject().toString())) {
                            return true;
                        }
                    }
                }
                return false;
            }

        };
    }

    private static Condition<RangeToken, RangeReadWriteLock> regexReadBlockingCondition(
            final Value proposed) {
        return new Condition<RangeToken, RangeReadWriteLock>() {

            @Override
            public boolean satisfiedBy(RangeToken key, RangeReadWriteLock value) {
                if(value.isWriteLocked()
                        && !value.isWriteLockedByCurrentThread()) {
                    for (Value myValue : key.getValues()) {
                        if(myValue.getTObject().toString()
                                .matches(proposed.getTObject().toString())) {
                            return true;
                        }
                    }
                }
                return false;
            }

        };
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
        RangeTokenMap<RangeReadWriteLock> rtm = (RangeTokenMap<RangeReadWriteLock>) locks;
        if(type == LockType.READ) {
            Preconditions.checkArgument(token.getOperator() != null);
            switch (token.getOperator()) {
            case REGEX:
                return rtm.contains(token.getKey(),
                        regexReadBlockingCondition(value));
            case NOT_REGEX:
                return rtm.contains(token.getKey(),
                        notRegexReadBlockingCondition(value));
            default:
                return rtm.contains(token, readBlockingCondition());
            }
        }
        else {
            // If I want to WRITE X, I am blocked if there is a READ that
            // touches X (e.g. direct read for X or a range read that includes
            // X)
            return rtm.contains(token, writeBlockingCondition());
        }
    }

    @Override
    protected RangeReadWriteLock createLock(RangeToken token) {
        return new RangeReadWriteLock(this, token);
    }
}
