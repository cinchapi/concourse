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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

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
        Iterator<Entry<RangeToken, RangeReadWriteLock>> it = locks.entrySet()
                .iterator();
        if(type == LockType.READ) {
            Preconditions.checkArgument(token.getOperator() != null);
            while (it.hasNext()) {
                Entry<RangeToken, RangeReadWriteLock> entry = it.next();
                RangeToken myToken = entry.getKey();
                RangeReadWriteLock myLock = entry.getValue();
                if(myToken.getKey().equals(token.getKey())
                        && myLock.isWriteLocked()
                        && !myLock.isWriteLockedByCurrentThread()) {
                    for (Value myValue : myToken.getValues()) {
                        switch (token.getOperator()) {
                        // If I want to READ X, I am blocked if:
                        case BETWEEN:
                            if((myValue.compareTo(value) >= 0 && myValue
                                    .compareTo(token.getValues()[1]) < 0)
                                    || (myValue.compareTo(token.getValues()[1])) >= 0
                                    && myValue.compareTo(value) < 0) {
                                return true;
                            }
                            break;
                        case EQUALS:
                            if(myValue.equals(value)) {
                                return true;
                            }
                            break;
                        case GREATER_THAN:
                            if(myValue.compareTo(value) > 0) {
                                return true;
                            }
                            break;
                        case GREATER_THAN_OR_EQUALS:
                            if(myValue.compareTo(value) >= 0) {
                                return true;
                            }
                            break;
                        case LESS_THAN:
                            if(myValue.compareTo(value) < 0) {
                                return true;
                            }
                            break;
                        case LESS_THAN_OR_EQUALS:
                            if(myValue.compareTo(value) <= 0) {
                                return true;
                            }
                            break;
                        case NOT_EQUALS:
                            if(!myValue.equals(value)) {
                                return true;
                            }
                            break;
                        case NOT_REGEX:
                            if(!myValue.getTObject().toString()
                                    .matches(value.getTObject().toString())) {
                                return true;
                            }
                            break;
                        case REGEX:
                            if(myValue.getTObject().toString()
                                    .matches(value.getTObject().toString())) {
                                return true;
                            }
                            break;
                        default:
                            break;
                        }
                    }
                }

            }
            return false;
        }
        else {
            // If I want to WRITE X, I am blocked if there is a READ smaller
            // than X and another READ larger than X OR there is a READ for X
            boolean foundSmaller = false;
            boolean foundLarger = false;
            while (it.hasNext() && (!foundSmaller || !foundLarger)) {
                Entry<RangeToken, RangeReadWriteLock> entry = it.next();
                RangeToken myToken = entry.getKey();
                RangeReadWriteLock myLock = entry.getValue();
                if(myToken.getKey().equals(token.getKey())
                        && myLock.getReadLockCount() > 0) {
                    for (Value myValue : myToken.getValues()) {
                        if(value.equals(myValue)) {
                            return true;
                        }
                        else {
                            foundSmaller = value.compareTo(myValue) >= 0 ? true
                                    : foundSmaller;
                            foundLarger = value.compareTo(myValue) < 0 ? true
                                    : foundLarger;
                        }
                    }
                }
            }
            return foundSmaller && foundLarger;
        }
    }

    @Override
    protected RangeReadWriteLock createLock(RangeToken token) {
        return new RangeReadWriteLock(this, token);
    }
}
