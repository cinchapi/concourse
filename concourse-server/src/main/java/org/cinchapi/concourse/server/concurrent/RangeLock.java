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
package org.cinchapi.concourse.server.concurrent;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

import org.cinchapi.concourse.annotate.Experimental;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.AutoMap;
import org.cinchapi.concourse.util.AutoSkipListMap;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * A RangeLock is a special {@link TLock} that is designed to protect concurrent
 * access to secondary indices (i.e. Reader A wants to find key between X and Z
 * and Writer B wants to write Z).
 * <p>
 * RangeLocks are governed by static logic that determine if, given an action,
 * key, operator (optional) and value(s),it is acceptable to grab a lock
 * representing the range.
 * </p>
 * 
 * @author jnelson
 */
@SuppressWarnings("serial")
@Experimental
public final class RangeLock extends TLock {
    // NOTE: This class does not define hashCode() or equals() because the
    // protocol defined in the parent class is the desired behaviour.

    /**
     * Return a {@link RangeLock} that can be used for reading {@code key}
     * {@code operator} {@code values}.
     * 
     * @param key
     * @param operator
     * @param values
     * @return the RangeLock
     */
    public static RangeLock grabForReading(Text key, Operator operator,
            Value... values) {
        return new RangeLock(key, operator, values);
    }

    /**
     * Return a {@link RangeLock} that can be used for writing {@code key} as
     * {@code value}.
     * 
     * @param key
     * @param value
     * @return the RangeLock
     */
    public static RangeLock grabForWriting(Text key, Value value) {
        return new RangeLock(key, null, value);
    }

    /**
     * Return {@code true} if an attempt to grab a {@code type} lock (to perform
     * {@code operator} if applicable) on {@code key} as {@code values} is range
     * blocked. Range blocking occurs when there is another READ or WRITE
     * happening such that allowing the proposed operation to proceed could lead
     * to inconsistent results (i.e. I want to write X but there is a READ
     * trying to find all values less than Y).
     * 
     * @param type
     * @param operator
     * @param key
     * @param values
     * @return {@code true} if range blocked
     */
    protected static boolean isRangeBlocked(LockType type,
            @Nullable Operator operator, Text key, Value... values) {
        Value value = values[0];
        if(type == LockType.READ) {
            Preconditions.checkArgument(operator != null);
            // If I want to read X, I am blocked if:
            switch (operator) {
            case EQUALS:
                // There is a EQ WRITE lock for anything equal to X
                return existsEqWriteLock(key, value);
            case NOT_EQUALS:
                // There is an EQ WRITE lock for anything not equal to X
                for (Entry<Value, ReentrantReadWriteLock> entry : EQ.get(key)
                        .entrySet()) {
                    if(!entry.getKey().equals(value)
                            && entry.getValue().isWriteLocked()
                            && !entry.getValue().isWriteLockedByCurrentThread()) {
                        return true;
                    }
                }
                return false;
            case GREATER_THAN:
                // There is GT WRITE lock for anything greater than X
                return existsGtWriteLock(key, value);
            case GREATER_THAN_OR_EQUALS:
                // A. There is a GT WRITE lock for anything greater than X
                // B. There is an EQ WRITE lock for anything equal to X
                return existsGtWriteLock(key, value)
                        || existsEqWriteLock(key, value);
            case LESS_THAN:
                // There is a LT WRITE lock for anything less than X
                return existsLtWriteLock(key, value);
            case LESS_THAN_OR_EQUALS:
                // A. There is a LT WRITE lock for anything greater than X
                // B. There an EQ WRITE lock for anything equal to X
                return existsLtWriteLock(key, value)
                        || existsEqWriteLock(key, value);
            case BETWEEN:
                // A. There is a GT WRITE lock for anything greater than X1
                // but less than X2
                // B. There is a EQ WRITE lock for any equal to X1
                // C there is a LT WRITE lock for anything greater than X2
                return existsGtWriteLock(key, value, values[0])
                        || existsEqWriteLock(key, value)
                        || existsLtWriteLock(key, value, values[1]);
            case REGEX:
                // There a REGEX WRITE lock for anything matching X
                for (Entry<Value, ReentrantReadWriteLock> entry : REGEX
                        .get(key).entrySet()) {
                    if(entry.getKey().getTObject().toString()
                            .matches(value.getTObject().toString())) {
                        if(entry.getValue().isWriteLocked()
                                && !entry.getValue()
                                        .isWriteLockedByCurrentThread()) {
                            return true;
                        }
                    }
                }
                return false;
            case NOT_REGEX:
                // There a REGEX WRITE lock for anything not matching X
                for (Entry<Value, ReentrantReadWriteLock> entry : REGEX
                        .get(key).entrySet()) {
                    if(!entry.getKey().getTObject().toString()
                            .matches(value.getTObject().toString())) {
                        if(entry.getValue().isWriteLocked()
                                && !entry.getValue()
                                        .isWriteLockedByCurrentThread()) {
                            return true;
                        }
                    }
                }
                return false;
            default:
                return false;
            }
        }
        else {
            // IF I WANT TO WRITE [X] I AM BLOCKED IF:
            // A: There is a GT READ lock for anything less than me
            Map<Value, ReentrantReadWriteLock> less = GT.get(key)
                    .headMap(value);
            for (ReentrantReadWriteLock lock : less.values()) {
                if(lock.getReadLockCount() > 0) {
                    return true;
                }
            }
            // B: There is a LT READ lock for anything greater than me
            Map<Value, ReentrantReadWriteLock> greater = LT.get(key).tailMap(
                    value);
            for (ReentrantReadWriteLock lock : greater.values()) {
                if(lock.getReadLockCount() > 0) {
                    return true;
                }
            }
            // C: There is an EQ READ/WRITE lock for anything equal to me
            if(EQ.get(key).get(value).isWriteLocked()
                    || EQ.get(key).get(value).getReadLockCount() > 0) {
                return true;
            }
            // D: There is a REGEX READ lock for anything that matches me
            for (Entry<Value, ReentrantReadWriteLock> entry : REGEX.get(key)
                    .entrySet()) {
                if(entry.getKey().getTObject().toString()
                        .matches(value.getTObject().toString())) {
                    if(entry.getValue().getReadLockCount() > 0) {
                        return true;
                    }
                }
            }
            // E: There is a NREGEX READ lock for anything that does not
            // match me
            for (Entry<Value, ReentrantReadWriteLock> entry : NREGEX.get(key)
                    .entrySet()) {
                if(!entry.getKey().getTObject().toString()
                        .matches(value.getTObject().toString())) {
                    if(entry.getValue().getReadLockCount() > 0) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * Return {@code true} if there is a EQ WRITE lock on any key as value equal
     * to {@code value}.
     * 
     * @param key
     * @param value
     * @return {@code true} if applicable lock is held
     */
    private static boolean existsEqWriteLock(Text key, Value value) {
        if(EQ.get(key).get(value).isWriteLocked()
                && !EQ.get(key).get(value).isWriteLockedByCurrentThread()) {
            return true;
        }
        return false;
    }

    /**
     * Return {@code true} if there is a GT WRITE lock on a key as a value
     * greater
     * than {@code value}.
     * 
     * @param key
     * @param value
     * @return {@code true} if applicable lock is held
     */
    private static boolean existsGtWriteLock(Text key, Value value) {
        for (ReentrantReadWriteLock lock : GT.get(key).tailMap(value, false)
                .values()) {
            if(lock.isWriteLocked() && !lock.isWriteLockedByCurrentThread()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return {@code true} if there is a GT WRITE lock for {@code key} as any
     * value greater than {@code value} but less than {@code value1}.
     * 
     * @param key
     * @param value
     * @param value1
     * @return {@code true} if applicable lock is held
     */
    private static boolean existsGtWriteLock(Text key, Value value, Value value1) {
        for (ReentrantReadWriteLock lock : GT.get(key)
                .subMap(value, false, value1, false).values()) {
            if(lock.isWriteLocked() && !lock.isWriteLockedByCurrentThread()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return {@code true} if there is a LT WRITE lock for {@code key} as any
     * value
     * less than {@code value}.
     * 
     * @param key
     * @param value
     * @return {@code true} if applicable lock is held
     */
    private static boolean existsLtWriteLock(Text key, Value value) {
        for (ReentrantReadWriteLock lock : LT.get(key).headMap(value, false)
                .values()) {
            if(lock.isWriteLocked() && !lock.isWriteLockedByCurrentThread()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return {@code true} if there is a LT WRITE lock for {@code key} as any
     * value greater than {@code value} but less than {@code value1}.
     * 
     * @param key
     * @param value
     * @param value1
     * @return {@code true} if applicable lock is held
     */
    private static boolean existsLtWriteLock(Text key, Value value, Value value1) {
        for (ReentrantReadWriteLock lock : LT.get(key)
                .subMap(value, false, value1, false).values()) {
            if(lock.isWriteLocked() && !lock.isWriteLockedByCurrentThread()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get all the ReadLocks for {@code key} {@code operator} {@code values}.
     * 
     * @param key
     * @param operator
     * @param values
     * @return an array of ReadLocks
     */
    private static ReadLock[] getReadLocks(Text key, Operator operator,
            Value... values) {
        Value value = values[0];
        switch (operator) {
        case EQUALS:
            return new ReadLock[] { EQ.get(key).get(value).readLock() };
        case NOT_EQUALS:
            return new ReadLock[] { GT.get(key).get(value).readLock(),
                    LT.get(key).get(value).readLock() };
        case GREATER_THAN:
            return new ReadLock[] { GT.get(key).get(value).readLock() };
        case GREATER_THAN_OR_EQUALS:
            return new ReadLock[] { GT.get(key).get(value).readLock(),
                    EQ.get(key).get(value).readLock() };
        case LESS_THAN:
            return new ReadLock[] { LT.get(key).get(value).readLock() };
        case LESS_THAN_OR_EQUALS:
            return new ReadLock[] { LT.get(key).get(value).readLock(),
                    EQ.get(key).get(value).readLock() };
        case BETWEEN:
            return new ReadLock[] { GT.get(key).get(value).readLock(),
                    EQ.get(key).get(value).readLock(),
                    LT.get(key).get(values[1]).readLock() };
        case REGEX:
            return new ReadLock[] { REGEX.get(key).get(value).readLock() };
        case NOT_REGEX:
            return new ReadLock[] { NREGEX.get(key).get(value).readLock() };
        default:
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Get all the WriteLocks for {@code key} as {@code value}.
     * 
     * @param key
     * @param value
     * @return an array of WriteLocks
     */
    private static WriteLock[] getWriteLocks(Text key, Value value) {
        return new WriteLock[] { EQ.get(key).get(value).writeLock(),
                GT.get(key).get(value).writeLock(),
                LT.get(key).get(value).writeLock(),
                REGEX.get(key).get(value).writeLock(),
                NREGEX.get(key).get(value).writeLock() };
    }

    /**
     * Perform {@link #isRangeBlocked(LockType, Operator, Text, Value...)} and
     * quit after the specified {@code timeout}.
     * 
     * @param timeout
     * @param unit
     * @param type
     * @param operator
     * @param key
     * @param values
     * @return {@code true} if ranged blocked after the specified timeout
     */
    private static boolean isRangedBlockedWithTimeout(long timeout,
            TimeUnit unit, LockType type, Operator operator, Text key,
            Value... values) {
        long nanosTimeout = TimeUnit.NANOSECONDS.convert(timeout, unit);
        long lastTime = System.nanoTime();
        for (;;) {
            if(!isRangeBlocked(type, operator, key, values)) {
                return false;
            }
            if(nanosTimeout <= 0) {
                return true;
            }
            long now = System.nanoTime();
            nanosTimeout -= now - lastTime;
            lastTime = now;
        }
    }

    // --- A dummy lock used to created a delegating ReadLock or WriteLock ---
    private static final ReentrantReadWriteLock MASTERLOCK = new ReentrantReadWriteLock();

    /**
     * The function that creates a new lock for a value.
     */
    private static final Function<Value, ReentrantReadWriteLock> VALUE_LOADER;

    /**
     * The function that removes a value lock if it is not held.
     */
    private static final Function<ReentrantReadWriteLock, Boolean> VALUE_CLEANER;
    /**
     * The function that creates a new value to lock mapping for a key.
     */
    private static final Function<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> KEY_LOADER;

    /**
     * The function that removes a key if it does not contain any values.
     */
    private static final Function<AutoSkipListMap<Value, ReentrantReadWriteLock>, Boolean> KEY_CLEANER;
    /*
     * RANGE GUARDERS:
     * The maps below contain various locks that are used to guard ranges. Check
     * the individual locking methods for details on the logic for using these
     * collections in concert.
     * 
     * NOTE: Each collection: map: key -> map: value -> lock
     * 
     * NOTE: These collections are secure for multi threaded access because of
     * concurrency controls implemented in AutoHashMap and AutoTreeMap.
     */
    private static final AutoMap<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> EQ;
    private static final AutoMap<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> GT;
    private static final AutoMap<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> LT;

    private static final AutoMap<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> REGEX;

    private static final Map<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> NREGEX;

    static {

        // Create a new ReentrantReadWriteLock whenever a new Value is locked
        VALUE_LOADER = new Function<Value, ReentrantReadWriteLock>() {

            @Override
            public ReentrantReadWriteLock apply(Value input) {
                return new ReentrantReadWriteLock();
            }
        };

        // Delete a Value when it is not locked by any readers or writers and
        // does not have any threads waiting to lock it
        VALUE_CLEANER = new Function<ReentrantReadWriteLock, Boolean>() {

            @Override
            public Boolean apply(ReentrantReadWriteLock input) {
                synchronized (input) {
                    return !input.isWriteLocked() && !input.hasQueuedThreads()
                            && input.getReadLockCount() == 0;
                }
            }

        };

        // Create a new map whenever a new Key is locked
        KEY_LOADER = new Function<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>>() {

            @Override
            public AutoSkipListMap<Value, ReentrantReadWriteLock> apply(
                    Text input) {
                return AutoMap.newAutoSkipListMap(VALUE_LOADER, VALUE_CLEANER);
            }

        };

        // Delete a Key when it has no locked values.
        KEY_CLEANER = new Function<AutoSkipListMap<Value, ReentrantReadWriteLock>, Boolean>() {

            @Override
            public Boolean apply(
                    AutoSkipListMap<Value, ReentrantReadWriteLock> input) {
                synchronized (input) {
                    return input.isEmpty();
                }
            }

        };
        EQ = AutoMap.newAutoHashMap(KEY_LOADER, KEY_CLEANER);
        GT = AutoMap.newAutoHashMap(KEY_LOADER, KEY_CLEANER);
        LT = AutoMap.newAutoHashMap(KEY_LOADER, KEY_CLEANER);
        REGEX = AutoMap.newAutoHashMap(KEY_LOADER, KEY_CLEANER);
        NREGEX = AutoMap.newAutoHashMap(KEY_LOADER, KEY_CLEANER);
    }
    // --- These define the scope of the RangeLock.
    private final Text key;

    private final Value[] values;

    @Nullable
    private final Operator operator;

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param operator
     * @param values
     */
    private RangeLock(Text key, @Nullable Operator operator, Value... values) {
        super(RangeToken.wrap(key, operator, values));
        this.key = key;
        this.values = values;
        this.operator = operator;
    }

    @Override
    public RangeToken getToken() {
        return (RangeToken) super.getToken();
    }

    @Override
    public boolean isStaleInstance() {
        return false;
    }

    @Override
    public ReadLock readLock() {
        final AtomicLock delegate = AtomicLock.newInstance(getReadLocks(key,
                operator, values));
        return new ReadLock(MASTERLOCK) {
            @Override
            public void lock() {
                while (isRangeBlocked(LockType.READ, operator, key, values)) {
                    continue;
                }
                delegate.lock();
            }

            @Override
            public void lockInterruptibly() throws InterruptedException {
                while (isRangeBlocked(LockType.READ, operator, key, values)) {
                    continue;
                }
                delegate.lockInterruptibly();
            }

            @Override
            public Condition newCondition() {
                return delegate.newCondition();
            }

            @Override
            public String toString() {
                return delegate.toString();
            }

            @Override
            public boolean tryLock() {
                while (isRangeBlocked(LockType.READ, operator, key, values)) {
                    continue;
                }
                return delegate.tryLock();
            }

            @Override
            public boolean tryLock(long timeout, TimeUnit unit)
                    throws InterruptedException {
                if(!isRangedBlockedWithTimeout(timeout, unit, LockType.READ,
                        operator, key, values)) {
                    return delegate.tryLock(timeout, unit);
                }
                return false;
            }

            @Override
            public void unlock() {
                delegate.unlock();
            }

        };
    }

    @Override
    public WriteLock writeLock() {
        final AtomicLock delegate = AtomicLock.newInstance(getWriteLocks(key,
                values[0]));
        return new WriteLock(MASTERLOCK) {
            @Override
            public void lock() {
                while (isRangeBlocked(LockType.WRITE, operator, key, values)) {
                    continue;
                }
                delegate.lock();
            }

            @Override
            public void lockInterruptibly() throws InterruptedException {
                while (isRangeBlocked(LockType.WRITE, operator, key, values)) {
                    continue;
                }
                delegate.lockInterruptibly();
            }

            @Override
            public Condition newCondition() {
                return delegate.newCondition();
            }

            @Override
            public String toString() {
                return delegate.toString();
            }

            @Override
            public boolean tryLock() {
                while (isRangeBlocked(LockType.WRITE, operator, key, values)) {
                    continue;
                }
                return delegate.tryLock();
            }

            @Override
            public boolean tryLock(long timeout, TimeUnit unit)
                    throws InterruptedException {
                if(!isRangedBlockedWithTimeout(timeout, unit, LockType.WRITE,
                        operator, key, values)) {
                    return delegate.tryLock(timeout, unit);
                }
                return false;
            }

            @Override
            public void unlock() {
                delegate.unlock();
            }
        };
    }
}
