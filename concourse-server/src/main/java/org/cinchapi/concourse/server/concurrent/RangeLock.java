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

import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

import org.cinchapi.concourse.annotate.Experimental;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Functions;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.AutoHashMap;
import org.cinchapi.concourse.util.AutoMap;
import org.cinchapi.concourse.util.AutoSkipListMap;
import org.cinchapi.concourse.util.Transformers;

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
    public static RangeLock grabForReading(String key, Operator operator,
            TObject... values) {
        return grabForReading(Text.wrap(key), operator,
                Transformers.transformArray(values, Functions.TOBJECT_TO_VALUE,
                        Value.class));
    }

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
    public static RangeLock grabForWriting(String key, TObject value) {
        return grabForWriting(Text.wrap(key), Value.wrap(value));
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
     * Return the {@link RangeLock} that is identified by {@code token}.
     * 
     * @param token
     * @return the RangeLock
     */
    public static RangeLock grabWithToken(RangeToken token) {
        return new RangeLock(token);
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
            // IF I WANT TO READ [X], I AM BLOCKED IF:
            switch (operator) {
            case EQUALS:
                // There is a DIRECT WRITE equal to X
                return DIRECT.get(key).get(value).isWriteLocked()
                        && !DIRECT.get(key).get(value)
                                .isWriteLockedByCurrentThread();
            case NOT_EQUALS:
                // There is a DIRECT WRITE not equal to X
                for (Entry<Value, ReentrantReadWriteLock> entry : DIRECT.get(
                        key).entrySet()) {
                    if(!entry.getKey().equals(value)
                            && entry.getValue().isWriteLocked()
                            && !entry.getValue().isWriteLockedByCurrentThread()) {
                        return true;
                    }
                }
                return false;
            case GREATER_THAN:
                // There is a RANGE WRITE greater than X
                for (ReentrantReadWriteLock lock : RANGE.get(key)
                        .tailMap(value, false).values()) {
                    if(lock.isWriteLocked()
                            && !lock.isWriteLockedByCurrentThread()) {
                        return true;
                    }
                }
                return false;
            case GREATER_THAN_OR_EQUALS:
                // There is a RANGE WRITE greater than or equal to X
                for (ReentrantReadWriteLock lock : RANGE.get(key)
                        .tailMap(value, true).values()) {
                    if(lock.isWriteLocked()
                            && !lock.isWriteLockedByCurrentThread()) {
                        return true;
                    }
                }
                return false;
            case LESS_THAN:
                // There is a RANGE WRITE less than X
                for (ReentrantReadWriteLock lock : RANGE.get(key)
                        .headMap(value, false).values()) {
                    if(lock.isWriteLocked()
                            && !lock.isWriteLockedByCurrentThread()) {
                        return true;
                    }
                }
                return false;
            case LESS_THAN_OR_EQUALS:
                // There is a RANGE WRITE less than or equal to X
                for (ReentrantReadWriteLock lock : RANGE.get(key)
                        .headMap(value, true).values()) {
                    if(lock.isWriteLocked()
                            && !lock.isWriteLockedByCurrentThread()) {
                        return true;
                    }
                }
                return false;
            case BETWEEN:
                // There is a RANGE WRITE between X1 and X2
                for (ReentrantReadWriteLock lock : RANGE.get(key)
                        .subMap(value, true, values[1], false).values()) {
                    if(lock.isWriteLocked()
                            && !lock.isWriteLockedByCurrentThread()) {
                        return true;
                    }
                }
                return false;
            case REGEX:
                // There a RANGE WRITE matching X
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
                // There a RANGE WRITE not matching X
                for (Entry<Value, ReentrantReadWriteLock> entry : NREGEX.get(
                        key).entrySet()) {
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
            // A1: The smallest RANGE READ is less than or equal to X, and
            for (Entry<Value, ReentrantReadWriteLock> entry : RANGE.get(key)
                    .entrySet()) {
                if(entry.getKey().compareTo(value) <= 0
                        && entry.getValue().getReadLockCount() > 0) {
                    // A2: The largest RANGE READ is greater than X
                    for (Entry<Value, ReentrantReadWriteLock> entry2 : RANGE
                            .get(key).descendingMap().entrySet()) {
                        if(entry2.getKey().compareTo(value) > 0
                                && entry.getValue().getReadLockCount() > 0) {
                            return true;
                        }
                        else if(entry.getKey().compareTo(value) > 0) {
                            continue;
                        }
                        else {
                            break;
                        }
                    }
                }
                else if(entry.getKey().compareTo(value) <= 0) {
                    continue;
                }
                else {
                    break;
                }
            }
            // B: There is a DIRECT READ for anything equal to X
            if(DIRECT.get(key).get(value).getReadLockCount() > 0) {
                return true;
            }
            // C: There is a REGEX READ for anything matching X
            for (Entry<Value, ReentrantReadWriteLock> entry : REGEX.get(key)
                    .entrySet()) {
                if(entry.getKey().getTObject().toString()
                        .matches(value.getTObject().toString())) {
                    if(entry.getValue().getReadLockCount() > 0) {
                        return true;
                    }
                }

            }
            // D: There is a NREGEX READ for for anything not matching X
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
            return new ReadLock[] { DIRECT.get(key).get(value).readLock() };
        case NOT_EQUALS:
            return new ReadLock[] {}; // TODO
        case GREATER_THAN:
            return new ReadLock[] { RANGE.get(key).get(value).readLock(),
                    RANGE.get(key).get(Value.POSITIVE_INFINITY).readLock() };
        case GREATER_THAN_OR_EQUALS:
            return new ReadLock[] { RANGE.get(key).get(value).readLock(),
                    RANGE.get(key).get(Value.POSITIVE_INFINITY).readLock(),
                    DIRECT.get(key).get(value).readLock() };
        case LESS_THAN:
            return new ReadLock[] { RANGE.get(key).get(value).readLock(),
                    RANGE.get(key).get(Value.NEGATIVE_INFINITY).readLock() };
        case LESS_THAN_OR_EQUALS:
            return new ReadLock[] { RANGE.get(key).get(value).readLock(),
                    RANGE.get(key).get(Value.NEGATIVE_INFINITY).readLock(),
                    DIRECT.get(key).get(value).readLock() };
        case BETWEEN:
            return new ReadLock[] { RANGE.get(key).get(value).readLock(),
                    DIRECT.get(key).get(value).readLock(),
                    RANGE.get(key).get(values[1]).readLock() };
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
        return new WriteLock[] { DIRECT.get(key).get(value).writeLock(),
                RANGE.get(key).get(value).writeLock(),
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

    // --- The loaders and cleaners used in the range guarding auto maps.
    private static final Function<Value, ReentrantReadWriteLock> VALUE_LOADER;
    private static final Function<ReentrantReadWriteLock, Boolean> VALUE_CLEANER;
    private static final Function<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> SL_KEY_LOADER;
    private static final Function<AutoSkipListMap<Value, ReentrantReadWriteLock>, Boolean> SL_KEY_CLEANER;
    private static final Function<Text, AutoHashMap<Value, ReentrantReadWriteLock>> H_KEY_LOADER;
    private static final Function<AutoHashMap<Value, ReentrantReadWriteLock>, Boolean> H_KEY_CLEANER;

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
    private static final AutoMap<Text, AutoHashMap<Value, ReentrantReadWriteLock>> DIRECT;
    private static final AutoMap<Text, AutoHashMap<Value, ReentrantReadWriteLock>> REGEX;
    private static final AutoMap<Text, AutoHashMap<Value, ReentrantReadWriteLock>> NREGEX;
    private static final AutoMap<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>> RANGE;

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
        SL_KEY_LOADER = new Function<Text, AutoSkipListMap<Value, ReentrantReadWriteLock>>() {

            @Override
            public AutoSkipListMap<Value, ReentrantReadWriteLock> apply(
                    Text input) {
                return AutoMap.newAutoSkipListMap(VALUE_LOADER, VALUE_CLEANER);
            }

        };

        H_KEY_LOADER = new Function<Text, AutoHashMap<Value, ReentrantReadWriteLock>>() {

            @Override
            public AutoHashMap<Value, ReentrantReadWriteLock> apply(Text input) {
                return AutoMap.newAutoHashMap(VALUE_LOADER, VALUE_CLEANER);
            }

        };

        // Delete a Key when it has no locked values.
        SL_KEY_CLEANER = new Function<AutoSkipListMap<Value, ReentrantReadWriteLock>, Boolean>() {

            @Override
            public Boolean apply(
                    AutoSkipListMap<Value, ReentrantReadWriteLock> input) {
                synchronized (input) {
                    return input.isEmpty();
                }
            }

        };

        H_KEY_CLEANER = new Function<AutoHashMap<Value, ReentrantReadWriteLock>, Boolean>() {

            @Override
            public Boolean apply(
                    AutoHashMap<Value, ReentrantReadWriteLock> input) {
                synchronized (input) {
                    return input.isEmpty();
                }
            }

        };
        DIRECT = AutoMap.newAutoHashMap(H_KEY_LOADER, H_KEY_CLEANER);
        NREGEX = AutoMap.newAutoHashMap(H_KEY_LOADER, H_KEY_CLEANER);
        REGEX = AutoMap.newAutoHashMap(H_KEY_LOADER, H_KEY_CLEANER);
        RANGE = AutoMap.newAutoHashMap(SL_KEY_LOADER, SL_KEY_CLEANER);
    }
    // --- These define the scope of the RangeLock.
    private final Text key;

    private final Value[] values;

    @Nullable
    private final Operator operator;

    /**
     * Construct a new instance.
     * 
     * @param token
     */
    private RangeLock(RangeToken token) {
        super(token);
        this.key = token.getKey();
        this.values = token.getValues();
        this.operator = token.getOperator();
    }

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
        final ReadLock[] locks = getReadLocks(key, operator, values);
        final AtomicLock delegate = AtomicLock.newInstance(locks);
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
