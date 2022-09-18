/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.concurrent;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nullable;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link LockBrocker} provides the ability to lock granular notions of things
 * (e.g., records, fields, ranges, keys ,etc) that are identified by a
 * {@link Token}.
 * <p>
 * The {@link LockBroker} uses dynamic canonical locks that are created
 * on-demand. A {@link LockBrocker} should be used for managing concurrent
 * access to dynamic resources that aren't tangibly defined in their own right.
 * </p>
 * <p>
 * When the {@link LockBroker} is used to successfully acquire a lock, it
 * returns a {@link Permit} that permits access to the guarded resource. The
 * {@link Permit} can later be {@link Permit#release() released} when exclusive
 * access to the resource is no longer required.
 * </p>
 * <p>
 * A {@link LockBroker} can manage locks for all kinds of {@link Token Tokens};
 * including {@link RangeToken RangeTokens}. Conceptually, {@link RangeToken
 * a.k.a range token} locks (range locks) are designed to protect concurrent
 * access to secondary indices (i.e. Reader A wants to find key K between values
 * X and Z and Writer B wants to write Key K as value Y).
 * </p>
 * <p>
 * <strong>NOTE:</strong> Locking facilitated by a {@link LockBroker} is not
 * reentrant. The {@link Permit permits} that are returned when a lock is
 * acquired does not need to be released by the same thread that acquired it.
 * </p>
 *
 * @author Jeff Nelson
 */
public class LockBroker {

    /**
     * Return a new {@link LockBroker} that provides a set of distinct locks.
     * 
     * @return the {@link LockBroker}
     */
    public static LockBroker create() {
        return new LockBroker(new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), true);
    }

    /**
     * Return a {@link LockBroker} that does not provide any locking.
     * <p>
     * This should be used in situations where access is guaranteed (or at least
     * assumed) to be isolated (e.g. a Transaction) and we need to simulate
     * locking for polymorphic consistency.
     * </p>
     * 
     * @return the {@link LockBroker}
     */
    public static LockBroker noOp() {
        return NO_OP;
    }

    /**
     * The amount of time to wait between GC cycles.
     */
    private static int GC_DELAY = 1000;

    /**
     * A collection of brokers that are GC eligible.
     */
    private static Set<LockBroker> brokers = new HashSet<>();

    /**
     * The service that is responsible for carrying out garbage collection for
     * all the lock services.
     */
    private static final ScheduledExecutorService gc = Executors
            .newScheduledThreadPool(1, new ThreadFactoryBuilder()
                    .setNameFormat("Lock Broker GC").setDaemon(true).build());

    static {
        gc.scheduleWithFixedDelay(() -> {
            for (LockBroker broker : brokers) {
                try {
                    broker.gc();
                }
                catch (ConcurrentModificationException e) {
                    return;
                }
            }
        }, GC_DELAY, GC_DELAY, TimeUnit.MILLISECONDS);
    }

    /**
     * Return from {@link #noOp()}.
     */
    private static final LockBroker NO_OP = new LockBroker(ImmutableMap.of(),
            ImmutableMap.of(), ImmutableMap.of(), false) {

        @Override
        public Permit readLock(Token token) {
            return new Permit(token, Mode.READ);
        }

        @Override
        public Permit tryReadLock(Token token) {
            return readLock(token);
        }

        @Override
        public Permit tryWriteLock(Token token) {
            return writeLock(token);
        }

        @Override
        public void unlock(Permit permit) {}

        @Override
        public Permit writeLock(Token token) {
            return new Permit(token, Mode.WRITE);
        }

        @Override
        boolean isRangeBlocked(Mode mode, Token token) {
            return false;
        }
    };

    /**
     * A mapping from a {@link Token token}, representing a resource, to the
     * {@link Lock lock} that controls concurrent access to that resource.
     * <p>
     * The {@link Lock} is encapsulated within a {@link LockReference} to keep
     * track of active usage. Periodically, {@link Lock locks} that are not
     * actively being used are garbage collected.
     * </p>
     */
    private final Map<Token, LockReference> locks;

    /**
     * Information about the values that are subject to a locked range read
     * (e.g., a {@link RangeToken} was used to acquire a lock).
     */
    private final Map<Text, RangeSet<Value>> reads;

    /**
     * Information about the values that are subject to a locked range write
     * (e.g., a {@link RangeToken} was used to acquire a lock).
     */
    private final Map<Text, Set<Value>> writes;

    /**
     * Construct a new instance.
     * 
     * @param locks
     * @param gc
     */
    private LockBroker(Map<Token, LockReference> locks,
            Map<Text, RangeSet<Value>> reads, Map<Text, Set<Value>> writes,
            boolean enableGC) {
        this.locks = locks;
        this.reads = reads;
        this.writes = writes;
        if(enableGC) {
            brokers.add(this);
        }
    }

    /**
     * Acquire a read lock for the resource represented by {@code token},
     * blocking if necessary, and return a {@link Permit} to proceed.
     * 
     * @param token
     * @return the {@link Permit}
     */
    public Permit readLock(Token token) {
        while (isRangeBlocked(Mode.READ, token)) {
            Thread.yield();
            continue;
        }
        LockReference reference = ensureLockReference(token);
        Permit permit;
        if(reference.lock instanceof StampedLock) {
            StampedLock lock = (StampedLock) reference.lock;
            long stamp = lock.readLock();
            permit = new StampedPermit(token, Mode.READ, stamp);
        }
        else {
            ReadWriteLock lock = (ReadWriteLock) reference.lock;
            lock.readLock().lock();
            permit = new Permit(token, Mode.READ);
        }
        afterLockAcquired(Mode.READ, token);
        return permit;
    }

    /**
     * Shutdown the {@link LockBroker}.
     */
    public final void shutdown() {
        brokers.remove(this);
    }

    /**
     * Try to acquire a read lock for the resource represented by {@code token},
     * if it is immediately available, and return a {@link Permit} to proceed if
     * successful. If the lock cannot be immediately acquired, return
     * {@code null}
     * 
     * @param token
     * @return the {@link Permit} if the lock is acquired; otherwise
     *         {@code null}
     */
    @Nullable
    public Permit tryReadLock(Token token) {
        if(isRangeBlocked(Mode.READ, token)) {
            return null;
        }
        else {
            LockReference reference = ensureLockReference(token);
            if(reference.lock instanceof StampedLock) {
                StampedLock lock = (StampedLock) reference.lock;
                long stamp = lock.tryReadLock();
                if(stamp != 0) {
                    afterLockAcquired(Mode.READ, token);
                    return new StampedPermit(token, Mode.READ, stamp);
                }
            }
            else {
                ReadWriteLock lock = (ReadWriteLock) reference.lock;
                if(lock.readLock().tryLock()) {
                    afterLockAcquired(Mode.READ, token);
                    return new Permit(token, Mode.READ);
                }
            }
            reference.count.decrementAndGet();
            return null;
        }
    }

    /**
     * Try to acquire a write lock for the resource represented by
     * {@code token}, if it is immediately available, and return a
     * {@link Permit} to proceed if successful. If the lock cannot be
     * immediately acquired, return {@code null}
     * 
     * @param token
     * @return the {@link Permit} if the lock is acquired; otherwise
     *         {@code null}
     */
    @Nullable
    public Permit tryWriteLock(Token token) {
        if(isRangeBlocked(Mode.WRITE, token)) {
            return null;
        }
        else {
            LockReference reference = ensureLockReference(token);
            if(reference.lock instanceof StampedLock) {
                StampedLock lock = (StampedLock) reference.lock;
                long stamp = lock.tryWriteLock();
                if(stamp != 0) {
                    afterLockAcquired(Mode.WRITE, token);
                    return new StampedPermit(token, Mode.WRITE, stamp);
                }
            }
            else {
                ReadWriteLock lock = (ReadWriteLock) reference.lock;
                if(lock.writeLock().tryLock()) {
                    afterLockAcquired(Mode.WRITE, token);
                    return new Permit(token, Mode.WRITE);
                }
            }
            reference.count.decrementAndGet();
            return null;
        }
    }

    /**
     * Release the lock that was acquired and corresponds to {@code permit}.
     * <p>
     * Only a {@code permit} issued by this {@link LockBroker} is a valid input
     * to this method.
     * </p>
     * 
     * @param permit
     */
    public void unlock(Permit permit) {
        if(permit.issuer() != this) {
            throw new IllegalArgumentException(AnyStrings.format(
                    "Invalid permit. Permit {} was not issued by {}", permit,
                    this));
        }
        Token token = permit.token();
        Mode mode = permit.mode();
        LockReference reference = locks.get(token);
        if(reference != null) {
            if(permit instanceof StampedPermit) {
                StampedLock lock = (StampedLock) reference.lock;
                long stamp = ((StampedPermit) permit).stamp();
                if(mode == Mode.READ) {
                    lock.unlockRead(stamp);
                }
                else {
                    lock.unlockWrite(stamp);
                }
            }
            else {
                ReadWriteLock lock = (ReadWriteLock) reference.lock;
                if(mode == Mode.READ) {
                    lock.readLock().unlock();
                }
                else {
                    lock.writeLock().unlock();
                }
            }
            reference.count.decrementAndGet();
            afterLockReleased(mode, token);
        }
        else {
            throw new IllegalStateException(AnyStrings.format(
                    "An active lock for {} was GCed in {}", token, this));
        }
    }

    /**
     * Acquire a write lock for the resource represented by {@code token},
     * blocking if necessary, and return a {@link Permit} to proceed.
     * 
     * @param token
     * @return the {@link Permit}
     */
    public Permit writeLock(Token token) {
        while (isRangeBlocked(Mode.WRITE, token)) {
            Thread.yield();
            continue;
        }
        LockReference reference = ensureLockReference(token);
        Permit permit;
        if(reference.lock instanceof StampedLock) {
            StampedLock lock = (StampedLock) reference.lock;
            long stamp = lock.writeLock();
            permit = new StampedPermit(token, Mode.WRITE, stamp);
        }
        else {
            ReadWriteLock lock = (ReadWriteLock) reference.lock;
            lock.writeLock().lock();
            permit = new Permit(token, Mode.WRITE);
        }
        afterLockAcquired(Mode.WRITE, token);
        return permit;
    }

    /**
     * Clear any {@link LockReference LockReferences} that are not active.
     */
    @VisibleForTesting
    void gc() {
        Iterator<Entry<Token, LockReference>> it = locks.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Token, LockReference> entry = it.next();
            LockReference reference = entry.getValue();
            if(reference.count.compareAndSet(0, Integer.MIN_VALUE)) {
                it.remove();
            }
        }
    }

    /**
     * Return {@code true} if an attempt to acquire a {@code mode} type lock for
     * the resource represented by {@code token} is <strong>range
     * blocked</strong>.
     * <p>
     * A range block occurs when there is another {@link Mode#READ read} or
     * {@link Mode#WRITE write} lock held that intersects {@code token}.
     * </p>
     * 
     * @param mode
     * @param token
     * @return {@code true} if the the expressed lock attempt is range blocked
     */
    @VisibleForTesting
    boolean isRangeBlocked(Mode mode, Token token) {
        if(token instanceof RangeToken) {
            RangeToken rangeToken = (RangeToken) token;
            Text key = rangeToken.getKey();
            Value value = rangeToken.getValues()[0];
            if(mode == Mode.READ) {
                Operator operator = rangeToken.getOperator();
                if(operator != null) {
                    Set<Value> rangeWrites = writes.get(key);
                    if(rangeWrites == null) {
                        Set<Value> created = Sets.newConcurrentHashSet();
                        rangeWrites = writes.putIfAbsent(key, created);
                        rangeWrites = MoreObjects.firstNonNull(rangeWrites,
                                created);
                    }
                    switch (operator) {
                    case EQUALS:
                        return rangeWrites.contains(value);
                    case NOT_EQUALS:
                        return rangeWrites.size() > 1
                                || (rangeWrites.size() == 1
                                        && !rangeWrites.contains(value));
                    default:
                        Iterable<Range<Value>> ranges = rangeToken.ranges();
                        Iterator<Value> it = rangeWrites.iterator();
                        while (it.hasNext()) {
                            Value rangeWrite = it.next();
                            Range<Value> point = Range.singleton(rangeWrite);
                            for (Range<Value> range : ranges) {
                                if(range.isConnected(point)
                                        && !range.intersection(point).isEmpty()
                                        && locks.get(RangeToken.forWriting(key,
                                                rangeWrite)) != null) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                }
                else {
                    throw new IllegalArgumentException(
                            "A RangeToken associated with a read lock must contain an operator");
                }
            }
            else {
                /*
                 * If I want to WRITE X, I am blocked if there is a READ that
                 * touches X (e.g. direct read for X or a range read that
                 * includes X)
                 */
                RangeSet<Value> rangeReads = reads.get(key);
                if(rangeReads == null) {
                    RangeSet<Value> created = TreeRangeSet.create();
                    rangeReads = reads.putIfAbsent(key, created);
                    rangeReads = MoreObjects.firstNonNull(rangeReads, created);
                }
                synchronized (rangeReads) {
                    return rangeReads.contains(value);
                }
            }
        }
        else {
            // NOTE: Currently, a non-RangeToken cannot be analyzed for range
            // blocking because its individual components are not preserved.
            return false;
        }
    }

    /**
     * Note that a {@code mode} type lock for the resource represented by
     * {@code token} was acquired.
     * 
     * @param mode
     * @param token
     */
    private void afterLockAcquired(Mode mode, Token token) {
        if(token instanceof RangeToken) {
            RangeToken rangeToken = (RangeToken) token;
            Text key = rangeToken.getKey();
            if(mode == Mode.READ) {
                Iterable<Range<Value>> ranges = rangeToken.ranges();
                RangeSet<Value> existing = reads.get(key);
                if(existing == null) {
                    RangeSet<Value> created = TreeRangeSet.create();
                    existing = reads.putIfAbsent(key, created);
                    existing = MoreObjects.firstNonNull(existing, created);
                }
                synchronized (existing) {
                    for (Range<Value> range : ranges) {
                        existing.add(range);
                    }
                }
            }
            else {
                Value value = rangeToken.getValues()[0];
                Set<Value> existing = writes.get(key);
                if(existing == null) {
                    Set<Value> created = Sets.newConcurrentHashSet();
                    existing = writes.putIfAbsent(key, created);
                    existing = MoreObjects.firstNonNull(existing, created);
                }
                existing.add(value);
            }
        }
    }

    /**
     * Note that a {@code mode} type lock for the resource represented by
     * {@code token} was released.
     * 
     * @param mode
     * @param token
     */
    private void afterLockReleased(Mode mode, Token token) {
        if(token instanceof RangeToken) {
            RangeToken rangeToken = (RangeToken) token;
            Text key = rangeToken.getKey();
            if(mode == Mode.READ) {
                Iterable<Range<Value>> ranges = rangeToken.ranges();
                RangeSet<Value> existing = reads.get(key);
                synchronized (existing) {
                    for (Range<Value> range : ranges) {
                        existing.remove(range);
                    }
                    if(existing.isEmpty()) {
                        reads.remove(key);
                    }
                }
            }
            else {
                Value value = rangeToken.getValues()[0];
                Set<Value> existing = writes.get(key);
                if(existing != null) {
                    existing.remove(value);
                    if(existing.isEmpty()) {
                        writes.remove(key);
                    }
                }
            }
        }
    }

    /**
     * Create a new Lock for the resource identified by {@code token}.
     * <p>
     * The returned value is designed to be wrapped in a {@link LockReference}.
     * </p>
     * 
     * @param token
     * @return the new Lock
     */
    private Object createLock(Token token) {
        if(token instanceof SharedToken) {
            return new SharedReadWriteLock();
        }
        else {
            return new StampedLock();
        }
    }

    /**
     * Ensure that a canonical {@link LockReference} for {@code token} exists
     * and return it.
     * <p>
     * The returned {@link LockReference} is guaranteed to have a
     * {@link LockReference#count} that is greater than 0.
     * </p>
     * 
     * @param token
     * @return the active {@link LockReference}
     */
    private LockReference ensureLockReference(Token token) {
        LockReference reference = locks.get(token);
        if(reference == null) {
            Object lock = createLock(token);
            LockReference created = new LockReference(lock);
            reference = locks.putIfAbsent(token, created);
            reference = MoreObjects.firstNonNull(reference, created);
        }
        reference.count.incrementAndGet();
        LockReference gced = null;
        if(reference.count.get() <= 0
                || (gced = locks.putIfAbsent(token, reference)) != reference) {
            // We lost a race and #reference was either garbage collected or
            // marked eligible for garbage collection, so we have to try again.
            reference.count.decrementAndGet();
            Logger.debug("Lock Broker GC Race Condition: Expected "
                    + "{} but was {}", reference, gced);
            Thread.yield();
            return ensureLockReference(token);
        }
        else {
            return reference;
        }
    }

    /**
     * A {@link Permit} is issued when a {@link Lock} is successfully acquired.
     * <p>
     * The {@link Permit} is implicitly associated with a locked resource and
     * serves as a ticket to exchange in the future when that resource can be
     * {@link #release() unlocked} and made available for others.
     * </p>
     *
     * @author Jeff Nelson
     */
    public class Permit {

        /**
         * The {@link Token} representing the locked resource and associated
         * with the resource's canonical {@link Lock}.
         */
        private final Token token;

        /**
         * The {@link Mode} that was used for locking the resource.
         */
        private final Mode mode;

        /**
         * Back reference to the issuing {@link LockBroker} for validation
         * purposes.
         */
        private final LockBroker issuer;

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param mode
         */
        Permit(Token token, Mode mode) {
            this.token = token;
            this.mode = mode;
            this.issuer = LockBroker.this;
        }

        /**
         * Unlock the resource represented by this {@link Permit} .
         */
        public final void release() {
            issuer.unlock(this);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("token", token)
                    .add("mode", mode).toString();
        }

        /**
         * Return the {@link #issuer}.
         * 
         * @return the {@link #issuer}
         */
        final LockBroker issuer() {
            return issuer;
        }

        /**
         * Return the {@link #mode}.
         * 
         * @return the {@link #mode}
         */
        final Mode mode() {
            return mode;
        }

        /**
         * Return the {@link #token}.
         * 
         * @return the {@link #token}
         */
        final Token token() {
            return token;
        }

    }

    enum Mode {
        READ, WRITE
    }

    /**
     * A {@link Permit} that is associated with a stamp returned from
     * successfully acquiring a {@link StampedLock}.
     *
     *
     * @author Jeff Nelson
     */
    final class StampedPermit extends Permit {

        /**
         * The stamp.
         */
        private final long stamp;

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param lock
         * @param mode
         */
        StampedPermit(Token token, Mode mode, long stamp) {
            super(token, mode);
            if(stamp != 0) {
                this.stamp = stamp;
            }
            else {
                throw new IllegalArgumentException(
                        "Cannot create a Permit for stamp " + stamp);
            }
        }

        /**
         * Return the {@link #stamp}.
         * 
         * @return the {@link #stamp}
         */
        final long stamp() {
            return stamp;
        }

    }

    /**
     * Encapsulates a Lock that has been acquired by the {@link LockBroker}.
     *
     * @author Jeff Nelson
     */
    private class LockReference {

        /**
         * The lock object.
         * <p>
         * This will either be a StampedLock or ReadWriteLock, but we have to
         * type it as a generic Object because those classes don't have a common
         * ancestor.
         * </p>
         */
        private final Object lock;

        /**
         * The number of active references/holds.
         */
        private final AtomicInteger count;

        /**
         * Construct a new instance.
         * 
         * @param lock
         */
        private LockReference(Object lock) {
            this.lock = lock;
            this.count = new AtomicInteger(0);
        }
    }

}
