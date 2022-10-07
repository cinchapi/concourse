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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nullable;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.server.model.Ranges;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
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
 * reentrant. So, the {@link Permit permits} that are returned when a lock is
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
        return new LockBroker(true);
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
    private static final LockBroker NO_OP = new LockBroker(false) {

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
     * The ranges that are locked.
     * <p>
     * This data is used by the {@link LockBroker broker's}
     * {@link RangeReadWriteLock RangeReadWriteLocks} to provide locking for
     * abstract ranges.
     * </p>
     */
    private final Map<Text, Map<Range<Value>, AtomicInteger>> rangeLocks;

    /**
     * Threads that are queued to be {@link LockSupport#unpark(Thread) unparked}
     * when a {@link RangeReadWriteLock.RangeLock#unlock() range unlock} occurs.
     */
    private final ConcurrentLinkedQueue<Thread> parked;

    /**
     * Construct a new instance.
     * 
     * @param enabled
     */
    private LockBroker(boolean enabled) {
        if(enabled) {
            this.locks = new ConcurrentHashMap<>();
            this.rangeLocks = new ConcurrentHashMap<>();
            this.parked = new ConcurrentLinkedQueue<>();
            brokers.add(this);
        }
        else {
            this.locks = null;
            this.rangeLocks = null;
            this.parked = null;
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
        LockReference reference = ensureLockReference(token);
        if(reference.lock instanceof StampedLock) {
            StampedLock lock = (StampedLock) reference.lock;
            long stamp = lock.tryReadLock();
            if(stamp != 0) {
                return new StampedPermit(token, Mode.READ, stamp);
            }
        }
        else {
            ReadWriteLock lock = (ReadWriteLock) reference.lock;
            if(lock.readLock().tryLock()) {
                return new Permit(token, Mode.READ);
            }
        }
        reference.count.decrementAndGet();
        return null;
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
        LockReference reference = ensureLockReference(token);
        if(reference.lock instanceof StampedLock) {
            StampedLock lock = (StampedLock) reference.lock;
            long stamp = lock.tryWriteLock();
            if(stamp != 0) {
                return new StampedPermit(token, Mode.WRITE, stamp);
            }
        }
        else {
            ReadWriteLock lock = (ReadWriteLock) reference.lock;
            if(lock.writeLock().tryLock()) {
                return new Permit(token, Mode.WRITE);
            }
        }
        reference.count.decrementAndGet();
        return null;
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
        return permit;
    }

    /**
     * Clear any {@link LockReference LockReferences} that are not active.
     */
    @VisibleForTesting
    void gc() {
        Iterator<Entry<Token, LockReference>> locksIt = locks.entrySet()
                .iterator();
        while (locksIt.hasNext()) {
            Entry<Token, LockReference> entry = locksIt.next();
            LockReference reference = entry.getValue();
            if(reference.count.compareAndSet(0, Integer.MIN_VALUE)) {
                locksIt.remove();
            }
        }
        Iterator<Entry<Text, Map<Range<Value>, AtomicInteger>>> rangeKeys = rangeLocks
                .entrySet().iterator();
        while (rangeKeys.hasNext()) {
            Entry<Text, Map<Range<Value>, AtomicInteger>> rangeEntry = rangeKeys
                    .next();
            Map<Range<Value>, AtomicInteger> rangeKeyLocks = rangeEntry
                    .getValue();
            Iterator<Entry<Range<Value>, AtomicInteger>> rangeKeyLocksIt = rangeKeyLocks
                    .entrySet().iterator();
            while (rangeKeyLocksIt.hasNext()) {
                Entry<Range<Value>, AtomicInteger> entry = rangeKeyLocksIt
                        .next();
                AtomicInteger state = entry.getValue();
                int s = state.get();
                if(s == 0 && state.compareAndSet(s, s)) {
                    rangeKeyLocksIt.remove();
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
        else if(token instanceof RangeToken) {
            return new RangeReadWriteLock((RangeToken) token);
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
        LockReference reference = locks.computeIfAbsent(token,
                $ -> new LockReference(createLock($)));
        reference.count.incrementAndGet();
        LockReference gced = null;
        if(reference.count.get() <= 0
                || (gced = locks.putIfAbsent(token, reference)) != reference) {
            // We lost a race and #reference was either garbage collected or
            // marked eligible for garbage collection, so we have to try
            // again.
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

    /**
     * A {@link RangeReadWriteLock} provides and atomic {@link ReadWriteLock
     * read/write lock} over one or more {@link Range Ranges} of {@link Value
     * Values} stored for a specific {@link Text key}.
     * <p>
     * Because the number of discrete items within a {@link Range} is infinite,
     * it is impossible to use individual locks and a {@link RangeReadWriteLock}
     * is necessary to protect concurrent access only everything that is
     * considered within the boundaries of the {@link Range}.
     * </p>
     *
     *
     * @author Jeff Nelson
     */
    @VisibleForTesting
    protected class RangeReadWriteLock implements ReadWriteLock {

        /**
         * The {@link RangeToken} that represents the {@link Range Ranges} of
         * interest.
         */
        private final RangeToken token;

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param range
         */
        protected RangeReadWriteLock(RangeToken token) {
            this.token = token;
        }

        @Override
        public Lock readLock() {
            return new RangeLock(Mode.READ);
        }

        @Override
        public Lock writeLock() {
            return new RangeLock(Mode.WRITE);
        }

        /**
         * An {@link Lock} that is provided by a {@link RangeReadWriteLock}.
         *
         * @author Jeff Nelson
         */
        private class RangeLock implements Lock {

            /**
             * The {@link Mode} to use for locking.
             */
            private final Mode mode;

            /**
             * Construct a new instance.
             * 
             * @param mode
             */
            private RangeLock(Mode mode) {
                this.mode = mode;
            }

            @Override
            public void lock() {
                for (;;) {
                    if(tryLock()) {
                        return;
                    }
                    else {
                        parked.add(Thread.currentThread());
                        LockSupport.park(this);
                        continue;
                    }
                }
            }

            @Override
            public void lockInterruptibly() throws InterruptedException {
                for (;;) {
                    if(Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException();
                    }
                    if(tryLock()) {
                        return;
                    }
                    else {
                        parked.add(Thread.currentThread());
                        LockSupport.park(this);
                        continue;
                    }
                }
            }

            @Override
            public Condition newCondition() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean tryLock() {
                Text key = token.getKey();
                Operator operator = token.getOperator();
                Map<Range<Value>, AtomicInteger> ranges = rangeLocks
                        .computeIfAbsent(key, $ -> new ConcurrentHashMap<>());
                outer: for (;;) {
                    /*
                     * NOTE: Continuation of the #outer loop (e.g., retry)
                     * occurs when a lock has been newly acquired or released
                     * while checking its state.
                     */
                    for (Range<Value> range : token.ranges()) {
                        AtomicInteger state = ranges.computeIfAbsent(range,
                                $ -> new AtomicInteger(0));
                        int s = state.get();
                        if(mode == Mode.WRITE && (s >= 1 || s < 0)) {
                            // If the same #range is locked in any state, this
                            // attempt to WRITE lock is blocked.
                            if(state.compareAndSet(s, s)) {
                                return false;
                            }
                            else {
                                continue outer;
                            }
                        }
                        if(mode == Mode.READ && operator == Operator.EQUALS) {
                            if(s <= 0 && state.compareAndSet(s, s)) {
                                // A EQUAL READ is only blocked if there is a
                                // concurrent WRITE for the exact value. If that
                                // isn't the case we can proceed to the next of
                                // #token.ranges() without checking the current
                                // #range for conflicts with other locked
                                // connected ranges.
                                continue;
                            }
                            else if(s > 0 && state.compareAndSet(s, s)) {
                                return false;
                            }
                            else {
                                continue outer;
                            }
                        }
                        // As a last resort, check for locked connected ranges
                        // and determine if this lock is blocked as a result.
                        Iterator<Entry<Range<Value>, AtomicInteger>> it = ranges
                                .entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Range<Value>, AtomicInteger> entry = it
                                    .next();
                            Range<Value> locked = entry.getKey();
                            state = entry.getValue();
                            s = state.get();
                            if((s == 0 || (mode == Mode.READ && s < 0))) {
                                if(state.compareAndSet(s, s)) {
                                    // If the #locked range is actually unlocked
                                    // or we are attempting a READ lock and the
                                    // #locked range is also a READ lock, we can
                                    // move onto checking for conflicts with the
                                    // next #locked range.
                                    continue;
                                }
                                else {
                                    continue outer;
                                }
                            }
                            else if(Ranges.haveNonEmptyIntersection(locked,
                                    range)
                                    && ((mode == Mode.READ && s > 0)
                                            || (mode == Mode.WRITE && s < 0))) {
                                if(state.compareAndSet(s, s)) {
                                    return false;
                                }
                                else {
                                    continue outer;
                                }
                            }
                        }
                    }
                    List<AtomicInteger> undos = new ArrayList<>(1);
                    for (Range<Value> range : token.ranges()) {
                        AtomicInteger state = ranges.computeIfAbsent(range,
                                $ -> new AtomicInteger(0));
                        int s = state.get();
                        if(!state.compareAndSet(s,
                                s + (mode == Mode.READ ? -1 : 1))) {
                            // The lock has been newly acquired or released
                            // since we checked the state, so undo any ranges we
                            // intermediately acquired and tryLock again.
                            for (AtomicInteger undo : undos) {
                                undo.addAndGet(mode == Mode.READ ? 1 : -1);
                            }
                            continue outer;
                        }
                        else {
                            undos.add(state);
                        }
                    }
                    return true;
                }
            }

            @Override
            public boolean tryLock(long time, TimeUnit unit)
                    throws InterruptedException {
                Stopwatch watch = Stopwatch.createStarted();
                for (;;) {
                    watch.stop();
                    long elapsed = watch.elapsed(unit);
                    time = time - elapsed;
                    if(time > 0) {
                        watch.start();
                        if(tryLock()) {
                            return true;
                        }
                        else {
                            Thread.yield();
                            continue;
                        }
                    }
                    else {
                        return false;
                    }
                }
            }

            @Override
            public void unlock() {
                Text key = token.getKey();
                Map<Range<Value>, AtomicInteger> ranges = rangeLocks
                        .computeIfAbsent(key, $ -> new ConcurrentHashMap<>());
                outer: for (;;) {
                    List<AtomicInteger> undos = new ArrayList<>(1);
                    for (Range<Value> range : token.ranges()) {
                        AtomicInteger state = ranges.computeIfAbsent(range,
                                $ -> new AtomicInteger(0));
                        int s = state.get();
                        if(s == 0 || (mode == Mode.READ && s > 0)
                                || (mode == Mode.WRITE && s < 0)) {
                            throw new IllegalMonitorStateException();
                        }
                        if(state.compareAndSet(s,
                                s + (mode == Mode.READ ? 1 : -1))) {
                            undos.add(state);
                        }
                        else {
                            // The lock has been newly acquired or released
                            // since we checked the state, so try to unlock
                            // again
                            for (AtomicInteger undo : undos) {
                                undo.addAndGet(mode == Mode.READ ? -1 : 1);
                            }
                            continue outer;
                        }
                    }
                    break;
                }
                Thread t;
                while ((t = parked.poll()) != null) {
                    LockSupport.unpark(t);
                }
            }

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
