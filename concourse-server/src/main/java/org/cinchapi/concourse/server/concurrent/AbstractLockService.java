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

import java.util.ConcurrentModificationException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This is the base class from all the internal lock services that provide
 * dynamic locks for notions of things (i.e. records, keys in records, ranges,
 * keys, etc). This class implements the bulk of the logic for concurrently
 * dealing with the locks in a secure way.
 * 
 * @author jnelson
 */
public abstract class AbstractLockService<T extends Token, L extends ReferenceCountingLock> {

    // --- Global GC State

    /**
     * The amount of time to wait between GC cycles.
     */
    private static int GC_DELAY = 1000;

    /**
     * A collection of services that are GC eligible.
     */
    private static Set<AbstractLockService<?, ?>> services = Sets.newHashSet();

    /**
     * The service that is responsible for carrying out garbage collection for
     * all the lock services.
     */
    private static final ScheduledExecutorService gc = Executors
            .newScheduledThreadPool(1, new ThreadFactoryBuilder()
                    .setNameFormat("Lock Service GC").setDaemon(true).build());
    static {
        gc.scheduleWithFixedDelay(new GarbageCollector(), GC_DELAY, GC_DELAY,
                TimeUnit.MILLISECONDS);
    }

    /**
     * A cache of locks that have been requested, each of which is mapped from a
     * corresponding {@link Token}. This cache is periodically cleaned (e.g.
     * stale locks are removed) up using a protocol defined in the subclass.
     */
    protected final ConcurrentMap<T, L> locks;

    /**
     * Construct a new NOOP instance.
     */
    protected AbstractLockService() {
        this.locks = null;
    }

    /**
     * Construct a new instance.
     * 
     * @param locks
     */
    protected AbstractLockService(ConcurrentMap<T, L> locks) {
        this.locks = locks;
        services.add(this);
    }

    /**
     * Return the ReadLock that is identified by {@code token}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param token
     * @return the ReadLock
     */
    public ReadLock getReadLock(T token) {
        return (ReadLock) getLock(token, true);
    }

    /**
     * Return the WriteLock that is identified by {@code token}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param token
     * @return the WriteLock
     */
    public WriteLock getWriteLock(T token) {
        return (WriteLock) getLock(token, false);
    }

    /**
     * Shutdown the lock service.
     */
    public void shutdown() {
        services.remove(this);
    }

    /**
     * Retrieve the lock that corresponds to {@code token} with the option to
     * return a shared (read) view or an exclusive (write) one.
     * <p>
     * This method will handle race conditions where another thread manages to
     * add a new lock for {@code token} into the cache while this operation is
     * executing. This method will also handle race conditions with the internal
     * garbage collection to ensure that any lock that is returned is indeed the
     * canonical one in the cache and safe from garbage collection during the
     * duration of its existence.
     * </p>
     * 
     * @param token
     * @param readLock
     * @return a lock view of the canonical lock for {@code token}
     */
    private Lock getLock(T token, boolean readLock) {
        L existing = locks.get(token);
        if(existing == null) {
            L created = createLock(token);
            existing = locks.putIfAbsent(token, created);
            existing = Objects.firstNonNull(existing, created);
        }
        existing.refs.incrementAndGet();
        if(existing.refs.get() <= 0
                || locks.putIfAbsent(token, existing) != existing) { // GC
                                                                     // happened
            existing.refs.decrementAndGet();
            return getLock(token, readLock);
        }
        else {
            return readLock ? existing.readLock() : existing.writeLock();
        }
    }

    /**
     * Return a new {@code lock} that is associated with {@code token}.
     * 
     * @param token
     * @return the new lock
     */
    protected abstract L createLock(T token);

    /**
     * The internal garbage collector is a task that periodically checks the
     * {@link #locks} cache for entries with 0 references and removes them.
     * 
     * @author jnelson
     */
    private static class GarbageCollector implements Runnable {

        @Override
        public void run() {
            try {
                for (AbstractLockService<?, ?> service : services) {
                    for (Object token : service.locks.keySet()) {
                        ReferenceCountingLock lock = service.locks.get(token);
                        if(lock.refs.compareAndSet(0, Integer.MIN_VALUE)) {
                            service.locks.remove(token, lock);
                        }
                    }
                }
            }
            catch (ConcurrentModificationException e) {
                return;
            }

        }
    }

}
