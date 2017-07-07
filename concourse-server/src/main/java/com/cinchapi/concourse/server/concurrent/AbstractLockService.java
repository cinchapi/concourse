/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.cinchapi.concourse.util.Logger;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Internally, Concourse uses various lock services to control concurrent
 * access to resources. In order to ensure high throughput, each of the lock
 * services provide dynamic locks for granular notions of things (i.e. records,
 * keys in records, ranges,
 * keys, etc).
 * <p>
 * This base class implements the bulk of the logic for concurrently dealing
 * with the locks in a secure way.
 * </p>
 * 
 * @author Jeff Nelson
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
            existing = MoreObjects.firstNonNull(existing, created);
        }
        existing.refs.incrementAndGet();
        L gced = null;
        if(existing.refs.get() <= 0
                || (gced = locks.putIfAbsent(token, existing)) != existing) { // Indicates
                                                                              // that
                                                                              // the
                                                                              // existing
                                                                              // lock
                                                                              // was
                                                                              // garbage
                                                                              // collected
            existing.refs.decrementAndGet();
            Logger.debug("Lock Service GC Race Condition: Expected "
                    + "{} but was {}", existing, gced);
            Thread.yield();
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
     * @author Jeff Nelson
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
