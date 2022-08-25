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
package com.cinchapi.concourse.collect;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A concurrent {@link Set} implementation that stores data across multiple
 * shards.
 * <p>
 * A {@link ShardedHashSet} is specifically designed to allow concurrent
 * modifications during iteration while maintaining strong consistency. In
 * particular, it is possible to ensure that usage of the
 * {@link #concurrentIterator() collection's iterator} while concurrently
 * {@link #add(Object) adding} and {@link #remove(Object) removing} elements
 * will result in the {@link CloseableIterator iterator} seeing every data
 * modification that successfully completed while the iteration was in progress.
 * </p>
 * <p>
 * If a thread is iterating a {@link ShardedHashSet} and a second thread tries
 * to modify data that either was already or <em>would have already been</em>
 * seen by the first thread's iterator, the second thread will be blocked until
 * the first thread's iterator is {@link Iterators#close(Iterator) closed}. If
 * the second thread tries to modify that that was not already or <em>would not
 * have already been</em> seen by the first thread's iterator, the second thread
 * won't be blocked and the first thread's iterator will eventually see second
 * thread's modification.
 * </p>
 * <p>
 * Because of a {@link ShardedHashSet} uses internal locks and the duration of
 * iteration is unknown, it is essential to call
 * {@link Iterators#close(Iterator)} on the values returned from
 * {@link #iterator()} and {@link #concurrentIterator()}.
 * </p>
 *
 * @author Jeff Nelson
 */
@ThreadSafe
public final class ShardedHashSet<V> extends AbstractSet<V> {

    /**
     * The default number of shards.
     */
    private static final int DEFAULT_NUMBER_OF_SHARDS = 16;

    /**
     * The shards that store data.
     */
    private final List<Shard> shards;

    /**
     * Construct a new instance.
     * 
     * @param factory
     */
    public ShardedHashSet(Supplier<Set<V>> factory) {
        this(factory, DEFAULT_NUMBER_OF_SHARDS);
    }

    /**
     * Construct a new instance.
     * 
     * @param factory
     * @param shards
     */
    public ShardedHashSet(Supplier<Set<V>> factory, int shards) {
        this.shards = new ArrayList<>(shards);
        for (int i = 0; i < shards; ++i) {
            this.shards.add(new Shard(factory.get()));
        }
    }

    /**
     * Construct a new instance.
     */
    public ShardedHashSet() {
        this(HashSet::new);
    }

    @Override
    public boolean add(V e) {
        int index = Math.abs(e.hashCode() % shards.size());
        Shard shard = shards.get(index);
        shard.lock.writeLock().lock();;
        try {
            return shard.data.add(e);
        }
        finally {
            shard.lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        Iterator<Shard> it = shards.iterator();
        List<ReadWriteLock> locks = new ArrayList<>(shards.size());
        try {
            while (it.hasNext()) {
                Shard shard = it.next();
                shard.lock.writeLock().lock();
                locks.add(shard.lock);
                shard.data.clear();
            }
        }
        finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Return a {@link CloseableIterator} that provides strong consistency when
     * there are concurrent modifications.
     * 
     * @return the {CloseableIterator iterator}
     */
    public CloseableIterator<V> concurrentIterator() {
        Iterator<Shard> shardIt = shards.iterator();

        return new CloseableIterator<V>() {

            private boolean verified = false;
            private Iterator<V> it;
            private final List<ReadWriteLock> locks = new ArrayList<>(
                    shards.size());

            {
                rotate();
            }

            @Override
            public void close() throws IOException {
                Iterator<ReadWriteLock> it = locks.iterator();
                while (it.hasNext()) {
                    it.next().readLock().unlock();
                    it.remove();
                }
            }

            @Override
            public boolean hasNext() {
                verified = true;
                if(it == null) {
                    return false;
                }
                if(it.hasNext()) {
                    return true;
                }
                else {
                    rotate();
                    return hasNext();
                }
            }

            @Override
            public V next() {
                if(!verified) {
                    hasNext();
                }
                verified = false;
                if(it == null) {
                    throw new NoSuchElementException();
                }
                else {
                    return it.next();
                }
            }

            @Override
            public void remove() {
                // NOTE: This implementation violates strong consistency. Since
                // only the read lock has been grabbed, it is possible for
                // an iterator to remove an element that was previously seen by
                // another still in-process iterator.
                //
                // This is acceptable for the TokenEventAnnouncer use case of
                // the storage Engine because there is no harm in a committing
                // write from one client causing the concurrent removal of a
                // TokenEventObserver that previously observed a write from
                // another committing client.
                it.remove();
            }

            @Override
            protected void finalize() throws Throwable {
                close();
            }

            private final void rotate() {
                if(shardIt.hasNext()) {
                    Shard shard = shardIt.next();
                    shard.lock.readLock().lock();
                    locks.add(shard.lock);
                    it = shard.data.iterator();
                }
                else {
                    it = null;
                }
            }

        };
    }

    @Override
    public boolean contains(Object o) {
        int index = Math.abs(o.hashCode() % shards.size());
        Shard shard = shards.get(index);
        shard.lock.readLock().lock();
        try {
            return shard.data.contains(o);
        }
        finally {
            shard.lock.readLock().unlock();
        }
    }

    @Override
    public boolean equals(Object o) {
        try {
            return super.equals(o);
        }
        finally {
            for (Shard shard : shards) {
                shard.lock.readLock().unlock();
            }
        }

    }

    @Override
    public int hashCode() {
        try {
            return super.hashCode();
        }
        finally {
            for (Shard shard : shards) {
                shard.lock.readLock().unlock();
            }
        }
    }

    @Override
    public boolean isEmpty() {
        Iterator<Shard> it = shards.iterator();
        List<ReadWriteLock> locks = new ArrayList<>(shards.size());
        try {
            while (it.hasNext()) {
                Shard shard = it.next();
                shard.lock.readLock().lock();
                locks.add(shard.lock);
                if(!shard.data.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
        finally {
            for (ReadWriteLock lock : locks) {
                lock.readLock().unlock();
            }
        }
    }

    @Override
    public Iterator<V> iterator() {
        return concurrentIterator();
    }

    @Override
    public boolean remove(Object o) {
        int index = o.hashCode() % shards.size();
        Shard shard = shards.get(index);
        shard.lock.writeLock().lock();;
        try {
            return shard.data.remove(o);
        }
        finally {
            shard.lock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        int size = 0;
        CloseableIterator<V> it = concurrentIterator();
        try {
            while (it.hasNext()) {
                it.next();
                ++size;
            }
            return size;
        }
        finally {
            Iterators.close(it);
        }

    }

    @Override
    public String toString() {
        try {
            return super.toString();
        }
        finally {
            for (Shard shard : shards) {
                shard.lock.readLock().unlock();
            }
        }

    }

    /**
     * A {@link Shard} that stores data.
     *
     * @author Jeff Nelson
     */
    private class Shard {

        /**
         * The stored data.
         */
        final Set<V> data;

        /**
         * The shard's lock
         */
        final ReadWriteLock lock;

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        Shard(Set<V> data) {
            this.data = data;
            this.lock = new ReentrantReadWriteLock();
        }

    }

}
