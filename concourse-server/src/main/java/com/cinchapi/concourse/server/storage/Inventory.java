/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Integers;
import com.cinchapi.concourse.util.LongBitSet;
import com.google.common.collect.Lists;

/**
 * The {@link Inventory} is a persistent collection of longs that represents a
 * listing of all the records that exist within an environment (e.g. have data
 * or a history of data).
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class Inventory {

    /**
     * Return a new {@link Inventory} that is persisted to the
     * {@code backingStore} in calls to {@link #sync()}.
     * 
     * @param backingStore
     * @return the Inventory
     */
    public static Inventory create(String backingStore) {
        return new Inventory(backingStore);
    }

    /**
     * The amount of memory and disk space allocated each time we need to extend
     * the backing store. This value is rounded up to the nearest power of two
     * for the most efficient I/O.
     */
    private static final int MEMORY_MAPPING_SIZE = 8 * 20000;

    /**
     * The location where the inventory is stored on disk.
     */
    private final String backingStore;

    /**
     * The bitset that contains the read-efficient version of the data in the
     * inventory.
     */
    private final LongBitSet bitSet;

    /**
     * A memory mapped buffer that is used to handle writes to the backing
     * store.
     */
    private MappedByteBuffer content;

    /**
     * A collection of dirty writes that have not been synced to disk yet.
     */
    protected final transient List<Long> dirty = Lists
            .newArrayListWithExpectedSize(1); // visible for testing

    /**
     * Concurrency control
     */
    private final StampedLock lock = new StampedLock();

    /**
     * Construct a new instance. If the {@code backingStore} has data then it
     * will be read into memory.
     * 
     * @param backingStore
     */
    private Inventory(String backingStore) {
        this.backingStore = backingStore;
        this.bitSet = LongBitSet.create();
        this.content = FileSystem.map(backingStore, MapMode.READ_ONLY, 0,
                FileSystem.getFileSize(backingStore));
        while (content.position() < content.capacity()) {
            long record = content.getLong();
            if(record == 0 && content.getLong() == 0) { // if there is a null
                                                        // (0) record, check to
                                                        // see if the next one
                                                        // is also null which
                                                        // will tell us we've
                                                        // read too far
                content.position(content.position() - 16);
                break;
            }
            else {
                bitSet.set(record);
            }
        }
        map0(content.position(), MEMORY_MAPPING_SIZE);
    }

    /**
     * Add an {@code record} to the inventory.
     * 
     * @param record
     */
    public void add(long record) {
        long stamp = lock.writeLock();
        try {
            if(bitSet.set(record)) {
                dirty.add(record);
            }
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Return {@code true} if the {@code record} exists within the inventory.
     * 
     * @param record
     * @return {@code true} if the record is contained
     */
    public boolean contains(long record) {
        long stamp = lock.tryOptimisticRead();
        boolean result = bitSet.get(record);
        if(lock.validate(stamp)) {
            return result;
        }
        else {
            stamp = lock.readLock();
            try {
                return bitSet.get(record);
            }
            finally {
                lock.unlockRead(stamp);
            }
        }
    }

    /**
     * Return {@code Set<Long>} if records that ever had data exist.
     * 
     * @return {@code Set<Long>}
     */
    public Set<Long> getAll() {
        return (Set<Long>) bitSet.toIterable();
    }

    /**
     * Perform an fsync and flush any dirty writes to disk.
     */
    public void sync() {
        long stamp = lock.writeLock();
        try {
            if(!dirty.isEmpty()) {
                Iterator<Long> it = dirty.iterator();
                while (it.hasNext()) {
                    if(content.remaining() < 8) {
                        map0(content.position(), content.position() * 2);
                    }
                    content.putLong(it.next());
                }
                content.force();
                dirty.clear();
            }
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Map the {@link #backingStore} in memory for the specified
     * {@code position} for at least {@code length} bytes.
     * 
     * @param position
     * @param length
     */
    private void map0(long position, int length) {
        FileSystem.unmap(content);
        content = FileSystem.map(backingStore, MapMode.READ_WRITE, position,
                Integers.nextPowerOfTwo(length));
    }
}
