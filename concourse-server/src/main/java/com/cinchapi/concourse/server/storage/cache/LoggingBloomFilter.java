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
package com.cinchapi.concourse.server.storage.cache;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.LongBitSet;
import com.google.common.hash.Hashing;

/**
 * A {@link LoggingBloomFilter} is one that uses append-only logging for
 * serialization.
 * <p>
 * In this Bloom Filter, all changes to the internal bit set are periodically
 * synced to a file on disk. This allows the filter to serialize its state in a
 * dynamic fashion that affords optimal performance and high throughput. Disk
 * writes only happen when an external caller invokes the {@link #diskSync()}
 * method and there are relevant changes to record.
 * </p>
 * <p>
 * A LoggingBloomFilter should be used instead of a regular {@link BloomFilter}
 * when writes will continue to occur after the filter has been synced to disk.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class LoggingBloomFilter {

    /**
     * Create a new {@link LoggingBloomFilter} that is backed by {@code file}
     * and has enough capacity to handle the number of
     * {@code expectedInsertions} with the specified false positive probability
     * ({@code fpp}).
     * 
     * @param file
     * @param expectedInsertions
     * @param fpp
     * @return the LoggingBloomFilter
     */
    public static LoggingBloomFilter create(String file,
            int expectedInsertions, double fpp) {
        int numBits = getNumBits(expectedInsertions, fpp);
        return new LoggingBloomFilter(file, numBits, getNumHashFunctions(
                expectedInsertions, numBits));
    }

    /**
     * Given the number of {@code expectedInsertions} and the acceptable false
     * positive probability ({@code fpp}), return the number of bits necessary
     * to include in the bloom filter.
     * <p>
     * <em>Math courtesy of http://hur.st/bloomfilter</em>
     * </p>
     * 
     * @param expectedInsertions
     * @param fpp
     * @return the number of bits to use
     */
    private static int getNumBits(int expectedInsertions, double fpp) {
        if(fpp == 0) {
            fpp = Double.MIN_VALUE;
        }
        return (int) Math.ceil((expectedInsertions * Math.log(fpp))
                / Math.log(1 / Math.pow(2, Math.log(2))));
    }

    /**
     * Given the number of {@code expectedInsertions} and the {@code numBits} in
     * the bloom filter, return the number of ideal hash functions.
     * <p>
     * Use {@link #getNumBits(int, double)} to determine the number of bits that
     * are necessary to use in the bloom filter.
     * </p>
     * <p>
     * <em>Math courtesy of http://hur.st/bloomfilter</em>
     * </p>
     * 
     * @param expectedInsertions
     * @param numBits
     * @return the ideal number of hash function
     */
    private static int getNumHashFunctions(int expectedInsertions, int numBits) {
        return (int) Math.round(Math.log(2) * (numBits / expectedInsertions));
    }

    /**
     * The internal bit set that holds the boom filter's state.
     */
    private final LongBitSet bits;

    /**
     * The buffer that records recent changes to the state of the {@link #bits}
     * set.
     */
    private ByteBuffer buffer;

    /**
     * The backing file. We write to this periodically in an append-only
     * fashion.
     */
    private final String file;

    /**
     * The ideal number of hash functions to use in the bloom filter.
     */
    private final int numHashFunctions;

    /**
     * A lock for concurrency control.
     */
    private final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    /**
     * The position where we should begin appending data in the backing
     * {@link #file}.
     */
    private int position;

    /**
     * Since {@link #buffer} is intentionally oversized, we record the actual
     * length of the recent changes so we know how much data to append to disk
     * when {@link #diskSync()} is invoked.
     */
    private int lengthOfRecentChanges;

    private int numBits;

    /**
     * Construct a new instance.
     * 
     * @param directory
     * @param numBits
     * @param numHashFunctions
     */
    private LoggingBloomFilter(String file, int numBits, int numHashFunctions) {
        this.bits = LongBitSet.create();
        this.numBits = numBits;
        this.numHashFunctions = numHashFunctions;
        this.file = file;
        diskSyncCleanup();
        if(position > 0) { // load the existing changes into memory
            ByteBuffer bytes = FileSystem.readBytes(file);
            while (bytes.position() < position) {
                bits.set(bytes.getInt(), true);
            }
        }
    }

    /**
     * Force a sync of the recent changes to this bloom filter to disk. This
     * method should be called in conjunction with the page turning
     * functionality in the {@link Buffer}.
     */
    public void diskSync() {
        MappedByteBuffer data = FileSystem.map(file, MapMode.READ_WRITE,
                position, lengthOfRecentChanges);
        data.put(ByteBuffers.slice(buffer, 0, lengthOfRecentChanges));
        data.force();
        diskSyncCleanup();
    }

    /**
     * Return {@code true} if it is possible that the {@code byteables} have
     * been placed in this bloom filter. Return {@code false} if they have
     * definitely not been placed in this bloom filter.
     * 
     * @param byteables
     * @return {@code true} if the filter <em>might</em> contain the
     *         {@code byteables}
     */
    public boolean mightContain(Byteable... byteables) {
        masterLock.readLock().lock();
        try {
            int[] hashes = hash(Composite.create(byteables));
            for (int hash : hashes) {
                if(!bits.get(hash)) {
                    return false;
                }
            }
            return true;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Put the {@code byteables} into this bloom filter and ensure subsequent
     * invocations of {@link #mightContain(Byteable...)} with the same items
     * will always return {@code true}.
     * 
     * @param byteables
     * @return {@code true} if the bloom filter's bits have changed as a result
     *         of this operation. If the bits changed, this is definitely the
     *         first time the {@code byteables} have been added to this filter.
     *         If the bits haven't changed, this might be the first time the
     *         items have been added. Please note that this method always
     *         returns the opposite of what {@link #mightContain(Byteable...)}
     *         would have returned at the same time
     */
    public boolean put(Byteable... byteables) {
        masterLock.writeLock().lock();
        try {
            int[] hashes = hash(Composite.create(byteables));
            boolean bitsChanged = true;
            for (int hash : hashes) {
                if(!bits.get(hash)) {
                    bits.set(hash);
                    bitsChanged = true;
                    buffer.putInt(hash);
                    lengthOfRecentChanges += 4;
                }
            }
            return bitsChanged;
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    /**
     * Cleanup the in-memory meta data during initialization or after a call to
     * {@link #diskSync()}.
     */
    private void diskSyncCleanup() {
        // We allocate a ByteBuffer that is equal to the BUFFER_PAGE_SIZE so
        // that we can be sure that we'll have enough space to store all the
        // possible changes before the Buffer calls #diskSync() in conjunction
        // with adding a new page.
        buffer = ByteBuffer.allocate(GlobalState.BUFFER_PAGE_SIZE);
        lengthOfRecentChanges = 0;
        position = (int) FileSystem.getFileSize(file);
    }

    /**
     * Return the hashes for the {@code composite} object based on the ideal
     * {@link #numHashFunctions} and the size of the underlying {@link #bitSet}.
     * 
     * @param composite
     * @return the hashes for {@code composite}.
     */
    private int[] hash(Composite composite) {
        long hash64 = Hashing.murmur3_128()
                .hashBytes(ByteBuffers.toByteArray(composite.getBytes()))
                .asLong();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        int[] hashes = new int[numHashFunctions];
        for (int i = 1; i <= numHashFunctions; ++i) {
            hashes[i - 1] = Math.abs((hash1 + i * hash2) % numBits);
        }
        return hashes;
    }

}
