/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.cache;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.Composite;
import org.cinchapi.concourse.server.io.MappedBitSet;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.hash.Hashing;

/**
 * A memory mapped implementation of a Bloom Filter, which is a space efficient
 * probabilistic data structure that is used to test whether an element is a
 * member of a set. False positive matches are possible, but false negatives are
 * not.
 * <p>
 * The memory mapped implementation is intended for cases where it is necessary
 * to have a persistent bloom filter that is consistently updated. The fact that
 * the bloom filter is memory mapped ensures that updates to the filter happen
 * in an efficient manner and are guaranteed to be stored to disk immediately.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
public class MappedBloomFilter {

    /**
     * Create a new {@link MappedBloomFilter} that is backed by {@code file} and
     * has enough capacity to handle the number of {@code expectedInsertions}
     * with the specified false positive probability ({@code fpp}).
     * 
     * @param file
     * @param expectedInsertions
     * @param fpp
     * @return the MappedBloomFilter
     */
    public static MappedBloomFilter create(String file, int expectedInsertions,
            double fpp) {
        int numBits = getNumBits(expectedInsertions, fpp);
        return new MappedBloomFilter(file, numBits, getNumHashFunctions(
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
     * The backing bit set that contains the data in the bloom filter.
     */
    private final MappedBitSet bitSet;

    /**
     * The ideal number of hash functions to use in the bloom filter.
     */
    private final int numHashFunctions;

    private final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    /**
     * Construct a new instance.
     * 
     * @param file
     * @param numBits
     * @param numHashFunctions
     */
    public MappedBloomFilter(String file, int numBits, int numHashFunctions) {
        this.bitSet = MappedBitSet.create(file, numBits);
        this.numHashFunctions = numHashFunctions;
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
            long[] hashes = hash(Composite.create(byteables));
            boolean bitsChanged = true;
            for (long hash : hashes) {
                bitsChanged = bitSet.compareAndFlip(hash, false) ? true
                        : bitsChanged;
            }
            return bitsChanged;
        }
        finally {
            masterLock.writeLock().unlock();
        }
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
            long[] hashes = hash(Composite.create(byteables));
            for (long hash : hashes) {
                if(!bitSet.get(hash)) {
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
     * Return the hashes for the {@code composite} object based on the ideal
     * {@link #numHashFunctions} and the size of the underlying {@link #bitSet}.
     * 
     * @param composite
     * @return the hashes for {@code composite}.
     */
    private long[] hash(Composite composite) {
        long hash64 = Hashing.murmur3_128()
                .hashBytes(ByteBuffers.toByteArray(composite.getBytes()))
                .asLong();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        long[] hashes = new long[numHashFunctions];
        for (int i = 1; i <= numHashFunctions; i++) {
            hashes[i - 1] = Math.abs((hash1 + i * hash2) % bitSet.size());
        }
        return hashes;
    }

}
