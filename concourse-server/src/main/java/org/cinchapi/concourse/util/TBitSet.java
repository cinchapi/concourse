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
package org.cinchapi.concourse.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.server.io.Bits;

import gnu.trove.map.TIntByteMap;
import gnu.trove.map.hash.TIntByteHashMap;

import com.google.common.base.Preconditions;

/**
 * A {@link TBitSet} is an optimized vector of bits that grows as needed. Each
 * component is indexed by a nonnegative integer and has a {@code boolean}
 * value, which is {@code false} by default.
 * <p>
 * TBitSet implements a subset of the functionality of a {@link BitSet} in a
 * more efficient manner. The Java BitSet has notable short falls including the
 * fact that it is not memory efficient (space is allocated from 0 to n, where n
 * is the highest bit ever seen in the bit set, even if most of the bits between
 * 0 and n are turned off).
 * </p>
 * <p>
 * TBitSet is a lot more space efficient because space is only allocated for
 * bits that are turned "on". A TBitSet also uses more efficient internal
 * collections than the native JDK ones, to help with performance.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
public class TBitSet {

    /**
     * Create a new {@link TBitSet}.
     * 
     * @return the TBitSet
     */
    public static TBitSet create() {
        return new TBitSet();
    }

    /**
     * Check to see if the position is valid, if not throw an
     * IllegalArgumentException.
     * 
     * @param position
     * @throws IllegalArgumentException
     */
    private static void checkPosition(int position)
            throws IllegalArgumentException {
        Preconditions.checkArgument(position >= 0);
    }

    /**
     * Return the "relative" index that indicates the appropriate bit position
     * in the word that is identified by {@link #getWordIndex(int)}.
     * 
     * @param position
     * @return
     */
    private static byte getRelativeIndex(int position) {
        return (byte) (position % NUM_BITS_PER_WORD);
    }

    /**
     * Return the "global" index that indicates the word where the bit at
     * {@code position} is stored.
     * 
     * @param position
     * @return the global index
     */
    private static int getWordIndex(int position) {
        return position / NUM_BITS_PER_WORD;
    }

    /**
     * The number of bits per word indicates how many bits are stored in each of
     * the values in {@link #data}. This number corresponds to the data type we
     * are storing as the value (i.e. a word expressed as a byte will store 8
     * bits whereas a word expressed as a long will store 64 bits, etc).
     */
    private static final byte NUM_BITS_PER_WORD = 8; // best to represent bits
                                                     // with a sequence of bytes
                                                     // (the smallest data type)
                                                     // since we store data
                                                     // lazily

    /**
     * A mapping from word position to word. For space efficiency, we only store
     * a word in this collection of it has at least 1 positive bit. So, if a bit
     * position maps to a word that is not in this collection then we know that
     * bit position is turned off.
     */
    private final TIntByteMap data;

    /**
     * The lock that controls concurrency.
     */
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();

    /**
     * Construct a new instance.
     * 
     * @param numBits
     */
    private TBitSet() {
        this.data = new TIntByteHashMap();
    }

    /**
     * Atomically check the bit at {@code position} and flip it the complement
     * of its current value if its current value is equal to {@code expected}.
     * 
     * @param position
     * @param expected
     * @return {@code true} if the comparison and flip succeeds, {@code false}
     *         otherwise
     */
    public boolean compareAndFlip(int position, boolean expected) {
        checkPosition(position);
        master.writeLock().lock();
        try {
            if(get(position) == expected) {
                set(position, !expected);
                return true;
            }
            else {
                return false;
            }
        }
        finally {
            master.writeLock().unlock();
        }
    }

    /**
     * Set the bit at {@code position} to the complement of its current value.
     * 
     * @param index
     */
    public void flip(int position) {
        checkPosition(position);
        master.writeLock().lock();
        try {
            set(position, !get(position));
        }
        finally {
            master.writeLock().unlock();
        }
    }

    /**
     * Return the value of the bit at {@code position}.
     * 
     * @param position
     * @return the bit value
     */
    public boolean get(int position) {
        checkPosition(position);
        master.readLock().lock();
        try {
            int global = getWordIndex(position);
            if(data.containsKey(global)) {
                byte relative = getRelativeIndex(position);
                return Bits.get(relative, data.get(global));
            }
            else {
                return false;
            }
        }
        finally {
            master.readLock().unlock();
        }
    }

    /**
     * Set the bit at {@code position} to {@code value}.
     * 
     * @param position
     * @param value
     */
    public void set(int position, boolean value) {
        checkPosition(position);
        master.writeLock().lock();
        try {
            int global = getWordIndex(position);
            byte relative = getRelativeIndex(position);
            byte word = data.get(global);
            word = Bits.get(relative, word) != value ? Bits
                    .flip(relative, word) : word;
            if(word == 0) {
                data.remove(global);
            }
            else {
                data.put(global, word);
            }
        }
        finally {
            master.writeLock().unlock();
        }

    }

}
