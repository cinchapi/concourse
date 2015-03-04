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
package org.cinchapi.concourse.util;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Sets;

/**
 * <p>
 * Similar to a {@link BitSet java.util.BitSet}. The only difference is that its
 * keys are {@code} long instead of {@code int}. There is also support for
 * negative indexes.
 * </p>
 * <p>
 * The {@link LongBitSet} is backed by Map<Long, BitSet>. The idea is to use
 * high bits of a key as a key to this map and low bits as a key to a value
 * BitSet. Thus we will support both partitioning and long keys. Internally,
 * each partition can hold about a million entries, which means that they
 * require no more than 128K, keeping total size of a {@link LongBitSet} rather
 * small.
 * </p>
 * <p>
 * <strong>NOTE:</strong> Implementation and documentation adapted from <a
 * href="http://java-performance.info/bit-sets/"
 * >http://java-performance.info/bit-sets/</a>
 * </p>
 * 
 * @author Mikhail Vorontsov
 * 
 */
@NotThreadSafe
public class LongBitSet {

    /**
     * Return a new {@link LongBitSet}.
     * 
     * @return the LongBitSet
     */
    public static LongBitSet create() {
        return new LongBitSet();
    }

    /**
     * Number of bits allocated to a value in an index
     */
    private static final int VALUE_BITS = 20; // 1M values per bit set

    /**
     * Mask for extracting values
     */
    private static final long VALUE_MASK = (1 << VALUE_BITS) - 1;

    /**
     * Map from a value stored in high bits of a long index to a bit set mapped
     * to the lower bits of an index.
     * Bit sets size should be balanced - not to long (otherwise setting a
     * single bit may waste megabytes of memory)
     * but not too short (otherwise this map will get too big). Update value of
     * {@code VALUE_BITS} for your needs.
     * In most cases it is ok to keep 1M - 64M values in a bit set, so each bit
     * set will occupy 128Kb - 8Mb.
     */
    private Map<Long, BitSet> m_sets = new HashMap<Long, BitSet>(20);

    /**
     * Return {@code true} if this set contains an element at {@code index}.
     * 
     * @param index
     * @return the value of {@link #get(long)}.
     */
    public boolean contains(long index) {
        return get(index);
    }

    /**
     * Get a value for a given index
     * 
     * @param index Long index
     * @return Value associated with a given index
     */
    public boolean get(final long index) {
        final BitSet bitSet = m_sets.get(getSetIndex(index));
        return bitSet != null && bitSet.get(getPos(index));
    }

    /**
     * Return an iterator that will traverse all the elements in the set (e.g.
     * all the bits that are turned on).
     * 
     * @return the Iterator
     */
    public Iterator<Long> iterator() {
        return toIterable().iterator();
    }

    /**
     * Set the bit at {@code index} to {@code true}, if it is currently
     * {@code false}.
     * 
     * @param index
     * @return {@code true} if this operation results in a change to the
     *         underlying bit set (e.g. the bit was previously turned off and is
     *         not turned on), {@code false} otherwise
     */
    public boolean set(final long index) {
        BitSet bs = getBitSet(index);
        int pos = getPos(index);
        if(!bs.get(pos)) {
            bs.set(pos, true);
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Set a given value for a given index
     * 
     * @param index Long index
     * @param value Value to set
     */
    public void set(final long index, final boolean value) {
        if(value) {
            getBitSet(index).set(getPos(index), value);
        }
        else { // if value shall be cleared, check first if given partition
               // exists
            final BitSet bitSet = m_sets.get(getSetIndex(index));
            if(bitSet != null)
                bitSet.clear(getPos(index));
        }
    }

    /**
     * Helper method to get (or create, if necessary) a bit set for a given long
     * index
     * 
     * @param index Long index
     * @return A bit set for a given index (always not null)
     */
    private BitSet getBitSet(final long index) {
        final Long iIndex = getSetIndex(index);
        BitSet bitSet = m_sets.get(iIndex);
        if(bitSet == null) {
            bitSet = new BitSet(1024);
            m_sets.put(iIndex, bitSet);
        }
        return bitSet;
    }

    /**
     * Get index of a value in a bit set (bits 0-19)
     * 
     * @param index Long index
     * @return Index of a value in a bit set
     */
    private int getPos(final long index) {
        return (int) (index & VALUE_MASK);
    }

    /**
     * Get set index by long index (extract bits 20-63)
     * 
     * @param index Long index
     * @return Index of a bit set in the inner map
     */
    private long getSetIndex(final long index) {
        return index >> VALUE_BITS;
    }

    /**
     * Convert the bit set to an iterable that can be used to return an
     * iterator.
     * 
     * @return the iterable
     */
    public Iterable<Long> toIterable() {
        Set<Long> collection = Sets.newTreeSet();
        for (final Map.Entry<Long, BitSet> entry : m_sets.entrySet()) {
            final BitSet bs = entry.getValue();
            final long baseIndex = entry.getKey() << VALUE_BITS;
            for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
                collection.add(baseIndex + i);
            }
        }
        return collection;
    }
}
