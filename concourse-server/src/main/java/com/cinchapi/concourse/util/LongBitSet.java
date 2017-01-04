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
package com.cinchapi.concourse.util;

import java.util.AbstractSet;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.util.ReadOnlyIterator;

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
        return new InternalIterator();
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
     * Convert the bit set to an iterable that can be used to return an
     * iterator.
     * 
     * @return the iterable
     */
    public Iterable<Long> toIterable() {
        return new InternalSetView();
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
     * The {@link Iterator} returned from the {@link #iterator()} method.
     * 
     * @author Jeff Nelson
     */
    private class InternalIterator extends ReadOnlyIterator<Long> {

        private long baseIndex = 0;
        private Iterator<Map.Entry<Long, BitSet>> bitIt = m_sets.entrySet()
                .iterator();
        private BitSet bitSet = null;
        private int position = -1;
        {
            flip();
        }

        @Override
        public boolean hasNext() {
            if(position >= 0) {
                return true;
            }
            else {
                flip();
                return position >= 0;
            }
        }

        @Override
        public Long next() {
            if(position >= 0) {
                long next = position + baseIndex;
                position = bitSet.nextSetBit(position + 1);
                return next;
            }
            else {
                return -1L;
            }
        }

        /**
         * Flip to the next internal {@link BitSet} and establish the
         * {@link #position} of the next set bit.
         */
        private void flip() {
            while (bitIt.hasNext() && position < 0) {
                Map.Entry<Long, BitSet> entry = bitIt.next();
                baseIndex = entry.getKey() << VALUE_BITS;
                bitSet = entry.getValue();
                position = bitSet.nextSetBit(0);
            }
        }

    }

    /**
     * A {@link Set} that lazily loads elements from the {@link #iterator()}
     * within this LongBitSet.
     * 
     * @author Jeff Nelson
     */
    @Immutable
    private class InternalSetView extends AbstractSet<Long> {

        /**
         * A flag that indicates if the Set is empty upon creation.
         */
        private final boolean empty;

        /**
         * A cache of the size. We can only compute the size by iterating
         * through all the elements, so this is lazily computed and cached so
         * that this work is only done once, if necessary.
         */
        private int size = 0;

        /**
         * Construct a new instance.
         * 
         * @param iterator
         */
        private InternalSetView() {
            this.empty = !iterator().hasNext();
        }

        @Override
        public Iterator<Long> iterator() {
            return LongBitSet.this.iterator();
        }

        @Override
        public int size() {
            if(size == 0 && !empty) {
                Iterator<Long> it = iterator();
                while (it.hasNext()) {
                    it.next();
                    ++size;
                }
            }
            return size;
        }

        @Override
        public boolean isEmpty() {
            return empty;
        }

    }
}
