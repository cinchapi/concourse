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
package org.cinchapi.common.util;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * A {@link NonBlockingRangeMap} is one that focuses on high throughput in the
 * face of concurrent access.
 * 
 * @author jnelson
 */
public class NonBlockingRangeMap<K extends Comparable<K>, V> implements
        RangeMap<K, V> {

    /**
     * Return a new instance of a {@link NonBlockingRangeMap}.
     * 
     * @return the NonBlockingRangeMap
     */
    public static <K extends Comparable<K>, V> NonBlockingRangeMap<K, V> create() {
        return new NonBlockingRangeMap<K, V>();
    }

    /**
     * This class does not contain a particularly efficient implementation of a
     * RangeMap. Data is stored in this collection. We merely store a mapping
     * from every added range to the associated value. If a range is added, that
     * overlaps with a previously stored range, we don't do any work to expand
     * the previously stored range. Instead, we just store the new range in a
     * new entry. This means that all operations are O(n) with respect to the
     * number of distinct range/value mappings inserted.
     */
    private final NonBlockingHashMultimap<Range<K>, V> data = NonBlockingHashMultimap.create();

    @Override
    public boolean contains(K point) {
        return contains(Range.point(point));
    }

    @Override
    public boolean contains(Range<K> range) {
        for (Range<K> stored : data.keySet()) {
            if(stored.intersects(range)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean put(Range<K> range, V value) {
        return data.put(range, value);
    }

    @Override
    public boolean remove(Range<K> range, V value) {
        boolean changed = false;
        for (Range<K> stored : data.keySet()) {
            if(stored.intersects(range) && data.get(stored).contains(value)) {
                Set<Range<K>> xor = stored.xor(range);
                for (Range<K> r : xor) {
                    if(stored.contains(r)) {
                        data.put(r, value);
                    }
                }
                changed = data.remove(stored, value) ? true : changed;
            }
        }
        return changed;
    }

    @Override
    public Set<V> get(K point) {
        return get(Range.point(point));
    }

    @Override
    public Set<V> get(Range<K> range) {
        Set<V> values = Sets.newHashSet();
        for (Range<K> stored : data.keySet()) {
            if(stored.intersects(range)) {
                values.addAll(data.get(stored));
            }
        }
        return values;
    }

    private NonBlockingRangeMap() {/* noop */}

}
