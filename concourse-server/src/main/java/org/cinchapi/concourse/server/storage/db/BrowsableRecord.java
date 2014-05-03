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
package org.cinchapi.concourse.server.storage.db;

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.server.io.Byteable;

import com.google.common.collect.Maps;

/**
 * A {@link Record} that can return a browsable view of its data in the present
 * or a historical state.
 * 
 * @author jnelson
 */
abstract class BrowsableRecord<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends Record<L, K, V> {

    /**
     * Construct a new instance.
     * 
     * @param locator
     * @param key
     */
    protected BrowsableRecord(L locator, K key) {
        super(locator, key);
    }

    /**
     * Return a view of all the data that is presently contained in this record.
     * 
     * @return the data
     */
    public Map<K, Set<V>> browse() {
        read.lock();
        try {
            Map<K, Set<V>> data = Maps.newLinkedHashMap();
            for (K key : describe()) {
                data.put(key, get(key));
            }
            return data;
        }
        finally {
            read.unlock();
        }

    }

    /**
     * Return a view of all the data that was contained in this record at
     * {@code timestamp}.
     * 
     * @param timestamp
     * @return the data
     */
    public Map<K, Set<V>> browse(long timestamp) {
        read.lock();
        try {
            Map<K, Set<V>> data = Maps.newLinkedHashMap();
            for (K key : describe(timestamp)) {
                data.put(key, get(key, timestamp));
            }
            return data;
        }
        finally {
            read.unlock();
        }
    }

}
