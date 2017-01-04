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
package com.cinchapi.concourse.server.storage.db;

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.server.io.Byteable;
import com.google.common.collect.Maps;

/**
 * A {@link Record} that can return a browsable view of its data in the present
 * or a historical state.
 * 
 * @author Jeff Nelson
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
