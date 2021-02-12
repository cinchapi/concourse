/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import java.util.Set;

import com.cinchapi.concourse.thrift.TObject;

/**
 * A {@link Store} that can indirectly {@link #gather(String, long) gather}
 * values that are stored for a {@code key} in a {@code record}.
 *
 * @author Jeff Nelson
 */
public interface Gatherable extends Store {

    /**
     * Gather the values that are stored for {@code key} in {@code record}.
     * <p>
     * This method is slightly similar to {@link #select(String, long)}. All the
     * values that would be returned from the {@link #select(String, long)
     * select} method are returned here, but the order of the values returned
     * from this method are not necessarily in insertion order.
     * </p>
     * <p>
     * This performance of this method for a single gather is not likely to be
     * better than the performance of a single {@link #select(String, long)
     * selection}; especially, if a normalized index for the record in which the
     * key is held in memory. On the other hand, this method may perform better
     * when gathering one or a few keys across <strong>many</strong> records.
     * </p>
     * 
     * @param key
     * @param record
     * @return a possibly empty Set of values
     */
    public default Set<TObject> gather(String key, long record) {
        return select(key, record);
    }

    /**
     * Gather the values that are stored for {@code key} in {@code record} at
     * {@code timestamp}.
     * <p>
     * This method is slightly similar to {@link #select(String, long, long)}.
     * All the values that would be returned from the
     * {@link #select(String, long) select} method are returned here, but the
     * order of the values returned from this method are not necessarily in
     * insertion order.
     * </p>
     * <p>
     * This performance of this method for a single gather is not likely to be
     * better than the performance of a single
     * {@link #select(String, long, long) selection}; especially, if a
     * normalized index for the record in which the key is held in memory. On
     * the other hand, this method may perform better when gathering one or a
     * few keys across <strong>many</strong> records.
     * </p>
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return a possibly empty Set of values
     */
    public default Set<TObject> gather(String key, long record,
            long timestamp) {
        return select(key, record, timestamp);
    }

}
