/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query.sort;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.data.sort.Sorter;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.cinchapi.concourse.server.storage.Store;

/**
 * A {@link StoreSorter} imposes an {@link Order} by value on a result set. The
 * values on which the sorting is based don't have to be part of the result set.
 * In this case, the {@link StoreSorter} uses a {@link Store} to lookup the
 * values.
 *
 * @author Jeff Nelson
 */
abstract class StoreSorter<V> implements Sorter<V> {

    /**
     * The {@link Order} to impose.
     */
    private final Order order;

    /**
     * The {@link Store} from which to retrieve values if those values are not
     * in the result set being sorted.
     */
    protected final Store store;

    /**
     * Construct a new instance.
     * 
     * @param order
     * @param store
     */
    protected StoreSorter(Order order, Store store) {
        this.order = order;
        this.store = store;
    }

    @Override
    public final Stream<Entry<Long, Map<String, V>>> sort(
            Map<Long, Map<String, V>> data) {
        return sort(data, null);
    }

    @Override
    public final Stream<Entry<Long, Map<String, V>>> sort(
            Map<Long, Map<String, V>> data, @Nullable Long at) {
        ArrayBuilder<Comparator<Entry<Long, Map<String, V>>>> comparators = ArrayBuilder
                .builder();
        for (OrderComponent component : order.spec()) {
            String key = component.key();
            Timestamp timestamp = component.timestamp();
            Direction direction = component.direction();
            Comparator<Entry<Long, Map<String, V>>> $comparator = (e1, e2) -> {
                long r1 = e1.getKey();
                long r2 = e2.getKey();
                Map<String, V> d1 = e1.getValue();
                Map<String, V> d2 = e2.getValue();
                V v1;
                V v2;
                if(timestamp == null) {
                    v1 = d1.get(key);
                    if(v1 == null) {
                        v1 = at != null ? lookup(key, r1, at) : lookup(key, r1);
                    }
                    v2 = d2.get(key);
                    if(v2 == null) {
                        v2 = at != null ? lookup(key, r2, at) : lookup(key, r2);
                    }
                }
                else {
                    v1 = lookup(key, r1, timestamp.getMicros());
                    v2 = lookup(key, r2, timestamp.getMicros());
                }
                if(!Empty.ness().describes(v1) && !Empty.ness().describes(v2)) {
                    // The coefficient is only applied when both values are
                    // non-empty. Otherwise, the empty value should float to the
                    // end of the sort, regardless of the specified direction
                    return direction.coefficient() * compare(v1, v2);
                }
                else if(!Empty.ness().describes(v1)) {
                    return -1;
                }
                else if(!Empty.ness().describes(v2)) {
                    return 1;
                }
                else {
                    return 0;
                }

            };
            comparators.add($comparator);
        }
        comparators.add((e1, e2) -> e1.getKey().compareTo(e2.getKey()));
        Comparator<Entry<Long, Map<String, V>>> comparator = CompoundComparator
                .of(comparators.build());
        return data.entrySet().stream().sorted(comparator);
    }

    /**
     * Compare {@code v1} and {@code v2} in the same manner that a
     * {@link Comparator} would.
     * 
     * @param v1
     * @param v2
     * @return
     */
    protected abstract int compare(V v1, V v2);

    /**
     * Lookup the value stored for {@code key} in {@code record} within the
     * {@link #store}.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return the stored value
     */
    protected abstract V lookup(String key, long record);

    /**
     * Lookup the value stored for {@code key} in {@code record} at
     * {@code timestamp} within the {@link #store}.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return the stored value
     */
    protected abstract V lookup(String key, long record, long timestamp);

}
