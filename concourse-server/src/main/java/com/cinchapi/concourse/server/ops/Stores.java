/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.ops;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.validate.Keys;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * A collection of "smart" operations that delegate to functionality in a
 * {@link Store} in the most efficient way possible while maintaining data
 * consistency.
 *
 * @author Jeff Nelson
 */
public final class Stores {

    /**
     * Browse the values stored for {@code key} in the {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#browse(String) browse}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#browseNavigationKeyOptionalAtomic(String, long, Store)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param timestamp
     * 
     * @return the values stored for {@code key} across the {@code store}
     */
    public static Map<TObject, Set<Long>> browse(Store store, String key) {
        return browse(store, key, Time.NONE);
    }

    /**
     * Browse the values stored for {@code key} at {@code timestamp} in the
     * {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#browse(String) browse}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#browseNavigationKeyOptionalAtomic(String, long, Store)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param timestamp
     * 
     * @return the values stored for {@code key} across the {@code store}
     */
    public static Map<TObject, Set<Long>> browse(Store store, String key,
            long timestamp) {
        if(Keys.isNavigationKey(key)) {
            if(store instanceof AtomicOperation || timestamp != Time.NONE) {
                return Operations.browseNavigationKeyOptionalAtomic(key,
                        timestamp, store);
            }
            else if(store instanceof AtomicSupport) {
                AtomicReference<Map<TObject, Set<Long>>> data = new AtomicReference<>(
                        ImmutableMap.of());
                AtomicOperations.executeWithRetry((AtomicSupport) store,
                        (atomic) -> {
                            data.set(Operations
                                    .browseNavigationKeyOptionalAtomic(key,
                                            timestamp, atomic));
                        });
                return data.get();
            }
            else {
                throw new UnsupportedOperationException(
                        "Cannot browse the current values of a navigation key using a Store that does not support atomic operations");
            }
        }
        else {
            return timestamp == Time.NONE ? store.browse(key)
                    : store.browse(key, timestamp);
        }
    }

    /**
     * Select the values stored for {@code key} in {@code record} from the
     * {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#select(long) select}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#traverseKeyRecordOptionalAtomic(String, long, long, AtomicOperation)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param record
     * @param timestamp
     * 
     * @return the values stored for {@code key} in {@code record} according to
     *         the {@code store}
     */
    public static Set<TObject> select(Store store, String key, long record) {
        return select(store, key, record, Time.NONE);
    }

    /**
     * Select the values stored for {@code key} in {@code record} at
     * {@code timestamp} from the {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#select(long) select}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#traverseKeyRecordOptionalAtomic(String, long, long, AtomicOperation)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param record
     * @param timestamp
     * 
     * @return the values stored for {@code key} in {@code record} at
     *         {@code timestamp} according to the {@code store}
     */
    public static Set<TObject> select(Store store, String key, long record,
            long timestamp) {
        if(Keys.isNavigationKey(key)) {
            if(store instanceof AtomicOperation || timestamp != Time.NONE) {
                return Operations.traverseKeyRecordOptionalAtomic(key, record,
                        timestamp, store);
            }
            else if(store instanceof AtomicSupport) {
                AtomicReference<Set<TObject>> value = new AtomicReference<>(
                        ImmutableSet.of());
                AtomicOperations.executeWithRetry((AtomicSupport) store,
                        (atomic) -> {
                            value.set(
                                    Operations.traverseKeyRecordOptionalAtomic(
                                            key, record, timestamp, atomic));
                        });
                return value.get();
            }
            else {
                throw new UnsupportedOperationException(
                        "Cannot fetch the current values of a navigation key using a Store that does not support atomic operations");
            }
        }
        else {
            return timestamp == Time.NONE ? store.select(key, record)
                    : store.select(key, record, timestamp);
        }
    }

    private Stores() {/* no-init */}

}
