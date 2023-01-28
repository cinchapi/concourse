/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;

/**
 * A {@link Cache} that doesn't actually cache anything. It ignores attempts to
 * {@link #put(Object, Object)} elements into the cache and always returns
 * {@code null} when a lookup is performed.
 *
 * @author Jeff Nelson
 */
public class NoOpCache<K, V> extends AbstractCache<K, V> {

    @Override
    public V get(K key, Callable<? extends V> valueLoader)
            throws ExecutionException {
        try {
            return valueLoader.call();
        }
        catch (Exception e) {
            throw new ExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public @Nullable V getIfPresent(Object key) {
        return null;
    }

    @Override
    public void invalidate(Object key) { /* no-op */ }

    @Override
    public void invalidateAll() {/* no-op */}

    @Override
    public void put(K key, V value) { /* no-op */ }

}
