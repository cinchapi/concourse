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
package com.cinchapi.concourse.server.concurrent;

import java.lang.reflect.Method;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * A collection of methods that perform hacks in order to access some helpful
 * internal methods in third party libraries like Guava.
 * 
 * @author Jeff Nelson
 */
public final class GuavaInternals {

    private GuavaInternals() {/* noop */}

    /**
     * com.google.common.collect.Synchronized
     * <p>
     * Keep a cached copy so we only incur the reflection cost once.
     * </p>
     */
    private static final Class<?> synchronizedClass;

    /**
     * Synchronized#multiset(Multiset, Object)
     * <p>
     * Keep a cached copy so we only incur the reflection cost once.
     * </p>
     */
    private static final Method createSynchronizedMultisetMethod;
    static {
        try {
            synchronizedClass = Class
                    .forName("com.google.common.collect.Synchronized");
            createSynchronizedMultisetMethod = synchronizedClass
                    .getDeclaredMethod("multiset", Multiset.class, Object.class);
            createSynchronizedMultisetMethod.setAccessible(true);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return a hash multiset that uses synchronization to control concurrent
     * access. This is typically appropriate in cases where contention is
     * possible but not high enough to warrant the overhead of the
     * {@link com.google.common.collect.ConcurrentHashMultiset
     * ConcurrentHashMultiset}.
     * 
     * @return the multiset
     */
    @SuppressWarnings("unchecked")
    public static <T> Multiset<T> newSynchronizedHashMultiset() {
        try {
            return (Multiset<T>) createSynchronizedMultisetMethod.invoke(null,
                    HashMultiset.create(), new Object());
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

}
