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
package org.cinchapi.concourse.server.concurrent;

import java.lang.reflect.Method;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * A collection of methods that perform hacks in order to access some helpful
 * internal methods in third party libraries like Guava.
 * 
 * @author jnelson
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
     * {@link ConcurrentHashMultiset}.
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
