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
package com.cinchapi.concourse.cache;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * <p>
 * A concurrent cache that holds references to objects so as to prevent
 * unnecessary constructor calls. The cache does not have a maximum capacity or
 * expulsion policy, but it uses a SoftReference for each stored object so that
 * the garbage collector will clear the cache only before an OOM occurs.
 * </p>
 * <h2>Example:</h2>
 * 
 * <code><pre>
 *  private class Foo {
 *    
 *    int id;
 *    String label;
 *    Bar bar;
 *    
 *    private static final ReferenceCache{@literal <Foo>} cache = new ReferenceCache{@literal <Foo>}();
 *    
 *    public static Foo newInstance(int id, String label, Bar bar){
 *    	Foo foo = cache.get(id, label, bar); // use the combination of id, label and bar as a cacheKey
 *    	if(foo == null){
 *    	    foo = new Foo(id, label, bar);
 *    		cache.put(foo, id, label, bar);
 *    	}
 *    	return foo;
 *    }
 *    
 *    private Foo(int id, String label, Bar bar){
 *    	this.id = id;
 *    	this.label = label;
 *    	this.bar = bar;
 *    }
 * }
 * </pre></code>
 * 
 * @author Jeff Nelson
 * @param <T> - the cached object type.
 */
public class ReferenceCache<T> {

    private static final int INITIAL_CAPACITY = 500000;
    private static final int CONCURRENCY_LEVEL = 16;

    private final Cache<HashCode, T> cache = CacheBuilder.newBuilder()
            .initialCapacity(INITIAL_CAPACITY)
            .concurrencyLevel(CONCURRENCY_LEVEL).softValues().build();

    /**
     * Return the cache value associated with the group of {@code args} or
     * {@code null} if not value is found.
     * 
     * @param args
     * @return the cached value.
     */
    @Nullable
    public T get(Object... args) {
        HashCode id = getCacheKey(args);
        return cache.getIfPresent(id);
    }

    /**
     * Cache {@code value} and associated it with the group of {@code args}.
     * Each arg should be a value that is used to construct
     * the object.
     * 
     * @param value
     * @param args
     */
    public void put(T value, Object... args) {
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(args);
        Preconditions.checkArgument(args.length > 0,
                "You must specify at least one key");
        HashCode id = getCacheKey(args);
        cache.put(id, value);
    }

    /**
     * Remove the value associated with the group of {@code args} from the
     * cache.
     * 
     * @param args
     */
    public void remove(Object... args) {
        cache.invalidate(getCacheKey(args));
    }

    /**
     * Return a unique identifier for a group of {@code args}.
     * 
     * @param args
     * @return the identifier.
     */
    private HashCode getCacheKey(Object... args) {
        StringBuilder key = new StringBuilder();
        for (Object o : args) {
            key.append(o.hashCode());
            key.append(o.getClass().getName());
        }
        return Hashing.md5().hashUnencodedChars(key.toString());
    }

}
