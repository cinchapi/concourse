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
package org.cinchapi.concourse.cache;

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
 * @author jnelson
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
