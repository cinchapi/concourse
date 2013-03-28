/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.common.cache;

import java.util.Map;

import javax.annotation.Nullable;

import com.cinchapi.common.Hash;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

/**
 * A thread-safe cache that holds references to objects so as to prevent
 * unnecessary constructor calls. The cache does not have a maximum capacity or
 * expulsion policy, but it uses a {@link SoftReference} for each stored
 * {@code value} so that the garbage collector will clear the cache only
 * before an OOM occurs. <h2>Example:</h2>
 * 
 * <code><pre>
 *  private class Foo {
 *    
 *    int id;
 *    String label;
 *    Bar bar;
 *    
 *    private static final ObjectResuseCache<Foo> cache = new ObjectResuseCache<Foo>();
 *    
 *    public static Foo newInstance(int id, String label, Bar bar){
 *    	Foo foo = cache.get(id, label, bar);
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
 * @param <T>
 *            - the object type.
 */
public class ObjectReuseCache<T> {

	private static final int defaultInitialCapacity = 500000;
	private static final int defaultConcurrencyLevel = 16;

	Map<String, T> cache = new MapMaker()
			.initialCapacity(defaultInitialCapacity)
			.concurrencyLevel(defaultConcurrencyLevel).softValues().makeMap();

	/**
	 * Return {@code true} if the cache contains a value identified by the
	 * group of {@code args}.
	 * 
	 * @param args
	 * @return {@code true} if a value is found.
	 */
	public boolean contains(Object... args) {
		String id = getIdentifier(args);
		return cache.containsKey(id);
	}

	/**
	 * Return the cache value associated with the group of {@code args} or
	 * {@code null} if not value is found.
	 * 
	 * @param args
	 * @return the cached value.
	 */
	@Nullable
	public T get(Object... args) {
		String id = getIdentifier(args);
		return cache.get(id);
	}

	/**
	 * Cache {@code value} and associated it with the group of {@code args}.
	 * Each arg should be a value that is used to construct
	 * the object.
	 * 
	 * @param value
	 * @param args
	 * @return {@code true} if {@code value} is successfully cached.
	 */
	public boolean put(T value, Object... args) {
		Preconditions.checkNotNull(value);
		Preconditions.checkNotNull(args);
		Preconditions.checkArgument(args.length > 0,
				"You must specify at least one key");
		String id = getIdentifier(args);
		synchronized (cache) {
			cache.put(id, value);
		}
		return get(args) == value;
	}

	/**
	 * Remove the value associated with the group of {@code args} from the
	 * cache.
	 * 
	 * @param args
	 * @return {@code true} if the associated value is successfully removed.
	 */
	public boolean remove(Object... args) {
		String id = getIdentifier(args);
		synchronized (cache) {
			if(cache.containsKey(id)) {
				cache.remove(id);
				return !cache.containsKey(id);
			}
			return false;
		}
	}

	/**
	 * Return a unique identifier for a group of {@code args}.
	 * 
	 * @param args
	 * @return the identifier.
	 */
	private String getIdentifier(Object... args) {
		StringBuilder key = new StringBuilder();
		for (Object o : args) {
			key.append(o);
			key.append(o.getClass().getName());
		}
		return Hash.md5(key.toString());
	}

}
