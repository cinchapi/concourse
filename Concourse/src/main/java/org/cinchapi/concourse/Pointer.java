/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.common.cache.ReferenceCache;

import com.google.common.primitives.Longs;

/**
 * A {@code Pointer} is a reference to the PrimaryKey of a Record used to add
 * Links in Concourse. Create Pointers using the {@link Pointer#to(long)}
 * method.
 * 
 * @author jnelson
 */
@Immutable
public final class Pointer {

	/**
	 * Return a Reference to the Record identified by {@code primaryKey}.
	 * 
	 * @param primaryKey
	 * @return the Pointer
	 */
	public static Pointer to(long primaryKey) {
		Pointer pointer = cache.get(primaryKey);
		if(pointer == null) {
			pointer = new Pointer(primaryKey);
			cache.put(pointer, primaryKey);
		}
		return pointer;
	}

	// Since Pointers are unique, we use a cache to ensure that we don't
	// create duplicate objects in memory;
	private static final ReferenceCache<Pointer> cache = new ReferenceCache<Pointer>();

	private final long primaryKey;

	/**
	 * Construct a new instance.
	 * 
	 * @param primaryKey
	 */
	private Pointer(long primaryKey) {
		this.primaryKey = primaryKey;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Pointer) {
			Pointer other = (Pointer) obj;
			return Longs.compare(primaryKey, other.primaryKey) == 0;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Longs.hashCode(primaryKey);
	}

	/**
	 * Return the long value that is wrapped within the Pointer.
	 * 
	 * @return the long value
	 */
	public long longValue() {
		return primaryKey;
	}

	@Override
	public String toString() {
		return Long.toString(primaryKey);
	}

}
