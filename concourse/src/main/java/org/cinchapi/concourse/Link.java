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

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.cache.ReferenceCache;

import com.google.common.primitives.Longs;

/**
 * A {@link Link} is a pointer to the Primary Key of a Record.
 * 
 * @author jnelson
 */
@Immutable
public final class Link {

	/**
	 * Return a Link to {@code record}
	 * 
	 * @param record
	 * @return the Link
	 */
	@PackagePrivate
	public static Link to(long record) {
		Link pointer = cache.get(record);
		if(pointer == null) {
			pointer = new Link(record);
			cache.put(pointer, record);
		}
		return pointer;
	}

	// Since Links are unique, we use a cache to ensure that we don't
	// create duplicate objects in memory;
	private static final ReferenceCache<Link> cache = new ReferenceCache<Link>();

	private final long record;

	/**
	 * Construct a new instance.
	 * 
	 * @param record
	 */
	private Link(long record) {
		this.record = record;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Link) {
			Link other = (Link) obj;
			return Longs.compare(record, other.record) == 0;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Longs.hashCode(record);
	}

	/**
	 * Return the long value that is wrapped within the Link.
	 * 
	 * @return the long value
	 */
	public long longValue() {
		return record;
	}

	@Override
	public String toString() {
		return Long.toString(record);
	}

}
