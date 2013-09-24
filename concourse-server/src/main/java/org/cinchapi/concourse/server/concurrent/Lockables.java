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
package org.cinchapi.concourse.server.concurrent;

import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.collect.Maps;

/**
 * Static factory methods for objects that implement the {@link Lockable}
 * interface. 
 * 
 * @author jnelson
 */
public abstract class Lockables {

	/**
	 * Implements the {@link Lockable#readLock()} method.
	 * 
	 * @param object
	 * @return a {@link Lock} for the current thread
	 */
	public static Lock readLock(Lockable object) {
		String cacheKey = getCacheKey(object);
		ReadWriteLock lock = cache.get(cacheKey);
		if(lock == null) {
			lock = new ReentrantReadWriteLock();
			cache.put(cacheKey, lock);
		}

		try {
			lock.readLock().lockInterruptibly();
			return new ReadLock(lock);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		return new InterruptedLock();

	}

	/**
	 * Implements the {@link Lockable#writeLock()} method.
	 * 
	 * @param object
	 * @return a {@link Lock} for the current thread
	 */
	public static Lock writeLock(Lockable object) {
		String cacheKey = getCacheKey(object);
		ReadWriteLock lock = cache.get(cacheKey);
		if(lock == null) {
			lock = new ReentrantReadWriteLock();
			cache.put(cacheKey, lock);
		}

		try {
			lock.writeLock().lockInterruptibly();
			return new WriteLock(lock);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return new InterruptedLock();
	}

	/**
	 * Returns the cacheKey for {@code object} which is the MD5 hash of its
	 * defined hash code, fully qualified class name and identity hash code.
	 * 
	 * @param object
	 * @return a String representing the cacheKey for {@code object}
	 */
	private static String getCacheKey(Lockable object) {
		return DigestUtils
				.md5Hex(object.hashCode() + object.getClass().getName()
						+ System.identityHashCode(object));
	}

	/**
	 * The cache associates objects (via a cacheKey) to ReadWriteLock instances
	 * so that the Lockable object does not need to declare its own lock
	 * variable.
	 */
	private static final Map<String, ReadWriteLock> cache = Maps.newHashMap();

}