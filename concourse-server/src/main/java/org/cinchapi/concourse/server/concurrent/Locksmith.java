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

import java.util.concurrent.ExecutionException;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableComposite;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A service that provides identifiable locks.
 * 
 * @author jnelson
 */
public class Locksmith {

	/**
	 * The cache holds locks that have been recently used.
	 */
	private final LoadingCache<ByteableComposite, IdentifiableReentrantReadWriteLock> cache = CacheBuilder
			.newBuilder()
			.maximumSize(100000)
			.build(new CacheLoader<ByteableComposite, IdentifiableReentrantReadWriteLock>() {

				@Override
				public IdentifiableReentrantReadWriteLock load(
						ByteableComposite key) throws Exception {
					return IdentifiableReentrantReadWriteLock.identifiedBy(key);
				}

			});

	/**
	 * Get the lock identified by {@code components}.
	 * 
	 * @param components
	 * @return the lock
	 */
	public IdentifiableReentrantReadWriteLock getLock(Byteable... components) {
		try {
			return cache.get(ByteableComposite.create(components));
		}
		catch (ExecutionException e) {
			throw Throwables.propagate(e);
		}
	}

}
