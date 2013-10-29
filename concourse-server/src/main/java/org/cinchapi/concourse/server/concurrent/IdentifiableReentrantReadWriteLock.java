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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableComposite;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A {@link ReentrantReadWriteLock} that is identified by a
 * {@link ByteableComposite}. The lock defines its hashCode and equals methods
 * in terms of its id. An identifiable lock is useful for situations in which a
 * lock is placed in a collection and needs to be identified for subsequent
 * retrieval.
 * 
 * @author jnelson
 */
@Immutable
public class IdentifiableReentrantReadWriteLock extends ReentrantReadWriteLock {

	/**
	 * Create a new IdentifiableReentrantReadWriteLock whose {@link #id} will
	 * created with {@code components}.
	 * 
	 * @param components
	 * @return the IdentifiableReentrantReadWriteLock
	 */
	public static IdentifiableReentrantReadWriteLock create(
			Byteable... components) {
		return identifiedBy(ByteableComposite.create(components));
	}

	/**
	 * Return a IdentifiableReentrantReadWriteLock that is identified by
	 * {@code id}.
	 * 
	 * @param id
	 * @return the IdentifiableReentrantReadWriteLock
	 */
	public static IdentifiableReentrantReadWriteLock identifiedBy(
			ByteableComposite id) {
		try {
			return CACHE.get(id);
		}
		catch (ExecutionException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * The cache holds locks that have been recently used. This helps to ensure
	 * that we return the same lock for the same key.
	 */
	private static final LoadingCache<ByteableComposite, IdentifiableReentrantReadWriteLock> CACHE = CacheBuilder
			.newBuilder()
			.maximumSize(1000)
			.build(new CacheLoader<ByteableComposite, IdentifiableReentrantReadWriteLock>() {

				@Override
				public IdentifiableReentrantReadWriteLock load(
						ByteableComposite key) throws Exception {
					return new IdentifiableReentrantReadWriteLock(key);
				}

			});

	private static final long serialVersionUID = 1L; // Serializability
														// inherited from super
														// class

	/**
	 * The id not only identifies this lock, but governs rules for the
	 * {@link #hashCode()} and {@link #equals(Object)} methods.
	 */
	private final ByteableComposite id;

	/**
	 * Construct a new instance.
	 * 
	 * @param id
	 */
	public IdentifiableReentrantReadWriteLock(ByteableComposite id) {
		super();
		this.id = id;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof IdentifiableReentrantReadWriteLock) {
			IdentifiableReentrantReadWriteLock other = (IdentifiableReentrantReadWriteLock) obj;
			return id.equals(other.id);
		}
		return false;
	}

	/**
	 * Return the {@link #id} for this lock.
	 * 
	 * @return the id
	 */
	public ByteableComposite getId() {
		return id;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return id + " " + super.toString();
	}

}
