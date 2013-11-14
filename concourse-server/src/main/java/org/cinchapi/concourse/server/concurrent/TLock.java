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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A {@link ReentrantReadWriteLock} that is identified by a {@link Token}.
 * The lock defines its hashCode and equals methods in terms of its token, and
 * is useful for situations where lock is placed in a collection and needs to be
 * identified for subsequent retrieval.
 * 
 * @author jnelson
 */
@Immutable
public class TLock extends ReentrantReadWriteLock {

	/**
	 * Return the TLock that is identified by the {@code objects}.
	 * 
	 * @param objects
	 * @return the TLock
	 */
	public static TLock grab(Object... objects) {
		return grabWithToken(Token.wrap(objects));
	}

	/**
	 * Return the TLock that is identified by {@code token}.
	 * 
	 * @param token
	 * @return the TLock
	 */
	public static TLock grabWithToken(Token token) {
		try {
			return CACHE.get(token);
		}
		catch (ExecutionException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * The cache holds locks that have been recently used. This helps to ensure
	 * that we return the same lock for the same key.
	 */
	private static final LoadingCache<Token, TLock> CACHE = CacheBuilder
			.newBuilder().maximumSize(1000)
			.build(new CacheLoader<Token, TLock>() {

				@Override
				public TLock load(Token token) throws Exception {
					return new TLock(token);
				}

			});

	private static final long serialVersionUID = 1L; // Serializability
														// inherited from super
														// class

	/**
	 * The token not only identifies this lock, but governs rules for the
	 * {@link #hashCode()} and {@link #equals(Object)} methods.
	 */
	private final Token token;

	/**
	 * Construct a new instance.
	 * 
	 * @param token
	 */
	public TLock(Token token) {
		super();
		this.token = token;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TLock) {
			TLock other = (TLock) obj;
			return token.equals(other.token);
		}
		return false;
	}

	/**
	 * Return the {@link #id} for this lock.
	 * 
	 * @return the id
	 */
	public Token getToken() {
		return token;
	}

	@Override
	public int hashCode() {
		return token.hashCode();
	}

	@Override
	public String toString() {
		String[] toks = super.toString().split("\\[");
		return getClass().getSimpleName() + " " + token + " [" + toks[1];
	}

}
