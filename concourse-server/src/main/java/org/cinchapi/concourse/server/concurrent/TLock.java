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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.time.Time;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A {@link ReentrantReadWriteLock} that is identified by a {@link Token}.
 * TLocks are used to globally lock <em>notions of things</em> that aren't
 * strictly defined in their own right (i.e. a {@code key} in a {@code record}).
 * <p>
 * Repeated attempts to grab a TLock for a given token will return the same
 * instance to all callers within a fixed range of the last grab (see
 * {@link #CACHE_TTL} and {@link #CACHE_TTL_UNIT}. This means that TLock
 * instances will be GCed regularly (not just when we are close too an OOM), but
 * it opens the possibility that a caller may be holding a stale lock that
 * doesn't properly block new callers (i.e A grabs Lock at t1 [..] Lock is
 * evicted from cache at t2 [..] B grabs Lock at t3 and gets new instance [..] A
 * and B are both concurrently operation on content.)
 * </p>
 * <p>
 * To reduce the probability of the aforementioned scenario, each caller should
 * periodically verify that her instance is not stale (see
 * {@link #isStateInstance()}) if her work may last longer than the cache TTL.
 * If the instance is stale, the caller should simply grab a new lock for the
 * token and lock the instance immediately.
 * </p>
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
			return CACHE.get(token).touch();
		}
		catch (ExecutionException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * The number of {@link #CACHE_TTL_UNIT} after each access that an instance
	 * has to live before being evicted from {@link #CACHE}.
	 */
	private static final int CACHE_TTL = 600;

	/**
	 * The time unit used to measure {@link #CACHE_TTL}.
	 */
	private static final TimeUnit CACHE_TTL_UNIT = TimeUnit.SECONDS;

	/**
	 * The cache that is responsible for returning appropriate TLock instances
	 * for a given {@link Token}. This cache uses a time-based eviction policy,
	 * which has implications that are discussed in this class'
	 * documentation.
	 */
	private static final LoadingCache<Token, TLock> CACHE = CacheBuilder
			.newBuilder().expireAfterAccess(CACHE_TTL, CACHE_TTL_UNIT)
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
	 * The timestamp of the last {@link #touch()}.
	 */
	private long timestamp;

	/**
	 * Construct a new instance.
	 * 
	 * @param token
	 */
	public TLock(Token token) {
		super();
		this.token = token;
		touch();
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
	 * Get the amount of time that has passed since the Lock was last grabbed
	 * (i.e. the {@link #grab(Object...)} or {@link #grabWithToken(Token)}
	 * method was invoked and returned this.
	 * 
	 * @param unit
	 * @return the time since the Lock was last grabbed
	 */
	public long getTimeSinceLastGrab(TimeUnit unit) {
		return unit.convert(Time.now(), TimeUnit.MICROSECONDS)
				- unit.convert(timestamp, TimeUnit.MICROSECONDS);
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

	/**
	 * Return {@code true} if the amount of time since this instance was grabbed
	 * is greater than {@link GlobalState#TRANSACTION_TTL}.
	 * 
	 * @return {@code true} if this instance is stale
	 */
	public boolean isStateInstance() {
		return getTimeSinceLastGrab(TimeUnit.SECONDS) >= GlobalState.TRANSACTION_TTL;
	}

	@Override
	public String toString() {
		String[] toks = super.toString().split("\\[");
		return getClass().getSimpleName() + " " + token + " [" + toks[1];
	}

	/**
	 * Update the last access timestamp.
	 * 
	 * @return this
	 */
	protected TLock touch() { // exposed for testing
		return touch(Time.now());
	}

	/**
	 * Set the last access timestamp to {@link time}.
	 * 
	 * @param time
	 * @return this
	 */
	protected TLock touch(long time) { // exposed for testing
		timestamp = time;
		return this;
	}

}
