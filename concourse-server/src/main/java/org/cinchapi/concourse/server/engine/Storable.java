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
package org.cinchapi.concourse.server.engine;

import org.cinchapi.common.io.Byteable;

/**
 * <p>
 * A {@link Byteable} object that is held within a {@link Field}.
 * </p>
 * <p>
 * Each {@code Storable} object is versioned by a unique timestamp. The
 * timestamp is stored directly with the object so that the version information
 * does not change when the object's storage context changes (i.e. when the
 * write buffer is flushed, when data is replicated, or when shards are
 * re-balanced, etc).
 * </p>
 * 
 * @author jnelson
 */
public interface Storable extends Byteable {

	/**
	 * Represents a null timestamp, indicating the object is notForStorage.
	 */
	public static final long NIL = 0;

	/**
	 * This method does not take timestamp into account because it is expected
	 * that there will be instances when two objects have different timestamps
	 * but are otherwise equal and should be treated as such. The associated
	 * #timestamp is meant to version the object and not necessarily to alter
	 * its essence in relation to other objects outside of temporal sorting.
	 */
	@Override
	public boolean equals(Object obj);

	/**
	 * This method does not take timestamp into account because it is expected
	 * that there will be instances when two objects have different timestamps
	 * but otherwise hash to the same value and should be treated as such. The
	 * associated #timestamp is meant to version the object and not necessarily
	 * to alter its essence in relation to other objects outside of temporal
	 * sorting.
	 * 
	 * @return
	 */
	@Override
	public int hashCode();

	/**
	 * Return the associated {@code timestamp}. This is guaranteed to be unique
	 * amongst forStorage values so it a defacto identifier. For notForStorage
	 * objects, the timestamp is always {@link #NIL}.
	 * 
	 * @return the {@code timestamp}
	 */
	public long getTimestamp();

	/**
	 * Return {@code true} if the object is suitable for use in storage
	 * functions.
	 * 
	 * @return {@code true} of {@link #isNotForStorage()} is {@code false}.
	 */
	boolean isForStorage();

	/**
	 * Return {@code true} if the object is not suitable for storage functions
	 * and is only suitable for comparisons.
	 * 
	 * @return {@code true} if the timestamp is {@link #NIL}.
	 */
	boolean isNotForStorage();

}

