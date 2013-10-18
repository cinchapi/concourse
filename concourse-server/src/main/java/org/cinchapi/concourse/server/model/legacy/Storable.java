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
package org.cinchapi.concourse.server.model.legacy;

import org.cinchapi.concourse.server.io.Byteable;

/**
 * A {@link Byteable} object that can be versioned by a unique timestamp and
 * appended sequentially to disk. The timestamp is stored directly with the
 * object so the version information does not change when the object's storage
 * context changes (i.e. buffer transport, data replication, cluster rebalance,
 * etc).
 * 
 * @author jnelson
 */
public interface Storable extends Byteable {

	/**
	 * Represents a null timestamp, indicating the object is notForStorage.
	 */
	public static final long NO_TIMESTAMP = 0;

	/**
	 * Return {@code true} if {@code obj} is <em>logically</em> equal to this
	 * one, meaning all of its attributes other than its {@code timestamp} are
	 * equal to those in this object. The associated {@code timestamp} only
	 * versions the object and does not alter its essence in relation to other
	 * objects outside of temporal sorting.
	 */
	@Override
	public boolean equals(Object obj);

	/**
	 * Return the <em>logical</em> hash code value for this object, which does
	 * not take the {@code timestamp} into account. The associated
	 * {@code timestamp} only versions the object and does not alter its essence
	 * in relation to other objects outside of temporal sorting.
	 * 
	 * @return the hash code
	 */
	@Override
	public int hashCode();

	/**
	 * Return the associated {@code timestamp}. This is guaranteed to be unique
	 * amongst forStorage objects. For notForStorage objects, the timestamp is
	 * always {@link #NO_TIMESTAMP}.
	 * 
	 * @return the {@code timestamp}
	 */
	public long getTimestamp();

	/**
	 * Return {@code true} if the object is versioned and therefore appropriate
	 * for storage.
	 * 
	 * @return {@code true} of {@link #isNotForStorage()} is {@code false}.
	 */
	boolean isForStorage();

	/**
	 * Return {@code true} if the object is not versioned and therefore not
	 * appropriate for storage.
	 * 
	 * @return {@code true} if the timestamp is {@link #NO_TIMESTAMP}.
	 */
	boolean isNotForStorage();

}
