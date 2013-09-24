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

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Abstract implementation of the {@link Lock} interface.
 * 
 * @author jnelson
 */
abstract class AbstractLock implements Lock { /* package-private */

	/**
	 * This lock must be a member of the {@link Lockable} in which the
	 * {@link Lockable#readLock()} or {@link Lockable#writeLock()} method was
	 * invoked.
	 */
	protected final ReadWriteLock lock;

	/**
	 * Construct a new instance.
	 * 
	 * @param lock
	 */
	public AbstractLock(ReadWriteLock lock) {
		this.lock = lock;
	}

	/**
	 * Return the parent {@link #lock} for testing purposes.
	 * 
	 * @return the parent lock
	 */
	ReadWriteLock getParent() { /* package-private */
		return lock;
	}

	public String toString() {
		return lock.toString() + " in " + Thread.currentThread().getName()
				+ " (" + Thread.currentThread().getId() + ")";
	}

}
