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
 * <p>
 * This interface is a simplified wrapper around the {@link ReadWriteLock} API
 * so that objects have the ability to be locked by external callers without
 * directly exposing a java lock object.
 * </p>
 * <p>
 * This interface is designed for objects in a collection, so each of the
 * methods returns a {@link Lock} which can be used to unlock the object without
 * accessing it again in the collection or storing a separate reference to it.
 * </p>
 * 
 * @author jnelson
 */
public interface Lockable {

	/**
	 * Acquire a read lock on the object in the current thread.
	 * 
	 * @return a {@link Lock} for the current thread
	 * @see {@link Lockables#readLock(Lockable)}
	 */
	public Lock readLock();

	/**
	 * Acquire the write lock on the object in the current thread.
	 * 
	 * @return a {@link Lock} for the current thread
	 * @see {@link Lockables#writeLock(Lockable)}
	 */
	public Lock writeLock();

}