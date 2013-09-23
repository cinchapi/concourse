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

import java.util.Iterator;
import java.util.List;

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.multithread.Lock;
import org.cinchapi.concourse.server.model.Write;

import com.google.common.collect.Lists;

/**
 * A {@link Queue} is a very simple form of {@link Limbo} that represents
 * data as a sequence of {@link Write} objects. New data is appended to the
 * sequence and the returned {@link Iterator} traverses the list.
 * 
 * @author jnelson
 */
@PackagePrivate
class Queue extends Limbo {

	/**
	 * Revisions are stored as a sequential list of {@link Write} objects, which
	 * means most reads are <em>at least</em> an O(n) scan.
	 */
	protected final List<Write> writes;

	/**
	 * Construct a Limbo with enough capacity for {@code initialSize}. If
	 * necessary, the structure will grow to accommodate more data.
	 * 
	 * @param initialSize
	 */
	protected Queue(int initialSize) {
		writes = Lists.newArrayListWithCapacity(initialSize);
	}

	@Override
	public Iterator<Write> iterator() {
		return writes.iterator();
	}

	@Override
	protected boolean insert(Write write) {
		Lock lock = writeLock();
		try {
			return writes.add(write);
		}
		finally {
			lock.release();
		}
	}

	@Override
	public void start() {
		// do nothing
	}

}
