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

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.multithread.Lock;

/**
 * A {@code Transporter} is an iterator that returns the {@link Write} objects
 * stored in a {@link Buffer} one-by-one for the purpose of moving those writes
 * to a {@link Destination}. The {@code Transporter} only moves the Writes
 * from the Buffer. The Destination is responsible for ensuring that
 * the Writes are stored in the {@link Destination#accept(Write)} method.
 * <p>
 * <em>The Buffer must make a call to {@link #ack()} after each call to
 * {@link Destination#accept(Write)}.</em>
 * </p>
 * 
 * @author jnelson
 */
@PackagePrivate
class Transporter implements Iterator<Write> {

	private final Buffer buffer;
	private int postAckPosition = 0;

	/**
	 * Construct a new instance.
	 * 
	 * @param buffer
	 */
	@PackagePrivate
	Transporter(Buffer buffer) {
		buffer.content.rewind();
		this.buffer = buffer;
	}

	/**
	 * The user of the Transporter must call this method to acknowledge that
	 * most recently returned Write from {@link #next()} was successfully
	 * transported to the Destination context, otherwise calls to
	 * {@link #next()} will return the same Write over and over again.
	 */
	public void ack() {
		buffer.writes.remove(0);
		buffer.content.position(postAckPosition);
		buffer.content.compact();
		buffer.content.position(0);
		buffer.content.force();
	}

	@Override
	public boolean hasNext() {
		Lock lock = buffer.readLock();
		try {
			return !buffer.writes.isEmpty();
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Write next() {
		Lock lock = buffer.writeLock();
		try {
			Write next = buffer.writes.get(0);
			postAckPosition = buffer.content.position() + next.size() + 4;
			return next;
		}
		finally {
			lock.release();
		}

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
