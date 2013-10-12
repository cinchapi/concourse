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
package org.cinchapi.concourse.server.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.primitives.Longs;

/**
 * 
 * 
 * @author jnelson
 */
public class Block<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> implements
		Byteable {

	private final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();
	private final ReadLock readLock = masterLock.readLock();
	private final WriteLock writeLock = masterLock.writeLock();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.io.Byteable#size()
	 */
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.io.Byteable#getBytes()
	 */
	@Override
	public ByteBuffer getBytes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Immutable
	private final class Entry implements Byteable, Comparable<Entry> {

		private static final int CONSTANT_SIZE = 20; // timestamp(8),
														// locatorSize (4),
														// keySize(4),
														// valueSize(4)

		private final L locator;
		private final K key;
		private final V value;
		private final long timestamp;
		private final transient ByteBuffer bytes;

		/**
		 * Construct a new instance.
		 * 
		 * @param timestamp
		 * @param locator
		 * @param key
		 * @param value
		 */
		public Entry(long timestamp, L locator, K key, V value) {
			this.timestamp = timestamp;
			this.locator = locator;
			this.key = key;
			this.value = value;
			bytes = ByteBuffer.allocate(CONSTANT_SIZE + locator.size()
					+ key.size() + value.size());
			bytes.putLong(timestamp);
			bytes.putInt(locator.size());
			bytes.putInt(key.size());
			bytes.putInt(value.size());
			bytes.put(locator.getBytes());
			bytes.put(key.getBytes());
			bytes.put(value.getBytes());

		}

		@Override
		public int size() {
			return bytes.capacity();
		}

		@Override
		public ByteBuffer getBytes() {
			return ByteBuffers.asReadOnlyBuffer(bytes);
		}

		/**
		 * Return the {@link locator} associated with this Entry.
		 * 
		 * @return the locator
		 */
		public L getLocator() {
			return locator;
		}

		/**
		 * Return the {@link key} associated with this Entry.
		 * 
		 * @return the key
		 */
		public K getKey() {
			return key;
		}

		/**
		 * Return the {@link value} associated with this Entry.
		 * 
		 * @return the value
		 */
		public V getValue() {
			return value;
		}

		/**
		 * Return the {@link timestamp} associated with this Entry.
		 * 
		 * @return the timestamp
		 */
		public long getTimestamp() {
			return timestamp;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return super.hashCode();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			return super.equals(obj);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return super.toString();
		}

		@Override
		public int compareTo(Entry other) {
			int order;
			return (order = locator.compareTo(other.locator)) != 0 ? order
					: ((order = key.compareTo(other.key)) != 0 ? order : (Longs
							.compare(timestamp, other.timestamp)));
		}

	}

}
