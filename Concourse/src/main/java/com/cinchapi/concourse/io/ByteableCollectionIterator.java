/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.io;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * An {@link Iterator} that traverses a byte array and returns sequences as
 * a byte buffer. The iterator assumes that each sequence is preceded by a 4
 * byte integer (a peek) that specifies how many bytes should be read and
 * returned with the next sequence. The iterator will fail to return a next
 * element when its peak is less than 1 or the byte array has no more
 * elements.
 * 
 * @author jnelson
 */
public class ByteableCollectionIterator implements Iterator<ByteBuffer> {

	/**
	 * Return a byte iterator over {@code bytes}.
	 * 
	 * @param bytes
	 * @return the iterator.
	 */
	public static ByteableCollectionIterator over(byte[] bytes) {
		return new ByteableCollectionIterator(bytes);
	}

	protected final ByteBuffer bytes;
	protected ByteBuffer next;

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	protected ByteableCollectionIterator(byte[] bytes) {
		this.bytes = ByteBuffer.wrap(bytes);
		readNext();
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public ByteBuffer next() {
		ByteBuffer next = this.next;
		next.rewind();
		readNext();
		return next;
	}

	/**
	 * Return the current position of the iterator in the underlying byte
	 * array.
	 * 
	 * @return the position
	 */
	public int position() {
		return bytes.position();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(
				"This method is not supported.");
	}

	/**
	 * Read the next element from {@code bytes}.
	 */
	protected void readNext() {
		next = null;
		if(bytes.remaining() >= 4) {
			int peek = bytes.getInt();
			if(peek > 0 && bytes.remaining() >= peek) {
				byte[] _next = new byte[peek];
				bytes.get(_next);
				next = ByteBuffer.wrap(_next);
			}
			else {
				bytes.position(bytes.position() - 4);
			}
		}
	}
}