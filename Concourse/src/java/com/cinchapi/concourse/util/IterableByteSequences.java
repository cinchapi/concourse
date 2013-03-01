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
package com.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * <p>
 * A class should implement this interface if it wishes to return an iterator
 * over an array of byte sequences where the number of sequences and the size of
 * each sequence is variable. This is usually appropriate for classes that
 * maintain some collection(s) of non-primitive/non-fixed-size objects that are
 * stored on disk.
 * </p>
 * <p>
 * The implementing class is responsible for maintaining the byte array that
 * holds a contiguous block of bytes representing the sequences over which to
 * iterate. Each sequence should be preceded by a 4 byte integer that specifies
 * how many bytes the next sequence contains. Pass the byte array into a
 * {@link ByteSequencesIterator#over(byte[])}, which provides built-in logic for
 * reading sequences from a correctly formatted byte array.
 * </p>
 * 
 * @author jnelson
 */
public interface IterableByteSequences {

	/**
	 * Return a {@link ByteSequencesIterator} over an array of
	 * <code>bytes</code>.
	 * 
	 * @param bytes
	 * @return the iterator.
	 */
	public ByteSequencesIterator iterator();

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
	public class ByteSequencesIterator implements Iterator<ByteBuffer> {

		/**
		 * Return a byte iterator over <code>bytes</code>.
		 * 
		 * @param bytes
		 * @return the iterator.
		 */
		public static ByteSequencesIterator over(byte[] bytes) {
			return new ByteSequencesIterator(bytes);
		}

		protected final ByteBuffer bytes;
		protected ByteBuffer next;

		/**
		 * Construct a new instance.
		 * 
		 * @param bytes
		 */
		protected ByteSequencesIterator(byte[] bytes) {
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

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"This method is not supported.");
		}

		/**
		 * Read the next element from <code>bytes</code>.
		 */
		protected void readNext() {
			next = null;
			if(bytes.remaining() >= 4) {
				int peek = bytes.getInt();
				if(peek > 0 && bytes.remaining() >= peek) {
					next = ByteBuffer.allocate(peek);
					while (next.remaining() > 0) {
						next.put(bytes.get());
					}
				}
			}
		}
	}
}
