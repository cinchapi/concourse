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
package com.cinchapi.common.io;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A {@link IterableByteSequences} object where every sequence is the same size.
 * Therefore, it only necessary to have 4 bytes at the beginning of the
 * collection that specifies how many sequences are present and another 4 bytes
 * the specifies the size of each sequence once (instead of 4 bytes before every
 * sequence in the collection).
 * 
 * @author jnelson
 */
public interface FixedSizeIterableByteSequences extends IterableByteSequences {

	/**
	 * Return a {@link FixedSizeByteSequencesIterator} over an array of
	 * {@code bytes}.
	 * 
	 * @param bytes
	 * @return the iterator.
	 */
	@Override
	public FixedSizeByteSequencesIterator iterator();

	/**
	 * An {@link Iterator} that traverses a byte array and returns sequences as
	 * a byte buffer. The iterator assumes that each the first 4 bytes of the
	 * sequence specifies the number of sequences, the next four bytes specify
	 * the size of each sequence and the remaining bytes are the sequences over
	 * which to iterate. The iterator will fail to return a next element when
	 * its has read up to the specified number of sequences or the capacity of
	 * its byte buffer is less than the specified sequence size.
	 * 
	 * @author jnelson
	 */
	public class FixedSizeByteSequencesIterator extends ByteSequencesIterator {

		/**
		 * Return a byte iterator over {@code bytes}.
		 * 
		 * @param bytes
		 * @return the iterator.
		 */
		public static FixedSizeByteSequencesIterator over(byte[] bytes) {
			return new FixedSizeByteSequencesIterator(bytes);
		}

		private final int numSequences;
		private final int sequenceSize;
		private int nextSequence = 0;

		/**
		 * Construct a new instance.
		 * 
		 * @param bytes
		 */
		protected FixedSizeByteSequencesIterator(byte[] bytes) {
			super(bytes);
			this.numSequences = this.bytes.getInt();
			this.sequenceSize = this.bytes.getInt();
			readFixedNext();
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public ByteBuffer next() {
			ByteBuffer next = this.next;
			readFixedNext();
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"This method is not supported.");
		}

		private void readFixedNext() {
			next = null;
			if(nextSequence < numSequences && bytes.remaining() >= sequenceSize) {
				next = ByteBuffer.allocate(sequenceSize);
				while (next.remaining() > 0) {
					next.put(bytes.get());
				}
			}
		}

		@Override
		protected void readNext() {} // do nothing
	}

}
