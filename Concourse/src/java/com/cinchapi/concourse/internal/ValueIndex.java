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
package com.cinchapi.concourse.internal;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;

import com.cinchapi.common.Strings;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.concourse.io.ByteSized;
import com.cinchapi.concourse.io.ByteSizedCollections;
import com.google.common.collect.Sets;

/**
 * Encapsulates the mapping of a value to a set of {@link Key} objects.
 * 
 * @author jnelson
 */
class ValueIndex implements Comparable<ValueIndex>, ByteSized {
	// NOTE: This class does not define hashCode() or equals() because the
	// defaults are the desired behaviour.

	/**
	 * Return the value index represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the value index
	 */
	static ValueIndex fromByteSequence(ByteBuffer bytes) {
		int valueSize = bytes.getInt();

		byte[] valueBytes = new byte[valueSize];
		bytes.get(valueBytes);
		Value value = Value.fromByteSequence(ByteBuffer.wrap(valueBytes));

		KeySet keys = KeySet.fromByteSequences(bytes);

		return new ValueIndex(value, keys);
	}

	/**
	 * Return a new {@link ValueIndex} for {@code value}.
	 * 
	 * @param value
	 * @return the value index
	 */
	static ValueIndex forValue(Value value) {
		return new ValueIndex(value, KeySet.newInstance());
	}

	private static final int FIXED_SIZE_IN_BYTES = Integer.SIZE / 8; // valueSize

	private final Value value;
	private final KeySet keys;

	/**
	 * Construct a new instance.
	 * 
	 * @param value
	 * @param keys
	 */
	private ValueIndex(Value value, KeySet keys) {
		this.value = value;
		this.keys = keys;
	}

	@Override
	public int compareTo(ValueIndex o) {
		return this.value.compareToLogically(o.value);
	}

	@Override
	public byte[] getBytes() {
		return asByteBuffer().array();
	}

	@Override
	public int size() {
		return FIXED_SIZE_IN_BYTES + value.size()
				+ (keys.size() * Key.SIZE_IN_BYTES);
	}

	/**
	 * Add {@code key} to the index.
	 * 
	 * @param key
	 */
	void add(Key key) {
		keys.add(key);
	}

	/**
	 * Return the indexed {@code keys}.
	 * 
	 * @return the keys
	 */
	HashSet<Key> getKeys() {
		return (HashSet<Key>) Collections.unmodifiableSet(keys.keys);
	}

	/**
	 * Return the indexed {@code value}.
	 * 
	 * @return the value
	 */
	Value getValue() {
		return value;
	}

	/**
	 * Return {@code true} if the index is empty.
	 * 
	 * @return {@code true} if there are no keys
	 */
	boolean isEmpty() {
		return keys.keys.isEmpty();
	}

	/**
	 * Remove {@code key} from the index.
	 * 
	 * @param key
	 */
	void remove(Key key) {
		keys.remove(key);
	}

	/**
	 * Return a new byte buffer that contains the current view of the cell with
	 * the following order:
	 * <ol>
	 * <li><strong>valueSize</strong> - first 4 bytes</li>
	 * <li><strong>value</strong> - next valueSize bytes</li>
	 * <li><strong>keys</strong> - remaining bytes</li>
	 * </ol>
	 * 
	 * <strong>NOTE</strong>: The <em>keys</em> bytes conform to the interface
	 * for {@link IterableByteSequences}.
	 * 
	 * @return a byte buffer.
	 */
	private ByteBuffer asByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(size());
		buffer.putInt(value.size());
		buffer.put(value.getBytes());
		buffer.put(keys.getBytes());
		buffer.rewind();
		return buffer;
	}

	/**
	 * A {@link ByteSized} collection of {@link Key} objects that is used in a
	 * {@link ValueIndex}.
	 * 
	 * 
	 * @author jnelson
	 */
	private static final class KeySet implements ByteSized {
		// NOTE: This class does not define hashCode() or equals() because the
		// defaults are the desired behaviour.

		/**
		 * Return the key set represented by {@code bytes}. Use this method when
		 * reading and reconstructing from a file. This method assumes that
		 * {@code bytes} was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 * @return the key set
		 */
		static KeySet fromByteSequences(ByteBuffer bytes) {
			HashSet<Key> keys = Sets.newHashSetWithExpectedSize(bytes
					.capacity() / Key.SIZE_IN_BYTES);
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(bytes.array());
			while (bsit.hasNext()) {
				keys.add(Key.fromLong(bsit.next().getLong()));
			}
			return new KeySet(keys);
		}

		/**
		 * Return a new and empty {@link KeySet}.
		 * 
		 * @return the key set
		 */
		static KeySet newInstance() {
			HashSet<Key> keys = Sets.newHashSet();
			return new KeySet(keys);
		}

		private HashSet<Key> keys;

		/**
		 * Construct a new instance.
		 * 
		 * @param keys
		 */
		private KeySet(HashSet<Key> keys) {
			this.keys = keys;
		}

		@Override
		public byte[] getBytes() {
			return ByteSizedCollections.toByteArray(keys);
		}

		@Override
		public int size() {
			return keys.size() * Key.SIZE_IN_BYTES;
		}

		@Override
		public String toString() {
			return Strings.toString(this);
		}

		/**
		 * Add {@code key}.
		 * 
		 * @param key
		 */
		void add(Key key) {
			keys.add(key);
		}

		/**
		 * Remove {@code key}.
		 * 
		 * @param key
		 */
		void remove(Key key) {
			keys.remove(key);
		}
	}
}
