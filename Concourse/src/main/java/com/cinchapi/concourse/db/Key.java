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
package com.cinchapi.concourse.db;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.time.Time;
import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedLongs;

/**
 * <p>
 * An immutable and {@link Storable} identifier for a {@link Row}. Each key is
 * based on an unsigned long, which means that the total pool of identifiers is
 * between 0 and 2^64 - 1 inclusive.
 * </p>
 * 
 * @author jnelson
 */
@Immutable
public final class Key extends Number implements Comparable<Key>, Storable {

	/**
	 * Return a key that is appropriate for storage, with the current
	 * timestamp.
	 * 
	 * @param value
	 * @return the new instance.
	 */
	public static Key forStorage(long value) {
		return new Key(value, Time.now()); // do not use cache because
											// forStorage values must have a
											// unique timestamp and will
											// thus never be duplicated
	}

	/**
	 * Return the key represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the key
	 */
	public static Key fromByteSequence(ByteBuffer bytes) {
		long key = bytes.getLong();
		long timestamp = bytes.getLong();
		return new Key(key, timestamp);
	}

	/**
	 * Return a key that is not appropriate for storage, but can be used in
	 * comparisons. This is the preferred way to create keys unless the key
	 * will be stored.
	 * 
	 * @param value
	 * @return the new instance.
	 */
	public static Key notForStorage(long value) {
		Key key = cache.get(value);
		if(key == null) {
			key = new Key(value);
			cache.put(key, value);
		}
		return key;
	}

	static final int SIZE_IN_BYTES = 2 * (Long.SIZE / 8);

	private static final long serialVersionUID = 1L;
	private static final ObjectReuseCache<Key> cache = new ObjectReuseCache<Key>();

	private final long key;
	private final long timestamp;

	/**
	 * Construct a new notForStorage instance.
	 * 
	 * @param key
	 */
	private Key(long key) {
		this(key, NIL);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param timestamp
	 */
	private Key(long key, long timestamp) {
		this.key = key;
		this.timestamp = timestamp;
	}

	/**
	 * Return a long that represents the two's complement.
	 * 
	 * @return the long value
	 */
	public long asLong() {
		return key;
	}

	/**
	 * Compares keys such that they are sorted in descending order.
	 */
	@Override
	public int compareTo(Key o) {
		return -1 * UnsignedLongs.compare(key, o.key);
	}

	@Override
	public double doubleValue() {
		return (double) key;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Key) {
			final Key other = (Key) obj;
			return Objects.equal(this.key, other.key);
		}
		return false;
	}

	@Override
	public float floatValue() {
		return (float) key;
	}

	@Override
	public byte[] getBytes() {
		return asByteBuffer().array();
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(key);
	}

	@Override
	public int intValue() {
		return (int) key;
	}

	@Override
	public boolean isForStorage() {
		return Storables.isForStorage(this);
	}

	@Override
	public boolean isNotForStorage() {
		return Storables.isNotForStorage(this);
	}

	@Override
	public long longValue() {
		return asLong();
	}

	@Override
	public int size() {
		return SIZE_IN_BYTES;
	}

	@Override
	public String toString() {
		return UnsignedLongs.toString(key); // for compatibility with {@link
											// com.cinchapi.common.Numbers.compare(Number,
											// Number)}
	}

	/**
	 * Return a new byte buffer that contains the value with the following
	 * order:
	 * <ol>
	 * <li><strong>key</strong> - first 8 bytes</li>
	 * <li><strong>timestamp</strong> - last 8 bytes</li>
	 * </ol>
	 * 
	 * @return a byte buffer.
	 */
	private ByteBuffer asByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(size());
		buffer.putLong(key);
		buffer.putLong(timestamp);
		buffer.rewind();
		return buffer;
	}

}
