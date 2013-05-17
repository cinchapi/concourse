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
 * The primary identifier for a {@link FileRow}.
 * </p>
 * <p>
 * A key is {@link Containable} as the content of a {@link ColumnCell}. Each key is
 * an unsigned 8 byte long paired with an 8 byte timestamp. The total required
 * storage space is {@value #SIZE_IN_BYTES} bytes.
 * </p>
 * <p>
 * The pool of possible keys ranges from 0 to 2^64 - 1 inclusive.
 * </p>
 * 
 * @author jnelson
 */
@Immutable
final class PrimaryKey extends Number implements
		Comparable<PrimaryKey>,
		Containable {
	// NOTE: This class extends Number so that it can be treated like other
	// numerical values during comparisons. Whenever a cell contains a relation,
	// the related Key is stored as a {@link Value} which is expected to be
	// sorted amongst other values as if it were a Long.

	/**
	 * Return a key that is appropriate for storage, with the current
	 * timestamp.
	 * 
	 * @param value
	 * @return the new instance.
	 */
	public static PrimaryKey forStorage(long value) {
		// NOTE: I don't perform a cache lookup here because forStorage object
		// must have a unique timestamp and will never be duplicated on
		// creation. But, I do add the new object to the cache for lookup in the
		// event that a value is read from a byte sequence.
		PrimaryKey key = new PrimaryKey(value, Time.now());
		Object[] cacheKey = { value, key.getTimestamp() };
		cache.put(key, cacheKey);
		return key;
	}

	/**
	 * Return the key represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the key
	 */
	public static PrimaryKey fromByteSequence(ByteBuffer bytes) {
		long value = bytes.getLong();
		long timestamp = bytes.getLong();

		Object[] cacheKey = { value, timestamp };
		PrimaryKey key = cache.get(cacheKey);
		if(key == null) {
			key = new PrimaryKey(value, timestamp);
			cache.put(key, cacheKey);
		}
		return key;
	}

	/**
	 * Return a key that is not appropriate for storage, but can be used in
	 * comparisons. This is the preferred way to create keys unless the key
	 * will be stored.
	 * 
	 * @param value
	 * @return the new instance.
	 */
	public static PrimaryKey notForStorage(long value) {
		Object[] cacheKey = { value, NIL };
		PrimaryKey key = cache.get(cacheKey);
		if(key == null) {
			key = new PrimaryKey(value);
			cache.put(key, cacheKey);
		}
		return key;
	}

	static final int SIZE_IN_BYTES = 2 * (Long.SIZE / 8);

	private static final long serialVersionUID = 1L; // serializability
														// inherited from {@link
														// Number}
	private static final ObjectReuseCache<PrimaryKey> cache = new ObjectReuseCache<PrimaryKey>();

	private final long key;
	private final long timestamp;
	private transient ByteBuffer buffer = null; // initialize lazily

	/**
	 * Construct a new notForStorage instance.
	 * 
	 * @param key
	 */
	private PrimaryKey(long key) {
		this(key, NIL);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param timestamp
	 */
	private PrimaryKey(long key, long timestamp) {
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
	public int compareTo(PrimaryKey o) {
		return -1 * UnsignedLongs.compare(key, o.key);
	}

	@Override
	public double doubleValue() {
		return (double) key;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PrimaryKey) {
			final PrimaryKey other = (PrimaryKey) obj;
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
		return getBuffer().array();
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
		return Containables.isForStorage(this);
	}

	@Override
	public boolean isNotForStorage() {
		return Containables.isNotForStorage(this);
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
	 * Determine if the comparison to {@code o} should be done temporally or
	 * {@code logically}.
	 * 
	 * @param o
	 * @param logically
	 *            if {@code true} the value based comparison occurs, otherwise
	 *            based on timestamp/equality
	 * @return a negative integer, zero, or a positive integer as this object is
	 *         less than, equal to, or greater than the specified object.
	 * @see {@link #compareTo(Value)}
	 * @see {@link #compareToLogically(Value)}
	 * @see {@link Containables#compare(Containable, Containable)}
	 */
	int compareTo(PrimaryKey o, boolean logically) {
		return logically ? compareTo(o) : Containables.compare(this, o);
	}

	/**
	 * Rewind and return {@link #buffer}. Use this method instead of accessing
	 * the variable directly to ensure that it is rewound.
	 * </p>
	 * <p>
	 * The buffer is encoded with the following order:
	 * <ol>
	 * <li><strong>key</strong> - first 8 bytes</li>
	 * <li><strong>timestamp</strong> - last 8 bytes</li>
	 * </ol>
	 * </p>
	 * 
	 * @return the internal byte buffer representation
	 */
	private ByteBuffer getBuffer() {
		// NOTE: A copy of the buffer is not made for performance/space reasons.
		// I am okay with this because {@link #buffer} is only used internally.
		if(buffer == null) {
			buffer = ByteBuffer.allocate(size());
			buffer.putLong(key);
			buffer.putLong(timestamp);
		}
		buffer.rewind();
		return buffer;
	}

}
