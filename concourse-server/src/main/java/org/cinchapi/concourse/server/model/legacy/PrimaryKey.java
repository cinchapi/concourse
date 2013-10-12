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
package org.cinchapi.concourse.server.model.legacy;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.cache.ReferenceCache;
import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedLongs;

/**
 * A {@link Storable} primary identifier for a normalized {@link Record}.
 * <p>
 * Each key is an unsigned 8 byte long paired with an 8 byte timestamp. The
 * total required storage space is {@value #SIZE} bytes.
 * </p>
 * <p>
 * The pool of possible keys ranges from 0 to 2^64 1 inclusive.
 * </p>
 * 
 * @author jnelson
 */
// NOTE: This class extends Number so that it can be treated like other
// numerical values during comparisons. Whenever a field contains a link,
// the linked PrimaryKey is stored as a {@link Value} which is expected to be
// sorted amongst other values as if it were a Long.
@Immutable
@Deprecated
public final class PrimaryKey extends Number implements
		Comparable<PrimaryKey>,
		Storable {

	/**
	 * Return a key that is appropriate for storage, with the current
	 * timestamp.
	 * 
	 * @param value
	 * @return the PrimaryKey
	 */
	public static PrimaryKey forStorage(long value) {
		return new PrimaryKey(value, Time.now());
	}

	/**
	 * Return the PrimaryKey encoded in {@code buffer} so long as those bytes
	 * adhere to the format specified by the {@link #getBytes()} method. This
	 * method assumes that all the bytes in the {@code buffer} belong to the
	 * Value. In general, it is necessary to get the appropriate Value slice
	 * from the parent ByteBuffer using
	 * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
	 * 
	 * @param buffer
	 * @return the PrimaryKey
	 */
	public static PrimaryKey fromByteBuffer(ByteBuffer buffer) {
		return Byteables.read(buffer, PrimaryKey.class); // We are using
															// Byteables#read(ByteBuffer,
															// Class) instead of
															// calling
															// the constructor
															// directly
															// so as to take
															// advantage
															// of the automatic
															// reference caching
															// that is
															// provided in the
															// utility
															// class
	}

	/**
	 * Return a PrimaryKey that is not appropriate for storage, but can be used
	 * in comparisons. This is the preferred way to create keys unless the key
	 * will be stored.
	 * 
	 * @param value
	 * @return the PrimaryKey
	 */
	public static PrimaryKey notForStorage(long value) {
		Object[] cacheKey = { value, NO_TIMESTAMP };
		PrimaryKey key = cache.get(cacheKey);
		if(key == null) {
			key = new PrimaryKey(value);
			cache.put(key, cacheKey);
		}
		return key;
	}

	@PackagePrivate
	static final int SIZE = 16; // timestamp, number

	/**
	 * A ReferenceCache is generated in {@link Byteables} for PrimaryKeys read
	 * from ByteBuffers, so this cache is only for notForStorage PrimaryKeys.
	 */
	private static final ReferenceCache<PrimaryKey> cache = new ReferenceCache<PrimaryKey>();

	/**
	 * Serializability is inherited from {@link Number}.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * The {@code timestamp} is used to version the PrimaryKey when used as a
	 * {@link Storable} value.
	 */
	private final long timestamp;

	/**
	 * The {@code number} captures the numerical value quantity.
	 */
	private final long number;

	/**
	 * Master byte sequence that represents this object. Read-only duplicates
	 * are made when returning from {@link #getBytes()}.
	 */
	private final transient ByteBuffer bytes;

	/**
	 * Construct an instance that represents an existing PrimaryKey from a
	 * ByteBuffer. This constructor is public so as to comply with the
	 * {@link Byteable} interface. Calling this constructor directly is not
	 * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
	 * advantage of reference caching.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public PrimaryKey(ByteBuffer bytes) {
		this.bytes = bytes;
		this.timestamp = bytes.getLong();
		this.number = bytes.getLong();
	}

	/**
	 * Construct a new notForStorage instance.
	 * 
	 * @param number
	 */
	private PrimaryKey(long number) {
		this(number, NO_TIMESTAMP);
	}

	/**
	 * Construct a forStorage instance.
	 * 
	 * @param number
	 * @param timestamp
	 */
	private PrimaryKey(long number, long timestamp) {
		this.number = number;
		this.timestamp = timestamp;
		this.bytes = ByteBuffer.allocate(SIZE);
		this.bytes.putLong(timestamp);
		this.bytes.putLong(number);
	}

	/**
	 * Compares keys such that they are sorted in ascending order.
	 */
	@Override
	public int compareTo(PrimaryKey o) {
		return UnsignedLongs.compare(longValue(), o.longValue());
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
	 * @see {@link #compareTo(PrimaryKey)}
	 * @see {@link Storables#compare(Storable, Storable)}
	 */
	public int compareTo(PrimaryKey o, boolean logically) {
		return logically ? compareTo(o) : Storables.compare(this, o);
	}

	@Override
	public double doubleValue() {
		return (double) longValue();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PrimaryKey) {
			final PrimaryKey other = (PrimaryKey) obj;
			return UnsignedLongs.compare(longValue(), other.longValue()) == 0;
		}
		return false;
	}

	@Override
	public float floatValue() {
		return (float) longValue();
	}

	/**
	 * Return a byte array that represents the value with the following order:
	 * <ol>
	 * <li><strong>timestamp</strong> position 0</li>
	 * <li><strong>key</strong> position 8</li>
	 * </ol>
	 * 
	 * @return a byte array.
	 */
	@Override
	public ByteBuffer getBytes() {
		return ByteBuffers.asReadOnlyBuffer(bytes);
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(longValue());
	}

	@Override
	public int intValue() {
		return (int) longValue();
	}

	@Override
	public boolean isForStorage() {
		return Storables.isForStorage(this);
	}

	@Override
	public boolean isNotForStorage() {
		return Storables.isNotForStorage(this);
	}

	/**
	 * Return a long that represents the two's complement.
	 * 
	 * @return the long value
	 */
	@Override
	public long longValue() {
		return number;
	}

	@Override
	public int size() {
		return SIZE;
	}

	@Override
	public String toString() {
		String string = UnsignedLongs.toString(longValue()); // for
																// compatibility
																// with
																// {@link
																// com.cinchapi.common.Numbers.compare(Number,
																// Number)}
		return string;
	}

}