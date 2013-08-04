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

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.cache.ReferenceCache;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteables;
import org.cinchapi.common.time.Time;

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
@PackagePrivate
final class PrimaryKey extends Number implements
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
		Object[] cacheKey = { value, NIL };
		PrimaryKey key = cache.get(cacheKey);
		if(key == null) {
			key = new PrimaryKey(value);
			cache.put(key, cacheKey);
		}
		return key;
	}

	/**
	 * Encode {@code key} and {@code timestamp} into a ByteBuffer that
	 * conforms to the format specified for {@link PrimaryKey#getBytes()}.
	 * 
	 * @param key
	 * @param timestamp
	 * @return the ByteBuffer encoding
	 */
	static ByteBuffer encodeAsByteBuffer(long key, long timestamp) {
		ByteBufferOutputStream out = new ByteBufferOutputStream(SIZE);
		out.write(timestamp);
		out.write(key);
		ByteBuffer bytes = out.toByteBuffer();
		out.close();
		return bytes;
	}

	/**
	 * The start position of the encoded timestamp in {@link #bytes}.
	 */
	private static final int TS_POS = 0;

	/**
	 * The number of bytes used to encoded the timestamp in {@link #bytes}.
	 */
	private static final int TS_SIZE = Long.SIZE / 8;

	/**
	 * The start position of the encoded key in {@link #bytes}.
	 */
	private static final int KEY_POS = TS_POS + TS_SIZE;

	/**
	 * The number of bytes used to encode the key in {@link #bytes}.
	 */
	private static final int KEY_SIZE = Long.SIZE / 8;

	/**
	 * The total number of bytes used to encode each PrimaryKey.
	 */
	@PackagePrivate
	static final int SIZE = TS_SIZE + KEY_SIZE;

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
	 * <p>
	 * In order to optimize heap usage, we encode the PrimaryKey as a single
	 * ByteBuffer instead of storing each component as a member variable.
	 * </p>
	 * <p>
	 * To retrieve a component, we navigate to the appropriate position and
	 * convert the necessary bytes to the correct type, which is a cheap since
	 * binary conversion is trivial. Once a component is loaded onto the heap,
	 * it may be stored in an ReferenceCache for further future efficiency.
	 * </p>
	 * 
	 * The content conforms to the specification described by the
	 * {@link #getBytes()} method.
	 */
	private final ByteBuffer bytes;

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
	}

	/**
	 * Construct a new notForStorage instance.
	 * 
	 * @param key
	 */
	private PrimaryKey(long key) {
		this(key, NIL);
	}

	/**
	 * Construct a forStorage instance.
	 * 
	 * @param key
	 * @param timestamp
	 */
	private PrimaryKey(long key, long timestamp) {
		this.bytes = encodeAsByteBuffer(key, timestamp);
	}

	/**
	 * Compares keys such that they are sorted in descending order.
	 */
	@Override
	public int compareTo(PrimaryKey o) {
		return 1 * UnsignedLongs.compare(longValue(), o.longValue());
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
	 * <li><strong>timestamp</strong> position {@value #TS_POS}</li>
	 * <li><strong>key</strong> position {@value #KEY_POS}</li>
	 * </ol>
	 * 
	 * @return a byte array.
	 */
	@Override
	public synchronized ByteBuffer getBytes() {
		ByteBuffer clone = ByteBuffers.clone(bytes);
		clone.rewind();
		return clone;
	}

	@Override
	public synchronized long getTimestamp() {
		bytes.position(TS_POS);
		return bytes.getLong();
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
	public synchronized long longValue() {
		bytes.position(KEY_POS);
		return bytes.getLong();
	}

	@Override
	public int size() {
		return SIZE;
	}

	@Override
	public String toString() {
		int position = bytes.position();
		String string = UnsignedLongs.toString(longValue()); // for
																// compatibility
																// with
																// {@link
																// com.cinchapi.common.Numbers.compare(Number,
																// Number)}
		bytes.position(position);
		return string;
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
	@PackagePrivate
	int compareTo(PrimaryKey o, boolean logically) {
		return logically ? compareTo(o) : Storables.compare(this, o);
	}

}