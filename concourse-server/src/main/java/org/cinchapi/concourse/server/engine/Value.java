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
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;

import com.google.common.base.Objects;

/**
 * A statically typed {@link Storable} quantity.
 * <p>
 * A Value is the most basic element of data in Concourse. Values are both
 * temporally sortable by timestamp and logically sortable using weak typing. A
 * single value cannot exceed {@value #MAX_SIZE} bytes.
 * </p>
 * <p>
 * <h2>Storage Requirements</h2>
 * Each Value requires at least {@value #CONSTANT_SIZE} bytes of space in
 * addition to the following type specific requirements:
 * <ul>
 * <li>BOOLEAN requires an additional 1 byte</li>
 * <li>DOUBLE requires an additional 8 bytes</li>
 * <li>FLOAT requires an additional 4 bytes</li>
 * <li>INTEGER requires an additional 4 bytes</li>
 * <li>LONG requires an additional 8 bytes</li>
 * <li>RELATION requires an additional 8 bytes</li>
 * <li>STRING requires an additional 14 bytes for every character (uses UTF8
 * encoding)</li>
 * </ul>
 * </p>
 * 
 * @author jnelson
 */
@Immutable
@PackagePrivate
final class Value implements Comparable<Value>, Storable {

	/**
	 * Return a Value that is appropriate for storage, with the current
	 * timestamp.
	 * 
	 * @param quantity
	 * @return the Value
	 */
	public static Value forStorage(TObject quantity) {
		return new Value(quantity, Time.now());
	}

	/**
	 * Return the Value encoded in {@code buffer} so long as those bytes adhere
	 * to the format specified by the {@link #getBytes()} method. This method
	 * assumes that all the bytes in the {@code buffer} belong to the Value. In
	 * general, it is necessary to get the appropriate Value slice from the
	 * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
	 * 
	 * @param buffer
	 * @return the Value
	 */
	public static Value fromByteBuffer(ByteBuffer buffer) {
		return Byteables.read(buffer, Value.class); // We are using
													// Byteables#read(ByteBuffer,
													// Class) instead of calling
													// the constructor directly
													// so as to take advantage
													// of the automatic
													// reference caching that is
													// provided in the utility
													// class
	}

	/**
	 * Return a Value that is not appropriate for storage, but can be used in
	 * comparisons. This is the preferred way to create values unless the value
	 * will be stored.
	 * 
	 * @param quantity
	 * @return the Value
	 */
	public static Value notForStorage(TObject quantity) {
		Object[] cacheKey = { quantity, NIL };
		Value value = cache.get(quantity, cacheKey);
		if(value == null) {
			value = new Value(quantity);
			cache.put(value, cacheKey);
		}
		return value;
	}

	/**
	 * Get an object of {@code type} from {@code buffer}. This method will read
	 * starting from the current position up until enough bytes for {@code type}
	 * have been read. If {@code type} equals {@link Type#STRING}, all of the
	 * remaining bytes in the buffer will be read.
	 * 
	 * @param buffer
	 * @param type
	 * @return the object.
	 */
	private static TObject extractQuantity(ByteBuffer buffer, Type type) {
		Object[] cacheKey = { ByteBuffers.encodeAsHexString(buffer), type };
		TObject object = quantityCache.get(cacheKey);
		if(object == null) {
			// Must allocate a heap buffer because TObject assumes it has a
			// backing array and because of THRIFT-2104 that buffer must wrap a
			// byte array in order to assume that the TObject does not lose data
			// when transferred over the wire.
			byte[] array = new byte[buffer.remaining()];
			buffer.get(array); // We CANNOT simply slice {@code buffer} and use
								// the slice's backing array because the backing
								// array of the slice is the same as the
								// original, which contains more data than we
								// need for the quantity
			object = new TObject(ByteBuffer.wrap(array), type);
			quantityCache.put(object, cacheKey);
		}
		return object;
	}

	/**
	 * Maintains a cache of all the quantities that are extracted from
	 * ByteBuffers in the {@link #extractQuantity(ByteBuffer, Type)} method.
	 */
	private static final ReferenceCache<TObject> quantityCache = new ReferenceCache<TObject>();

	/**
	 * The number of bytes needed to encode every Value.
	 */
	@PackagePrivate
	static final int CONSTANT_SIZE = 12; // timestamp(8), type(4)

	/**
	 * The maximum number of bytes that can be used to encode a single Value.
	 */
	@PackagePrivate
	static final int MAX_SIZE = Integer.MAX_VALUE;

	/**
	 * A ReferenceCache is generated in {@link Byteables} for Values read from
	 * ByteBuffers, so this cache is only for notForStorage Values.
	 */
	private static final ReferenceCache<Value> cache = new ReferenceCache<Value>();
	private static final ValueComparator comparator = new ValueComparator();

	/**
	 * The {@code timestamp} is used to version the PrimaryKey when used as a
	 * {@link Storable} value.
	 */
	private final long timestamp;

	/**
	 * The quantity is the expressed essence of the Value.
	 */
	private final TObject quantity;
	private final transient int size;

	/**
	 * Construct an instance that represents an existing Value from a
	 * ByteBuffer. This constructor is public so as to comply with the
	 * {@link Byteable} interface. Calling this constructor directly is not
	 * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
	 * advantage of reference caching.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public Value(ByteBuffer bytes) {
		this.timestamp = bytes.getLong();
		Type type = Type.values()[bytes.getInt()];
		this.quantity = extractQuantity(bytes, type);
		this.size = bytes.capacity();
	}

	/**
	 * Construct a notForStorage instance.
	 * 
	 * @param quantity
	 */
	private Value(TObject quantity) {
		this(quantity, NIL);
	}

	/**
	 * Construct a forStorage instance.
	 * 
	 * @param quantity
	 * @param timestamp
	 */
	private Value(TObject quantity, long timestamp) {
		this.timestamp = timestamp;
		this.quantity = quantity;
		this.size = quantity.bufferForData().capacity() + CONSTANT_SIZE;
	}

	/**
	 * Temporal comparison where the value with the larger timestamp is less
	 * than the other. This enables sorting by timestamp in descending order.
	 * This method correctly accounts for comparing a forStorage value to a
	 * notForStorage one.
	 */
	@Override
	public int compareTo(Value o) {
		return compareTo(o, false);
	}

	/**
	 * Determine if the comparison to {@code o} should be done naturally or
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
	 * @see {@link Storables#compare(Storable, Storable)}
	 */
	public int compareTo(Value o, boolean logically) {
		return logically ? comparator.compare(this, o) : Storables.compare(
				this, o);
	}

	/**
	 * Logical comparison where appropriate casting is done to the encapsulated
	 * quantities (weak typing) using {@link ValueComparator}.
	 * 
	 * @param o
	 * @return a negative integer, zero, or a positive integer as this object is
	 *         less than, equal to, or greater than the specified object.
	 */
	public int compareToLogically(Value o) {
		return compareTo(o, true);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Value) {
			final Value other = (Value) obj;
			return Objects.equal(this.getQuantity(), other.getQuantity())
					&& Objects.equal(getType(), other.getType());
		}
		return false;
	}

	/**
	 * Return a byte buffer that represents the value with the following order:
	 * <ol>
	 * <li><strong>timestamp</strong> position 0</li>
	 * <li><strong>type</strong> position 8</li>
	 * <li><strong>quantity</strong> position 12</li>
	 * </ol>
	 * 
	 * @return a byte array.
	 */
	@Override
	public ByteBuffer getBytes() {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		out.write(timestamp);
		out.write(quantity.getType().ordinal());
		out.write(quantity.bufferForData());
		ByteBuffer bytes = out.toByteBuffer();
		out.close();
		return bytes;
	}

	/**
	 * Return an object that represents the encapsulated {@code quantity}.
	 * 
	 * @return the value.
	 */
	public TObject getQuantity() {
		return quantity;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Return the Value {@code type}.
	 * 
	 * @return the type
	 */
	public Type getType() {
		return quantity.getType();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(quantity, quantity.getType());
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
	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return quantity.toString();
	}
}
