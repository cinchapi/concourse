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
	 * Encode {@code quantity} and {@code timestamp} into a ByteBuffer that
	 * conforms to the format specified for {@link Value#getBytes()}.
	 * 
	 * @param quantity
	 * @param timestamp
	 * @return the ByteBuffer encoding
	 */
	public static ByteBuffer encodeAsByteBuffer(TObject quantity, long timestamp) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		out.write(timestamp);
		out.write(quantity.getType().ordinal());
		out.write(quantity.bufferForData());
		ByteBuffer bytes = out.toByteBuffer();
		out.close();
		return bytes;
	}

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
	static TObject getQuantityFromByteBuffer(ByteBuffer buffer, Type type) {
		// Must allocate a heap buffer because TObject assumes it has a
		// backing array.
		Object[] cacheKey = { ByteBuffers.encodeAsHexString(buffer), type };
		TObject object = quantityCache.get(cacheKey);
		if(object == null) {
			// Must allocate a heap buffer because TObject assumes it has a
			// backing array.
			object = new TObject(ByteBuffer.allocate(buffer.remaining()).put(
					buffer), type);
			quantityCache.put(object, cacheKey);
		}
		return object;
	}

	/**
	 * Maintains a cache of all the quantities that are extracted from
	 * ByteBuffers in the {@link #getQuantityFromByteBuffer(ByteBuffer, Type)}
	 * method.
	 */
	private static final ReferenceCache<TObject> quantityCache = new ReferenceCache<TObject>();

	/**
	 * The start position of the encoded timestamp in {@link #bytes}.
	 */
	private static final int TS_POS = 0;

	/**
	 * The number of bytes used to encoded the timestamp in {@link #bytes}.
	 */
	private static final int TS_SIZE = Long.SIZE / 8;

	/**
	 * The start position of the encoded type in {@link #bytes}.
	 */
	private static final int TYPE_POS = TS_POS + TS_SIZE;

	/**
	 * The number of bytes used to encoded the type in {@link #bytes}.
	 */
	private static final int TYPE_SIZE = Integer.SIZE / 8;

	/**
	 * The start position of the encoded quantity in {@link #bytes}.
	 */
	private static final int QTY_POS = TYPE_POS + TYPE_SIZE;
	/**
	 * The number of bytes needed to encode every Value.
	 */
	@PackagePrivate
	static final int CONSTANT_SIZE = TS_SIZE + TYPE_SIZE;

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
	 * <p>
	 * In order to optimize heap usage, we encode the Value as a single
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
		this.bytes = bytes;
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
		this.bytes = Value.encodeAsByteBuffer(quantity, timestamp);
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
	 * <li><strong>timestamp</strong> position {@value #TS_POS}</li>
	 * <li><strong>type</strong> position {@value #TYPE_POS}</li>
	 * <li><strong>quantity</strong> position {@value #QTY_POS}</li>
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

	/**
	 * Return an object that represents the encapsulated {@code quantity}.
	 * 
	 * @return the value.
	 */
	public synchronized TObject getQuantity() {
		bytes.position(QTY_POS);
		return Value.getQuantityFromByteBuffer(bytes, getType());
	}

	@Override
	public synchronized long getTimestamp() {
		bytes.position(TS_POS);
		return bytes.getLong();
	}

	/**
	 * Return the Value {@code type}.
	 * 
	 * @return the type
	 */
	public synchronized Type getType() {
		bytes.position(TYPE_POS);
		return Type.values()[bytes.getInt()];
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(getQuantity(), getType());
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
		return bytes.capacity();
	}

	@Override
	public String toString() {
		return getQuantity().toString();
	}
}
