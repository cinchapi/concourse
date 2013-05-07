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
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.time.Time;
import com.cinchapi.common.util.Hash;
import com.cinchapi.common.util.Strings;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.*;

/**
 * <p>
 * A typed quantity that is {@link Bucketable} within a {@link RowCell} and is
 * also the identifier for a {@link ColumnCell}.
 * </p>
 * <p>
 * A Value is a versioned<sup>1</sup> wrapper for a generic object that handles
 * logical sorting amongst the various {@link Type}s and temporal sorting
 * between forStorage and notForStorage instances. A Value is the most basic
 * element of data in Concourse. A single value cannot exceed 2GB in memory. <br>
 * <br>
 * <sup>1</sup> - The timestamp is unique per values and acts as a defacto
 * version id
 * </p>
 * <p>
 * <h2>Storage Requirements</h2>
 * Each Value requires at least {@value #MIN_SIZE_IN_BYTES} bytes of space.
 * Additional space requirements are as follows:
 * <ul>
 * <li>BOOLEAN requires an additional 1 byte</li>
 * <li>DOUBLE requires an additional 8 bytes</li>
 * <li>FLOAT requires an additional 4 bytes</li>
 * <li>INTEGER requires an additional 4 bytes</li>
 * <li>LONG requires an additional 8 bytes</li>
 * <li>RELATION requires an additional 8 bytes</li>
 * <li>STRING requires an additional 1-4 bytes for every character (uses UTF-8
 * encoding)</li>
 * </ul>
 * <strong>NOTE</strong>: There is no way to recommend a precise storage measure
 * for a generic value, but the weighted size of
 * {@value #WEIGHTED_SIZE_IN_BYTES} bytes can be used with caution as a general
 * guide.
 * </p>
 * 
 * @author jnelson
 */
@Immutable
final class Value implements Comparable<Value>, Bucketable {

	/**
	 * Return a value that is appropriate for storage, with the current
	 * timestamp.
	 * 
	 * @param quantity
	 * @return the new instance.
	 */
	public static Value forStorage(Object quantity) {
		// NOTE: I don't perform a cache lookup here because forStorage object
		// must have a unique timestamp and will never be duplicated on
		// creation. But, I do add the new object to the cache for lookup in the
		// event that a value is read from a byte sequence.
		Value value = new Value(quantity, Time.now());
		Object[] cacheKey = { value.getQuantity(), value.getTimestamp() };
		cache.put(value, cacheKey);
		return value;
	}

	/**
	 * Return the value represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the value
	 */
	public static Value fromByteSequence(ByteBuffer bytes) {
		long timestamp = bytes.getLong();
		Type type = Type.values()[bytes.getInt()];
		int qtySize = bytes.getInt();

		byte[] qty = new byte[qtySize];
		bytes.get(qty);
		ByteBuffer quantity = ByteBuffer.wrap(qty);
		quantity.rewind();

		// The buffer -> object conversation happens both when calculating the
		// #cacheKey and when creating the Value, but it is okay perf wise
		// because the converted #object gets cached
		// @see {Utilities#getObjectFromByteBuffer}
		Object[] cacheKey = {
				Utilities.getObjectFromByteBuffer(quantity, type), timestamp };
		Value value = cache.get(cacheKey);
		if(value == null) {
			value = new Value(quantity, type, timestamp);
			cache.put(value, cacheKey);
		}
		return value;
	}

	/**
	 * Return a value that is not appropriate for storage, but can be used in
	 * comparisons. This is the preferred way to create values unless the value
	 * will be stored.
	 * 
	 * @param quantity
	 * @return the new instance.
	 */
	public static Value notForStorage(Object quantity) {
		Object[] cacheKey = { quantity, NIL };
		Value value = cache.get(quantity, cacheKey);
		if(value == null) {
			value = new Value(quantity);
			cache.put(value, cacheKey);
		}
		return value;
	}

	private static final ObjectReuseCache<Value> cache = new ObjectReuseCache<Value>();
	private static final int FIXED_SIZE_IN_BYTES = (2 * (Integer.SIZE / 8))
			+ (Long.SIZE / 8);
	private static final int MAX_QUANTITY_SIZE_IN_BYTES = Integer.MAX_VALUE
			- FIXED_SIZE_IN_BYTES;
	private static final ValueComparator comparator = new ValueComparator();

	/**
	 * <p>
	 * Every value requires a minimum of {@value #MIN_SIZE_IN_BYTES} bytes plus
	 * <em>additional</em> storage according to the following rules:
	 * <ul>
	 * <li>BOOLEAN requires an additional 1 byte</li>
	 * <li>DOUBLE requires an additional 8 bytes</li>
	 * <li>FLOAT requires an additional 4 bytes</li>
	 * <li>INTEGER requires an additional 4 bytes</li>
	 * <li>LONG requires an additional 8 bytes</li>
	 * <li>RELATION requires an additional 8 bytes</li>
	 * <li>STRING requires an additional 1-4 bytes for every character (uses
	 * UTF-8 encoding)</li>
	 * </ul>
	 * </p>
	 */
	public static final int MIN_SIZE_IN_BYTES = FIXED_SIZE_IN_BYTES;

	/**
	 * <p>
	 * This value attempts to express average quantity size based on the
	 * expected nature of stored data. The individual weights are fairly
	 * arbitrary, but the trends are very intentional (i.e. data is expected to
	 * be weighted most heavily towards storing relations).
	 * </p>
	 * <p>
	 * The weighting is broken down as follows:
	 * <ul>
	 * <li>BOOLEAN quantities are weighed at <strong>5 percent</strong></li>
	 * <li>DOUBLE quantities are weighed at <strong>12 percent</strong></li>
	 * <li>FLOAT quantities are weighed at <strong>7.5 percent</strong></li>
	 * <li>INTEGER quantities are weighed at <strong>7.5 percent</strong></li>
	 * <li>LONG quantities are weighed at <strong>12 percent</strong></li>
	 * <li>RELATION quantities are weighed at <strong>28 percent</strong></li>
	 * <li>STRING quantities are weighed at <strong>28 percent</strong>
	 * (assumption is a 100 byte string)</li>
	 * </ul>
	 * </p>
	 */
	public static final int WEIGHTED_QTY_SIZE_IN_BYTES = (int) ((.05 * 1)
			+ (.12 * 8) + (.075 * 4) + (.075 * 4) + (.12 * 8) + (.28 * 8) + (.28 * 100));

	/**
	 * This is a more realistic measure of the storage required for a single
	 * value. This value combines the minimally required
	 * {@value #MIN_SIZE_IN_BYTES} bytes with the additional weighted
	 * expectation of {@value #WEIGHTED_QTY_SIZE_IN_BYTES} bytes for the
	 * quantity.
	 */
	public static final int WEIGHTED_SIZE_IN_BYTES = MIN_SIZE_IN_BYTES
			+ WEIGHTED_QTY_SIZE_IN_BYTES;

	private final long timestamp;
	private final ByteBuffer quantity;
	private final Type type;
	private final int size;
	private transient ByteBuffer buffer = null; // initialize lazily

	/**
	 * Construct a new instance.
	 * 
	 * @param quantity
	 * @param type
	 * @param timestamp
	 */
	private Value(ByteBuffer quantity, Type type, long timestamp) {
		// NOTE: A copy of the #quantity is not made for performance/space
		// reasons. I am okay with this because {@link #quantity} is only used
		// locally and is never modified
		checkNotNull(quantity);
		checkNotNull(type);
		this.quantity = quantity;
		this.type = type;
		this.timestamp = timestamp;
		this.size = getQuantityBuffer().capacity() + FIXED_SIZE_IN_BYTES;
	}

	/**
	 * Construct a new notForStorage instance.
	 * 
	 * @param quantity
	 */
	private Value(Object quantity) {
		this(quantity, NIL);
	}

	/**
	 * Construct a new forStorage instance.
	 * 
	 * @param quantity
	 * @param timestamp
	 */
	private Value(Object quantity, long timestamp) {
		this(Utilities.getByteBufferForObject(quantity), Type.of(quantity),
				timestamp);
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

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Value) {
			final Value other = (Value) obj;
			return Objects.equal(this.getQuantity(), other.getQuantity())
					&& Objects.equal(type, type);
		}
		return false;
	}

	/**
	 * Return a byte array that represents the value with the following order:
	 * <ol>
	 * <li><strong>timestamp</strong> - first 8 bytes</li>
	 * <li><strong>type</strong> - next 4 bytes</li>
	 * <li><strong>quantitySize</strong> - next 4 bytes</li>
	 * <li><strong>quantity</strong> - remaining bytes</li>
	 * </ol>
	 * 
	 * @return a byte array.
	 */
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
		return Objects.hashCode(getQuantity(), type);
	}

	@Override
	public boolean isForStorage() {
		return Bucketables.isForStorage(this);
	}

	@Override
	public boolean isNotForStorage() {
		return Bucketables.isNotForStorage(this);
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return getQuantity() + " : " + Strings.toString(this);
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
	 * @see {@link Bucketables#compare(Bucketable, Bucketable)}
	 */
	int compareTo(Value o, boolean logically) {
		return logically ? comparator.compare(this, o) : Bucketables.compare(
				this, o);
	}

	/**
	 * Logical comparison where appropriate casting is done to the encapsulated
	 * quantities.
	 * 
	 * @param o
	 * @return a negative integer, zero, or a positive integer as this object is
	 *         less than, equal to, or greater than the specified object.
	 */
	int compareToLogically(Value o) {
		return compareTo(o, true);
	}

	/**
	 * Return an object that represents the encapsulated {@code quantity}.
	 * 
	 * @return the value.
	 */
	Object getQuantity() {
		return Utilities.getObjectFromByteBuffer(getQuantityBuffer(), type);
	}

	/**
	 * Return a string description of the value type.
	 * 
	 * @return the value type
	 */
	String getType() {
		return type.toString();
	}

	/**
	 * <p>
	 * Rewind and return {@link #buffer}. Use this method instead of accessing
	 * the variable directly to ensure that it is rewound.
	 * </p>
	 * <p>
	 * The buffer is encoded with the following order:
	 * <ol>
	 * <li><strong>timestamp</strong> - first 8 bytes</li>
	 * <li><strong>type</strong> - next 4 bytes</li>
	 * <li><strong>quanitySize</strong> - next 4 bytes</li>
	 * <li><strong>quantity</strong> - remaining bytes</li>
	 * </ol>
	 * </p>
	 * 
	 * @return the internal byte buffer representation
	 */
	private ByteBuffer getBuffer() {
		// NOTE: A copy of the buffer is not made for performance/space reasons.
		// I am okay with this because {@link #buffer} is only used internally.
		if(buffer == null) {
			buffer = ByteBuffer.allocate(this.size);
			buffer.put(ByteBuffers.toByteBuffer(timestamp));
			buffer.put(ByteBuffers.toByteBuffer(type.ordinal()));
			buffer.put(ByteBuffers
					.toByteBuffer(this.size - FIXED_SIZE_IN_BYTES));
			buffer.put(getQuantityBuffer());

		}
		buffer.rewind();
		return buffer;
	}

	/**
	 * Rewind and return {@link #quantity}. Use this method instead of accessing
	 * the variable directly to ensure that it is rewound.
	 * 
	 * @return
	 */
	private ByteBuffer getQuantityBuffer() {
		quantity.rewind();
		return quantity;
	}

	/**
	 * Internal utility functions.
	 * 
	 * @author jnelson
	 */
	private abstract static class Utilities {

		private static final ObjectReuseCache<Object> objectCache = new ObjectReuseCache<Object>();

		/**
		 * Encode a {@link ByteBuffer} as a hex string.
		 * 
		 * @param buffer
		 * @return the hex string
		 */
		static String encodeHexString(ByteBuffer buffer) {
			return Hash.toString(buffer.array());
		}

		/**
		 * Return a {@link ByteBuffer} that represents {@code object}.
		 * 
		 * @param object
		 * @return the byte buffer.
		 */
		static ByteBuffer getByteBufferForObject(Object object) {
			Type type = Type.of(object);
			return getByteBufferForObject(object, type);
		}

		/**
		 * Return a {@link ByteBuffer} that represents {@code object} of
		 * {@code type}.
		 * 
		 * @param object
		 * @return the byte buffer.
		 */
		static ByteBuffer getByteBufferForObject(Object object, Type type) {
			ByteBuffer buffer = null;

			switch (type) {
			case BOOLEAN:
				buffer = ByteBuffers.toByteBuffer((boolean) object);
				break;
			case DOUBLE:
				buffer = ByteBuffers.toByteBuffer((double) object);
				break;
			case FLOAT:
				buffer = ByteBuffers.toByteBuffer((float) object);
				break;
			case INTEGER:
				buffer = ByteBuffers.toByteBuffer((int) object);
				break;
			case LONG:
				buffer = ByteBuffers.toByteBuffer((long) object);
				break;
			case RELATION:
				buffer = ByteBuffers.toByteBuffer(((Key) (object)).asLong());
				break;
			default:
				String _object = object.toString();
				checkArgument(
						_object.getBytes(ByteBuffers.charset()).length < MAX_QUANTITY_SIZE_IN_BYTES,
						"Cannot create a byte buffer for %s because it is larger than the %s maximum allowed bytes",
						object, MAX_QUANTITY_SIZE_IN_BYTES);
				buffer = ByteBuffers.toByteBuffer(object.toString());
				break;
			}

			return buffer;
		}

		/**
		 * Return the object of {@code type} that is represented by
		 * {@code buffer}
		 * 
		 * @param buffer
		 * @param type
		 * @return the object.
		 */
		static Object getObjectFromByteBuffer(ByteBuffer buffer, Type type) {
			Object[] cacheKey = { encodeHexString(buffer), type };
			Object object = objectCache.get(cacheKey);
			if(object == null) {
				switch (type) {
				case BOOLEAN:
					object = ByteBuffers.getBoolean(buffer);
					break;
				case DOUBLE:
					object = ByteBuffers.getDouble(buffer);
					break;
				case FLOAT:
					object = ByteBuffers.getFloat(buffer);
					break;
				case INTEGER:
					object = ByteBuffers.getInt(buffer);
					break;
				case LONG:
					object = ByteBuffers.getLong(buffer);
					break;
				case RELATION:
					object = Key.notForStorage(ByteBuffers.getLong(buffer));
					break;
				default:
					object = ByteBuffers.getString(buffer);
					break;
				}
				objectCache.put(object, cacheKey);
			}
			return object;
		}

	}
}
