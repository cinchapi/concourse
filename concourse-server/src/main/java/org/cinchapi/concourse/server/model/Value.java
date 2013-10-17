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
package org.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.util.Comparator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Numbers;

/**
 * A Value is an abstraction for a {@link TObject} that records type information
 * and serves as the most basic element of data in Concourse. Values are
 * logically sortable using weak typing and cannot exceed 2^32 bytes.
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
 * <li>LINK requires an additional 8 bytes</li>
 * <li>STRING requires an additional 14 bytes for every character (uses UTF8
 * encoding)</li>
 * </ul>
 * </p>
 * 
 * @author jnelson
 */
@Immutable
public final class Value implements Byteable, Comparable<Value> {

	/**
	 * Return the Value encoded in {@code bytes} so long as those bytes adhere
	 * to the format specified by the {@link #getBytes()} method. This method
	 * assumes that all the bytes in the {@code bytes} belong to the Value. In
	 * general, it is necessary to get the appropriate Value slice from the
	 * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
	 * 
	 * @param bytes
	 * @return the Value
	 */
	public static Value fromByteBuffer(ByteBuffer bytes) {
		Type type = Type.values()[bytes.get()];
		TObject data = extractTObjectAndCache(bytes, type);
		return new Value(data, bytes);
	}

	/**
	 * Return a Value that is backed by {@code data}.
	 * 
	 * @param data
	 * @return the Value
	 */
	public static Value wrap(TObject data) {
		return new Value(data);
	}

	/**
	 * Return the {@link TObject} of {@code type} represented by {@code bytes}.
	 * This method reads the remaining bytes from the current position into the
	 * returned TObject.
	 * 
	 * @param bytes
	 * @param type
	 * @return the TObject
	 */
	private static TObject extractTObjectAndCache(ByteBuffer bytes, Type type) {
		// Must allocate a heap buffer because TObject assumes it has a
		// backing array and because of THRIFT-2104 that buffer must wrap a
		// byte array in order to assume that the TObject does not lose data
		// when transferred over the wire.
		byte[] array = new byte[bytes.remaining()];
		bytes.get(array); // We CANNOT simply slice {@code buffer} and use
							// the slice's backing array because the backing
							// array of the slice is the same as the
							// original, which contains more data than we
							// need for the quantity
		return new TObject(ByteBuffer.wrap(array), type);
	}

	/**
	 * The minimum number of bytes needed to encode every Value.
	 */
	private static final int CONSTANT_SIZE = 1; // type(1)

	/**
	 * The underlying data represented by this Value. This representation is
	 * used when serializing/deserializing the data for RPC or disk and network
	 * I/O.
	 */
	private final TObject data;

	/**
	 * A cached copy of the binary representation that is returned from
	 * {@link #getBytes()}.
	 */
	private transient ByteBuffer bytes = null;

	/**
	 * The java representation of the underlying {@link #data}. This
	 * representation is used when interacting with other components in the JVM.
	 */
	private final transient Object object;

	/**
	 * Construct a new instance.
	 * 
	 * @param data
	 */
	private Value(TObject data) {
		this(data, null);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param data
	 * @param bytes
	 */
	private Value(TObject data, @Nullable ByteBuffer bytes) {
		this.data = data;
		this.object = Convert.thriftToJava(data);
		this.bytes = bytes;
	}

	@Override
	public int compareTo(Value other) {
		return Sorter.INSTANCE.compare(this, other);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Value) {
			final Value other = (Value) obj;
			return object.equals(other.object);
		}
		return false;
	}

	/**
	 * Return a byte buffer that represents this Value with the following order:
	 * <ol>
	 * <li><strong>type</strong> - position 0</li>
	 * <li><strong>data</strong> - position 1</li>
	 * </ol>
	 * 
	 * @return the ByteBuffer representation
	 */
	@Override
	public ByteBuffer getBytes() {
		if(bytes == null) {
			bytes = ByteBuffer.allocate(size());
			bytes.put((byte) data.getType().ordinal());
			bytes.put(data.bufferForData());
		}
		return ByteBuffers.asReadOnlyBuffer(bytes);
	}

	/**
	 * Return the java object that is represented by this Value.
	 * 
	 * @return the object representation
	 */
	public Object getObject() {
		return object;
	}

	/**
	 * Return the TObject that is represented by this Value.
	 * 
	 * @return the TObject representation
	 */
	public TObject getTObject() {
		return data;
	}

	/**
	 * Return the {@link Type} that describes the underlying data represented by
	 * this Value.
	 * 
	 * @return the type
	 */
	public Type getType() {
		return data.getType();
	}

	@Override
	public int hashCode() {
		return object.hashCode();
	}

	@Override
	public int size() {
		return CONSTANT_SIZE + data.bufferForData().capacity();
	}

	@Override
	public String toString() {
		return object.toString();
	}

	/**
	 * A {@link Comparator} that is used to sort Values using weak typing.
	 * 
	 * @author jnelson
	 */
	public static enum Sorter implements Comparator<Value> {
		INSTANCE;

		@Override
		public int compare(Value v1, Value v2) {
			Object o1 = v1.getObject();
			Object o2 = v2.getObject();
			if(o1 instanceof Number && o2 instanceof Number) {
				return Numbers.compare((Number) o1, (Number) o2);
			}
			else {
				return o1.toString().compareTo(o2.toString());
			}
		}
	}

}
