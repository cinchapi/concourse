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
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.cache.ReferenceCache;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.io.Byteables;
import org.cinchapi.concourse.thrift.TObject;

/**
 * A {@code Write} is the temporary representation of revision before it is
 * stored in the {@link Database} and indexed.
 * <p>
 * A Write is a necessary component for two reasons:
 * <ul>
 * <li>
 * It is the bridge from the Java primitives accepted in API to the the engine
 * friendly counterparts which contain necessary metadata (i.e timestamp).</li>
 * <li>It ensures that metadata does not change when storage contexts change
 * (i.e. we keep the same timestamp when moving a revision from a {@link Buffer}
 * to the {@link Database}.</li>
 * </ul>
 * </p>
 * 
 * @author jnelson
 */
@PackagePrivate
@Immutable
final class Write implements Byteable {

	/**
	 * Return a forStorage Write that represents a revision to ADD {@code key}
	 * as {@code value} TO {@code record}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return the Write
	 */
	public static Write add(String key, TObject value, long record) {
		return new Write(WriteType.ADD, Text.fromString(key),
				Value.forStorage(value), PrimaryKey.forStorage(record));
	}

	/**
	 * Return the Write encoded in {@code buffer} so long as those bytes adhere
	 * to the format specified by the {@link #getBytes()} method. This method
	 * assumes that all the bytes in the {@code buffer} belong to the Value. In
	 * general, it is necessary to get the appropriate Write slice from the
	 * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
	 * 
	 * @param buffer
	 * @return the Value
	 */
	public static Write fromByteBuffer(ByteBuffer buffer) {
		return Byteables.read(buffer, Write.class);
	}

	/**
	 * Return a notForStorage Write that represents any revision involving
	 * {@code key} and {@code value} in {@code record}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return the Write
	 */
	public static Write notForStorage(String key, TObject value, long record) {
		Object[] cacheKey = { key, value, record };
		Write write = cache.get(cacheKey);
		if(write == null) {
			write = new Write(WriteType.NOT_FOR_STORAGE, Text.fromString(key),
					Value.notForStorage(value),
					PrimaryKey.notForStorage(record));
		}
		return write;
	}

	/**
	 * Return a forStorage Write that represents a revision to REMOVE
	 * {@code key} as {@code value} FROM {@code record}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return the Write
	 */
	public static Write remove(String key, TObject value, long record) {
		return new Write(WriteType.REMOVE, Text.fromString(key),
				Value.forStorage(value), PrimaryKey.forStorage(record));
	}

	private static final ReferenceCache<Write> cache = new ReferenceCache<Write>();

	/**
	 * The start position of the encoded type in {@link #bytes}.
	 */
	private static final int TYPE_POS = 0;

	/**
	 * The number of bytes used to encoded the type in {@link #bytes}.
	 */
	private static final int TYPE_SIZE = Integer.SIZE / 8;

	/**
	 * The start position of the encoded record in {@link #bytes}.
	 */
	private static final int RECORD_POS = TYPE_POS + TYPE_SIZE;

	/**
	 * The number of bytes used to encoded the record in {@link #bytes}.
	 */
	private static final int RECORD_SIZE = PrimaryKey.SIZE;

	/**
	 * The start position of the encoded keySize in {@link #bytes}.
	 */
	@PackagePrivate
	static final int KEY_SIZE_POS = RECORD_POS + RECORD_SIZE;

	/**
	 * The number of bytes used to encoded the keySize in {@link #bytes}.
	 */
	private static final int KEY_SIZE_SIZE = Integer.SIZE / 8;

	/**
	 * The start position of the encoded valueSize in {@link #bytes}.
	 */
	@PackagePrivate
	static final int VALUE_SIZE_POS = KEY_SIZE_POS + KEY_SIZE_SIZE;

	/**
	 * The number of bytes used to encoded the valueSize in {@link #bytes}.
	 */
	private static final int VALUE_SIZE_SIZE = Integer.SIZE / 8;

	/**
	 * The start position of the encoded key in {@link #bytes}.
	 */
	@PackagePrivate
	private static final int KEY_POS = VALUE_SIZE_POS + VALUE_SIZE_SIZE;

	/**
	 * The start position of the encoded value {@link #bytes}.
	 */
	@PackagePrivate
	private final int VALUE_POS;

	/**
	 * <p>
	 * In order to optimize heap usage, we encode the Write as a single
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

	// Cached components that are encoded in {@link #bytes}
	private transient PrimaryKey record = null;
	private transient Text key = null;
	private transient Value value = null;
	private transient WriteType type = null;

	/**
	 * Construct an instance that represents an existing Write from a
	 * ByteBuffer. This constructor is public so as to comply with the
	 * {@link Byteable} interface. Calling this constructor directly is not
	 * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
	 * advantage of reference caching.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public Write(ByteBuffer bytes) {
		this.bytes = bytes;
		this.VALUE_POS = KEY_POS + Writes.getKeySize(bytes);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param type
	 * @param key
	 * @param value
	 * @param record
	 */
	@DoNotInvoke
	public Write(WriteType type, Text key, Value value, PrimaryKey record) {
		this.VALUE_POS = KEY_POS + key.size();
		this.bytes = Writes.encodeAsByteBuffer(type, key, value, record);
		this.record = record;
		this.key = key;
		this.value = value;
		this.type = type;
	}

	/**
	 * Two Writes are considered equal if their associated records, keys and
	 * values are considered equal. This means that Writes with different types
	 * may be considered equal.
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Write) {
			Write other = (Write) obj;
			return getRecord().equals(other.getRecord())
					&& getKey().equals(other.getKey())
					&& getValue().equals(other.getValue());
		}
		return false;
	}

	/**
	 * Return a byte buffer that represents the write with the following order:
	 * <ol>
	 * <li><strong>type</strong> - position {@value #TYPE_POS}</li>
	 * <li><strong>record</strong> - position {@value #RECORD_POS}</li>
	 * <li><strong>keySize</strong> - position {@value #KEY_SIZE_POS}</li>
	 * <li><strong>valueSize</strong> - position {@value #VALUE_SIZE_POS}</li>
	 * <li><strong>key</strong> - position {@value #KEY_POS}</li>
	 * <li><strong>value</strong> - position (key) + keySize</li>
	 * </ol>
	 * 
	 * @return a byte array.
	 */
	@Override
	public ByteBuffer getBytes() {
		bytes.rewind();
		return bytes;
	}

	/**
	 * The hash code for a Write is based on the associated record, key and
	 * value.
	 */
	@Override
	public int hashCode() {
		return Objects.hash(getRecord(), getKey(), getValue());
	}

	@Override
	public int size() {
		return bytes.capacity();
	}

	@Override
	public String toString() {
		return Writes.describe(this);
	}

	/**
	 * Return the {@code key} associated with this Write.
	 * 
	 * @return the {@code key}
	 */
	@PackagePrivate
	Text getKey() {
		if(key == null) {
			key = Text.fromByteBuffer(ByteBuffers.slice(bytes, KEY_POS,
					Writes.getKeySize(bytes)));
		}
		return key;
	}

	/**
	 * Return the {@code record} associated with this Write.
	 * 
	 * @return the {@code record}
	 */
	@PackagePrivate
	PrimaryKey getRecord() {
		if(record == null) {
			record = PrimaryKey.fromByteBuffer(ByteBuffers.slice(bytes,
					RECORD_POS, RECORD_SIZE));
		}
		return record;
	}

	/**
	 * Return the {@code timestamp} of the {@code value} associated with this
	 * Write. This is equivalent to calling {@link #getValue()}
	 * {@link Value#getTimestamp()}.
	 * 
	 * @return the {@code timestamp}
	 */
	@PackagePrivate
	long getTimestamp() {
		return getValue().getTimestamp();
	}

	/**
	 * Return the write {@code type} associated with this Write.
	 * 
	 * @return the write {@code type}
	 */
	@PackagePrivate
	WriteType getType() {
		if(type == null) {
			bytes.position(TYPE_POS);
			type = ByteBuffers.getEnum(bytes, WriteType.class);
		}
		return type;
	}

	/**
	 * Return the {@code value} associated with this Write.
	 * 
	 * @return the {@code value}
	 */
	@PackagePrivate
	Value getValue() {
		if(value == null) {
			value = Value.fromByteBuffer(ByteBuffers.slice(bytes, VALUE_POS,
					Writes.getValueSize(bytes)));
		}
		return value;
	}

	/**
	 * Return {@code true} if the Write is forStorage, meaning both the
	 * {@code record} and {@code value} are forStorage.
	 * 
	 * @return {@code true} if the Write is forStorage
	 */
	@PackagePrivate
	boolean isForStorage() {
		return getType() != WriteType.NOT_FOR_STORAGE;
	}

	/**
	 * Return {@code true} if the Write is notForStorage, meaning both the
	 * {@code record} and {@code value} are notForStorage.
	 * 
	 * @return {@code true} if the Write is notForStorage
	 */
	@PackagePrivate
	boolean isNotForStorage() {
		return getType() == WriteType.NOT_FOR_STORAGE;
	}

	/**
	 * Return {@code true} if this and and {@code other} are both equal and have
	 * the same {@code type}.
	 * 
	 * @param other
	 * @return {@code true} if this Write matches {@code other}
	 */
	@PackagePrivate
	boolean matches(Write other) {
		return equals(other) && getType() == other.getType();
	}

}
