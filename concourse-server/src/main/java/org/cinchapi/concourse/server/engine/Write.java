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
 * stored and indexed in the {@link Database}.
 * <p>
 * A Write is a necessary component for two reasons:
 * <ul>
 * <li>
 * It is the bridge from the Java objects accepted in API to the the engine
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
		return Byteables.read(buffer, Write.class); // We are using
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

	/**
	 * The minimum number of bytes needed to encode every Write.
	 */
	private static final int CONSTANT_SIZE = PrimaryKey.SIZE + 12; // record,
																	// type(4),
																	// keySize
																	// (4),
																	// valueSize(4)

	private static final ReferenceCache<Write> cache = new ReferenceCache<Write>();

	private final PrimaryKey record;
	private final Text key;
	private final Value value;
	private final WriteType type;
	private final transient int size;

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
		this.type = WriteType.values()[bytes.getInt()];
		byte[] record = new byte[PrimaryKey.SIZE];
		bytes.get(record);
		this.record = PrimaryKey.fromByteBuffer(ByteBuffer.wrap(record));
		byte[] key = new byte[bytes.getInt()];
		byte[] value = new byte[bytes.getInt()];
		bytes.get(key);
		bytes.get(value);
		this.key = Text.fromByteBuffer(ByteBuffer.wrap(key));
		this.value = Value.fromByteBuffer(ByteBuffer.wrap(value));
		this.size = bytes.capacity();
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
		this.record = record;
		this.key = key;
		this.value = value;
		this.type = type;
		this.size = CONSTANT_SIZE + key.size() + value.size();
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
	 * <li><strong>type</strong> position {@value #TYPE_POS}</li>
	 * <li><strong>record</strong> position {@value #RECORD_POS}</li>
	 * <li><strong>keySize</strong> position {@value #KEY_SIZE_POS}</li>
	 * <li><strong>valueSize</strong> position {@value #VALUE_SIZE_POS}</li>
	 * <li><strong>key</strong> position {@value #KEY_POS}</li>
	 * <li><strong>value</strong> position (key) + keySize</li>
	 * </ol>
	 * 
	 * @return a byte array.
	 */
	@Override
	public ByteBuffer getBytes() {
		ByteBuffer bytes = ByteBuffer.allocate(size);
		bytes.putInt(type.ordinal());
		bytes.put(record.getBytes());
		bytes.putInt(key.size());
		bytes.putInt(value.size());
		bytes.put(key.getBytes());
		bytes.put(value.getBytes());
		bytes.rewind();
		return bytes;
	}

	/**
	 * Return the {@code key} associated with this
	 * 
	 * @return the {@code key}
	 */
	public Text getKey() {
		return key;
	}

	/**
	 * Return the {@code record} associated with this
	 * 
	 * @return the {@code record}
	 */
	public PrimaryKey getRecord() {
		return record;
	}

	/**
	 * Return the {@code timestamp} of the {@code value} associated with this
	 * This is equivalent to calling {@link #getValue()}
	 * {@link Value#getTimestamp()}.
	 * 
	 * @return the {@code timestamp}
	 */
	public long getTimestamp() {
		return value.getTimestamp();
	}

	/**
	 * Return the write {@code type} associated with this
	 * 
	 * @return the write {@code type}
	 */
	public WriteType getType() {
		return type;
	}

	/**
	 * Return the {@code value} associated with this
	 * 
	 * @return the {@code value}
	 */
	public Value getValue() {
		return value;
	}

	/**
	 * The hash code for a Write is based on the associated record, key and
	 * value.
	 */
	@Override
	public int hashCode() {
		return Objects.hash(record, key, value);
	}

	/**
	 * Return {@code true} if the Write is forStorage, meaning both the
	 * {@code record} and {@code value} are forStorage.
	 * 
	 * @return {@code true} if the Write is forStorage
	 */
	@PackagePrivate
	public boolean isForStorage() {
		return type != WriteType.NOT_FOR_STORAGE;
	}

	/**
	 * Return {@code true} if the Write is notForStorage, meaning both the
	 * {@code record} and {@code value} are notForStorage.
	 * 
	 * @return {@code true} if the Write is notForStorage
	 */
	@PackagePrivate
	public boolean isNotForStorage() {
		return type == WriteType.NOT_FOR_STORAGE;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public String toString() {
		String verb = this.type.name();
		String key = this.key.toString();
		String value = this.value.toString();
		String preposition = this.type == WriteType.ADD ? "TO" : "FROM";
		String record = this.record.toString();
		return new StringBuilder().append(verb).append(" ").append(key)
				.append(" ").append("AS").append(" ").append(value).append(" ")
				.append(preposition).append(" ").append(record).toString();
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
		return equals(other) && type == other.type;
	}

}
