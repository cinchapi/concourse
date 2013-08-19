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
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteables;

/**
 * The association between a location and a {@link PrimaryKey}.
 * <p>
 * A Position is used in a {@link SearchIndex} to specify the precise location
 * of a term in a Record.
 * </p>
 * 
 * @author jnelson
 */
@Immutable
@PackagePrivate
final class Position implements Comparable<Position>, Storable {

	/**
	 * Return the Position encoded in {@code buffer} so long as those bytes
	 * adhere to the format specified by the {@link #getBytes()} method. This
	 * method assumes that all the bytes in the {@code buffer} belong to the
	 * Value. In general, it is necessary to get the appropriate Value slice
	 * from the parent ByteBuffer using
	 * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
	 * 
	 * @param buffer
	 * @return the Position
	 */
	public static Position fromByteBuffer(ByteBuffer buffer) {
		return Byteables.read(buffer, Position.class); // We are using
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
	 * Return a Position based on the {@code key} and {@code marker}. The
	 * Position will have the storage properties of {@code key} and a unique
	 * timestamp.
	 * 
	 * @param key
	 * @param marker
	 * @return the Position
	 */
	public static Position fromPrimaryKeyAndMarker(PrimaryKey key, int marker) {
		if(key.isForStorage()) { // need to make a new PrimaryKey to ensure that
									// timestamp is unique
			key = PrimaryKey.forStorage(key.longValue());
		}
		Object[] cacheKey = { key, marker, key.getTimestamp() };
		Position position = cache.get(cacheKey);
		if(position == null) {
			position = new Position(key, marker);
			cache.put(position, cacheKey);
		}
		return position;
	}

	/**
	 * The total number of bytes used to encoded each Position.
	 */
	@PackagePrivate
	static final int SIZE = PrimaryKey.SIZE + 4; //index

	/**
	 * A ReferenceCache is generated in {@link Byteables} for Positions read
	 * from ByteBuffers, so this cache is only for notForStorage Positions.
	 */
	private static final ReferenceCache<Position> cache = new ReferenceCache<Position>();
	
	/**
	 * The version of the PrimaryKey is used to version the Position.
	 */
	private final PrimaryKey key;
	
	/**
	 * The numerical index the Position represents.
	 */
	private final int index;

	/**
	 * Construct an instance that represents an existing Position from a
	 * ByteBuffer. This constructor is public so as to comply with the
	 * {@link Byteable} interface. Calling this constructor directly is not
	 * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
	 * advantage of reference caching.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public Position(ByteBuffer bytes) {
		this.key = PrimaryKey.fromByteBuffer(bytes);
		this.index = bytes.getInt();
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param index
	 */
	private Position(PrimaryKey key, int index) {
		this.key = key;
		this.index = index;
	}

	@Override
	public int compareTo(Position o) {
		int comparison = getPrimaryKey().compareTo(o.getPrimaryKey(), true);
		return comparison == 0 ? Integer
				.compare(getIndex(), o.getIndex()) : comparison;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Position) {
			Position other = (Position) obj;
			return Objects.equals(getPrimaryKey(), other.getPrimaryKey())
					&& Objects.equals(getIndex(), other.getIndex());
		}
		return false;
	}

	@Override
	public ByteBuffer getBytes() {
		ByteBufferOutputStream out = new ByteBufferOutputStream(PrimaryKey.SIZE);
		out.write(key);
		out.write(index);
		ByteBuffer bytes = out.toByteBuffer();
		out.close();
		return bytes;
	}

	/**
	 * Return the location marker that is associated with this Position.
	 * 
	 * @return the position
	 */
	@PackagePrivate
	public int getIndex() {
		return index;
	}

	/**
	 * Return the PrimaryKey that is associated with this Position.
	 * 
	 * @return the PrimaryKey
	 */
	@PackagePrivate
	public PrimaryKey getPrimaryKey() {
		return key;
	}

	@Override
	public long getTimestamp() {
		return key.getTimestamp();
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, index);
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
		return SIZE;
	}

	@Override
	public String toString() {
		return "Position " + getIndex() + " in PrimaryRecord "
				+ getPrimaryKey();
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
	 * @see {@link #compareTo(Position)}
	 * @see {@link Storables#compare(Storable, Storable)}
	 */
	@PackagePrivate
	int compareTo(Position o, boolean logically) {
		return logically ? compareTo(o) : Storables.compare(this, o);
	}

}
