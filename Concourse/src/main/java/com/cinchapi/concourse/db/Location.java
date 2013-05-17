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
import com.cinchapi.common.util.Strings;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * A Location is the association of a position and a {@link PrimaryKey}. This
 * structure is used in a {@link Concordance} to specify the location of a term.
 * 
 * @author jnelson
 */
@Immutable
final class Location implements Comparable<Location>, Containable {

	/**
	 * Return a location that is appropriate for storage, with the timestamp of
	 * {@code key}.
	 * 
	 * @param value
	 * @return the new instance.
	 */
	public static Location forStorage(PrimaryKey key, int position) {
		// NOTE: I don't perform a cache lookup here because forStorage object
		// must have a unique timestamp and will never be duplicated on
		// creation. But, I do add the new object to the cache for lookup in the
		// event that a value is read from a byte sequence.
		Preconditions.checkArgument(key.isForStorage());
		Location location = new Location(key, position);
		Object[] cacheKey = { key.asLong(), key.getTimestamp(), position };
		cache.put(location, cacheKey);
		return location;
	}

	/**
	 * Return the location represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the key
	 */
	public static Location fromByteSequence(ByteBuffer bytes) {
		byte[] keyBytes = new byte[PrimaryKey.SIZE_IN_BYTES];
		bytes.get(keyBytes);
		PrimaryKey key = PrimaryKey.fromByteSequence(ByteBuffer.wrap(keyBytes));
		int position = bytes.getInt();
		Object[] cacheKey = { key.asLong(), key.getTimestamp(), position };
		Location location = cache.get(cacheKey);
		if(location == null) {
			location = new Location(key, position);
			cache.put(location, cacheKey);
		}
		return location;
	}

	/**
	 * Return a location that is not appropriate for storage, but can be used in
	 * comparisons. This is the preferred way to create keys unless the key
	 * will be stored.
	 * 
	 * @param value
	 * @return the new instance.
	 */
	public static Location notForStorage(PrimaryKey key, int position) {
		Preconditions.checkArgument(key.isNotForStorage());
		Object[] cacheKey = { key.asLong(), key.getTimestamp(), position };
		Location location = cache.get(cacheKey);
		if(location == null) {
			location = new Location(key, position);
			cache.put(location, cacheKey);
		}
		return location;
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param position
	 */
	private Location(PrimaryKey key, int position) {
		this.key = key;
		this.position = position;
	}

	static final int SIZE_IN_BYTES = PrimaryKey.SIZE_IN_BYTES
			+ (Integer.SIZE / 8); // key, position
	private static final ObjectReuseCache<Location> cache = new ObjectReuseCache<Location>();

	private final int position;
	private final PrimaryKey key;
	private transient ByteBuffer buffer = null; // initialize lazily

	@Override
	public int size() {
		return SIZE_IN_BYTES;
	}

	@Override
	public byte[] getBytes() {
		return getBuffer().array();
	}

	@Override
	public long getTimestamp() {
		return key.getTimestamp();
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
	public int compareTo(Location o) {
		int compare = key.compareTo(o.key, true);
		return compare == 0 ? Integer.compare(position, o.position) : compare;
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
	int compareTo(Location o, boolean logically) {
		return logically ? compareTo(o) : Containables.compare(this, o);
	}

	/**
	 * Rewind and return {@link #buffer}. Use this method instead of accessing
	 * the variable directly to ensure that it is rewound.
	 * </p>
	 * <p>
	 * The buffer is encoded with the following order:
	 * <ol>
	 * <li><strong>primaryKey</strong> - first 16 bytes</li>
	 * <li><strong>position</strong> - last 4 bytes</li>
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
			buffer.put(key.getBytes());
			buffer.putInt(position);
		}
		buffer.rewind();
		return buffer;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(key.asLong(), position);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Location) {
			Location other = (Location) obj;
			return Objects.equal(key.asLong(), other.key.asLong())
					&& Objects.equal(position, position);
		}
		return false;
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

}
