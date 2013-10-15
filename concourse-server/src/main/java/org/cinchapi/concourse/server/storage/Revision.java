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
package org.cinchapi.concourse.server.storage;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.util.ByteBuffers;

/**
 * A Revision represents a modification involving a {@code locator}, {@code key}
 * and {@code value} at a {@code version} and is used to organize indexed data
 * that is permanently stored in a {@link Block} or viewed in a {@link Record}.
 * 
 * 
 * @author jnelson
 * @param <L> - the locator type
 * @param <K> - the key type
 * @param <V> - the value type
 */
/*
 * (non-Javadoc)
 * Unlike a Write, a Revision does not contain type information since it is
 * never transported to a different Store. Therefore, we can always infer the
 * revision type y checking in it appears an even or odd number of times
 * relative to equal Revisions at a given timestamp.
 */
@Immutable
public abstract class Revision<L extends Comparable<L> & Byteable, K extends Comparable<K> & Byteable, V extends Comparable<V> & Byteable> implements
		Byteable,
		Versioned {

	/**
	 * Create a PrimaryRevision for {@code key} as {@code value} in
	 * {@code record} at {@code version}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @param version
	 * 
	 * @return the PrimaryRevision
	 */
	public static PrimaryRevision createPrimaryRevision(Text key, Value value,
			PrimaryKey record, long version) {
		return new PrimaryRevision(record, key, value, version);
	}

	/**
	 * Create a SearchRevision for {@code word} at {@code position} for
	 * {@code key} at {@code version}.
	 * 
	 * @param key
	 * @param word
	 * @param position
	 * @param version
	 * @return the SearchRevision
	 */
	public static SearchRevision createSearchRevision(Text key, Text word,
			Position position, long version) {
		return new SearchRevision(key, word, position, version);
	}

	/**
	 * Create a SecondaryRevision for {@code key} as {@code value} in
	 * {@code record} at {@code version}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @param version
	 * @return the SecondaryRevision
	 */
	public static SecondaryRevision createSecondaryRevision(Text key,
			Value value, PrimaryKey record, long version) {
		return new SecondaryRevision(key, value, record, version);
	}

	/**
	 * Indicates that a component of the class has variable length and therefore
	 * must encode the size of that component for each instance.
	 */
	private static final int VARIABLE_SIZE = -1;

	/**
	 * The primary component used to locate the index, to which this Revision
	 * belongs.
	 */
	private final L locator;

	/**
	 * The secondary component used to locate the field in the index, to which
	 * this Revision belongs.
	 */
	private final K key;

	/**
	 * The tertiary component that typically represents the payload for what
	 * this Revision represents.
	 */
	private final V value;

	/**
	 * The unique version that identifies this Revision. Versions are assumed to
	 * be an atomically increasing values (i.e. timestamps).
	 */
	private final long version;

	/**
	 * A cached copy of the binary representation that is returned from
	 * {@link #getBytes()}.
	 */
	private transient ByteBuffer bytes = null;

	/**
	 * The number of bytes used to store the Revision. This value depends on
	 * the number of variable sized components.
	 */
	private transient final int size;

	/**
	 * Construct an instance that represents an existing Revision from a
	 * ByteBuffer. This constructor is public so as to comply with the
	 * {@link Byteable} interface. Calling this constructor directly is not
	 * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
	 * advantage of reference caching.
	 * 
	 * @param bytes
	 */
	/*
	 * (non-Javadoc)
	 * This constructor exists and is public so that subclass instances can be
	 * dynamically deserialized using the Byteables#read() method.
	 */
	@DoNotInvoke
	public Revision(ByteBuffer bytes) {
		this.bytes = bytes;
		this.version = bytes.getLong();
		this.locator = Byteables.readStatic(ByteBuffers.get(bytes,
				xLocatorSize() == VARIABLE_SIZE ? bytes.getInt()
						: xLocatorSize()), xLocatorClass());
		this.key = Byteables.readStatic(ByteBuffers.get(bytes,
				xKeySize() == VARIABLE_SIZE ? bytes.getInt() : xKeySize()),
				xKeyClass());
		this.value = Byteables.readStatic(ByteBuffers.get(bytes,
				xValueSize() == VARIABLE_SIZE ? bytes.getInt() : xValueSize()),
				xValueClass());
		this.size = bytes.capacity();
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 * @param key
	 * @param value
	 * @param version
	 */
	protected Revision(L locator, K key, V value, long version) {
		this.locator = locator;
		this.key = key;
		this.value = value;
		this.version = version;
		this.size = 8 + (xLocatorSize() == VARIABLE_SIZE ? 4 : 0)
				+ (xKeySize() == VARIABLE_SIZE ? 4 : 0)
				+ (xValueSize() == VARIABLE_SIZE ? 4 : 0) + locator.size()
				+ key.size() + value.size();
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean equals(Object obj) {
		if(obj.getClass() == this.getClass()) {
			Revision<L, K, V> other = (Revision<L, K, V>) obj;
			return locator.equals(other.locator) && key.equals(other.key)
					&& value.equals(other.value);
		}
		return false;
	}

	/**
	 * Return a byte buffer that represents this Revision with the following
	 * order:
	 * <ol>
	 * <li><strong>version</strong></li>
	 * <li><strong>locatorSize</strong> -
	 * <em>if {@link #xLocatorSize()} == {@link #VARIABLE_SIZE}</em></li>
	 * <li><strong>locator</strong></li>
	 * <li><strong>keySize</strong> -
	 * <em>if {@link #xKeySize()} == {@link #VARIABLE_SIZE}</em></li>
	 * <li><strong>key</strong></li>
	 * <li><strong>valueSize</strong> -
	 * <em>if {@link #xValueSize()} == {@link #VARIABLE_SIZE}</em></li>
	 * <li><strong>value</strong></li>
	 * 
	 * </ol>
	 * 
	 * @return the ByteBuffer representation
	 */
	@Override
	public ByteBuffer getBytes() {
		if(bytes == null) {
			bytes = ByteBuffer.allocate(size());
			bytes.putLong(version);
			if(xLocatorSize() == VARIABLE_SIZE) {
				bytes.putInt(locator.size());
			}
			bytes.put(locator.getBytes());
			if(xKeySize() == VARIABLE_SIZE) {
				bytes.putInt(key.size());
			}
			bytes.put(key.getBytes());
			if(xValueSize() == VARIABLE_SIZE) {
				bytes.putInt(value.size());
			}
			bytes.put(value.getBytes());
		}
		return ByteBuffers.asReadOnlyBuffer(bytes);
	}

	/**
	 * Return the {@link #key} associated with this Revision.
	 * 
	 * @return the key
	 */
	public K getKey() {
		return key;
	}

	/**
	 * Return the {@link #locator} associated with this Revision.
	 * 
	 * @return the locator
	 */
	public L getLocator() {
		return locator;
	}

	/**
	 * Return the {@link #value} associated with this Revision.
	 * 
	 * @return the value
	 */
	public V getValue() {
		return value;
	}

	@Override
	public long getVersion() {
		return version;
	}

	@Override
	public int hashCode() {
		return Objects.hash(locator, key, value);
	}

	@Override
	public boolean isStorable() {
		return version != NO_VERSION;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return key + " AS " + value + " IN " + locator;
	}

	/**
	 * Return the class of the {@link #key} type.
	 * 
	 * @return they key class
	 */
	protected abstract Class<K> xKeyClass();

	/**
	 * Return the size used to store each {@link #key}. If this value is not
	 * fixed, return {@link #VARIABLE_SIZE}.
	 * 
	 * @return the key size
	 */
	protected abstract int xKeySize();

	/**
	 * Return the class of the {@link locator} type.
	 * 
	 * @return the locator class
	 */
	protected abstract Class<L> xLocatorClass();

	/**
	 * Return the size used to store each {@link #locator}. If this value is not
	 * fixed, return {@link #VARIABLE_SIZE}.
	 * 
	 * @return the locator size
	 */
	protected abstract int xLocatorSize();

	/**
	 * Return the class of the {@link #value} type.
	 * 
	 * @return the value class
	 */
	protected abstract Class<V> xValueClass();

	/**
	 * Return the size used to store each {@link #value}. If this value is not
	 * fixed, return {@link #VARIABLE_SIZE}.
	 * 
	 * @return the value size
	 */
	protected abstract int xValueSize();

	/**
	 * A {@link Revision} that is used in a {@link PrimaryBlock} and maps a
	 * record to a key to a value.
	 * 
	 * @author jnelson
	 */
	@Immutable
	public final static class PrimaryRevision extends
			Revision<PrimaryKey, Text, Value> {

		/**
		 * Construct an instance that represents an existing PrimaryRevision
		 * from a ByteBuffer. This constructor is public so as to comply with
		 * the {@link Byteable} interface. Calling this constructor directly is
		 * not recommend.
		 * 
		 * @param bytes
		 */
		private PrimaryRevision(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param locator
		 * @param key
		 * @param value
		 * @param version
		 */
		private PrimaryRevision(PrimaryKey locator, Text key, Value value,
				long version) {
			super(locator, key, value, version);
		}

		@Override
		protected Class<Text> xKeyClass() {
			return Text.class;
		}

		@Override
		protected int xKeySize() {
			return VARIABLE_SIZE;
		}

		@Override
		protected Class<PrimaryKey> xLocatorClass() {
			return PrimaryKey.class;
		}

		@Override
		protected int xLocatorSize() {
			return PrimaryKey.SIZE;
		}

		@Override
		protected Class<Value> xValueClass() {
			return Value.class;
		}

		@Override
		protected int xValueSize() {
			return VARIABLE_SIZE;
		}

	}

	/**
	 * A {@link Revision} that is used in a {@link SearchBlock} and maps a key
	 * to a term to a position.
	 * 
	 * @author jnelson
	 */
	@Immutable
	public final static class SearchRevision extends
			Revision<Text, Text, Position> {

		/**
		 * Construct an instance that represents an existing SearchRevision from
		 * a ByteBuffer. This constructor is public so as to comply with the
		 * {@link Byteable} interface. Calling this constructor directly is not
		 * recommend.
		 * 
		 * @param bytes
		 */
		private SearchRevision(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param locator
		 * @param key
		 * @param value
		 * @param version
		 */
		private SearchRevision(Text locator, Text key, Position value,
				long version) {
			super(locator, key, value, version);
		}

		@Override
		protected Class<Text> xKeyClass() {
			return Text.class;
		}

		@Override
		protected int xKeySize() {
			return VARIABLE_SIZE;
		}

		@Override
		protected Class<Text> xLocatorClass() {
			return Text.class;
		}

		@Override
		protected int xLocatorSize() {
			return VARIABLE_SIZE;
		}

		@Override
		protected Class<Position> xValueClass() {
			return Position.class;
		}

		@Override
		protected int xValueSize() {
			return Position.SIZE;
		}

	}

	/**
	 * A {@link Revision} that is used in a {@link SecondayBlock} and maps a key
	 * to a value to a record.
	 * 
	 * @author jnelson
	 */
	@Immutable
	public final static class SecondaryRevision extends
			Revision<Text, Value, PrimaryKey> {

		/**
		 * Construct an instance that represents an existing SecondaryRevision
		 * from a ByteBuffer. This constructor is public so as to comply with
		 * the {@link Byteable} interface. Calling this constructor directly is
		 * not recommend.
		 * 
		 * @param bytes
		 */
		private SecondaryRevision(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param locator
		 * @param key
		 * @param value
		 * @param version
		 */
		private SecondaryRevision(Text locator, Value key, PrimaryKey value,
				long version) {
			super(locator, key, value, version);
		}

		@Override
		protected Class<Value> xKeyClass() {
			return Value.class;
		}

		@Override
		protected int xKeySize() {
			return VARIABLE_SIZE;
		}

		@Override
		protected Class<Text> xLocatorClass() {
			return Text.class;
		}

		@Override
		protected int xLocatorSize() {
			return VARIABLE_SIZE;
		}

		@Override
		protected Class<PrimaryKey> xValueClass() {
			return PrimaryKey.class;
		}

		@Override
		protected int xValueSize() {
			return PrimaryKey.SIZE;
		}

	}

}
