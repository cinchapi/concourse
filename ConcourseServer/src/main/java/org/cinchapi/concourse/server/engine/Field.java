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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.io.ByteableCollections;
import org.cinchapi.common.io.Byteables;
import org.cinchapi.common.multithread.Lock;
import org.cinchapi.common.multithread.Lockable;
import org.cinchapi.common.multithread.Lockables;
import org.cinchapi.common.tools.Numbers;
import org.cinchapi.common.tools.Strings;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.*;
import static org.mockito.Matchers.any;

/**
 * A versioned controlled collection of values, each of which is associated with
 * a {@code key} inside of a {@link Record}.
 * <p>
 * The values in a Field are maintained in insertion order and represented by:
 * <ul>
 * <li><strong>State</strong> - A snapshot of the values that <em>currently</em>
 * exist.</li>
 * <li><strong>History</strong> - A growing list of every written value ever
 * sorted by timestamp in ascending order. Whenever a write (add or remove)
 * occurs, the value is logged here. A value V is considered present at time T
 * if there are an odd number of values in the history <em>equal</em> to V with
 * a timestamp less than or equal to T.</li>
 * </ul>
 * <h2>Storage Requirements</h2>
 * <ul>
 * <li>The size required for the {@code state} is the sum of the size of each
 * Value plus 4 bytes.</li>
 * <li>The size required for the {@code history} is the product of 4 times the
 * total number of revisions plus the sum of the size of each value multiplied
 * by the number revisions involving that value.</li>
 * </ul>
 * <strong>Note:</strong> Because a Field is version controlled, it's size is
 * guaranteed to increase by at least the size of V for each write.
 * </p>
 * 
 * @param <K> - the {@link Byteable} key type
 * @param <V> - the {@link Storable} value type
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
abstract class Field<K extends Byteable, V extends Storable> implements
		Byteable,
		Lockable { /* package-private */

	/**
	 * Encode a Field identified by {@code key} with values belong to
	 * {@code valueClass} and {@code components} into a ByteBuffer. The encoded
	 * format is:
	 * <ol>
	 * <li>keyClassSize</li>
	 * <li>valueClassSize</li>
	 * <li>keySize</li>
	 * <li>component1Size</li>
	 * <li>...</li>
	 * <li>componentNSize</li>
	 * <li>keyClass</li>
	 * <li>valueClass</li>
	 * <li>key</li>
	 * <li>component1</li>
	 * <li>...</li>
	 * <li>componentN</li>
	 * </ol>
	 * 
	 * @param key
	 * @param valueClass
	 * @param components
	 * @return the encoded ByteBuffer
	 */
	@SafeVarargs
	public static <K extends Byteable, V extends Storable> ByteBuffer encodeAsByteBuffer(
			K key, Class<V> valueClass, Field<K, V>.Component... components) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
	
		// The key and value class names are stored in the Field so that the
		// components can be deserialized reflectively.
		Text keyClassName = Text.fromString(key.getClass().getName());
		Text valueClassName = valueClass == null ? Text.EMPTY : Text
				.fromString(valueClass.getName());
	
		// encode sizes
		out.write(keyClassName.size());
		out.write(valueClassName.size());
		out.write(key.size());
		for (Field<K, V>.Component component : components) {
			out.write(component.size());
		}
	
		// encode data
		out.write(keyClassName);
		out.write(valueClassName);
		out.write(key);
		for (Field<K, V>.Component component : components) {
			out.write(component);
		}
		out.close();
		log.debug("INFO FOR FIELD IDENTIFIED BY {}", key);
		log.debug("keyClassName is {} and {} bytes", keyClassName, keyClassName.size());
		log.debug("valueClassName is {} and {} bytes", valueClassName, valueClassName.size());
		log.debug("key is {} and {} bytes", key, key.size());
		log.debug("state is {} and {} bytes", components[0], components[0].size());
		log.debug("history is {} and {} bytes", components[1], components[1].size());
		log.debug("Total buffer is {} bytes", out.size());
		return out.toByteBuffer();
	}
	/**
	 * Return a <em>mock</em> field of {@code type}. Use this method instead
	 * of mocking {@code type} directly to ensure that the mock is compatible
	 * with the assumptions made in {@link Record}.
	 * 
	 * @param type
	 * @return the {@code field}
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Field<K, V>, K extends Byteable, V extends Storable> T mock(
			Class<T> type) {
		T field = Mockito.mock(type);
		Mockito.doReturn(Lists.<V>newArrayList()).when(field).getValues();
		Mockito.doNothing().when(field).add((V) any(Storable.class));
		Mockito.doNothing().when(field).remove((V) any(Storable.class));
		Mockito.doThrow(UnsupportedOperationException.class).when(field)
				.getBytes();
		Mockito.doThrow(UnsupportedOperationException.class).when(field).size();
		return field;
	
	}

	/**
	 * The maximum number of bytes that can be used to encode a single Value.
	 */
	static final int MAX_SIZE = Integer.MAX_VALUE; /* package-private */
	protected static final Logger log = LoggerFactory.getLogger(Field.class);
	private final K key;

	private final State state;

	private final History history;

	// The valueClass is here so that we can make more efficient reflection
	// calls to create component values.
	private transient Class<V> valueClass = null;

	/**
	 * Copy Constructor
	 * 
	 * @param source
	 */
	public @DoNotInvoke
	Field(Field<K, V> source) {
		this.key = source.getKey();
		this.state = new State();
		this.history = new History();
		List<V> revisions = source.history.getValues();
		for (V value : revisions) {
			if(this.contains(value)) {
				this.remove(value);
			}
			else {
				this.add(value);
			}
		}
	}

	/**
	 * Construct the Field instance represented by {@code bytes}. Use this
	 * constructor when reading and reconstructing from a file. This constructor
	 * assumes that {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the Field
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	protected Field(ByteBuffer buffer) throws ClassNotFoundException {
		int keyClassSize = buffer.getInt();
		int valueClassSize = buffer.getInt();
		int keySize = buffer.getInt();
		int stateSize = buffer.getInt();
		int historySize = buffer.getInt();
		int totalSize = keyClassSize + valueClassSize + keySize + stateSize
				+ historySize + 20;

		int keyClassPosition = buffer.position();
		int valueClassPosition = keyClassPosition + keyClassSize;
		int keyPosition = valueClassPosition + valueClassSize;
		int statePosition = keyPosition + keySize;
		int historyPosition = statePosition + stateSize;

		Preconditions.checkState(buffer.capacity() == totalSize, "Cannot "
				+ "deserialize the %s because there are an insufficient "
				+ "number of bytes in the file. Expecting %s bytes, but only "
				+ "%s bytes present.", getClass().getSimpleName(), totalSize,
				buffer.capacity());

		buffer.position(keyClassPosition);
		String keyClass = ByteBuffers.getString(ByteBuffers.slice(buffer,
				keyClassSize));

		buffer.position(valueClassPosition);
		this.valueClass = (Class<V>) Class.forName(ByteBuffers
				.getString(ByteBuffers.slice(buffer, valueClassSize)));

		buffer.position(keyPosition);
		this.key = Byteables.<K> read(ByteBuffers.slice(buffer, keySize),
				keyClass);

		buffer.position(statePosition);
		this.state = new State(ByteBuffers.slice(buffer, stateSize));

		buffer.position(historyPosition);
		this.history = new History(ByteBuffers.slice(buffer, historySize));
	}

	/**
	 * Construct a <em>new</em> Field identified by {@code id}, with a clean
	 * state and no history.
	 * 
	 * @return the new Field
	 */
	protected Field(K key) {
		this.key = key;
		this.state = new State();
		this.history = new History();
	}

	/**
	 * Add the forStorage {@code value} to the Field. This will modify the
	 * {@code state} and log {@code value} in the {@code history}.
	 * 
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	@GuardedBy("this.writeLock")
	public void add(V value) {
		// I am assuming that #value is a new forStorage instance, meaning it
		// has a unique timestamp. The caller must ensure compliance.
		checkState(!contains(value),
				"Cannot add value '%s' because it is already contained", value);
		checkArgument(value.isForStorage(),
				"Cannot add value '%s' because it is notForStorage", value);
		Lock lock = writeLock();
		try {
			state.add(value);
			history.add(value);
			valueClass = (Class<V>) (valueClass == null ? value.getClass()
					: valueClass);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return a map from timestamp to a description of associated modification
	 * that occurred.
	 * 
	 * @return the audit map
	 */
	@GuardedBy("this.readLock")
	public Map<Long, String> audit() {
		Lock lock = readLock();
		try {
			Map<Long, String> audit = Maps.newTreeMap();
			Map<V, Integer> counts = Maps.newHashMap();
			List<V> log = history.getValues();
			for (V value : log) {
				int count = counts.containsKey(value) ? counts.get(value) + 1
						: 1;
				counts.put(value, count);
				String description;
				if(Numbers.isEven(count)) {
					description = "ADDED " + value + " to " + key;
				}
				else {
					description = "REMOVED " + value + " from " + key;
				}
				audit.put(value.getTimestamp(), description);
			}
			return audit;
		}
		finally {
			lock.release();
		}

	}

	/**
	 * Return {@code true} if {@code value} is found in the current
	 * {@code state}.
	 * 
	 * @param value
	 * @return {@code true} if {@code value} is presently contained
	 */
	@GuardedBy("this.readLock")
	public boolean contains(V value) {
		Lock lock = readLock();
		try {
			return state.contains(value);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return {@code true} if {@code value} is found to exist in the history at
	 * {@code timestamp}.
	 * 
	 * @param value
	 * @param timestamp
	 * @return {@code true} if value was contained at {@code timestamp}
	 */
	@GuardedBy("this.readLock")
	public boolean contains(V value, long timestamp) {
		Lock lock = readLock();
		try {
			return history.existsAt(value, timestamp);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return the number of values presently contained in the Field.
	 * 
	 * @return the count
	 */
	@GuardedBy("this.readLock")
	public int count() {
		Lock lock = readLock();
		try {
			return state.count();
		}
		finally {
			lock.release();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	@GuardedBy("synchronized")
	public synchronized boolean equals(Object obj) {
		if(obj instanceof Field) {
			Field<K, V> other = (Field<K, V>) obj;
			return Objects.equal(key, other.key)
					&& Objects.equal(state, other.state)
					&& Objects.equal(history, other.history)
					&& Objects.equal(valueClass, other.valueClass);
		}
		return false;
	}

	/**
	 * Return a byte array that represents the value with the following order:
	 * <ol>
	 * <li>keyClassSize</li>
	 * <li>valueClassSize</li>
	 * <li>keySize</li>
	 * <li>stateSize</li>
	 * <li>historySize.</li>
	 * <li>keyClass</li>
	 * <li>valueClass</li>
	 * <li>key</li>
	 * <li>state</li>
	 * <li>history</li>
	 * </ol>
	 * 
	 * @return a byte array.
	 */
	@Override
	@GuardedBy("this.readLock")
	public ByteBuffer getBytes() {
		Lock lock = readLock();
		try {
			return encodeAsByteBuffer(key, valueClass, state, history);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return the key.
	 * 
	 * @return the id
	 */
	public K getKey() {
		return key;
	}

	/**
	 * Return a list of the presently contained values.
	 * 
	 * @return the values
	 */
	@GuardedBy("this.readLock")
	public List<V> getValues() {
		Lock lock = readLock();
		try {
			return state.getValues();
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return a list of the values contained at {@code timestamp}.
	 * 
	 * @param timestamp
	 * @return the values
	 */
	@GuardedBy("this.readLock")
	public List<V> getValues(long timestamp) {
		Lock lock = readLock();
		try {
			Iterator<V> it = history.rewind(timestamp).iterator();
			List<V> snapshot = Lists.newArrayList();
			while (it.hasNext()) {
				V value = it.next();
				if(snapshot.contains(value)) {
					snapshot.remove(value);
				}
				else {
					snapshot.add(value);
				}
			}
			return snapshot;
		}
		finally {
			lock.release();
		}
	}

	@Override
	@GuardedBy("synchronized")
	public synchronized int hashCode() {
		return Objects.hashCode(key, state, history, valueClass);
	}

	/**
	 * Return {@code true} if the Field is empty.
	 * 
	 * @return {@code true} if the state count is 0
	 */
	@GuardedBy("this.readLock")
	public boolean isEmpty() {
		Lock lock = readLock();
		try {
			return state.count() == 0;
		}
		finally {
			lock.release();
		}

	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this);
	}

	/**
	 * Remove the forStorage {@code value} from the Field. This will modify
	 * the {@code state} and log {@code value} in the {@code history}.
	 * 
	 * @param value
	 */
	@GuardedBy("this.writeLock")
	public void remove(V value) {
		// I am assuming that #value is a new forStorage instance, meaning it
		// has a unique timestamp. The caller must ensure compliance.
		checkState(contains(value),
				"Cannot remove value '%s' because it is not contained", value);
		checkArgument(value.isForStorage(),
				"Cannot remove value '%s' because it is notForStorage", value);
		Lock lock = writeLock();
		try {
			state.remove(value);
			history.remove(value);
		}
		finally {
			lock.release();
		}
	}

	@Override
	public int size() {
		return getBytes().capacity();
	}

	@Override
	@GuardedBy("synchronized")
	public synchronized String toString() {
		return Strings.toString(this);
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this);
	}

	/**
	 * A Component is a collection of {@code V} values that are maintained in
	 * insertion order within the Field. Each value must have a unique
	 * timestamp.
	 * 
	 * @author jnelson
	 */
	abstract class Component implements Byteable {

		/**
		 * The Component values are maintained as a list. The implementing class
		 * is responsible for enforcing uniqueness amongst members if necessary.
		 * This class DOES ensure that timestamps are unique and ordered within
		 * this list.
		 */
		private final List<V> values = Lists.newArrayList();

		/**
		 * The hash code cache.
		 * <p>
		 * Holds the Component's hash code at the time of the most recent call
		 * to {@link #getBytes()}. Whenever calling the method, we check to see
		 * if the Components has changed by computing the hash code. If changes
		 * have occurred, we regenerated {@link #bytes} and update the hash
		 * code. Otherwise, we return the existing value of {@link #bytes}.
		 * </p>
		 */
		private transient int hcc = 0;

		/**
		 * The return for {@link #getBytes()} is cached here. DO NOT use this
		 * variable directly because it is not guaranteed
		 * to be an accurate encoding of the Component. Use {@link #getBytes()}
		 * instead.
		 */
		private transient ByteBuffer bytes = null;

		/**
		 * Construct a new instance.
		 */
		protected Component() { /* Do Nothing */}

		/**
		 * Construct a new instance using a byte buffer that conforms to the
		 * rules for {@link ByteableCollection}. Make sure that
		 * {@link #valueClass} is set before invoking this constructor.
		 * 
		 * @param bytes
		 */
		protected Component(ByteBuffer bytes) {
			Iterator<ByteBuffer> bsit = ByteableCollections.iterator(bytes);
			while (bsit.hasNext()) {
				add(Byteables.<V> read(bsit.next(), valueClass));
			}
		}

		/**
		 * Add {@code value} to the Field. This method will check against the
		 * last timestamp in {@link #values} to ensure that the list is kept in
		 * ascending order and no duplicate timestamps are ever allowed.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} is added
		 */
		public boolean add(V value) {
			long timestamp = values.isEmpty() ? Storable.NIL : values.get(
					values.size() - 1).getTimestamp();
			checkArgument(value.getTimestamp() > timestamp,
					"Cannot add %s because it's associated timestamp "
							+ "is less than the most recent timestamp "
							+ "in the Field", value);
			return values.add(value);
		}

		/**
		 * Return {@code true} if {@code value} is present in the Field
		 * according to the definition of {@link V#equals(Value)}.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} is contained
		 */
		public boolean contains(V value) {
			return values.contains(value);
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj) {
			if(obj.getClass().equals(this.getClass())) {
				final Component other = (Component) obj;
				return Objects.equal(values, other.values);
			}
			return false;
		}

		@Override
		public ByteBuffer getBytes() {
			Lock lock = readLock();
			try{
				int hashCode = hashCode();
				if(hashCode != hcc || bytes == null) {
					bytes = ByteableCollections.toByteBuffer(values);
					hcc = hashCode;
				}
				bytes.rewind();
				return bytes;
			}
			finally{
				lock.release();
			}
		}

		/**
		 * Return an unmodifiable view of the values in the
		 * <strong>component</strong>. This method is not to be confused with
		 * {@link Field#getValues()}, which returns the values currently
		 * contained in the <strong>Field</strong>.
		 * 
		 * @return the values in the component.
		 */
		public List<V> getValues() {
			return Collections.unmodifiableList(values);
		}

		@Override
		public synchronized int hashCode() {
			return Objects.hashCode(values);
		}

		/**
		 * Remove the first instance of {@code value} from the Field.
		 * 
		 * @param value
		 * @return {@code true} if the first instance of the {@code value} is
		 *         removed
		 */
		public boolean remove(V value) {
			return values.remove(value);
		}

		@Override
		public int size() {
			return getBytes().capacity();
		}

		@Override
		public String toString() {
			return values.toString();
		}
	}

	/**
	 * An append-only log that keeps track of {@link Field} writes over
	 * time. The {@code V} value associated with each write is associated with a
	 * timestamp and added to the end of the log. An value is considered to
	 * exist in the Field if it appears in the history an odd number of
	 * times,
	 * otherwise it is considered to not exist.
	 * 
	 * @author jnelson
	 */
	private final class History extends Component {

		/**
		 * Construct a new instance.
		 */
		private History() {
			super();
		}

		/**
		 * Construct a new instance using a byte buffer assuming that the
		 * {@code bytes} conform to the rules for a {@link ByteableCollection}.
		 * 
		 * @param bytes
		 */
		private History(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Count the number of times that {@code value} appears in the history
		 * with a timestamp that is less than or equal to {@code at}.
		 * 
		 * @param value
		 * @param at
		 * @return the number of appearances
		 */
		public int count(V value, long at) {
			int count = 0;
			ListIterator<V> it = rewind(at).listIterator();
			while (it.hasNext()) {
				count += it.next().equals(value) ? 1 : 0; // I'm assuming that
															// {@link
															// V#equals(Value)}
															// does not account
															// for #timestamp
			}
			return count;
		}

		/**
		 * Return {@code true} if {@code value} existed in the Field prior
		 * to the specified timestamp (meaning there is an odd number of
		 * appearances for {@code value} in the history).
		 * 
		 * @param value
		 * @param before
		 * @return {@code true} if {@code value} existed.
		 */
		public boolean existsAt(V value, long at) {
			return Numbers.isOdd(count(value, at));
		}

		@Override
		public boolean remove(V value) {
			return add(value); // History is append only, so a removal means we
								// assume the #value has a new timestamp and is
								// added
		}

		/**
		 * Return a <em>new</em> list of revisions that were present {@code at}
		 * the specified timestamp in insertion/ascending order.
		 * 
		 * @param timestamp
		 * @return the list of revisions
		 */
		public List<V> rewind(long timestamp) {
			List<V> snapshot = Lists.newArrayList();
			Iterator<V> it = getValues().iterator();
			while (it.hasNext()) {
				V value = it.next();
				if(value.getTimestamp() <= timestamp) {
					snapshot.add(value);
				}
				else {
					break; // since the values are stored in insertion order, I
							// can stop looking once I find a timestamp greater
							// than the parameter
				}
			}
			return snapshot;
		}

	}

	/**
	 * A collection of the values that currently exist in the Field.
	 * The current state can be derived from the {@link Field#history}, but
	 * it is tracked explicitly for optimal performance.
	 * 
	 * @author jnelson
	 */
	private final class State extends Component {

		/**
		 * Construct a new instance.
		 */
		private State() {
			super();
		}

		/**
		 * Construct a new instance using a byte buffer assuming that the
		 * {@code bytes} conform to the rules for a {@link ByteableCollection}.
		 * 
		 * @param bytes
		 */
		private State(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Return the number of values in the state.
		 * 
		 * @return the count
		 */
		public int count() {
			return getValues().size();
		}
	}

}
