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

import static org.cinchapi.concourse.server.util.Loggers.getLogger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.ArrayUtils;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.cache.ReferenceCache;
import org.cinchapi.concourse.server.Context;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.concurrent.Lockable;
import org.cinchapi.concourse.server.concurrent.Lockables;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.server.io.Files;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Storable;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.util.BinaryFiles;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Numbers;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A version controlled collection of key/value mappings that are represented on
 * disk as an append-only {@link Revision} list. This class provides a wrapper
 * that contains read optimizing indices.
 * <p>
 * Each Record is identified by a {@code locator} and is stored in a distinct
 * file<sup>1</sup> on disk. The entire Record is deserialized or loaded from a
 * cache for each relevant read or write request.
 * </p>
 * <sup>1</sup> - Records are randomly grouped into locales. <br>
 * 
 * 
 * @author jnelson
 * @param <L> - the locator type
 * @param <K> - the key type
 * @param <V> - the value type
 */
@SuppressWarnings("unchecked")
@PackagePrivate
@ThreadSafe
abstract class Record<L extends Byteable, K extends Byteable, V extends Storable> implements
		Lockable {

	/**
	 * Returns the label used for the record type defined by {@code clazz} or
	 * {@code null} if {@code clazz} is not a valid/defined subclass of
	 * {@link Record}.
	 * 
	 * @param clazz
	 * @return the label for the record type
	 */
	@Nullable
	public static String getLabel(Class<?> clazz) {
		return LABELS.get(clazz.getName());
	}

	/**
	 * Return the {@link PrimaryRecord} that is identified by {@code key}.
	 * 
	 * @param key
	 * @param parentStore
	 * @param context
	 * @return the PrimaryRecord
	 */
	public static PrimaryRecord loadPrimaryRecord(PrimaryKey key,
			String parentStore, Context context) {
		return open(PrimaryRecord.class, PrimaryKey.class, key, parentStore
				+ File.separator + getLabel(PrimaryRecord.class), context);
	}

	/**
	 * Return the {@link SearchIndex} that is identified by {@code key}.
	 * 
	 * @param key
	 * @param parentStore
	 * @param context
	 * @return the SearchIndex
	 */
	public static SearchIndex loadSearchIndex(Text key, String parentStore,
			Context context) {
		return open(SearchIndex.class, Text.class, key, parentStore
				+ File.separator + getLabel(SearchIndex.class), context);
	}

	/**
	 * Return the {@link SecondaryIndex} that is identified by {@code key}.
	 * 
	 * @param key
	 * @param parentStore
	 * @param context
	 * @return the SecondaryIndex
	 */
	public static SecondaryIndex loadSecondaryIndex(Text key,
			String parentStore, Context context) {
		return open(SecondaryIndex.class, Text.class, key, parentStore
				+ File.separator + getLabel(SecondaryIndex.class), context);
	}

	/**
	 * Open the record of type {@code clazz} that is stored in {@code filename}.
	 * This method will store a reference to the record in a dynamically created
	 * cache.
	 * 
	 * @param clazz
	 * @param filename
	 * @param context
	 * @return the Record
	 */
	public static <T> T open(Class<T> clazz, String filename, Context context) {
		// Find RefereceCache for Record class
		ReferenceCache<T> cache = (ReferenceCache<T>) CACHES.get(clazz);
		if(cache == null) {
			cache = new ReferenceCache<T>();
			CACHES.put(clazz, cache);
		}

		// Find Record
		String cacheKey = getCacheKey(filename);
		T record = (T) cache.get(cacheKey);
		if(record == null) {
			try {
				Constructor<T> constructor = clazz.getConstructor(String.class,
						Context.class);
				constructor.setAccessible(true);
				record = constructor.newInstance(filename, context);
				cache.put(record, cacheKey);
			}
			catch (ReflectiveOperationException e) {
				throw Throwables.propagate(e);
			}
		}
		return record;

	}

	/**
	 * Returns the cache key for the record found using {@code locator} in
	 * {@code parentStore}. The cache key is equal to the absolute filename
	 * WITHOUT the file extension.
	 * 
	 * @param locator
	 * @param parentStore
	 * @return the cache key
	 */
	private static <L extends Byteable> String getCacheKey(L locator,
			String parentStore) {
		return parentStore + File.separator + getLocale(locator)
				+ File.separator + locator;
	}

	/**
	 * Returns the cache key for the record stored in {@code filename}. The
	 * cache key is equal to the absolute filename WITHOUT the file extension.
	 * 
	 * @param filename
	 * @return the cache key
	 */
	private static String getCacheKey(String filename) {
		return filename.split("\\.")[0];
	}

	/**
	 * Return the locale for {@code locator}. There is a 1:1 mapping between
	 * locators and locales.
	 * 
	 * @param locator
	 * @return the locale
	 */
	private static <L extends Byteable> String getLocale(L locator) {
		byte[] bytes = ByteBuffers.toByteArray(locator.getBytes());
		if(locator instanceof PrimaryKey) {
			// The first 8 bytes of PrimaryKey {@code bytes} differs depending
			// upon whether the key is forStorage or notForStorage, so we must
			// ignore those to guarantee consistency.
			bytes = ArrayUtils.subarray(bytes, 8, bytes.length);
		}
		char[] hex = DigestUtils.sha256Hex(bytes).toCharArray();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < hex.length; i++) {
			sb.append(hex[i]);
			int next = i + 1;
			if(next >= LOCALE_NESTING_FACTOR
					&& next % LOCALE_NESTING_FACTOR == 0 && next < hex.length) {
				sb.append(File.separator);
			}
		}
		return sb.toString();
	}

	/**
	 * Open the record of type {@code clazz} identified by a {@code locator} of
	 * type {@code locatorClass}. This method will store a reference to the
	 * record in a dynamically created cache.
	 * 
	 * @param clazz
	 * @param locatorClass
	 * @param locator
	 * @param parentStore
	 * @param context
	 * @return the Record
	 */
	private static <T extends Record<L, ?, ?>, L extends Byteable> T open(
			Class<T> clazz, Class<L> locatorClass, L locator,
			String parentStore, Context context) {
		// Find RefereceCache for Record class
		ReferenceCache<T> cache = (ReferenceCache<T>) CACHES.get(clazz);
		if(cache == null) {
			cache = new ReferenceCache<T>();
			CACHES.put(clazz, cache);
		}

		// Find Record
		String cacheKey = getCacheKey(locator, parentStore);
		T record = (T) cache.get(cacheKey);
		if(record == null) {
			try {
				Constructor<T> constructor = clazz.getConstructor(locatorClass,
						String.class, Context.class);
				constructor.setAccessible(true);
				record = constructor.newInstance(locator, parentStore, context);
				cache.put(record, cacheKey);
			}
			catch (ReflectiveOperationException e) {
				throw Throwables.propagate(e);
			}
		}
		return record;
	}

	/**
	 * The labels used for each subclass/record type. The Record label is
	 * primarily used for grouping records on the file system and as a filename
	 * extension.
	 */
	private static final Map<String, String> LABELS;

	static {
		// See http://stackoverflow.com/a/2626960/1336833 for an explanation as
		// to why the class object itself is NOT the map key.
		LABELS = Maps.newHashMapWithExpectedSize(3);
		LABELS.put(PrimaryRecord.class.getName(), "cpr");
		LABELS.put(SecondaryIndex.class.getName(), "csi");
		LABELS.put(SearchIndex.class.getName(), "cft");
	}

	/**
	 * Determines the depth of directory nesting for locales (=64/N) and the
	 * number of possible entries in a single directory along the path (=16^N).
	 * This number should be a multiple of 2.
	 */
	private static final int LOCALE_NESTING_FACTOR = 4;

	/**
	 * We maintain a dynamically generated ReferenceCache for each Record sub
	 * class, each of which holds a SoftReference to the deserialized and
	 * up-to-date Record objects. This helps to reduce the number of disk reads
	 * that must occur when accessing a Record.
	 */
	private static final Map<Class<?>, ReferenceCache<?>> CACHES = Maps
			.newHashMap();

	/**
	 * The index is used to efficiently determine the set of values currently
	 * mapped from a key. The subclass should specify the appropriate type of
	 * key sorting via the returned type for {@link #mapType()}.
	 */
	protected final transient Map<K, Set<V>> present = mapType();

	/**
	 * The index is used to efficiently see the number of times that a revision
	 * appears in the Record and therefore determine if the revision (e.g. the
	 * key/value pair) currently exists.
	 */
	protected final transient HashMap<Revision, Integer> counts = Maps
			.newHashMap();

	/**
	 * This index is used to efficiently handle historical reads. Given a
	 * revision (e.g key/value pair), and historical timestamp, we can count the
	 * number of times that the value appears <em>beforehand</em> at determine
	 * if the mapping existed or not.
	 */
	protected final transient HashMap<K, List<Revision>> history = Maps
			.newHashMap();

	/**
	 * The context that is passed to and around the Engine for global
	 * configuration and state.
	 */
	protected final transient Context context;

	/**
	 * A reference to the {@code locator} is not stored with the Record, so the
	 * filename is the only identifying information available once the Record is
	 * loaded into memory. It is NOT guaranteed to be possible to convert from
	 * filename to locator.
	 */
	private final transient String filename;

	/**
	 * The size is equal to the first free byte in the backing file that can
	 * used when appending a new revision.
	 */
	private transient long size = 0;

	/**
	 * The record version is equal to the version of its most recent
	 * {@link Revision}.
	 */
	private transient long version = 0;

	/**
	 * This set is returned when a key does not map to any values so that the
	 * caller can transparently interact without performing checks or
	 * compromising data consistency.
	 */
	private final Set<V> emptyValues;
	private final Logger log = getLogger();

	/**
	 * Construct a new instance from the revisions that exist in the record
	 * located by {@code locator} in {@code backingStore}.
	 * 
	 * @param backingStore
	 */
	protected Record(L locator, String parentStore, Context context) {
		this(getCacheKey(locator, parentStore), true, context);
	}

	/**
	 * Construct a new instance from the record that is stored in
	 * {@code filename}.
	 * 
	 * @param filename
	 */
	protected Record(String filename, Context context) {
		this(filename, false, context);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param filename
	 * @param useExt - set to {@code true} if it is necessary to append
	 *            extension to {@code filename}, which is usually the case when
	 *            loading from a cache instead of disk.
	 * @param context
	 */
	private Record(String filename, boolean useExt, Context context) {
		this.context = context;
		this.filename = filename
				+ (useExt ? "." + getLabel(this.getClass()) : "");
		ByteBuffer content = BinaryFiles.read(this.filename);
		if(content.capacity() > 0) {
			Iterator<ByteBuffer> it = ByteableCollections.iterator(content);
			while (it.hasNext()) {
				Revision revision = new Revision(it.next());
				append(revision, false);
				if(this instanceof SecondaryIndex) {
					// Checking to see if the Record is one of its subclasses in
					// an abstract constructor is very ugly, but this is
					// necessary so that we can add items to a bloom filter as
					// they are being read from disk. If we put this logic in
					// the child constructor, then we would have to do a second
					// iteration.
					String[] parts = filename.split("\\/");
					String key = parts[parts.length - 1].split("\\.")[0];
					// We must add items to a bloom filter when deserializing in
					// order to prevent that appearance of data loss (i.e. the
					// bloom filter reporting that data does not exist, when it
					// actually does).
					context.getBloomFilters().add(key,
							((Value) revision.getKey()).getQuantity(),
							((PrimaryKey) revision.getValue()).longValue());

				}
			}
		}
		this.emptyValues = Mockito.mock(Set.class);
		Mockito.doReturn(false).when(emptyValues).add((V) Matchers.anyObject());
		Mockito.doReturn(false).when(emptyValues)
				.remove((V) Matchers.anyObject());
		Mockito.doReturn(false).when(emptyValues)
				.contains((V) Matchers.anyObject());
		Mockito.doReturn(Collections.<V> emptyListIterator()).when(emptyValues)
				.iterator();
	}

	/**
	 * Add {@code value} to the field mapped from {@code key}. This method will
	 * writeLock the entire Record.
	 * 
	 * @param key
	 * @param value
	 */
	public void add(K key, V value) {
		Lock lock = writeLock();
		try {
			Revision revision = new Revision(key, value);
			Preconditions.checkArgument(!contains(revision),
					"Cannot add %s because the mapping already exists",
					revision);
			append(revision, true);
		}
		finally {
			lock.release();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if(obj.getClass() == this.getClass()) {
			Record<L, K, V> other = (Record<L, K, V>) obj;
			return this.filename.equals(other.filename);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return filename.hashCode();
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this);
	}

	/**
	 * Remove {@code value} from the field mapped from {@code key}. This method
	 * will writeLock the entire Record.
	 * 
	 * @param key
	 * @param value
	 */
	@GuardedBy("this.writeLock, Field.writeLock")
	public void remove(K key, V value) {
		Lock lock = writeLock();
		try {
			Revision revision = new Revision(key, value);
			Preconditions.checkArgument(contains(revision),
					"Cannot remove %s because the mapping does not exist",
					revision);
			append(revision, true);
		}
		finally {
			lock.release();
		}
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " " + filename;
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this);
	}

	/**
	 * Lazily retrieve an unmodifiable view of the current set of values mapped
	 * from {@code key}.
	 * 
	 * @param key
	 * @return the set of mapped values for {@code key}
	 */
	protected Set<V> get(K key) {
		Lock lock = readLock();
		try {
			return present.containsKey(key) ? Collections
					.unmodifiableSet(present.get(key)) : emptyValues;
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Lazily retrieve the historical set of values for {@code key} at
	 * {@code timestamp}.
	 * 
	 * @param key
	 * @param timestamp
	 * @return the set of mapped values for {@code key} at {@code timestamp}.
	 */
	protected Set<V> get(K key, long timestamp) {
		Lock lock = readLock();
		try {
			Set<V> values = emptyValues;
			if(history.containsKey(key)) {
				values = Sets.newLinkedHashSet();
				Iterator<Revision> it = history.get(key).iterator();
				while (it.hasNext()) {
					Revision revision = it.next();
					if(revision.getVersion() <= timestamp) {
						if(values.contains(revision.getValue())) {
							values.remove(revision.getValue());
						}
						else {
							values.add(revision.getValue());
						}
					}
					else {
						break;
					}
				}
			}
			return values;
		}
		finally {
			lock.release();
		}

	}

	/**
	 * The class for each {@code key} in the Record.
	 * <p>
	 * Since the parameterized types associated with a Record should
	 * <strong>never</strong> change, this metadata is transient and not stored
	 * with the Record. This means that <em> any changes to the return value
	 * of this method are not backwards compatible.</em>
	 * </p>
	 * 
	 * @return the key class
	 */
	protected abstract Class<K> keyClass();

	/**
	 * Initialize the appropriate data structure for the {@link #present}.
	 * 
	 * @return the initialized mappings
	 */
	protected abstract Map<K, Set<V>> mapType();

	/**
	 * The class for each {@code value} in the Record.
	 * <p>
	 * Since the parameterized types associated with a Record should
	 * <strong>never</strong> change, this metadata is transient and not stored
	 * with the Record. This means that <em> any changes to the return value
	 * of this method are not backwards compatible.</em>
	 * </p>
	 * 
	 * @return the value class
	 */
	protected abstract Class<V> valueClass();

	/**
	 * Append {@code revision} to the record by update the in-memory indices and
	 * optionally the backing file.
	 * 
	 * @param revision
	 * @param fsync - set to {@code true} if the revision should be appended to
	 *            the backing file, which is usually the case, except when this
	 *            method is called while deserializing an existing record
	 */
	private void append(Revision revision, boolean fsync) {
		Preconditions.checkArgument(revision.getVersion() > version,
				"Cannot add %s because its version is not greater than the "
						+ "Record's current version.", revision);
		Lock lock = writeLock();
		try {
			int tSize = revision.size() + 4;
			if(fsync) {
				FileChannel channel = Files.getChannel(filename);
				try {
					channel.position(size);
					channel.write((ByteBuffer) ByteBuffer.allocate(4)
							.putInt(revision.size()).rewind());
					channel.write(revision.getBytes());
					channel.force(false);
					log.debug("Wrote {} bytes to {}.", tSize, filename);
				}
				catch (IOException e) {
					throw Throwables.propagate(e);
				}
				finally {
					Files.close(channel);
				}
			}

			// Update revision count
			int count = count(revision) + 1;
			counts.put(revision, count);

			// Update present index
			Set<V> values = present.get(revision.getKey());
			if(values == null) {
				values = Sets.<V> newLinkedHashSet();
				present.put(revision.getKey(), values);
			}
			if(Numbers.isOdd(count)) {
				values.add(revision.getValue());
			}
			else {
				values.remove(revision.getValue());
				if(values.isEmpty()) {
					present.remove(revision.getKey());
				}
			}

			// Update history index
			List<Revision> revisions = history.get(revision.getKey());
			if(revisions == null) {
				revisions = Lists.newArrayList();
				history.put(revision.getKey(), revisions);
			}
			revisions.add(revision);

			// Update metadata
			version = revision.getVersion();
			size += tSize;
			log.debug("Record {} is now {} bytes at version {}", filename,
					size, version);
		}
		finally {
			lock.release();
		}

	}

	/**
	 * Return {@code true} if {@code revision} appears an odd number of times in
	 * the Record and therefore currently exists.
	 * 
	 * @param revision
	 * @return {@code true} if {@code revision} exists
	 */
	private boolean contains(Revision revision) {
		return Numbers.isOdd(count(revision));
	}

	/**
	 * Count the number of times that {@code revision} appears in the Record.
	 * This method uses the {@link #counts} index for efficiency. <strong>If
	 * {@code revision} does not appear, this method will create a mapping from
	 * the nonexistent revision to the count of 0.</strong> Therefore, this
	 * method should only be called if it will follow an operation that appends
	 * {@code revision} to the Record.
	 * 
	 * @param revision
	 * @return the number of times {@code revision} appears
	 */
	private int count(Revision revision) {
		Integer count = counts.get(revision);
		if(count == null) {
			count = 0;
			counts.put(revision, count);
		}
		return count;
	}

	/**
	 * A {@link Revision} is a modification involving a {@code key} and
	 * {@code value} that is versioned by the value's associated timestamp.
	 * 
	 * @author jnelson
	 */
	@Immutable
	protected class Revision implements Byteable {
		// NOTE: The location of a Revision never changes once its written, so
		// it isn't necessary to hold a reference to the revision type since
		// that information can be contextually gathered by whether the revision
		// appears in an even or odd index relative to equal revisions in the
		// Record.

		/**
		 * Each {@link Revision} contains {@value #CONSTANT_SIZE} bytes of
		 * metadata for the keySize and valueSize.
		 */
		private static final int CONSTANT_SIZE = 8;

		private final K key;
		private final V value;

		/**
		 * Deserialize an instance from {@code bytes}.
		 * 
		 * @param bytes
		 */
		public Revision(ByteBuffer bytes) {
			// Decode size metadata
			int keySize = bytes.getInt();
			int valueSize = bytes.getInt();

			// Calculate position offsets
			int keyPosition = bytes.position();
			int valuePosition = keyPosition + keySize;

			// Deserialize components
			this.key = Byteables.read(
					ByteBuffers.slice(bytes, keyPosition, keySize), keyClass());
			this.value = Byteables.read(
					ByteBuffers.slice(bytes, valuePosition, valueSize),
					valueClass());
		}

		/**
		 * Construct a new instance from the specified {@code key} and
		 * {@code value}.
		 * 
		 * @param key
		 * @param value
		 */
		public Revision(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof Record.Revision) {
				Record<L, K, V>.Revision other = (Record<L, K, V>.Revision) obj;
				return key.equals(other.key) && value.equals(other.value);
			}
			return false;
		}

		@Override
		public ByteBuffer getBytes() {
			ByteBuffer buffer = ByteBuffer.allocate(size());
			buffer.putInt(key.size());
			buffer.putInt(value.size());
			buffer.put(key.getBytes());
			buffer.put(value.getBytes());
			buffer.rewind();
			return buffer;
		}

		/**
		 * Return the associated {@code key}.
		 * 
		 * @return the key
		 */
		public K getKey() {
			return key;
		}

		/**
		 * Return the associated {@code value}.
		 * 
		 * @return the value
		 */
		public V getValue() {
			return value;
		}

		/**
		 * Return the unique {@code version} identifier for this revision, which
		 * is equal to the timestamp of the {@link #value}.
		 * 
		 * @return the version id
		 */
		public long getVersion() {
			return value.getTimestamp();
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, value);
		}

		@Override
		public int size() {
			return key.size() + value.size() + CONSTANT_SIZE;
		}

		@Override
		public String toString() {
			return key + " AS " + value + " AT " + getVersion();
		}

	}
}
