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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.lock.Lock;
import com.cinchapi.common.lock.Lockable;
import com.cinchapi.common.lock.Lockables;
import com.cinchapi.common.util.Hash;
import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.cinchapi.concourse.io.ByteSized;
import com.cinchapi.concourse.io.ByteSizedCollections;
import com.cinchapi.concourse.io.Persistable;
import com.cinchapi.concourse.io.Persistables;

import static com.google.common.base.Preconditions.*;
import static com.cinchapi.concourse.conf.Constants.*;

/**
 * <p>
 * A {@code Tuple} is a {@link Bucket} based abstract data structure that maps
 * keys to values.
 * </p>
 * <p>
 * Each Tuple is identified by a {@code locator} and is stored in a distinct
 * file<sup>1</sup> on disk. The entire tuple is deserialized or loaded from a
 * cache for each relevant read or write request. To afford the caller
 * flexibility<sup>2</sup>, changes are not automatically flushed to disk, but
 * must be done explicitly by calling {@link #fsync()}.
 * </p>
 * <sup>1</sup> - Tuples are randomly grouped into locales based on the
 * {@code locator}.<br>
 * <sup>2</sup> - Syncing data to disk read locks the entire tuple, so it is
 * ideal to give the caller discretion with the bottleneck.
 * <p>
 * In memory, each tuple maintains a mapping of key names to buckets. A tuple
 * can hold up to {@value #MAX_NUM_BUCKETS} buckets throughout its lifetime, but
 * in actuality this limit is lower because the size of a bucket can very widely
 * and is guaranteed to increase with every revision. Therefore it is more
 * useful to use the maximum allowable storage as a guide.
 * </p>
 * 
 * 
 * @author jnelson
 * @param <K> - the key type
 * @param <V> - the value type
 */
abstract class Tuple<K extends ByteSized, V extends Storable> implements
		IterableByteSequences,
		Persistable,
		Lockable {

	/*
	 * A larger name length allows more locales (and therefore a smaller
	 * locale:tuple ratio), however the filesystem can act funny if a single
	 * directory has too many files. This number should seek to have the
	 * locale:row ratio that is similar to the number of possible locales while
	 * being mindful of not having too many files in a single directory.
	 */
	private static final int LOCALE_NAME_LENGTH = 4;

	/**
	 * The child class should override this variable with a specific extension.
	 * The default is "ccs" which stands for
	 * <em><strong>c</strong>on<strong>c</strong>ourse <strong>s</strong>tore</em>
	 */
	protected static final String FILE_NAME_EXT = "ccs";

	/**
	 * The maximum allowable size of a tuple. This limitation exists because
	 * the
	 * size of the tuple is encoded as an integer.
	 */
	public static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;

	/**
	 * The weighted maximum number of buckets that can exist in a tuple.
	 * In actuality, this limit is much lower because the size of a bucket can
	 * very widely.
	 */
	public static final int MAX_NUM_BUCKETS = MAX_SIZE_IN_BYTES
			/ Bucket.WEIGHTED_SIZE_IN_BYTES;
	private static final int INITIAL_CAPACITY = 16;

	/*
	 * Since each bucket maintains its own key, it is possible to use a Set to
	 * contain the buckets instead of a map, but I decided to go with a Map
	 * because some Set implementations merely server as a wrapper for an
	 * internal Map anyway and managing keys here means that the Bucket
	 * abstraction doesn't need to worry about hashCode(), equals() and
	 * compareTo() methods (these can be handled by implementations of the
	 * Tuple abstraction as necessary).
	 * 
	 * Buckets should never be removed from the map. Therefore the only point of
	 * thread contention is insertion, which is accounted for in the
	 * {@link #newBucket()} method using a local write lock.
	 */
	private final Map<K, Bucket<K, V>> buckets;

	/*
	 * This is the absolute path filename, which is derived directly from the
	 * locator (meaning we don't have to store the locator with the tuple).
	 */
	private transient final String filename;
	private transient final ReentrantReadWriteLock locksmith = new ReentrantReadWriteLock();
	private transient final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * Construct a new instance. This constructor can be used to create a
	 * new/empty tuple or to deserialize an existing tuple identified by
	 * {@code locator}.
	 * 
	 * @param locator
	 */
	protected <L extends ByteSized> Tuple(L locator) {
		this.filename = Utilities.getFileName(locator);
		try {
			File file = new File(filename);
			file.getParentFile().mkdirs();
			this.buckets = file.createNewFile() ? getNewBuckets(INITIAL_CAPACITY)
					: getBucketsFromFile(filename);
		}
		catch (IOException e) {
			log.error("An error occured while trying to "
					+ "deserialize tuple {} from {}: {}", locator, filename, e);
			throw new ConcourseRuntimeException(e);
		}
	}

	@Override
	public byte[] getBytes() {
		Lock lock = readLock();
		try {
			return ByteSizedCollections.toByteArray(buckets.values());
		}
		finally {
			lock.release();
		}
	}

	@Override
	public ByteSequencesIterator iterator() {
		Lock lock = readLock();
		try {
			return IterableByteSequences.ByteSequencesIterator.over(getBytes());
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this, locksmith);
	}

	@Override
	public int size() {
		Lock lock = readLock();
		try {
			int size = 0;
			Iterator<Bucket<K, V>> it = buckets.values().iterator();
			while (it.hasNext()) {
				size += it.next().size();
			}
			return size;
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this, locksmith);
	}

	@Override
	public void writeTo(FileChannel channel) throws IOException {
		Lock lock = readLock();
		try {
			Persistables.write(this, channel);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return an unmodifiable view of the collection of buckets.
	 * 
	 * @return the collection of buckets
	 */
	protected Map<K, Bucket<K, V>> buckets() {
		return Collections.unmodifiableMap(buckets);
	}

	/**
	 * Return the appropriate {@link Bucket} from {@code bytes}.
	 * 
	 * @return the {@code bucket}
	 */
	protected abstract Bucket<K, V> getBucketFromByteSequence(ByteBuffer bytes);

	/**
	 * Return a {@code mock} bucket by calling {@link Bucket#mock(Class)} method
	 * with the appropriate class as input. Mock buckets are used as an
	 * abstraction during read requests for buckets that do not exist so that
	 * the caller doesn't have to perform explicit null or existence checks).
	 * 
	 * @return the mock {@code bucket}
	 */
	protected abstract Bucket<K, V> getMockBucket();

	/**
	 * Return a new bucket identified by {@code key}.
	 * 
	 * @return the new {@code bucket}
	 */
	protected abstract Bucket<K, V> getNewBucket(K key);

	/**
	 * Return a newly initialized map to hold the buckets in the tuple.
	 * 
	 * @param expectedSize
	 * @return the buckets map
	 */
	protected abstract Map<K, Bucket<K, V>> getNewBuckets(int expectedSize);

	/**
	 * Add {@code value} to the bucket identified by {@code key}. If necessary,
	 * the caller should lock externally before calling this method.
	 * 
	 * @param key
	 * @param value
	 */
	final void add(K key, V value) {
		get(key, true).add(value);
	}

	/**
	 * Flush all the data back to disk. This operation will read lock the entire
	 * tuple and overwrite the content of {@link #filename} with the data
	 * that currently exists in memory.
	 */
	/*
	 * I use this approach instead of a RandomAccessFile/MappedByteBuffer
	 * combination because it isn't possible to know the precise ways in which a
	 * file will change for a given write. Even if I could determine how the
	 * file would change, the overhead of constantly moving bytes around in
	 * an existing file is large and unsafe.
	 */
	final void fsync() {
		if(!buckets.isEmpty()) {
			Lock lock = readLock();
			try {
				FileChannel channel = new FileOutputStream(filename)
						.getChannel();
				channel.position(0);
				writeTo(channel);
			}
			catch (IOException e) {
				log.error(
						"An error occured while trying to fsync a {} to {}: {}",
						getClass().getName(), filename, e);
				throw new ConcourseRuntimeException(e);
			}
			finally {
				lock.release();
			}
		}
		else {
			// An empty collection of {#link #buckets} usually indicates that
			// the tuple was deserialized from a read operation before a
			// write operation occurred, so it is okay for me to
			// delete the file at sync time if there have not been any writes.
			new File(filename).delete();
		}
	}

	/**
	 * Return the {@code bucket} identified by {@code key}.
	 * 
	 * @param key
	 * @return the {@code bucket}
	 */
	final Bucket<K, V> get(K key) {
		return get(key, false);
	}

	/**
	 * Remove {@code value} from the bucket identified by {@code key}. If
	 * necessary, the caller should lock externally before calling this method.
	 * 
	 * @param key
	 * @param value
	 * @throws IllegalArgumentException if there is no bucket identified by
	 *             {@code key} in the tuple
	 */
	final void remove(K key, V value) throws IllegalArgumentException {
		checkArgument(buckets.containsKey(key),
				"There is no bucket identified by {} in the tuple", key);
		get(key).remove(value);
	}

	/**
	 * Return the {@code bucket} identified by {@code key} and optionally
	 * {@create} a new bucket to return if one does not already exist.
	 * 
	 * @param key
	 * @param create
	 * @return the {@code bucket}
	 */
	private Bucket<K, V> get(K key, boolean create) {
		return buckets.containsKey(key) ? buckets.get(key)
				: (create ? insert(key) : getMockBucket());
	}

	/**
	 * Return the collection of buckets stored in the file at {@code filename}.
	 * 
	 * @param filename
	 * @return the {@code buckets}
	 */
	private Map<K, Bucket<K, V>> getBucketsFromFile(String filename) {
		try {
			FileChannel channel = new FileInputStream(filename).getChannel();
			int size = (int) channel.size();
			ByteBuffer bytes = ByteBuffer.allocate(size);
			channel.read(bytes);
			Map<K, Bucket<K, V>> buckets = getNewBuckets(size
					/ Bucket.WEIGHTED_SIZE_IN_BYTES);
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(bytes.array());
			while (bsit.hasNext()) {
				Bucket<K, V> bucket = getBucketFromByteSequence(bsit.next());
				buckets.put(bucket.getKey(), bucket);
			}
			return buckets;
		}
		catch (IOException e) {
			log.error("An error occured while trying to "
					+ "deserialize the buckets from {}: {}", filename, e);
			throw new ConcourseRuntimeException(e);
		}
	}

	/**
	 * Put a new {@code bucket} identified by {@code key} to the tuple.
	 * 
	 * @param key
	 * @return the new {@code bucket}
	 */
	private Bucket<K, V> insert(K key) {
		Bucket<K, V> bucket = getNewBucket(key);
		Lock lock = writeLock();
		try {
			buckets.put(key, bucket);
		}
		finally {
			lock.release();
		}
		return bucket;
	}

	/**
	 * Utilities for the {@link Tuple} objects.
	 * 
	 * @author jnelson
	 */
	private static abstract class Utilities {

		/**
		 * Return the absolute path of the {@code filename} for the tuple
		 * identified by {@code locator}.
		 * 
		 * @param locator
		 * @return the {@code filename}
		 */
		private static <L extends ByteSized> String getFileName(L locator) {
			StringBuilder sb = new StringBuilder();
			sb.append(CONCOURSE_HOME);
			sb.append(File.separator);
			sb.append(getLocale(locator));
			sb.append(File.separator);
			sb.append(locator.toString());
			sb.append(".");
			sb.append(FILE_NAME_EXT);
			return sb.toString();
		}

		/**
		 * Return the {@code locale} for {@code key}.
		 * 
		 * @param key
		 * @return the {@code locale}
		 */
		private static <L extends ByteSized> String getLocale(L locator) {
			return Hash.toString(Hash.sha256(locator.getBytes())).substring(0,
					LOCALE_NAME_LENGTH);
		}
	}
}
