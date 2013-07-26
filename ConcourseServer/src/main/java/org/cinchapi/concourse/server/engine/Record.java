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

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.ArrayUtils;
import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.io.ByteableCollections;
import org.cinchapi.common.io.Byteables;
import org.cinchapi.common.io.Files;
import org.cinchapi.common.multithread.Lock;
import org.cinchapi.common.multithread.Lockable;
import org.cinchapi.common.multithread.Lockables;
import org.cinchapi.common.tools.Path;
import org.cinchapi.concourse.server.ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * A version controlled collection of key/value mappings that are encapsulated
 * as Fields.
 * <p>
 * Each Record is identified by a {@code locator} and is stored in a distinct
 * file<sup>1</sup> on disk. The entire Record is deserialized or loaded from a
 * cache for each relevant read or write request. In memory, each Record
 * maintains a key to {@link Field} mapping. To afford the caller
 * flexibility<sup>2</sup>, changes are not automatically flushed to disk, but
 * must be done explicitly by calling {@link #fsync()}.
 * </p>
 * <sup>1</sup> - Records are randomly grouped into locales. <br>
 * <sup>2</sup> - Syncing data to disk read locks the entire Record, so it is
 * ideal to give the caller discretion with the bottleneck.
 * 
 * 
 * @author jnelson
 * @param <L> - the locator type
 * @param <K> - the key type
 * @param <V> - the value type
 */
@PackagePrivate
@ThreadSafe
abstract class Record<L extends Byteable, K extends Byteable, V extends Storable> implements
		Lockable,
		Byteable {

	/**
	 * Return the locale for {@code locator}. There is a 1:1 mapping between
	 * locators and locales.
	 * 
	 * @param locator
	 * @return the locale
	 */
	static <L extends Byteable> String getLocale(L locator) {
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
	 * @return the Record
	 */
	static <T extends Record<L, ?, ?>, L extends Byteable> T open(
			Class<T> clazz, Class<L> locatorClass, L locator) {
		try {
			Constructor<T> constructor = clazz.getConstructor(locatorClass);
			constructor.setAccessible(true);
			return constructor.newInstance(locator);

		}
		catch (ReflectiveOperationException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * Determines the depth of directory nesting for locales (64/N) and the
	 * number of possible entries in a single directory along the path (16^N).
	 * This number should be a multiple of 2.
	 */
	@PackagePrivate
	static final int LOCALE_NESTING_FACTOR = 4;

	/**
	 * I use a Map instead of a Set here so that the we don't have to rely on
	 * the Field implementation class to specify hashCode(), equals() and
	 * compareTo() methods. If those things matter, we can just assume that
	 * they chosen Key type reflects that.
	 */
	protected final Map<K, Field<K, V>> fields;

	/**
	 * The mock is returned to callers to allow transparent interaction in the
	 * event that a field does not exist in the record without compromising data
	 * consistency (i.e. trying to read from a field that does not exist).
	 */
	private final transient Field<K, V> mock = Field
			.mock((Class<Field<K, V>>) fieldImplClass());

	/**
	 * This is a cache of the most recent byte encoding for the record. Every
	 * call to {@link #add(Byteable, Storable)} and
	 * {@link #remove(Byteable, Storable)} will nullify this variable.
	 */
	private transient ByteBuffer buffer = null;

	/**
	 * A reference to the {@code locator} is not stored with the Record, so the
	 * filename is the only identifying information available once the Record is
	 * loaded into memory. It is not possible to convert from filename to
	 * locator.
	 */
	private final transient String filename;

	private static final Logger log = LoggerFactory.getLogger(Record.class);

	/**
	 * Construct the Record found by {@code locator}. If the Record exists, its
	 * existing content is loaded. Otherwise, a new Record is created.
	 * 
	 * @param locator
	 */
	protected Record(L locator) {
		this.filename = new Path(false, ServerConstants.DATA_HOME, "db",
				Record.getLocale(locator)).setExt(fileNameExt()).toString();
		Files.makeParentDirs(filename);
		this.fields = init();
		long length = Files.length(filename);
		if(length > 0) {
			this.buffer = Files.map(filename, MapMode.READ_ONLY, 0, length);
			Iterator<ByteBuffer> it = ByteableCollections.iterator(buffer);
			while (it.hasNext()) {
				Field<K, V> field = (Field<K, V>) Byteables.read(it.next(),
						fieldImplClass());
				fields.put(field.getKey(), field);
			}
		}
	}

	/**
	 * Add {@code value} to the field mapped from {@code key}. This method will
	 * writeLock the entire Record.
	 * 
	 * @param key
	 * @param value
	 */
	@GuardedBy("this.writeLock, Field.writeLock")
	public void add(K key, V value) {
		get(key, true).add(value);
		buffer = null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if(obj.getClass() == this.getClass()) {
			Record<L, K, V> other = (Record<L, K, V>) obj;
			return this.filename.equals(other.filename);
		}
		return false;
	}

	/**
	 * Flush all the data in the Record back to disk. This operation read locks
	 * the entire Record and overwrites the content of {@link #filename} with
	 * the data that currently exists in memory.
	 */
	@GuardedBy("this.readLock")
	public final void fsync() {
		Lock lock = writeLock();
		try {
			if(!fields.isEmpty()) {
				String backup = filename + ".bak";
				Files.copy(filename, backup);
				Byteables.write(this, Files.getChannel(filename));
				Files.delete(backup);
				log.debug("Wrote a total of {} bytes during fsync of {}",
						size(), this);
			}
			else {
				// An empty collection of {#link #fields} usually indicates that
				// the Record was deserialized from a read operation before a
				// write operation occurred, so it is okay for me to
				// delete the file at sync time if there have not been any
				// writes.
				delete();
				log.debug("DELETED {} because it does not contain "
						+ "any data to fsync", this);
			}
		}
		finally {
			lock.release();
		}
	}

	@Override
	@GuardedBy("writeLock")
	public ByteBuffer getBytes() {
		Lock lock = writeLock();
		try {
			if(buffer == null) {
				buffer = ByteableCollections.toByteBuffer(fields.values());
			}
			buffer.rewind();
			return buffer;
		}
		finally {
			lock.release();
		}
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
		get(key).remove(value);
		buffer = null;
	}

	@Override
	public int size() {
		return getBytes().capacity();
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
	 * The class of the Field implementation that is used to store data within
	 * the Record.
	 * 
	 * @return the class
	 */
	protected abstract <T extends Field<K, V>> Class<T> fieldImplClass();

	/**
	 * The extension used for the storage filename.
	 * 
	 * @return the filename extensions
	 */
	protected abstract String fileNameExt();

	/**
	 * Lazily retrieve the field mapped from {@code key}.
	 * 
	 * @param key
	 * @return the Field for {@code key}
	 */
	@GuardedBy("this.writeLock")
	protected Field<K, V> get(K key) {
		return get(key, false);
	}

	/**
	 * Return a mapping from {@code K} objects to {@link Field} objects.
	 * 
	 * @return the fields mapping
	 */
	protected abstract Map<K, Field<K, V>> init();

	/**
	 * The class of the Key that is used to identify fields within the Record.
	 * 
	 * @return the class
	 */
	protected abstract Class<K> keyClass();

	/**
	 * Delete this Record. This method will delete the backing file, but the
	 * Record object will continue to reside in memory until it the
	 * object is garbage collected.
	 */
	@DoNotInvoke
	@PackagePrivate
	final void delete() {
		Files.delete(filename);
	}

	/**
	 * Lazily retrieve a field from {@link #fields} with the option to
	 * {@code create} a new field for {@code key} if one does not already exist
	 * or to return {@code mock}.
	 * 
	 * @param key
	 * @param create
	 * @return the Field for {@code key}
	 */
	@GuardedBy("this.writeLock")
	private Field<K, V> get(K key, boolean create) {
		if(fields.containsKey(key)) {
			return fields.get(key);
		}
		else if(create) {
			Lock lock = writeLock();
			try {
				Constructor<Field<K, V>> constructor = fieldImplClass()
						.getConstructor((Class<K>) keyClass());
				constructor.setAccessible(true);
				Field<K, V> field = constructor.newInstance(key);
				fields.put(key, field);
				return field;
			}
			catch (Exception e) {
				throw Throwables.propagate(e);
			}
			finally {
				lock.release();
			}
		}
		else {
			return mock;
		}
	}
}
