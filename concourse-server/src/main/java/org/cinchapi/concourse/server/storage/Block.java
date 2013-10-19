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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.io.Syncable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.TreeMultiset;
import com.google.common.primitives.Longs;

/**
 * <p>
 * A Block is a sorted collection of Revisions that is used by the Database to
 * store indexed data. When a Block is initially created, it resides solely in
 * memory and is able to insert new revisions, which are sorted on the fly by a
 * {@link Sorter}. Once the Block is synced to disk it becomes immutable and all
 * lookups are disk based. This means that writing to a block never incurs any
 * random disk I/O. A Block is not durable until the {@link #sync()} method is
 * called.
 * </p>
 * <p>
 * Each Block is stored with a {@link BloomFilter} and a {@link BlockIndex} to
 * make lookups more efficient. The BlockFilter is used to test whether a
 * Revision involving some locator and possibly key, and possibly value
 * <em>might</em> exist in the Block. The BlockIndex is used to find the exact
 * start and end positions for Revisions involving a locator and possibly some
 * key. This means that reading from a Block never incurs any unnecessary disk
 * I/O.
 * </p>
 * 
 * 
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
abstract class Block<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> implements
		Byteable,
		Syncable {

	/**
	 * Return a new PrimaryBlock that will be stored in {@code directory}.
	 * 
	 * @param id
	 * @param directory
	 * @return the PrimaryBlock
	 */
	public static PrimaryBlock createPrimaryBlock(String id, String directory) {
		return new PrimaryBlock(id, directory, false);
	}

	/**
	 * Return a new SearchBlock that will be stored in {@code directory}.
	 * 
	 * @param id
	 * @param directory
	 * @return the SearchBlock
	 */
	public static SearchBlock createSearchBlock(String id, String directory) {
		return new SearchBlock(id, directory, false);
	}

	/**
	 * Return a new SecondaryBlock that will be stored in {@code directory}.
	 * 
	 * @param id
	 * @param directory
	 * @return the SecondaryBlock
	 */
	public static SecondaryBlock createSecondaryBlock(String id,
			String directory) {
		return new SecondaryBlock(id, directory, false);
	}

	/**
	 * Return the block id from the name of the block file.
	 * 
	 * @param filename
	 * @return the block id
	 */
	public static String getId(String filename) {
		return FileSystem.getSimpleName(filename);
	}

	/**
	 * The expected number of Block insertions. This number is used to size the
	 * Block's internal data structures.
	 */
	private static final int EXPECTED_INSERTIONS = GlobalState.BUFFER_PAGE_SIZE;

	/**
	 * The extension for the block file.
	 */
	@PackagePrivate
	static final String BLOCK_NAME_EXTENSION = ".blk";

	/**
	 * The extension for the {@link BloomFilter} file.
	 */
	private static final String FILTER_NAME_EXTENSION = ".fltr";

	/**
	 * The extension for the {@link BlockIndex} file.
	 */
	private static final String INDEX_NAME_EXTENSION = ".indx";

	/**
	 * Lock used to ensure the object is ThreadSafe. This lock provides access
	 * to a masterLock.readLock()() and masterLock.writeLock()().
	 */
	protected final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

	/**
	 * The flag that indicates whether the Block is mutable or not. A Block is
	 * mutable until a call to {@link #sync()} stores it to disk.
	 */
	protected transient boolean mutable;

	/**
	 * The running size of the Block. This number only refers to the size of the
	 * Revisions that are stored in the block file. The size for the filter and
	 * index are tracked separately.
	 */
	private transient int size;

	/**
	 * The location of the block file.
	 */
	private final String file;

	/**
	 * The unique id for the block. Each component of the block is named after
	 * the id. It is assumed that block ids should be assigned in atomically
	 * increasing order (i.e. a timestamp).
	 */
	private final String id;

	/**
	 * A collection that contains all the Revisions that have been inserted into
	 * the Block. This collection is sorted on the fly as elements are inserted.
	 * This collection is only maintained for a mutable Block. A Block that is
	 * synced and subsequently read from disk does not rely on this collection
	 * at all.
	 */
	@Nullable
	private TreeMultiset<Revision<L, K, V>> revisions;

	/**
	 * A fixed size filter that is used to test whether elements are contained
	 * in the Block without actually looking through the Block.
	 */
	private final BloomFilter filter;

	/**
	 * The index to determine which bytes in the block pertain to a locator or
	 * locator/key pair.
	 */
	private final BlockIndex index;

	/**
	 * Construct a new instance.
	 * 
	 * @param id
	 * @param directory
	 * @param diskLoad - set to {@code true} to deserialize the block {@code id}
	 *            from {@code directory} on disk
	 */
	protected Block(String id, String directory, boolean diskLoad) {
		FileSystem.mkdirs(directory);
		this.id = id;
		this.file = directory + File.separator + id + BLOCK_NAME_EXTENSION;
		if(diskLoad) {
			this.mutable = false;
			this.size = (int) FileSystem.getFileSize(this.file);
			this.filter = BloomFilter.open(directory + File.separator + id
					+ FILTER_NAME_EXTENSION);
			this.index = BlockIndex.open(directory + File.separator + id
					+ INDEX_NAME_EXTENSION);
			this.revisions = null;
		}
		else {
			this.mutable = true;
			this.size = 0;
			this.revisions = TreeMultiset.create(Sorter.INSTANCE);
			this.filter = BloomFilter.create(
					(directory + File.separator + id + FILTER_NAME_EXTENSION),
					EXPECTED_INSERTIONS);
			this.index = BlockIndex.create(directory + File.separator + id
					+ INDEX_NAME_EXTENSION, EXPECTED_INSERTIONS);
		}
	}

	/*
	 * (non-Javadoc)
	 * This method updates the {@link #index} each time it is called because it
	 * is likely that elements in {@link #revisions} are shifted around during
	 * sorting. Therefore, this method should only be called when the Block is
	 * transitioning to an immutable state.
	 */
	@Override
	public ByteBuffer getBytes() {
		masterLock.readLock().lock();
		try {
			ByteBuffer bytes = ByteBuffer.allocate(size);
			L locator = null;
			K key = null;
			for (Revision<L, K, V> revision : revisions) {
				bytes.putInt(revision.size());
				bytes.put(revision.getBytes());
				if(locator == null || locator != revision.getLocator()) {
					index.putStart(bytes.position() - revision.size() - 4,
							revision.getLocator());
					if(locator != null) {
						index.putEnd(bytes.position() - revision.size() - 5,
								locator);
					}
				}
				if(key == null || key != revision.getKey()) {
					index.putStart(bytes.position() - revision.size() - 4,
							revision.getLocator(), revision.getKey());
					if(key != null) {
						index.putEnd(bytes.position() - revision.size() - 5,
								locator, key);
					}
				}
				locator = revision.getLocator();
				key = revision.getKey();
			}
			if(revisions.size() > 0) {
				index.putEnd(bytes.position() - 1, locator);
				index.putEnd(bytes.position() - 1, locator, key);
			}
			bytes.rewind();
			return bytes;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	/**
	 * Return the block id.
	 * 
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * Insert a revision for {@code key} as {@code value} in {@code locator} at
	 * {@code version} into this Block.
	 * 
	 * @param locator
	 * @param key
	 * @param value
	 * @param version
	 * @throws IllegalStateException if the Block is not mutable
	 */
	public void insert(L locator, K key, V value, long version)
			throws IllegalStateException {
		masterLock.writeLock().lock();
		try {
			Preconditions.checkState(mutable,
					"Cannot modify a block that is not mutable");
			Revision<L, K, V> revision = makeRevision(locator, key, value,
					version);
			revisions.add(revision);
			filter.put(revision.getLocator());
			filter.put(revision.getLocator(), revision.getKey());
			filter.put(revision.getLocator(), revision.getKey(),
					revision.getValue()); // NOTE: The entire revision is added
											// to the filter so that we can
											// quickly verify that a revision
											// DOES NOT exist using
											// #mightContain(L,K,V) without
											// seeking
			size += revision.size() + 4;
		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	/**
	 * Return {@code true} if this Block might contain revisions involving
	 * {@code key} as {@code value} in {@code locator}. This method <em>may</em>
	 * return a false positive, but never a false negative. If this method
	 * returns {@code true}, the caller should seek for {@code key} in
	 * {@code locator} and check if any of those revisions contain {@code value}
	 * as a component.
	 * 
	 * @param locator
	 * @param key
	 * @param value
	 * @return {@code true} if it is possible that relevant revisions exists
	 */
	public boolean mightContain(L locator, K key, V value) {
		masterLock.readLock().lock();
		try {
			return filter.mightContain(locator, key, value);
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	/**
	 * Seek revisions that contain {@code key} in {@code locator} and append
	 * them to {@code record} if it is <em>likely</em> that those revisions
	 * exist in this Block.
	 * 
	 * @param locator
	 * @param key
	 * @param record
	 */
	@GuardedBy("seek(Record, Byteable...)")
	public void seek(L locator, K key, Record<L, K, V> record) {
		seek(record, locator, key);
	}

	/**
	 * Seek revisions that contain any key in {@code locator} and append them to
	 * {@code record} if it is <em>likely</em> that those revisions exist in
	 * this Block.
	 * 
	 * @param locator
	 * @param record
	 */
	@GuardedBy("seek(Record, Byteable...)")
	public void seek(L locator, Record<L, K, V> record) {
		seek(record, locator);
	}

	@Override
	public int size() {
		masterLock.readLock().lock();
		try {
			return size;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	/**
	 * Flush the content to disk in a block file, sync the filter and index and
	 * finally make the Block immutable.
	 */
	@Override
	public void sync() {
		masterLock.writeLock().lock();
		try {
			if(size > 0) {
				Preconditions.checkState(mutable,
						"Cannot sync a block that is not mutable");
				mutable = false;
				FileChannel channel = FileSystem.getFileChannel(file);
				channel.write(getBytes());
				channel.force(false);
				filter.sync();
				index.sync();
				FileSystem.closeFileChannel(channel);
				revisions = null; // Set to NULL so that the Set is eligible for
									// GC while the Block stays in memory.
			}
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
		finally {
			masterLock.writeLock().unlock();
		}

	}

	/**
	 * Return a {@link Revision} for {@code key} as {@code value} in
	 * {@code locator} at {@code version}.
	 * 
	 * @param locator
	 * @param key
	 * @param value
	 * @param version
	 * @return the Revision
	 */
	protected abstract Revision<L, K, V> makeRevision(L locator, K key,
			V value, long version);

	/**
	 * Return the class of the {@code revision} type.
	 * 
	 * @return the revision class
	 */
	protected abstract Class<? extends Revision<L, K, V>> xRevisionClass();

	/**
	 * Seek revisions that contain components from {@code byteables} and append
	 * them to {@code record}. The seek will be perform in memory iff this block
	 * is mutable, otherwise, the seek is performed on disk.
	 * 
	 * @param record
	 * @param byteables
	 */
	private void seek(Record<L, K, V> record, Byteable... byteables) {
		masterLock.readLock().lock();
		try {
			if(filter.mightContain(byteables)) {
				if(mutable) {
					Iterator<Revision<L, K, V>> it = revisions.iterator();
					boolean processing = false;
					boolean checkSecond = byteables.length > 1;
					while (it.hasNext()) {
						Revision<L, K, V> revision = it.next();
						if(revision.getLocator() == byteables[0]
								&& ((checkSecond && revision.getKey() == byteables[1]) || !checkSecond)) {
							processing = true;
							record.append(revision);
						}
						else if(processing) {
							break;
						}
					}
				}
				else {
					int start = index.getStart(byteables);
					int length = index.getEnd(byteables) - (start - 1);
					if(start != BlockIndex.NO_ENTRY && length > 0) {
						ByteBuffer bytes = FileSystem.map(file,
								MapMode.READ_ONLY, start, length);
						Iterator<ByteBuffer> it = ByteableCollections
								.iterator(bytes);
						while (it.hasNext()) {
							Revision<L, K, V> revision = Byteables.read(
									it.next(), xRevisionClass());
							record.append(revision);
						}
					}
				}
			}
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	/**
	 * A Comparator that sorts Revisions in a block. The sort order is
	 * {@code locator} followed by {@code key} followed by {@code version}.
	 * 
	 * @author jnelson
	 */
	@SuppressWarnings("rawtypes")
	private enum Sorter implements Comparator<Revision> {
		INSTANCE;

		/**
		 * Sorts by locator followed by key followed by version.
		 */
		@Override
		@SuppressWarnings("unchecked")
		public int compare(Revision o1, Revision o2) {
			int order;
			return (order = o1.getLocator().compareTo(o2.getLocator())) != 0 ? order
					: ((order = o1.getKey().compareTo(o2.getKey())) != 0 ? order
							: (Longs.compare(o1.getVersion(), o2.getVersion())));
		}

	}
}
