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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.NaturalSorter;
import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static org.cinchapi.concourse.server.GlobalState.*;
import static org.cinchapi.concourse.util.Loggers.getLogger;

/**
 * A {@code Buffer} is a special implementation of {@link Limbo} that aims to
 * quickly accumulate writes in memory before performing a batch flush into some
 * {@link PermanentStore}.
 * <p>
 * A Buffer enforces the durability guarantee because all writes are also
 * immediately flushed to disk. Even though there is some disk I/O, the overhead
 * is minimal and writes are fast because the entire backing store is memory
 * mapped and the writes are always appended.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
final class Buffer extends Limbo {

	/**
	 * The average number of bytes used to store an arbitrary Write.
	 */
	private static final int AVG_WRITE_SIZE = 30; /* arbitrary */
	private static final Logger log = getLogger();

	/**
	 * The directory where the Buffer pages are stored.
	 */
	private final String directory;

	/**
	 * The sequence of Pages that make up the Buffer.
	 */
	private final List<Page> pages = Lists.newArrayList();

	/**
	 * The transport lock makes it possible to append new Writes and transport
	 * old writes concurrently while prohibiting reading the buffer and
	 * transporting writes at the same time.
	 */
	private final ReentrantReadWriteLock transportLock = new ReentrantReadWriteLock();

	/**
	 * A pointer to the current Page.
	 */
	private Page currentPage;

	/**
	 * A flag to indicate if the Buffer is running or not.
	 */
	private boolean running = false;

	/**
	 * Construct a Buffer that is backed by the default location, which is
	 * {@link GlobalState#BUFFER_DIRECTORY}.
	 * 
	 */
	public Buffer() {
		this(BUFFER_DIRECTORY);
	}

	/**
	 * 
	 * Construct a a Buffer that is backed by {@code backingStore}. Existing
	 * content, if available, will be loaded from the file. Otherwise, a new and
	 * empty Buffer will be returned.
	 * 
	 * @param directory
	 */
	public Buffer(String directory) {
		FileSystem.mkdirs(directory);
		this.directory = directory;
	}

	@Override
	public Map<Long, String> audit(long record) {
		transportLock.readLock().lock();
		try {
			return super.audit(record);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		transportLock.readLock().lock();
		try {
			return super.audit(key, record);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Set<String> describe(long record) {
		transportLock.readLock().lock();
		try {
			return super.describe(record);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		transportLock.readLock().lock();
		try {
			return super.describe(record, timestamp);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		transportLock.readLock().lock();
		try {
			return super.fetch(key, record);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		transportLock.readLock().lock();
		try {
			return super.fetch(key, record, timestamp);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		transportLock.readLock().lock();
		try {
			return super.find(timestamp, key, operator, values);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		transportLock.readLock().lock();
		try {
			return super.find(key, operator, values);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Iterator<Write> iterator() {

		return new Iterator<Write>() {

			private Iterator<Page> pageIterator = pages.iterator();
			private Iterator<Write> writeIterator = null;

			{
				flip();
			}

			@Override
			public boolean hasNext() {
				if(writeIterator == null) {
					return false;
				}
				else if(writeIterator.hasNext()) {
					return true;
				}
				else {
					flip();
					return hasNext();
				}
			}

			@Override
			public Write next() {
				return writeIterator.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			/**
			 * Flip to the next page in the Buffer.
			 */
			private void flip() {
				writeIterator = null;
				if(pageIterator.hasNext()) {
					Page next = pageIterator.next();
					writeIterator = next.iterator();
				}
			}

		};
	}

	@Override
	public boolean ping(long record) {
		transportLock.readLock().lock();
		try {
			return super.ping(record);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public Set<Long> search(String key, String query) {
		transportLock.readLock().lock();
		try {
			return super.search(key, query);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	public void start() {
		if(!running) {
			running = true;
			log.info("Buffer configured to store data in {}", directory);
			SortedMap<File, Page> pageSorter = Maps
					.newTreeMap(NaturalSorter.INSTANCE);
			for (File file : new File(directory).listFiles()) {
				Page page = new Page(file.getAbsolutePath());
				pageSorter.put(file, page);
				log.info("Loadding Buffer content from {}...", page);
			}
			pages.addAll(pageSorter.values());
			addPage();
		}
	}

	/**
	 * {@inheritDoc} This method will transport the first write in the buffer.
	 */
	@Override
	public void transport(PermanentStore destination) {
		// It makes sense to only transport one write at a time because
		// transporting blocks reading and all writes must read at least once,
		// so we want to minimize the overhead per write.
		if(pages.size() > 1
				&& !transportLock.writeLock().isHeldByCurrentThread()
				&& transportLock.writeLock().tryLock()) {
			try {
				Page page = pages.get(0);
				if(page.hasNext()) {
					destination.accept(page.next());
					page.remove();
				}
				else {
					((Database) destination).triggerSync();
					removePage();
				}
			}
			finally {
				transportLock.writeLock().unlock();
			}
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		transportLock.readLock().lock();
		try {
			return super.verify(key, value, record, timestamp);
		}
		finally {
			transportLock.readLock().unlock();
		}
	}

	@Override
	protected boolean insert(Write write) {
		masterLock.writeLock().lock();
		try {
			currentPage.append(write);
		}
		catch (BufferCapacityException e) {
			addPage();
			insert(write);
		}
		finally {
			masterLock.writeLock().unlock();
		}
		return true;
	}

	@Override
	protected boolean verify(Write write, long timestamp) {
		masterLock.readLock().lock();
		transportLock.readLock().lock();
		try {
			for (Page page : pages) {
				if(page.mightContain(write)) {
					return super.verify(write, timestamp);
				}
			}
			return false;
		}
		finally {
			masterLock.readLock().unlock();
			transportLock.readLock().unlock();
		}
	}

	/**
	 * Add a new Page to the Buffer.
	 */
	private void addPage() {
		masterLock.writeLock().lock();
		try {
			currentPage = new Page(BUFFER_PAGE_SIZE);
			pages.add(currentPage);
			log.debug("Added page {} to Buffer", currentPage);
		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	/**
	 * Remove the first page in the Buffer.
	 */
	private void removePage() {
		masterLock.writeLock().lock();
		try {
			pages.remove(0).delete();
		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	/**
	 * A {@link Page} represents a granular section of the {@link Buffer}. Pages
	 * are an append-only iterator over a sequence of {@link Write} objects.
	 * Pages differ from other iterators because they do not advance in the
	 * sequence until the {@link #remove()} method is called.
	 * 
	 * @author jnelson
	 */
	private class Page implements Iterator<Write>, Iterable<Write> {

		/**
		 * The filename extension.
		 */
		private static final String ext = ".buf";

		/**
		 * The append-only list of {@link Write} objects on the Page. Elements
		 * are never deleted from this list, but are marked as "removed"
		 * depending on the location of the {@link #head} index.
		 */
		private final Write[] writes;

		/**
		 * The filter that tests whether a Write <em>may</em> exist on this Page
		 * or not.
		 */
		private final BloomFilter filter = BloomFilter
				.create(GlobalState.BUFFER_PAGE_SIZE);

		/**
		 * The append-only buffer that contains the content of the backing file.
		 * Data is never deleted from the buffer, until the entire Page is
		 * removed.
		 */
		private final MappedByteBuffer content;

		/**
		 * The file that contains the content of the Page.
		 */
		private final String filename;

		/**
		 * Indicates the index in {@link #writes} that constitutes the first
		 * element. When writes are "removed", elements are not actually deleted
		 * from the list, so it is necessary to keep track of the head element
		 * so that the correct next() element can be returned.
		 */
		private transient int head = 0;

		/**
		 * The total number of elements in the list of {@link #writes}.
		 */
		private transient int size = 0;

		/**
		 * Construct an empty Page with {@code capacity} bytes.
		 * 
		 * @param size
		 */
		public Page(int capacity) {
			this(directory + File.separator + Time.now() + ext, capacity);
		}

		/**
		 * Construct a Page that is backed by {@code filename}. Existing
		 * content, if available, will be loaded from the file starting at the
		 * position specified in {@link #pos}.
		 * <p>
		 * Please note that this constructor is designed to deserialize a
		 * retired page, so the returned Object will be at capacity and unable
		 * to append additional {@link Write} objects.
		 * </p>
		 * 
		 * @param filename
		 */
		public Page(String filename) {
			this(filename, FileSystem.getFileSize(filename));
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param filename
		 * @param capacity
		 */
		private Page(String filename, long capacity) {
			this.filename = filename;
			this.content = FileSystem.map(filename, MapMode.READ_WRITE, 0,
					capacity);
			this.writes = new Write[(int) (capacity / AVG_WRITE_SIZE)];
			Iterator<ByteBuffer> it = ByteableCollections.iterator(content);
			while (it.hasNext()) {
				Write write = Write.fromByteBuffer(it.next());
				index(write);
				log.debug("Found existing write '{}' in the Buffer", write);
			}
		}

		/**
		 * Append {@code write} to the Page. This method <em>does not</em>
		 * verify that {@link #content} has enough remaining capacity to store
		 * {@code write}.
		 * 
		 * @param write
		 * @throws BufferCapacityException - if the size of {@code write} is
		 *             greater than the remaining capacity of {@link #content}
		 */
		@GuardedBy("Buffer#insert(Write)")
		public void append(Write write) throws BufferCapacityException {
			if(content.remaining() >= write.size() + 4) {
				index(write);
				content.putInt(write.size());
				content.put(write.getBytes());
				content.force();
			}
			else {
				throw new BufferCapacityException();
			}
		}

		/**
		 * Delete the page from disk. The Page object will reside in memory
		 * until garbage collection.
		 */
		public void delete() {
			FileSystem.deleteFile(filename);
			log.info("Deleting Buffer page {}", filename);
		}

		/**
		 * Returns {@code true} if {@link #head} is smaller than the largest
		 * occupied index in {@link #writes}. This means that it is possible for
		 * calls to this method to initially return {@code false} at t0, but
		 * eventually return {@code true} at t1 if an element is added to the
		 * Page between t0 and t1.
		 */
		@Override
		public boolean hasNext() {
			masterLock.readLock().lock();
			try {
				return head < size;
			}
			finally {
				masterLock.readLock().unlock();
			}
		}

		/**
		 * <p>
		 * Returns an iterator that is appropriate for the append-only list of
		 * {@link Write} objects that backs the Page. The iterator does not
		 * support the {@link Iterator#remove()} method and only throws a
		 * {@link ConcurrentModificationException} if an element is removed from
		 * the Page using the {@link #remove()} method.
		 * </p>
		 * <p>
		 * While the Page is, itself, an iterator (for transporting Writes), the
		 * iterator returned from this method is appropriate for cases when it
		 * is necessary to iterate through the page for reading.
		 * </p>
		 */
		/*
		 * (non-Javadoc)
		 * This iterator is only used for Limbo reads that traverse the
		 * collection of Writes. This iterator differs from the Page (which is
		 * also an Iterator over Write objects) by virtue of the fact that it
		 * does not allow removes and will detect concurrent modification.
		 */
		@Override
		public Iterator<Write> iterator() {

			return new Iterator<Write>() {

				/**
				 * The index of the "next" element in {@link #writes}.
				 */
				private int index = head;

				/**
				 * The distance between the {@link #head} element and the
				 * {@code next} element. This is used to detect for concurrent
				 * modifications.
				 */
				private int distance = 0;

				@Override
				public boolean hasNext() {
					masterLock.readLock().lock();
					try {
						if(index - head != distance) {
							throw new ConcurrentModificationException(
									"A write has been removed from the Page");
						}
						return index < size;
					}
					finally {
						masterLock.readLock().unlock();
					}
				}

				@Override
				public Write next() {
					masterLock.readLock().lock();
					try {
						if(index - head != distance) {
							throw new ConcurrentModificationException(
									"A write has been removed from the Page");
						}
						Write next = writes[index];
						index++;
						distance++;
						return next;
					}
					finally {
						masterLock.readLock().unlock();
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();

				}

			};
		}

		/**
		 * Return {@code true} if the Page <em>may</em> contain {@code write}.
		 * 
		 * @param write
		 * @return {@code true} if the write possibly exists
		 */
		public boolean mightContain(Write write) {
			masterLock.readLock().lock();
			try {
				return filter.mightContain(write.getRecord(), write.getKey(),
						write.getValue());
			}
			finally {
				masterLock.readLock().unlock();
			}
		}

		/**
		 * Returns the Write at index {@link #head} in {@link #writes}.
		 * <p>
		 * <strong>NOTE:</strong>
		 * <em>This method will return the same element on multiple
		 * invocations until {@link #remove()} is called.</em>
		 * </p>
		 */
		@Override
		public Write next() {
			masterLock.readLock().lock();
			try {
				return writes[head];
			}
			finally {
				masterLock.readLock().unlock();
			}
		}

		/**
		 * Simulates the removal of the head Write from the Page. This method
		 * only updates the {@link #head} and {@link #pos} metadata and does not
		 * actually delete any data, which is a performance optimization.
		 */
		@Override
		public void remove() {
			masterLock.writeLock().lock();
			try {
				head++;
			}
			finally {
				masterLock.writeLock().unlock();
			}
		}

		@Override
		public String toString() {
			return filename;
		}

		/**
		 * Insert {@code write} into the list of {@link #writes} and increment
		 * the {@link #size} counter.
		 * 
		 * @param write
		 * @throws BufferCapacityException
		 */
		@GuardedBy("Buffer#insert(Write)")
		private void index(Write write) throws BufferCapacityException {
			if(size < writes.length) {
				// The individual Write components are added instead of the
				// entire Write so that version information is not factored into
				// the bloom filter hashing
				filter.put(write.getRecord(), write.getKey(), write.getValue());
				writes[size] = write;
				size++;
			}
			else {
				throw new BufferCapacityException();
			}
		}
	}

}
