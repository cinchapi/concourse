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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.Context;
import org.cinchapi.concourse.server.Properties;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.concurrent.Lockable;
import org.cinchapi.concourse.server.concurrent.Lockables;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.Files;
import org.cinchapi.concourse.server.model.Write;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.perf4j.aop.Profiled;
import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

import static org.cinchapi.concourse.server.util.Loggers.getLogger;

/**
 * A {@code Buffer} is a special implementation of {@link Limbo} that aims to
 * quickly accumulate writes in memory before performing a batch flush into some
 * {@link PermanentStore}.
 * <p>
 * A Buffer enforces the durability guarantee because all writes are immediately
 * flushed to disk. Even though there is some disk I/O, the overhead is minimal
 * and writes are fast because the entire backing store is memory mapped and the
 * writes are always appended.
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
	 * The context that is passed to and around the Engine for global
	 * configuration and state.
	 */
	protected final transient Context context;

	/**
	 * A pointer to the current Page.
	 */
	private Page currentPage;

	/**
	 * A flag to indicate if the Buffer is running or not.
	 */
	private boolean running = false;

	/**
	 * Construct a Buffer that is backed by the default location, which is a
	 * file called "buffer" in the {@link Properties#DATA_HOME} directory.
	 * Existing content, if available, will be loaded from the file. Otherwise,
	 * a new and empty Buffer will be returned.
	 * 
	 * @param context
	 */
	public Buffer(Context context) {
		this(Properties.DATA_HOME + File.separator + "buffer", context);
	}

	/**
	 * 
	 * Construct a a Buffer that is backed by {@code backingStore}. Existing
	 * content, if available, will be loaded from the file. Otherwise, a new and
	 * empty Buffer will be returned.
	 * 
	 * @param directory
	 * @param context
	 */
	public Buffer(String directory, Context context) {
		Files.mkdirs(directory);
		this.directory = directory;
		this.context = context;
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
			SortedMap<File, Page> pageSorter = Maps
					.newTreeMap(new Comparator<File>() {

						@Override
						public int compare(File o1, File o2) {
							long t1 = Long
									.parseLong(o1.getName().split("\\.")[0]);
							long t2 = Long
									.parseLong(o2.getName().split("\\.")[0]);
							return Longs.compare(t1, t2);
						}

					});
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
		if(pages.size() > 1 && transportLock.writeLock().tryLock()) {
			try {
				Page page = pages.get(0);
				if(page.hasNext()) {
					destination.accept(page.next());
					page.remove();
				}
				else {
					removePage();
				}
			}
			finally {
				transportLock.writeLock().unlock();
			}
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		transportLock.readLock().lock();
		try {
			return super.verify(key, value, record);
		}
		finally {
			transportLock.readLock().unlock();
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
	@Profiled(tag = "Buffer.write_{$0}", logger = "org.cinchapi.concourse.server.engine.PerformanceLogger")
	protected boolean insert(Write write) {
		Lock lock = writeLock();
		try {
			currentPage.append(write);
		}
		catch (BufferCapacityException e) {
			addPage();
			insert(write);
		}
		finally {
			lock.release();
		}
		return true;
	}

	/**
	 * Add a new Page to the Buffer.
	 */
	private void addPage() {
		Lock lock = writeLock();
		try {
			currentPage = new Page(Properties.BUFFER_PAGE_SIZE);
			pages.add(currentPage);
			log.debug("Added page {} to Buffer", currentPage);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Remove the first page in the Buffer.
	 */
	private void removePage() {
		Lock lock = writeLock();
		try {
			pages.remove(0).delete();
		}
		finally {
			lock.release();
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
	private class Page implements Iterator<Write>, Iterable<Write>, Lockable {

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
		 * The append-only buffer that contains the content of the backing file
		 * starting at position 4. Data is never deleted from the buffer, but is
		 * marked as "removed" depending on the location of the {@link #pos}
		 * marker.
		 */
		private final MappedByteBuffer content;

		/**
		 * The file that contains the content of the Page.
		 */
		private final String filename;

		/**
		 * <p>
		 * Indicates the index in {@link #writes} that constitutes the first
		 * element. When writes are "removed", elements are not actually deleted
		 * from the list, so it is necessary to keep track of the head element
		 * so that the correct next() element can be returned.
		 * </p>
		 * <p>
		 * This value does not need to be stored to disk because the
		 * {@link #pos} tracker ensures that, when reconstructing the page from
		 * disk, only present Writes will be processed and added to the
		 * {@link #writes} list, so the value of this variable should always be
		 * 0 upon Page object construction.
		 * </p>
		 */
		private transient int head = 0;

		/**
		 * The total number of elements in the list of {@link #writes}.
		 */
		private transient int size = 0;

		/**
		 * <p>
		 * Indicates the current cursor position in the {@link #content} buffer.
		 * When writes are "removed" no data is actually deleted, so it is
		 * necessary to keep track of the position for the next present Write so
		 * that the page can be reconstructed from the file on disk in the
		 * correct position, if necessary.
		 * </p>
		 * <p>
		 * This variable is only held in memory. Whenever the value is updated,
		 * it is immediately written to {@link #posbuf}.
		 * </p>
		 */
		private transient int pos;

		/**
		 * A mapping for the first 4 bytes of the Page's backing file that holds
		 * the current value of {@link #pos} so that the Page can be
		 * reconstructed without reading old Writes in the event of a shutdown
		 * before the page is deleted.
		 */
		private final MappedByteBuffer posbuf;

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
			this(filename, Files.length(filename));
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param filename
		 * @param capacity
		 */
		private Page(String filename, long capacity) {
			this.filename = filename;
			this.posbuf = Files.map(filename, MapMode.READ_WRITE, 0, 4);
			this.pos = posbuf.getInt();
			this.content = Files.map(filename, MapMode.READ_WRITE, 4,
					capacity - 4);
			this.writes = new Write[(int) (capacity / AVG_WRITE_SIZE)];
			content.position(pos);
			Iterator<ByteBuffer> it = ByteableCollections.iterator(content);
			while (it.hasNext()) {
				Write write = Write.fromByteBuffer(it.next());
				index(write);
				// We must add items to a bloom filter when deserializing in
				// order to prevent that appearance of data loss (i.e. the
				// bloom filter reporting that data does not exist, when it
				// actually does).
				context.getBloomFilters().add(write.getKey().toString(),
						write.getValue().getQuantity(),
						write.getRecord().longValue());
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
		public void append(Write write) throws BufferCapacityException {
			Lock lock = writeLock();
			try {
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
			finally {
				lock.release();
			}
		}

		/**
		 * Delete the page from disk. The Page object will reside in memory
		 * until garbage collection.
		 */
		public void delete() {
			Files.delete(filename);
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
			Lock lock = readLock();
			try {
				return head < size;
			}
			finally {
				lock.release();
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
					Lock lock = readLock();
					try {
						if(index - head != distance) {
							throw new ConcurrentModificationException(
									"A write has been removed from the Page");
						}
						return index < size;
					}
					finally {
						lock.release();
					}
				}

				@Override
				public Write next() {
					Lock lock = readLock();
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
						lock.release();
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();

				}

			};
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
			Lock lock = readLock();
			try {
				return writes[head];
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
		 * Simulates the removal of the head Write from the Page. This method
		 * only updates the {@link #head} and {@link #pos} metadata and does not
		 * actually delete any data, which is a performance optimization.
		 */
		@Override
		public void remove() {
			Lock lock = writeLock();
			try {
				Write write = next();
				pos += write.size() + 4;
				posbuf.rewind();
				posbuf.putInt(pos);
				head++;
			}
			finally {
				lock.release();
			}
		}

		@Override
		public String toString() {
			return filename;
		}

		@Override
		public Lock writeLock() {
			return Lockables.writeLock(this);
		}

		/**
		 * Insert {@code write} into the list of {@link #writes} and increment
		 * the {@link #size} counter.
		 * 
		 * @param write
		 * @throws BufferCapacityException
		 */
		private void index(Write write) throws BufferCapacityException {
			Lock lock = writeLock();
			try {
				if(size < writes.length) {
					writes[size] = write;
					size++;
				}
				else {
					throw new BufferCapacityException();
				}
			}
			finally {
				lock.release();
			}
		}
	}

}
