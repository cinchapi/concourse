/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.temp;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.concurrent.Locks;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.PermanentStore;
import org.cinchapi.concourse.server.storage.cache.BloomFilter;
import org.cinchapi.concourse.server.storage.db.Database;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Logger;
import org.cinchapi.concourse.util.NaturalSorter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static org.cinchapi.concourse.server.GlobalState.*;

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
public final class Buffer extends Limbo {

    // NOTE: The Buffer does not ever lock itself because its delegates
    // concurrency control to each individual page. Furthermore, since each
    // Page is append-only, there is no need to ever lock any Page that is not
    // equal to #currentPage. The Buffer does grab the transport readLock for
    // most methods so that we don't end up in situations where a transport
    // happens while we're trying to read.

    /**
     * The average number of bytes used to store an arbitrary Write.
     */
    private static final int AVG_WRITE_SIZE = 30; /* arbitrary */

    /**
     * The directory where the Buffer pages are stored.
     */
    private final String directory;

    /**
     * The sequence of Pages that make up the Buffer.
     */
    private final List<Page> pages = new AbstractList<Page>() { // This List
                                                                // implementation
                                                                // provides an
                                                                // iterator that
                                                                // has
                                                                // "reloading"
                                                                // functionality
                                                                // such that we
                                                                // aren't halted
                                                                // by a CME that
                                                                // occurs when
                                                                // one thread
                                                                // adds a page
                                                                // to the
                                                                // underlying
                                                                // collection
                                                                // while another
                                                                // thread is
                                                                // using the
                                                                // iterator

        /**
         * The wrapped list that actually stores the data.
         */
        private final List<Page> delegate = Lists.newArrayList();

        @Override
        public void add(int index, Page element) {
            delegate.add(index, element);
        }

        @Override
        public Page remove(int index) {
            return delegate.remove(index);
        }

        @Override
        public Page get(int index) {
            return delegate.get(index);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public Iterator<Page> iterator() {
            return new Iterator<Page>() {

                int index = 0;
                ListIterator<Page> it = delegate.listIterator(index);

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Page next() {
                    try {
                        index = it.nextIndex();
                        return it.next();
                    }
                    catch (ConcurrentModificationException e) {
                        // CON-75: The exception is thrown because a new page
                        // was adding by another thread while the current thread
                        // (which owns the iterator) was in the middle of the
                        // read. We can ignore this exception, because adding a
                        // new page will not lead to inconsistent results since
                        // all the data we've read so far is still valid. This
                        // just means we have more work to do before finishing
                        // than we originally anticipated.
                        //
                        // It is worth noting that each read method grabs the
                        // transportLock which prevents the case of a page being
                        // removed in the middle of a read.
                        it = delegate.listIterator(index);
                        return next();
                    }
                }

                @Override
                public void remove() {
                    it.remove();
                }

            };
        }

    };

    /**
     * The transportLock makes it possible to append new Writes and transport
     * old writes concurrently while prohibiting reading the buffer and
     * transporting writes at the same time.
     */
    private final ReentrantReadWriteLock transportLock = new ReentrantReadWriteLock();

    /**
     * A monitor that is used to make a thread block while waiting for the
     * Buffer to become transportable. The {@link #waitUntilTransportable()}
     * waits for this monitor and the {@link #insert(Write)} method notifies the
     * threads waiting on this monitor whenever there is more than single page
     * worth of data in the Buffer.
     */
    private final Object transportable = new Object();

    /**
     * A pointer to the current Page.
     */
    private Page currentPage;

    /**
     * A flag to indicate if the Buffer is running or not.
     */
    private boolean running = false;

    /**
     * We keep track of the time when the last transport occurred so that the
     * Engine can determine if it should avoid busy waiting in the
     * BufferTransportThread.
     */
    private AtomicLong timeOfLastTransport = new AtomicLong(Time.now());

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
    public Map<String, Set<TObject>> browse(long record) {
        transportLock.readLock().lock();
        try {
            return super.browse(record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> browse(long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.browse(record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> browse(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        transportLock.readLock().lock();
        try {
            return super.browse(record, timestamp, context);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        transportLock.readLock().lock();
        try {
            return super.browse(key);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.browse(key, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp,
            Map<TObject, Set<Long>> context) {
        transportLock.readLock().lock();
        try {
            return super.browse(key, timestamp, context);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<String> describe(long record, long timestamp,
            Map<String, Set<TObject>> ktv) {
        transportLock.readLock().lock();
        try {
            return super.describe(record, timestamp, ktv);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    /**
     * Return dumps for all the pages in the Buffer.
     * 
     * @return the dump string
     */
    public String dump() {
        StringBuilder sb = new StringBuilder();
        for (Page page : pages) {
            sb.append(page.dump());
            sb.append("\n");
        }
        return sb.toString();
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
    public Set<TObject> fetch(String key, long record, long timestamp,
            Set<TObject> values) {
        transportLock.readLock().lock();
        try {
            return super.fetch(key, record, timestamp, values);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, Set<TObject>> explore(Map<Long, Set<TObject>> context,
            long timestamp, String key, Operator operator, TObject... values) {
        transportLock.readLock().lock();
        try {
            return super.explore(context, timestamp, key, operator, values);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    /**
     * Return the location where the Buffer stores its data.
     * 
     * @return the backingStore
     */
    @Restricted
    public String getBackingStore() {
        return directory;
    }

    /**
     * Return the timestamp of the most recent data transport from the Buffer.
     * 
     * @return the time of last transport
     */
    @Restricted
    public long getTimeOfLastTransport() {
        return timeOfLastTransport.get();
    }

    @Override
    public long getVersion(long record) {
        transportLock.readLock().lock();
        try {
            return super.getVersion(record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public long getVersion(String key) {
        transportLock.readLock().lock();
        try {
            return super.getVersion(key);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public long getVersion(String key, long record) {
        transportLock.readLock().lock();
        try {
            return super.getVersion(key, record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public boolean insert(Write write) {
        writeLock.lock();
        try {
            boolean notify = pages.size() == 2 && currentPage.size == 0;
            currentPage.append(write);
            if(notify) {
                synchronized (transportable) {
                    transportable.notify();
                }
            }
        }
        catch (CapacityException e) {
            addPage();
            insert(write);
        }
        finally {
            writeLock.unlock();
        }
        return true;
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
    public Iterator<Write> reverseIterator() {
        return new Iterator<Write>() {

            private ListIterator<Page> pageIterator = pages.listIterator(pages
                    .size());
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
                if(pageIterator.hasPrevious()) {
                    Page next = pageIterator.previous();
                    writeIterator = next.reverseIterator();
                }
            }

        };
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
            Logger.info("Buffer configured to store data in {}", directory);
            SortedMap<File, Page> pageSorter = Maps
                    .newTreeMap(NaturalSorter.INSTANCE);
            for (File file : new File(directory).listFiles()) {
                if(!file.isDirectory()) {
                    Page page = new Page(file.getAbsolutePath());
                    pageSorter.put(file, page);
                    Logger.info("Loading Buffer content from {}...", page);
                }
            }
            pages.addAll(pageSorter.values());
            if(pages.isEmpty()) {
                addPage();
            }
            else {
                currentPage = pages.get(pages.size() - 1);
            }
        }
    }

    @Override
    public void stop() {
        if(running) {
            running = false;
            synchronized (transportable) {
                transportable.notifyAll(); // notify to allow any waiting
                                           // threads to terminate
            }
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
                    timeOfLastTransport.set(Time.now());
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
    public boolean verify(Write write, long timestamp, boolean exists) {
        transportLock.readLock().lock();
        try {
            for (Page page : pages) {
                if(page.mightContain(write)
                        && page.locallyContains(write, timestamp)) {
                    exists ^= true; // toggle boolean
                }
            }
            return exists;
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public void waitUntilTransportable() {
        if(pages.size() <= 1) {
            synchronized (transportable) {
                try {
                    transportable.wait();
                }
                catch (InterruptedException e) {/* ignore */}
            }
        }
    }

    /**
     * Return {@code true} if the Buffer has more than 1 page and the
     * first page has at least one element that can be transported. If this
     * method returns {@code false} it means that the first page is the only
     * page or that the Buffer would need to trigger a Database sync and remove
     * the first page in order to transport.
     * 
     * @return {@code true} if the Buffer can transport a Write.
     */
    protected boolean canTransport() { // visible for testing
        return pages.size() > 1 && pages.get(0).hasNext();
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        transportLock.readLock().lock();
        try {
            return super.doExplore(timestamp, key, operator, values);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        transportLock.readLock().lock();
        try {
            return super.doExplore(key, operator, values);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    /**
     * Add a new Page to the Buffer.
     */
    private void addPage() {
        writeLock.lock();
        try {
            currentPage = new Page(BUFFER_PAGE_SIZE);
            pages.add(currentPage);
            Logger.debug("Added page {} to Buffer", currentPage);
        }
        finally {
            writeLock.unlock();
        }

    }

    /**
     * Remove the first page in the Buffer.
     */
    private void removePage() {
        writeLock.lock();
        try {
            pages.remove(0).delete();
        }
        finally {
            writeLock.unlock();
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

        // NOTE: This class does not define hashCode() and equals() because the
        // defaults are the desired behaviour.

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
        private MappedByteBuffer content;

        /**
         * The file that contains the content of the Page.
         */
        private final String filename;

        /**
         * The local lock for the page, which is only used if this page is equal
         * to the {@link #currentPage}.
         */
        private final transient ReentrantReadWriteLock pageLock = new ReentrantReadWriteLock();

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
                Logger.debug("Found existing write '{}' in the Buffer", write);
            }
        }

        /**
         * Append {@code write} to the Page if {@link #content} has enough
         * remaining capacity to store {@code write}. Since all inserts are
         * routed to this method, we grab a writeLock so that we don't have a
         * situation where the currentPage is ever changed in the middle of a
         * read.
         * 
         * @param write
         * @throws CapacityException - if the size of {@code write} is
         *             greater than the remaining capacity of {@link #content}
         */
        public void append(Write write) throws CapacityException {
            Preconditions.checkState(this == currentPage, "Illegal attempt to "
                    + "append a Write to an inactive Page");
            pageLock.writeLock().lock();
            try {
                if(content.remaining() >= write.size() + 4) {
                    index(write);
                    ByteBuffer bytes = ByteBuffer.allocate(write.size());
                    write.copyToByteBuffer(bytes);
                    bytes.rewind();
                    content.putInt(write.size());
                    content.put(bytes);
                    content.force();
                }
                else {
                    throw new CapacityException();
                }
            }
            finally {
                pageLock.writeLock().unlock();
            }
        }

        /**
         * Delete the page from disk. The Page object will reside in memory
         * until garbage collection.
         */
        public void delete() {
            FileSystem.deleteFile(filename);
            FileSystem.unmap(content); // CON-163 (authorized)
            Logger.info("Deleting Buffer page {}", filename);
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
            Locks.lockIfCondition(pageLock.readLock(), this == currentPage);
            try {
                return head < size;
            }
            finally {
                Locks.unlockIfCondition(pageLock.readLock(),
                        this == currentPage);
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
                    Locks.lockIfCondition(pageLock.readLock(),
                            Page.this == currentPage);
                    try {
                        if(index - head != distance) {
                            throw new ConcurrentModificationException(
                                    "A write has been removed from the Page");
                        }
                        return index < size;
                    }
                    finally {
                        Locks.unlockIfCondition(pageLock.readLock(),
                                Page.this == currentPage);
                    }
                }

                @Override
                public Write next() {
                    Locks.lockIfCondition(pageLock.readLock(),
                            Page.this == currentPage);
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
                        Locks.unlockIfCondition(pageLock.readLock(),
                                Page.this == currentPage);
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();

                }

            };
        }

        /**
         * Return {@code true} if the data in {@code write} exists locally
         * <em>on this page</em> at {@code timestamp}, which means that
         * {@code write} appears an odd number of times <em>on this page</em> at
         * or before {@code timestamp}. To really know if {@code write} exists
         * at {@code timestamp}, the caller must call this function for each
         * page with writes before {@code timestamp} and toggle the running
         * result if the function returns {@code true}.
         * 
         * @param write
         * @param timestamp
         * @return {@code true} if {@code write} exists at {@code timestamp} on
         *         this Page
         */
        public boolean locallyContains(Write write, long timestamp) {
            Locks.lockIfCondition(pageLock.readLock(), this == currentPage);
            try {
                boolean exists = false;
                Iterator<Write> it = iterator();
                while (it.hasNext()) {
                    Write current = it.next();
                    if(timestamp >= current.getVersion()) {
                        if(write.equals(current)) {
                            exists ^= true; // toggle boolean
                        }
                    }
                    else {
                        break;
                    }
                }
                return exists;
            }
            finally {
                Locks.unlockIfCondition(pageLock.readLock(),
                        this == currentPage);
            }
        }

        /**
         * Return {@code true} if the Page <em>might</em> have a Write equal to
         * {@code write}. If this function returns true, the caller should check
         * with certainty by calling {@link #doesContain(Write, long)}.
         * 
         * @param write
         * @return {@code true} if the write possibly exists
         */
        public boolean mightContain(Write write) {
            Locks.lockIfCondition(pageLock.readLock(), this == currentPage);
            try {
                Type valueType = write.getValue().getType();
                if(filter.mightContain(write.getRecord(), write.getKey(),
                        write.getValue())) {
                    return true;
                }
                else if(valueType == Type.STRING) {
                    return filter.mightContain(write.getRecord(), write
                            .getKey(), Value.wrap(Convert.javaToThrift(Tag
                            .create((String) write.getValue().getObject()))));
                }
                else if(valueType == Type.TAG) {
                    return filter.mightContain(
                            write.getRecord(),
                            write.getKey(),
                            Value.wrap(Convert.javaToThrift(write.getValue()
                                    .getObject().toString())));
                }
                else {
                    return false;
                }
            }
            finally {
                Locks.unlockIfCondition(pageLock.readLock(),
                        this == currentPage);
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
            Locks.lockIfCondition(pageLock.readLock(), this == currentPage);
            try {
                return writes[head];
            }
            finally {
                Locks.unlockIfCondition(pageLock.readLock(),
                        this == currentPage);
            }
        }

        /**
         * Simulates the removal of the head Write from the Page. This method
         * only updates the {@link #head} and {@link #pos} metadata and does not
         * actually delete any data, which is a performance optimization.
         */
        @Override
        public void remove() {
            Locks.lockIfCondition(pageLock.writeLock(), this == currentPage);
            try {
                head++;
            }
            finally {
                Locks.lockIfCondition(pageLock.writeLock(), this == currentPage);
            }
        }

        /**
         * Return an iterator that traverses the writes on the Page in reverse.
         * 
         * @return the iterator
         */
        public Iterator<Write> reverseIterator() {
            return new Iterator<Write>() {

                /**
                 * The index of the "next" element in {@link #writes}.
                 */
                private int index = size - 1;

                /**
                 * The distance between the {@link #head} element and the
                 * {@code next} element. This is used to detect for concurrent
                 * modifications.
                 */
                private int distance = index - head;

                @Override
                public boolean hasNext() {
                    Locks.lockIfCondition(pageLock.readLock(),
                            Page.this == currentPage);
                    try {
                        if(index - head != distance) {
                            throw new ConcurrentModificationException(
                                    "A write has been removed from the Page");
                        }
                        return index >= head;
                    }
                    finally {
                        Locks.unlockIfCondition(pageLock.readLock(),
                                Page.this == currentPage);
                    }
                }

                @Override
                public Write next() {
                    Locks.lockIfCondition(pageLock.readLock(),
                            Page.this == currentPage);
                    try {
                        if(index - head != distance) {
                            throw new ConcurrentModificationException(
                                    "A write has been removed from the Page");
                        }
                        Write next = writes[index];
                        index--;
                        distance--;
                        return next;
                    }
                    finally {
                        Locks.unlockIfCondition(pageLock.readLock(),
                                Page.this == currentPage);
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();

                }

            };
        }

        @Override
        public String toString() {
            return filename;
        }

        /**
         * Dump the contents of this page.
         * 
         * @return the dump string
         */
        protected String dump() {
            Locks.lockIfCondition(pageLock.readLock(), Page.this == currentPage);
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("Dump for " + getClass().getSimpleName() + " "
                        + filename);
                sb.append("\n");
                sb.append("------");
                sb.append("\n");
                for (Write write : writes) {
                    if(write == null) {
                        break;
                    }
                    else {
                        sb.append(write);
                        sb.append("\n");
                    }
                }
                sb.append("\n");
                return sb.toString();
            }
            finally {
                Locks.unlockIfCondition(pageLock.readLock(),
                        Page.this == currentPage);
            }
        }

        /**
         * Insert {@code write} into the list of {@link #writes} and increment
         * the {@link #size} counter.
         * 
         * @param write
         * @throws CapacityException
         */
        @GuardedBy("Buffer.Page#append(Write)")
        private void index(Write write) throws CapacityException {
            if(size < writes.length) {
                // The individual Write components are added instead of the
                // entire Write so that version information is not factored into
                // the bloom filter hashing
                filter.put(write.getRecord(), write.getKey(), write.getValue());
                writes[size] = write;
                size++;
            }
            else {
                throw new CapacityException();
            }
        }
    }
}
