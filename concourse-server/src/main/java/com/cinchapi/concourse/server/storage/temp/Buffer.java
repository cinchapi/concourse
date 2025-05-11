/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage.temp;

import static com.cinchapi.concourse.server.GlobalState.BUFFER_DIRECTORY;
import static com.cinchapi.concourse.server.GlobalState.BUFFER_PAGE_SIZE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.TernaryTruth;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.collect.CloseableIterator;
import com.cinchapi.concourse.collect.Iterators;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.plugin.data.WriteEvent;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.DurableStore;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Inventory;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.transporter.Batch;
import com.cinchapi.concourse.server.storage.transporter.BatchTransportable;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Integers;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.MultimapViews;
import com.cinchapi.concourse.util.NaturalSorter;
import com.cinchapi.concourse.util.ThreadFactories;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * A {@code Buffer} is a special implementation of {@link Limbo} that aims to
 * quickly accumulate writes in memory before performing a batch flush into some
 * {@link DurableStore}.
 * <p>
 * A Buffer enforces the durability guarantee because all writes are also
 * immediately flushed to disk. Even though there is some disk I/O, the overhead
 * is minimal and writes are fast because the entire backing store is memory
 * mapped and the writes are always appended.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public final class Buffer extends Limbo implements BatchTransportable {

    /**
     * The average number of bytes used to store an arbitrary Write.
     */
    private static final int AVG_WRITE_SIZE = 30; /* arbitrary */

    // NOTE: The Buffer does not ever lock itself because its delegates
    // concurrency control to each individual page. Furthermore, since each
    // Page is append-only, there is no need to ever lock any Page that is not
    // equal to #currentPage. The Buffer does grab the transport readLock for
    // most methods so that we don't end up in situations where a transport
    // happens while we're trying to read.

    /**
     * A global {@link ExecutorService} to asynchronously record all the
     * {@link WriteEvent write events} that are handled by any Buffer instance.
     */
    private final static ExecutorService GLOBAL_EXECUTOR = Executors
            .newCachedThreadPool(
                    ThreadFactories.namingDaemonThreadFactory("buffer-global"));

    /**
     * The number of slots to put in each Page's bloom filter. We want this
     * small enough to have few hash functions, but large enough so that the
     * bloom filter does not become saturated.
     */
    private static int PER_PAGE_BLOOM_FILTER_CAPACITY = GlobalState.BUFFER_PAGE_SIZE
            / 10;

    /**
     * The place where the {@link Buffer} logs every {@link WriteEvent} it
     * accepts.
     */
    @VisibleForTesting
    protected Collection<WriteEvent> eventLog = GlobalState.BINARY_QUEUE;

    /**
     * A pointer to the current Page.
     */
    private Page currentPage;

    /**
     * The directory where the Buffer pages are stored.
     */
    private final String directory;

    /**
     * The environment that is associated with {@link Engine}.
     */
    private String environment;

    /**
     * A pointer to the inventory that is used within the Engine.
     */
    private Inventory inventory = null;

    /**
     * A runnable that flushes the inventory to disk.
     */
    private Runnable inventorySync = () -> inventory.sync();

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
        private final List<Page> delegate = new ArrayList<>();

        @Override
        public void add(int index, Page element) {
            delegate.add(index, element);
        }

        @Override
        public Page get(int index) {
            return delegate.get(index);
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

        @Override
        public Page remove(int index) {
            return delegate.remove(index);
        }

        @Override
        public int size() {
            return delegate.size();
        }

    };

    /**
     * A runnable instance that flushes the content the current buffer page to
     * disk.
     */
    private Runnable pageSync = () -> currentPage.content.force();

    /**
     * A flag to indicate if the Buffer is running or not.
     */
    private boolean running = false;

    /**
     * Executor service that is responsible for {@link #sync() syncing} data.
     */
    private AwaitableExecutorService syncer;

    /**
     * The structure lock ensures that only a single thread can modify the
     * structure of the Buffer, without affecting any readers.
     */
    private final ReentrantLock structure = new ReentrantLock();

    /**
     * The prefix for the threads that are responsible for flushing data to
     * disk. This is normally set by the Engine using the
     * {@link #setThreadNamePrefix(String)} method.
     */
    private String threadNamePrefix;

    /**
     * A monitor that is used to make a thread block while waiting for the
     * Buffer to become transportable. The {@link #waitUntilTransportable()}
     * waits for this monitor and the {@link #insert(Write)} method notifies the
     * threads waiting on this monitor whenever there is more than single page
     * worth of data in the Buffer.
     */
    private final Object transportable = new Object();

    /**
     * A monitor that is used to make a reader thread block on a Page while
     * waiting for a writer (e.g., appender or transporter) to finish.
     */
    private final Object readable = new Object();

    /**
     * A counter that tracks the total number of {@link Batch batches} that have
     * been {@link #queueTransportBatch(Page) queued} for transport.
     * <p>
     * Each {@link Batch} is assigned a value from this counter as it's
     * {@link Batch#ordinal()} to ensure that batches are processed in the
     * correct chronological order, even when multiple transport threads are
     * running concurrently.
     * </p>
     */
    private final AtomicInteger batchCount = new AtomicInteger(0);

    /**
     * A queue of {@link Batch batches} that are ready for
     * {@link com.cinchapi.concourse.server.storage.transporter.Transporter
     * transport} to the database. Each {@link Batch batch} contains
     * {@link Write writes} from a {@link Page} that is full and no longer
     * {@link Page#isMutable() accepting} data.
     */
    private BlockingQueue<Batch> batches = new LinkedBlockingQueue<Batch>();

    /**
     * A collection of listeners that are notified whenever a reader scans the
     * {@link Buffer}.
     */
    private final Collection<Runnable> scanEventListeners = new ArrayList<>();

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
     * @param directory - the path to directory where the buffer files should be
     *            stored. If the directory does not exist, it'll be created
     *            automatically
     */
    public Buffer(String directory) {
        FileSystem.mkdirs(directory);
        this.directory = directory;
        this.inventory = Inventory.create(directory + File.separator + "meta"
                + File.separator + "inventory"); // just incase we are running
                                                 // from a unit test and
                                                 // there is no call to
                                                 // #setInventory
        this.threadNamePrefix = "buffer-" + System.identityHashCode(this);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp,
            Map<TObject, Set<Long>> context) {
        Iterator<Write> it = iterator(key, timestamp);
        try {
            while (it.hasNext()) {
                Write write = it.next();
                TObject value = write.getValue().getTObject();
                long record = write.getRecord().longValue();
                Action action = write.getType();
                Set<Long> records = context.computeIfAbsent(value,
                        $ -> new LinkedHashSet<>());
                if(action == Action.ADD) {
                    records.add(record);
                }
                else if(action == Action.REMOVE) {
                    records.remove(record);
                    if(records.isEmpty()) {
                        context.remove(value);
                    }
                }
            }
            return context;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end, Map<Long, Set<TObject>> context) {
        Set<TObject> snapshot = Iterables.getLast(context.values(),
                new LinkedHashSet<>());
        if(snapshot.isEmpty() && !context.isEmpty()) {
            // CON-474: Empty set is placed in the context if it was the last
            // snapshot known to the database
            context.remove(Time.NONE);
        }
        Iterator<Write> it = iterator(key, record, end - 1);
        try {
            while (it.hasNext()) {
                Write write = it.next();
                long timestamp = write.getVersion();
                Action action = write.getType();
                snapshot = new LinkedHashSet<>(snapshot);
                TObject value = write.getValue().getTObject();
                if(action == Action.ADD) {
                    snapshot.add(value);
                }
                else if(action == Action.REMOVE) {
                    snapshot.remove(value);
                }
                if(timestamp >= start) {
                    context.put(timestamp, snapshot);
                }
            }
            // NOTE: Empty snapshots couldn't be removed while processing
            // because that state needed to be preserved to calculate subsequent
            // diffs.
            context.values().removeIf(v -> v.isEmpty());
            return context;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    public boolean contains(long record) {
        return inventory.contains(record);
    }

    @Override
    public Set<String> describe(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        Iterator<Write> it = iterator(record, timestamp);
        try {
            while (it.hasNext()) {
                Write write = it.next();
                String key = write.getKey().toString();
                Action action = write.getType();
                TObject value = write.getValue().getTObject();
                Set<TObject> values = context.computeIfAbsent(key,
                        $ -> new HashSet<>());
                if(action == Action.ADD) {
                    values.add(value);
                }
                else if(action == Action.REMOVE) {
                    values.remove(value);
                    if(values.isEmpty()) {
                        context.remove(key);
                    }
                }
            }
            return context.keySet();
        }
        finally {
            Iterators.close(it);
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
    public Map<Long, Set<TObject>> explore(Map<Long, Set<TObject>> context,
            String key, Aliases aliases, long timestamp) {
        Iterator<Write> it = iterator(key, timestamp);
        try {
            Operator operator = aliases.operator();
            TObject[] values = aliases.values();
            while (it.hasNext()) {
                Write write = it.next();
                long record = write.getRecord().longValue();
                if(matches(write.getValue(), operator, values)) {
                    if(write.getType() == Action.ADD) {
                        MultimapViews.put(context, record,
                                write.getValue().getTObject());
                    }
                    else {
                        MultimapViews.remove(context, record,
                                write.getValue().getTObject());
                    }
                }
            }
            return context;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    public Set<Long> getAllRecords() {
        return inventory.getAll();
    }

    /**
     * Return the location where the Buffer stores its data.
     * 
     * @return {@link GlobalState#BUFFER_DIRECTORY} or the directory that was
     *         passed to the {@link #Buffer(String)} constructor
     */
    @Restricted
    public String getBackingStore() {
        return directory;
    }

    @Override
    public boolean insert(Write write, boolean sync) {
        structure.lock();
        try {
            boolean notify = pages.size() == 2 && currentPage.size == 0;
            currentPage.append(write, sync);
            if(notify) {
                synchronized (transportable) {
                    transportable.notifyAll();
                }
            }
            return true;
        }
        catch (CapacityException e) {
            addPage();
            return insert(write, sync);
        }
        finally {
            structure.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Be sure to call {@link Iterators#close(Iterator)} within a finally block
     * to ensure that lock starvation does not occur.
     * </p>
     */
    @Override
    public Iterator<Write> iterator() {
        return new AllSeekingIterator(Time.NONE);
    }

    /**
     * Retrieve the next {@link Batch batch} of {@link Write writes} that is
     * ready to be transported, waiting, if necessary, until one becomes
     * available.
     *
     * @return the next batch to transport
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    @Override
    public Batch nextBatch() throws InterruptedException {
        return batches.take();
    }

    /**
     * Register a {@code listener} to be notified whenever a reader scans the
     * {@link Buffer}.
     * <p>
     * Writes never scan the {@link Buffer}, but most reads do. So this method
     * can be used to get insight on when and how often reads/scans are
     * happening and react accordingly.
     * </p>
     * <p>
     * <strong>NOTE:</strong> Scan notifications are meant to be as unobtrusive
     * as possible, so it isn't possible to get insight into the specific
     * request that triggered a scan AND the notification of a scan may arrive
     * at some time after the scan actually occurred.
     * </p>
     *
     * @param listener
     */
    public void onScan(Runnable listener) {
        scanEventListeners.add(listener);
    }

    @Override
    public void purge(Batch batch) {
        // The merge of a transported Pages is guaranteed to follow the order
        // that the Pages appeared in the Buffer (even if multiple threads are
        // participating in the transport process). Therefore, we can always
        // remove the first page of the Buffer when instructed to purge.
        removePage();
    }

    @Override
    public Map<Long, List<String>> review(long record) {
        Iterator<Write> it = iterator(record, Time.NONE);
        try {
            Map<Long, List<String>> review = new LinkedHashMap<>();
            while (it.hasNext()) {
                Write write = it.next();
                review.computeIfAbsent(write.getVersion(),
                        $ -> new ArrayList<>()).add(write.toString());
            }
            return review;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    public Map<Long, List<String>> review(String key, long record) {
        Iterator<Write> it = iterator(key, record, Time.NONE);
        try {
            Map<Long, List<String>> review = new LinkedHashMap<>();
            while (it.hasNext()) {
                Write write = it.next();
                review.computeIfAbsent(write.getVersion(),
                        $ -> new ArrayList<>()).add(write.toString());
            }
            return review;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        Iterator<Write> it = iterator(record, timestamp);
        try {
            while (it.hasNext()) {
                Write write = it.next();
                String key = write.getKey().toString();
                TObject value = write.getValue().getTObject();
                Action action = write.getType();
                Set<TObject> values = context.computeIfAbsent(key,
                        $ -> new LinkedHashSet<>());
                if(action == Action.ADD) {
                    values.add(value);
                }
                else if(action == Action.REMOVE) {
                    values.remove(value);
                    if(values.isEmpty()) {
                        context.remove(key);
                    }
                }
            }
            return context;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp,
            Set<TObject> context) {
        Iterator<Write> it = iterator(key, record, timestamp);
        try {
            while (it.hasNext()) {
                Write write = it.next();
                if(write.getType() == Action.ADD) {
                    context.add(write.getValue().getTObject());
                }
                else {
                    context.remove(write.getValue().getTObject());
                }
            }
            return context;
        }
        finally {
            Iterators.close(it);
        }
    }

    /**
     *
     * Called by the parent {@link Engine} to set the environment to which the
     * Buffer is associated.
     *
     * @param environment
     */
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    /**
     * <p>
     * <strong>DO NOT CALL!!!</strong>
     * </p>
     * <p>
     * Called by the parent {@link Engine} to set the inventory that the Buffer
     * writes to when new records are added.
     * </p>
     * 
     * @param inventory
     */
    @Restricted
    public void setInventory(Inventory inventory) {
        this.inventory = inventory;
    }

    /**
     * <p>
     * <strong>DO NOT CALL!!!</strong>
     * </p>
     * <p>
     * Called by the parent {@link Engine} to set the thread name prefix that
     * the Buffer uses when spawning asynchronous threads.
     * </p>
     * 
     * @param threadNamePrefix
     */
    @Restricted
    public void setThreadNamePrefix(String threadNamePrefix) {
        this.threadNamePrefix = threadNamePrefix;
    }

    @Override
    public void start() {
        if(!running) {
            running = true;
            Logger.info("Buffer configured to store data in {}", directory);

            // Reset state (in case the same instance was previously started and
            // stopped)
            pages.clear();
            currentPage = null;
            batches.clear();
            batchCount.set(0);
            syncer = new AwaitableExecutorService(
                    Executors.newCachedThreadPool(ThreadFactories
                            .namingThreadFactory(threadNamePrefix + "-%d")));

            // Load existing Buffer pages from disk
            SortedMap<File, Page> pageSorter = new TreeMap<>(
                    NaturalSorter.INSTANCE);
            for (File file : new File(directory).listFiles()) {
                if(!file.isDirectory()) {
                    Page page = new Page(file.getAbsolutePath());
                    pageSorter.put(file, page);
                    Logger.info("Loading Buffer content from {}...", page);
                }
            }
            inventory.sync();

            // Setup shortcuts in memory to facilitate writes and transports
            Iterator<Page> it = pageSorter.values().iterator();
            if(!it.hasNext()) {
                addPage(false);
            }
            else {
                while (it.hasNext()) {
                    Page page = it.next();
                    pages.add(page);
                    if(!it.hasNext()) {
                        // Set the most recently added Page as the currentPage
                        // in case it has capacity for more Writes
                        currentPage = page;
                    }
                    else {
                        queueTransportBatch(page);
                    }
                }
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
            batches.clear();
        }
        syncer.shutdown();
    }

    @Override
    public void sync() {
        try {
            syncer.await(pageSync, inventorySync);
        }
        catch (InterruptedException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        catch (RejectedExecutionException e) {
            // This should not happen in normal situations but may occur if the
            // Buffer was prematurely stopped during a unit test.
            Logger.warn("The {} Buffer fsync executor service is "
                    + "rejecting tasks, so changes to the Buffer and "
                    + "Inventory were made serially", environment);
            pageSync.run();
            inventorySync.run();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method will transport at least one write from the buffer, in
     * chronological order.
     * </p>
     */
    @Override
    public void transport(DurableStore destination, boolean sync) {
        // NOTE: The #sync parameter is ignored because the Database does not
        // support allowing the Buffer to control when syncs happen.
        transport(1, destination);
    }

    /**
     * Attempt to transport <strong>up to</strong> {@count} {@link Write Writes}
     * to the {@code destination} and return {@code true} if any are
     * successfully transported.
     * <p>
     * If this method returns {@code true} it is guaranteed that the
     * {@link Buffer Buffer's} state has changed, but not necessarily that all
     * {@code count} {@link Write writes} have been transported. Therefore, the
     * {@code count} parameter is best used as a lever to control the max
     * transport rate to balance resource contention among readers and writers
     * throughout the system.
     * </p>
     * 
     * @param count
     * @param destination
     * @return {@code true} if at least one {@link Write write} has been
     *         transported to the {@code destination}
     */
    public boolean transport(int count, DurableStore destination) {
        Preconditions.checkArgument(count > 0,
                "The count parameter must be greater than 0");
        if(pages.size() > 1) {
            Page page = pages.get(0);
            long stamp;
            if((stamp = page.lock.tryWriteLock()) != 0) {
                try {
                    for (int i = 0; i < count; ++i) {
                        if(page.hasNext()) {
                            destination.accept(page.next());
                            page.remove();
                        }
                        else {
                            ((Database) destination).sync();
                            removePage(stamp);
                            break;
                        }
                    }
                    return true;
                }
                finally {
                    page.lock.unlockWrite(stamp);
                    synchronized (readable) {
                        readable.notifyAll();
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean verify(Write write, long timestamp) {
        boolean exists = false;
        Iterator<Write> it = iterator(write, timestamp);
        try {
            while (it.hasNext()) {
                it.next();
                exists ^= true; // toggle boolean
            }
            return exists;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    public TernaryTruth verifyFast(Write write, long timestamp) {
        if(inventory.contains(write.getRecord().longValue())) {
            return super.verifyFast(write, timestamp);
        }
        else {
            return TernaryTruth.FALSE;
        }
    }

    @Override
    public void waitUntilTransportable() {
        if(pages.size() <= 1) {
            synchronized (transportable) {
                while (pages.size() <= 1) {
                    try {
                        transportable.wait();
                    }
                    catch (InterruptedException e) {/* ignore */}
                }
            }
        }
    }

    /**
     * Return {@code true} if the Buffer has more than 1 page and the first page
     * has at least one element that can be transported. If this method returns
     * {@code false} it means that the first page is the only page or that the
     * Buffer would need to trigger a Database sync and remove the first page in
     * order to transport.
     * 
     * @return {@code true} if the Buffer can transport a Write.
     */
    @VisibleForTesting
    protected boolean canTransport() { // visible for testing
        return pages.size() > 1 && pages.get(0).hasNext();
    }

    @Nullable
    @Override
    protected Action getLastWriteAction(Write write, long timestamp) {
        // TODO: use ReverseSeekingIterator to optimize this
        Iterator<Write> it = iterator(write, timestamp);
        try {
            Action action = null;
            while (it.hasNext()) {
                action = it.next().getType();
            }
            return action;
        }
        finally {
            Iterators.close(it);
        }
    }

    @Override
    protected long getOldestWriteTimestamp() {
        return pages.get(0).getOldestWriteTimestamp();
    }

    @Override
    protected Iterator<Write> getSearchIterator(String key) {
        return iterator(key, Time.NONE);
    }

    @Override
    protected boolean isPossibleSearchMatch(String key, Write write,
            Value value) {
        return value.getType() == Type.STRING;
    }

    /**
     * Return an iterator over all writes in the buffer that occurred no later
     * than {@code timestamp}.
     * 
     * @param timestamp
     * @return the iterator
     */
    protected Iterator<Write> iterator(long timestamp) {
        return new AllSeekingIterator(timestamp);
    }

    /**
     * Return an iterator over all writes in the buffer with the specified
     * {@code record} component and that occurred no later than
     * {@code timestamp}.
     * 
     * @param record
     * @param timestamp
     * @return the iterator
     */
    protected Iterator<Write> iterator(long record, long timestamp) {
        return new RecordSeekingIterator(record, timestamp);
    }

    /**
     * Return an iterator over all writes in the buffer with the specified
     * {@code key} component and that occurred no later than {@code timestamp}.
     * 
     * @param key
     * @param timestamp
     * @return the iterator
     */
    protected Iterator<Write> iterator(String key, long timestamp) {
        return new KeySeekingIterator(key, timestamp);
    }

    /**
     * Return an iterator over all writes in the buffer with the specified
     * {@code key} and {@code record} components and that occurred no later than
     * {@code timestamp}.
     * 
     * @param timestamp
     * @return the iterator
     */
    protected Iterator<Write> iterator(String key, long record,
            long timestamp) {
        return new KeyInRecordSeekingIterator(key, record, timestamp);
    }

    /**
     * Return an iterator over all writes in the buffer that equal the input
     * {@code write} and that occurred no later than {@code timestamp}.
     * 
     * @param timestamp
     * @return the iterator
     */
    protected Iterator<Write> iterator(Write write, long timestamp) {
        return new WriteSeekingIterator(write, timestamp);
    }

    /**
     * Add a new Page to the Buffer.
     */
    private void addPage() {
        addPage(true);
    }

    /**
     * Add a new Page to the Buffer and optionally perform a {@code sync}.
     * 
     * @param sync - a flag that determines whether the {@link #sync()} method
     *            should be called to durably persist the current page to disk.
     *            This should only be false when called from the
     *            {@link #start()} method.
     */
    private void addPage(boolean sync) {
        structure.lock();
        try {
            if(sync) {
                sync();
            }
            Page previousPage = currentPage;
            currentPage = new Page(BUFFER_PAGE_SIZE);
            pages.add(currentPage);
            if(previousPage != null) {
                queueTransportBatch(previousPage);
            }
            Logger.debug("Added page {} to Buffer", currentPage);
        }
        finally {
            structure.unlock();
        }
    }

    /**
     * Broadcast that the {@link Buffer} has been scanned.
     */
    private void broadcastScanEvent() {
        if(!GlobalState.ENABLE_BATCH_TRANSPORTS) {
            // By convention, StreamingTransporter is the only one that
            // listens for scale back events, so don't waste cycles sending
            // notifications if it isn't necessary.
            for (Runnable listener : scanEventListeners) {
                ForkJoinPool.commonPool().execute(listener);
            }
        }
    }

    /**
     * Queue a page for batch transport processing.
     * <p>
     * When batch transport is enabled, this method adds the page to a queue
     * where it will be processed by the {@link BatchTransporter}. Each page
     * is assigned a sequential batch number to ensure pages are transported
     * in chronological order, even when multiple transport threads are running
     * concurrently.
     * </p>
     * <p>
     * This method should only be called for pages that are not the current
     * page (i.e., pages that are no longer accepting new writes).
     * </p>
     * 
     * @param page the page to queue for batch transport
     * @throws IllegalArgumentException if the current page is passed
     */
    private void queueTransportBatch(Page page) {
        if(GlobalState.ENABLE_BATCH_TRANSPORTS) {
            if(page != currentPage) {
                Batch batch = new Batch(page.filename, page.writes,
                        batchCount.getAndIncrement());
                batches.add(batch);
                Logger.info("Queuing Buffer page {} for transport", page);
            }
            else {
                throw new IllegalArgumentException(
                        "The current page cannot be transported");
            }
        }
    }

    /**
     * Remove the first {@link Page} of the {@link Buffer}.
     */
    private void removePage() {
        structure.lock();
        try {
            Preconditions.checkState(pages.size() > 1,
                    "The current Buffer page cannot be removed");
            pages.remove(0).delete();
        }
        finally {
            structure.unlock();
        }
    }

    /**
     * Remove the first {@link Page} of the {@link Buffer} as part of a
     * processed that has already grabbed that {@link Page Page's}
     * {@link Page#lock lock}.
     * 
     * @param stamp
     */
    private void removePage(long stamp) {
        structure.lock();
        try {
            Preconditions.checkState(pages.size() > 1,
                    "The current Buffer page cannot be removed");
            pages.remove(0).delete(stamp);
        }
        finally {
            structure.unlock();
        }
    }

    /**
     * A {@link SeekingIterator} for all the writes in the buffer.
     * 
     * @author Jeff Nelson
     */
    private class AllSeekingIterator extends SeekingIterator {

        /**
         * Construct a new instance.
         * 
         * @param timestamp
         */
        protected AllSeekingIterator(long timestamp) {
            super(timestamp);
            init();
        }

        @Override
        protected void finalize() throws Throwable {
            // TODO: Replace with Cleaner in Java 9+
            // (https://docs.oracle.com/javase/9/docs/api/java/lang/ref/Cleaner.html)
            super.finalize();
            close();
        }

        @Override
        protected boolean isRelevantWrite(Write write) {
            return true;
        }

        @Override
        protected boolean pageMightContainRelevantWrites(Page page) {
            return true;
        }

    }

    /**
     * A {@link SeekingIterator} that looks for writes with a particular key and
     * record component.
     * 
     * @author Jeff Nelson
     */
    private class KeyInRecordSeekingIterator extends SeekingIterator {

        /**
         * The relevant key.
         */
        private final Text key;

        /**
         * The relevant record.
         */
        private final Identifier record;

        /**
         * Construct a new instance.
         * 
         * @param timestamp
         */
        protected KeyInRecordSeekingIterator(String key, long record,
                long timestamp) {
            super(timestamp);
            this.key = Text.wrapCached(key);
            this.record = Identifier.of(record);
            init();
        }

        @Override
        protected boolean isRelevantWrite(Write write) {
            return write.getRecord().equals(record)
                    && write.getKey().equals(key);
        }

        @Override
        protected boolean pageMightContainRelevantWrites(Page page) {
            return page.mightContain(key, record);
        }

    }

    /**
     * A {@link SeekingIterator} that looks for writes with a particular key
     * component.
     * 
     * @author Jeff Nelson
     */
    private class KeySeekingIterator extends SeekingIterator {

        /**
         * The relevant key
         */
        private final Text key;

        /**
         * Construct a new instance.
         * 
         * @param timestamp
         */
        protected KeySeekingIterator(String key, long timestamp) {
            super(timestamp);
            this.key = Text.wrapCached(key);
            init();
        }

        @Override
        protected boolean isRelevantWrite(Write write) {
            return write.getKey().equals(key);
        }

        @Override
        protected boolean pageMightContainRelevantWrites(Page page) {
            return page.mightContain(key);
        }

    }

    /**
     * A {@link Page} represents a granular section of the {@link Buffer}. Pages
     * are an append-only iterator over a sequence of {@link Write} objects.
     * Pages differ from other iterators because they do not advance in the
     * sequence until the {@link #remove()} method is called.
     * 
     * @author Jeff Nelson
     */
    private class Page implements Iterable<Write> {

        /*
         * Thread-safety & Locking policy
         *
         * - #head and #size are modified only while the Page’s write-lock is
         * held, which is grabbed for #append() and #transport()
         * - All reads processed via a SeekingIterator hold the read-lock.
         * - The private #hasNext(), #next() and #remove() methods are called
         * only from #transport(...) while that write-lock is still held, so
         * they need no extra synchronization.
         *
         * Invariants:
         *
         * 0 ≤ head ≤ size ≤ writes.length
         * writes[i] is fully initialized for i ∈ [head, size)
         */

        // NOTE: This class does not define hashCode() and equals() because the
        // defaults are the desired behaviour.

        /**
         * The filename extension.
         */
        private static final String ext = ".buf";

        /**
         * Controls read/write access to this {@link Page}. By locking
         * individual pages, the {@link Buffer} can manage concurrency
         * at a granular level:
         * <ul>
         * <li>Concurrent readers and writers that are operating on different
         * pages</li>
         * <li>Concurrent transports and readers that are operating on different
         * pages</li>
         * <li>Concurrent transports and writers that are operating on different
         * pages</li>
         * </ul>
         */
        private transient StampedLock lock = new StampedLock();

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
         * Indicates the index in {@link #writes} that constitutes the first
         * element. When writes are "removed", elements are not actually deleted
         * from the list, so it is necessary to keep track of the head element
         * so that the correct next() element can be returned.
         */
        private transient int head = 0;

        /**
         * A bloom filter like cache that is used to help determine if it
         * possible that a key exists on the page.
         */
        private final boolean[] keyCache;

        /**
         * A bloom filter like cache that is used to help determine if it is
         * possible that a key/record exists on the page.
         */
        private final boolean[] keyRecordCache;

        /**
         * A bloom filter like cache that is used to help determine if it
         * possible that a record exists on the page.
         */
        private final boolean[] recordCache;

        /**
         * The total number of elements in the list of {@link #writes}.
         */
        private transient int size = 0;

        /**
         * The upper bound on the number of writes that this page can hold.
         */
        private final transient int sizeUpperBound;

        /**
         * A bloom filter like cache that is used to help determine if it
         * possible that a Write exists on the page.
         */
        private final BloomFilter writeCache;

        /**
         * The append-only list of {@link Write} objects on the Page. Elements
         * are never deleted from this list, but are marked as "removed"
         * depending on the location of the {@link #head} index.
         */
        private final Write[] writes;

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
            this.sizeUpperBound = Math.max(1,
                    (int) ((capacity / AVG_WRITE_SIZE) * 1.2));
            this.writes = new Write[sizeUpperBound];
            this.recordCache = new boolean[sizeUpperBound];
            this.keyCache = new boolean[sizeUpperBound];
            this.keyRecordCache = new boolean[sizeUpperBound];
            this.writeCache = BloomFilter
                    .create(PER_PAGE_BLOOM_FILTER_CAPACITY);
            Iterator<ByteBuffer> it = ByteableCollections.iterator(content);
            while (it.hasNext()) {
                Write write = Write.fromByteBuffer(it.next());
                index(write);
                inventory.add(write.getRecord().longValue());
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
         * @param write the {@link Write} to append
         * @param sync a flag that determines if the page should be fsynced
         *            (or the equivalent) after appending {@code write} so that
         *            the changes are guaranteed to be durably persisted, this
         *            flag should almost always be {@code true} if calling this
         *            method directly. It is set to {@code false} when called
         *            from the context of an atomic operation transporting
         *            writes to this Buffer using GROUP SYNC
         * @throws CapacityException
         *             - if the size of {@code write} is greater than the
         *             remaining capacity of {@link #content}
         */
        public void append(Write write, boolean sync) throws CapacityException {
            Preconditions.checkState(isMutable(), "Illegal attempt to "
                    + "append a Write to an inactive Page");
            long stamp = lock.writeLock();
            try {
                if(content.remaining() >= write.size() + 4) {
                    appendUnsafe(write, sync); /* (authorized) */
                }
                else if(content.position() == 0) {
                    // Handle corner case where a Write is larger than
                    // BUFFER_PAGE_SIZE by auto expanding the capacity for the
                    // page
                    content = FileSystem.map(filename, MapMode.READ_WRITE, 0,
                            write.size() + 4);
                    appendUnsafe(write, sync); /* (authorized) */
                }
                else {
                    throw CapacityException.INSTANCE;
                }
            }
            finally {
                lock.unlockWrite(stamp);
                synchronized (readable) {
                    readable.notifyAll();
                }
            }
        }

        /**
         * Delete the page from disk. The Page object will reside in memory
         * until garbage collection.
         */
        public void delete() {
            Preconditions.checkState(!isMutable(),
                    "The current Page cannot be deleted");
            long stamp = lock.writeLock();
            try {
                delete0();
            }
            finally {
                lock.unlockWrite(stamp);
                synchronized (readable) {
                    readable.notifyAll();
                }
            }
        }

        /**
         * Return the timestamp of the oldest (e.g. first) write on this page,
         * if it exists.
         * 
         * @return the oldest write timestamp
         */
        public long getOldestWriteTimestamp() {
            Write oldestWrite = writes[0];
            // When there is no data on the page return the max possible
            // timestamp so that no query's timestamp is less than this
            // timestamp
            return oldestWrite == null ? Long.MAX_VALUE
                    : oldestWrite.getVersion();
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
         * This iterator is only used for Limbo reads that
         * traverse the collection of Writes. This iterator differs from the
         * Page (which is also an Iterator over Write objects) by virtue of the
         * fact that it does not allow removes and will detect concurrent
         * modification.
         */
        @Override
        public Iterator<Write> iterator() {

            return new Iterator<Write>() {

                /**
                 * The distance between the {@link #head} element and the
                 * {@code next} element. This is used to detect for concurrent
                 * modifications.
                 */
                private int distance = 0;

                /**
                 * The index of the "next" element in {@link #writes}.
                 */
                private int index = head;

                @Override
                public boolean hasNext() {
                    if(index - head != distance) {
                        throw new ConcurrentModificationException(
                                "A write has been removed from the Page");
                    }
                    return index < size;
                }

                @Override
                public Write next() {
                    if(index - head != distance) {
                        throw new ConcurrentModificationException(
                                "A write has been removed from the Page");
                    }
                    Write next = writes[index];
                    ++index;
                    ++distance;
                    return next;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();

                }

            };
        }

        /**
         * Return {@code true} if the Page <em>might</em> have a Write with the
         * specified {@code record} component. If this function returns true,
         * the caller should perform a linear scan using the local
         * {@link #iterator()} .
         * 
         * @param record
         * @return {@code true} if a write within {@code record} possibly exists
         */
        public boolean mightContain(Identifier record) {
            return recordCache[slotify(record.hashCode())];
        }

        /**
         * Return {@code true} if the Page <em>might</em> have a Write with the
         * specified {@code key} component. If this function returns true,
         * the caller should perform a linear scan using the local
         * {@link #iterator()} .
         * 
         * @param key
         * @return {@code true} if a write for {@code key} possibly exists
         */
        public boolean mightContain(Text key) {
            return keyCache[slotify(key.hashCode())];
        }

        /**
         * Return {@code true} if the Page <em>might</em> have a Write with the
         * specified {@code key} and {@code record} components. If this function
         * returns true, the caller should perform a linear scan using the local
         * {@link #iterator()} .
         * 
         * @param key
         * @param record
         * @return {@code true} if a write for {@code key} in {@code record}
         *         possibly exists
         */
        public boolean mightContain(Text key, Identifier record) {
            return keyRecordCache[slotify(key.hashCode(), record.hashCode())];
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
            Type valueType = write.getValue().getType();
            if(writeCache.mightContainCached(write.getRecord(), write.getKey(),
                    write.getValue())) {
                return true;
            }
            else if(valueType == Type.STRING) {
                return writeCache.mightContainCached(write.getRecord(),
                        write.getKey(),
                        Value.wrap(Convert.javaToThrift(Tag.create(
                                (String) write.getValue().getObject()))));
            }
            else if(valueType == Type.TAG) {
                return writeCache.mightContainCached(write.getRecord(),
                        write.getKey(), Value.wrap(Convert.javaToThrift(
                                write.getValue().getObject().toString())));
            }
            else {
                return false;
            }
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
            long stamp = lock.readLock();
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
                lock.unlockRead(stamp);
            }
        }

        /**
         * Do the work to actually index and append {@code write} (while
         * optionally performing a {@code sync} WITHOUT grabbing any locks
         * (hence this method being UNSAFE) for unauthorized usage.
         * 
         * @param write the {@link Write} to append
         * @param sync a flag that determines if the page should be fsynced
         *            (or the equivalent) after appending {@code write} so that
         *            the changes are guaranteed to be durably persisted, this
         *            flag should almost always be {@code true} if calling this
         *            method directly. It is set to {@code false} when called
         *            from the context of an atomic operation transporting
         *            writes to this Buffer using GROUP SYNC
         */
        @GuardedBy("Buffer.Page#append(Write)")
        private void appendUnsafe(final Write write, boolean sync) {
            index(write);
            content.putInt(write.size());
            write.copyTo(content);
            inventory.add(write.getRecord().longValue());
            if(sync) {
                sync();
            }
            GLOBAL_EXECUTOR.execute(new Runnable() {

                @Override
                public void run() {
                    WriteEvent event = new WriteEvent(write.getKey().toString(),
                            write.getValue().getTObject(),
                            write.getRecord().longValue(), write.getVersion(),
                            WriteEvent.Type.valueOf(write.getType().name()),
                            environment);
                    eventLog.add(event);
                }
            });
        }

        /**
         * Delete the {@link Page} from disk, as part of a process that has
         * already grabbed the {@link Page page's} write {@link Page#lock lock}.
         * 
         * @param stamp
         */
        private void delete(long stamp) {
            Preconditions.checkState(!isMutable(),
                    "The current Page cannot be deleted");
            if(lock.validate(stamp)) {
                delete0();
            }
            else {
                throw new IllegalMonitorStateException(
                        "Invalid stamp provided when deleting a Page");
            }
        }

        /**
         * Do the work to delete the {@link Page} from disk and, as much as
         * possible, clear its content from memory.
         */
        private void delete0() {
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
        @GuardedBy("Buffer.Page#transport(int, DurableStore)")
        private boolean hasNext() {
            return head < size;
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
                writes[size] = write;
                int hashCodeRecord = write.getRecord().hashCode();
                int hashCodeKey = write.getKey().hashCode();
                // The individual Write components are added instead of the
                // entire Write so that version information is not factored into
                // the bloom filter hashing
                writeCache.putCached(write.getRecord(), write.getKey(),
                        write.getValue());
                keyRecordCache[slotify(hashCodeRecord, hashCodeKey)] = true;
                recordCache[slotify(hashCodeRecord)] = true;
                keyCache[slotify(hashCodeKey)] = true;
                ++size;
            }
            else {
                throw CapacityException.INSTANCE;
            }
        }

        /**
         * Return {@code true} if this {@link Page} is mutable and available for
         * additional {@link Write writes} to be {@link #append(Write, boolean)
         * appended}.
         * 
         * @return {@code true} if this {@link Page} is mutable
         */
        private boolean isMutable() {
            return this == currentPage;
        }

        /**
         * Returns the Write at index {@link #head} in {@link #writes}.
         * <p>
         * <strong>NOTE:</strong>
         * <em>This method will return the same element on multiple
         * invocations until {@link #remove()} is called.</em>
         * </p>
         */
        @GuardedBy("Buffer.Page#transport(int, DurableStore)")
        private Write next() {
            return writes[head];
        }

        /**
         * Simulates the removal of the head Write from the Page. This method
         * only updates the {@link #head} and {@link #pos} metadata and does not
         * actually delete any data, which is a performance optimization.
         */
        @GuardedBy("Buffer.Page#transport(int, DurableStore)")
        private void remove() {
            ++head;
        }

        /**
         * Convenience function to return the appropriate slot in one of the
         * Page's filter's between 0 and {@code #sizeUpperBound} for an object
         * with {@code hashCode}.
         * 
         * @param hashCode
         * @return the slot
         */
        private int slotify(int hashCode) {
            return Math.abs(hashCode % sizeUpperBound);
        }

        /**
         * Convenience function to return the appropriate slot in one of the
         * Page's filter's between 0 and {@code #sizeUpperBound} for a group of
         * objects with the {@code hashCodes}.
         * 
         * @param hashCode
         * @return the slot
         */
        private int slotify(int... hashCodes) {
            return Math.abs(Integers.avg(hashCodes) % sizeUpperBound);
        }
    }

    /**
     * A {@link SeekingIterator} that looks for writes with a particular record
     * component.
     * 
     * @author Jeff Nelson
     */
    private class RecordSeekingIterator extends SeekingIterator {

        /**
         * The relevant record.
         */
        private final Identifier record;

        /**
         * Construct a new instance.
         * 
         * @param timestamp
         */
        /**
         * Construct a new instance.
         * 
         * @param record
         * @param timestamp
         */
        protected RecordSeekingIterator(long record, long timestamp) {
            super(timestamp);
            this.record = Identifier.of(record);
            init();
        }

        @Override
        protected boolean isRelevantWrite(Write write) {
            return write.getRecord().equals(record);
        }

        @Override
        protected boolean pageMightContainRelevantWrites(Page page) {
            return page.mightContain(record);
        }

    }

    /**
     * An {@link Iterator} over the writes in the buffer that has logic to only
     * return writes that match a certain {@code seek} criteria. The iterator
     * also uses the criteria as a hint to perform more optimal searches over
     * the pages in the Buffer.
     * 
     * @author Jeff Nelson
     */
    private abstract class SeekingIterator implements
            Iterator<Write>,
            CloseableIterator<Write> {

        /**
         * A flag that indicates whether we should not perform a timestamp check
         * because we want all the writes up until the present state.
         */
        private boolean ignoreTimestamp = false;

        /**
         * The stamp returned from grabbing the read access lock from
         * the {@link #locked} {@link Page}.
         */
        private long stamp = 0L;

        /**
         * A reference to the {@link Page#lock} of the {@link Page} which the
         * iterator is currently traversing.
         */
        private StampedLock lock;

        /**
         * The next write to return.
         */
        private Write next = null;

        /**
         * An iterator over all the pages in the Buffer.
         */
        private Iterator<Page> pageIterator;

        /**
         * The max timestamp for which to seek. If a Write's version is greater
         * than this timestamp, then the iterator ceases to return elements.
         */
        private final long timestamp;

        /**
         * A flag that indicates whether this iterator has satisfied
         * preconditions and is useable. If it is not useable, it won't perform
         * any traversals or return any data.
         */
        private final boolean useable;

        /**
         * The iterator over the writes on the page at which the iterator is
         * currently traversing.
         */
        private Iterator<Write> writeIterator = null;

        /**
         * A flag that indicates that the iterator has started providing writes.
         * This is used to check that we haven't hit a race condition corner
         * case in {@link #flip()}.
         */
        private boolean started = false;

        /**
         * Construct a new instance.
         * 
         * @param timestamp
         */
        protected SeekingIterator(long timestamp) {
            this.timestamp = timestamp;
            if(timestamp >= getOldestWriteTimestamp()) {
                this.useable = true;
                this.ignoreTimestamp = timestamp == Long.MAX_VALUE;
            }
            else {
                this.useable = false;
            }

        }

        @Override
        public void close() throws IOException {
            unlock();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Write next() {
            Write next0 = next;
            this.next = advance();
            return next0;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Each subclass should call this method after constructing the initial
         * state to turn to the first page and get the first write.
         */
        protected void init() {
            if(useable) {
                flip(true);
                this.next = advance();
                broadcastScanEvent();
            }
        }

        /**
         * Return {@code true} if {@code write} is relevant to what this
         * iterator is seeking.
         * 
         * @param write
         * @return {@code true} if the write is relevant
         */
        protected abstract boolean isRelevantWrite(Write write);

        /**
         * Call the appropriate function to determine if the {@code page} might
         * contain the kinds of writes that this iterator is seeking.
         * 
         * @param page
         * @return {@code true} if the page can possibly contain relevant data
         */
        protected abstract boolean pageMightContainRelevantWrites(Page page);

        /**
         * Advance to the next write that this iterator should return, if it
         * exists.
         * 
         * @return the next write or {@code null}
         */
        private Write advance() {
            for (;;) {
                if(writeIterator == null) {
                    return null;
                }
                while (writeIterator.hasNext()) {
                    Write write = writeIterator.next();
                    if(!ignoreTimestamp && write.getVersion() > timestamp) {
                        // We've reach a point where the writes on this page are
                        // later than the timestamp we are seeking, which means
                        // we can stop iterating, entirely.
                        halt();
                        return null;
                    }
                    else if(isRelevantWrite(write)) {
                        return write;
                    }
                }
                flip();
            }
        }

        /**
         * Flip to the next page in the Buffer.
         */
        private void flip() {
            flip(false);
        }

        /**
         * Flip to the next {@link Page} in the {@link Buffer}, or {@code reset}
         * and flip to the first one.
         * 
         * @param reset
         */
        private void flip(boolean reset) {
            writeIterator = null;

            if(reset) {
                pageIterator = pages.iterator();
            }

            // Find the next Page with relevant writes and flip to it
            while (pageIterator.hasNext()) {
                Page next = pageIterator.next();
                if(lock(next)) {
                    if(!ignoreTimestamp
                            && next.getOldestWriteTimestamp() > timestamp) {
                        // The #page only contains Writes that are later than
                        // the timestamp we are seeking, which means we can stop
                        // iterating, entirely.
                        halt();
                        break;
                    }
                    else if(pageMightContainRelevantWrites(next)) {
                        started = true;
                        writeIterator = next.iterator();
                        break;
                    }
                    else {
                        // The #page doesn't contain any relevant Writes, so
                        // move onto the next one.
                        continue;
                    }
                }
                else if(!started) {
                    // This is a corner case where a Transporter is holding the
                    // first Page's write lock. In case the Transporter ends up
                    // removing the Page, we have to spin and flip to the new
                    // beginning so that we don't start on an Page that no
                    // longer exists
                    synchronized (readable) {
                        try {
                            readable.wait();
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    flip(true);
                    return;
                }
                else {
                    // Once the iteration has started, a Transporter should not
                    // beat us and acquire the write lock for a page before we
                    // get to it. If that happens, the results are undefined
                    // because there is a situation where both the previously
                    // iterated writes have been transported (which, alone is
                    // acceptable) AND some writes that have not yet been
                    // iterated have also been transported, which means the
                    // iterator will produce an inconsistent state.
                    throw new IllegalStateException(
                            "The iterator was unable to acquire a Page lock after iteration started");
                }
            }
        }

        /**
         * Halt the iterator.
         */
        private void halt() {
            writeIterator = null;
            pageIterator = null;
            unlock();
        }

        /**
         * Grab the necessary locks to protect {@code #page} while it is being
         * scanned during the course of the iteration.
         * 
         * @param page
         * @return {@code true} if the lock is eventually acquired or
         *         {@code false} if it cannot be acquired
         */
        private boolean lock(Page page) {
            long s;
            if(lock != null) {
                // We've successfully locked earlier pages during this
                // iteration, so acquire the lock for the next page before
                // releasing the current lock so we ensure that no Transporter
                // ever jumps ahead of us and messes up the results
                StampedLock prior = lock;
                s = page.lock.readLock();
                prior.unlockRead(stamp);
            }
            else if((s = page.lock.tryReadLock()) == 0) {
                // This is the first page we're trying to lock, so do wait if we
                // are unsuccessful in case we were beat by a Transporter that
                // may remove #page
                return false;
            }
            lock = page.lock;
            stamp = s;
            return true;
        }

        /**
         * Release the locks for {@link #locked}.
         */
        private void unlock() {
            if(lock != null && stamp > 0) {
                lock.unlockRead(stamp);
                lock = null;
                stamp = 0;
            }
        }

    }

    /**
     * A {@link SeekingIterator} that looks for writes that are equal to a
     * comparison write.
     * 
     * @author Jeff Nelson
     */
    private class WriteSeekingIterator extends SeekingIterator {

        /**
         * The relevant write.
         */
        private final Write write;

        /**
         * Construct a new instance.
         * 
         * @param timestamp
         */
        protected WriteSeekingIterator(Write write, long timestamp) {
            super(timestamp);
            this.write = write;
            init();
        }

        @Override
        protected boolean isRelevantWrite(Write write) {
            return write.equals(this.write);
        }

        @Override
        protected boolean pageMightContainRelevantWrites(Page page) {
            return page.mightContain(write);
        }
    }
}
