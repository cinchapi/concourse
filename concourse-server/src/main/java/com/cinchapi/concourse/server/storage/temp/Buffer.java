/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.TernaryTruth;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.ConcourseExecutors;
import com.cinchapi.concourse.server.concurrent.Locks;
import com.cinchapi.concourse.server.concurrent.PriorityReadWriteLock;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.plugin.data.WriteEvent;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Inventory;
import com.cinchapi.concourse.server.storage.InventoryTracker;
import com.cinchapi.concourse.server.storage.PermanentStore;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Integers;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.MultimapViews;
import com.cinchapi.concourse.util.NaturalSorter;
import com.cinchapi.concourse.util.ReadOnlyIterator;
import com.cinchapi.concourse.util.TMaps;
import com.cinchapi.concourse.util.ThreadFactories;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import static com.cinchapi.concourse.server.GlobalState.BINARY_QUEUE;
import static com.cinchapi.concourse.server.GlobalState.BUFFER_DIRECTORY;
import static com.cinchapi.concourse.server.GlobalState.BUFFER_PAGE_SIZE;
import static com.google.common.collect.Maps.newLinkedHashMap;

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
 * @author Jeff Nelson
 */
@ThreadSafe
public final class Buffer extends Limbo implements InventoryTracker {

    /**
     * Assuming {@code location} is a valid bufferStore, return an
     * {@link Iterator} to traverse the writes in the Buffer directly from disk
     * without loading the entire Buffer into memory.
     * 
     * @return the iterator
     */
    public static Iterator<Write> onDiskIterator(String location) {
        return new OnDiskIterator(location);
    }

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
     * A global {@link ExecutorService} to asynchronously record all the
     * {@link WriteEvent write events} that are handled by any Buffer instance.
     */
    private final static ExecutorService GLOBAL_EXECUTOR = Executors
            .newCachedThreadPool(
                    ThreadFactories.namingDaemonThreadFactory("buffer-global"));

    /**
     * Don't let the transport rate exceed this value.
     */
    private static int MAX_TRANSPORT_RATE = 8192;

    /**
     * The maximum number of milliseconds to sleep between transport cycles.
     */
    private static final int MAX_TRANSPORT_THREAD_SLEEP_TIME_IN_MS = 100;

    /**
     * The minimum number of milliseconds to sleep between transport cycles.
     */
    private static final int MIN_TRANSPORT_THREAD_SLEEP_TIME_IN_MS = 5;

    /**
     * The number of slots to put in each Page's bloom filter. We want this
     * small enough to have few hash functions, but large enough so that the
     * bloom filter does not become saturated.
     */
    private static int PER_PAGE_BLOOM_FILTER_CAPACITY = GlobalState.BUFFER_PAGE_SIZE
            / 10;
    /**
     * The multiplier that is used when increasing the rate of transport.
     */
    protected int transportRateMultiplier = 2; // visible for testing

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
    private Runnable inventorySync = new Runnable() {

        @Override
        public void run() {
            inventory.sync();
        }

    };

    /**
     * The number of verifies initiated.
     */
    private AtomicLong numVerifyRequests;

    /**
     * The number of verifies scanning the buffer.
     */
    private AtomicLong numVerifyScans;

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
    private Runnable pageSync = new Runnable() {

        @Override
        public void run() {
            currentPage.content.force();
        }

    };

    /**
     * A flag to indicate if the Buffer is running or not.
     */
    private boolean running = false;

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
     * We keep track of the time when the last transport occurred so that the
     * Engine can determine if it should avoid busy waiting in the
     * BufferTransportThread.
     */
    private AtomicLong timeOfLastTransport = new AtomicLong(Time.now());

    /**
     * A monitor that is used to make a thread block while waiting for the
     * Buffer to become transportable. The {@link #waitUntilTransportable()}
     * waits for this monitor and the {@link #insert(Write)} method notifies the
     * threads waiting on this monitor whenever there is more than single page
     * worth of data in the Buffer.
     */
    private final Object transportable = new Object();

    /**
     * The number of items to transport to the Database per attempt. There is a
     * tension between transporting and reading data (e.g. reads cannot happen
     * while a transport occurs and vice versa). Transports are most efficient
     * when they can batch up the amount of work per cycle, but that would be
     * reads are blocked longer. So this variable indicates how many items
     * should be transported in a single cycle. Each time a transport happens,
     * this value will increase, but it will be decreased whenever a read
     * occurs. This allows us to be more aggressive with transports when there
     * are no reads happening, and also allows us to scale back transports when
     * reads do occur.
     */
    private int transportRate = 1;

    /**
     * The number of milliseconds to sleep between transport cycles.
     */
    private int transportThreadSleepTimeInMs = MAX_TRANSPORT_THREAD_SLEEP_TIME_IN_MS;

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
        this.numVerifyRequests = new AtomicLong(0);
        this.numVerifyScans = new AtomicLong(0);
    }

    @Override
    public Map<Long, String> audit(long record) {
        Map<Long, String> audit = Maps.newTreeMap();
        for (Iterator<Write> it = iterator(record, Time.NONE); it.hasNext();) {
            Write write = it.next();
            audit.put(write.getVersion(), write.toString());
        }
        return audit;
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        Map<Long, String> audit = Maps.newTreeMap();
        for (Iterator<Write> it = iterator(key, record, Time.NONE); it
                .hasNext();) {
            Write write = it.next();
            audit.put(write.getVersion(), write.toString());
        }
        return audit;
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp,
            Map<TObject, Set<Long>> context) {
        for (Iterator<Write> it = iterator(key, timestamp); it.hasNext();) {
            Write write = it.next();
            Set<Long> records = context.get(write.getValue().getTObject());
            if(records == null) {
                records = Sets.newLinkedHashSet();
                context.put(write.getValue().getTObject(), records);
            }
            if(write.getType() == Action.ADD) {
                records.add(write.getRecord().longValue());
            }
            else {
                records.remove(write.getRecord().longValue());
            }
        }
        return Maps.newTreeMap((SortedMap<TObject, Set<Long>>) Maps
                .filterValues(context, emptySetFilter));
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end, Map<Long, Set<TObject>> context) {
        Set<TObject> snapshot = Iterables.getLast(context.values(),
                Sets.<TObject> newLinkedHashSet());
        if(snapshot.isEmpty() && !context.isEmpty()) {
            // CON-474: Empty set is placed in the context if it was the last
            // snapshot know to the database
            context.remove(Time.NONE);
        }
        for (Iterator<Write> it = iterator(key, record, end - 1); it
                .hasNext();) {
            Write write = it.next();
            long timestamp = write.getVersion();
            Text writtenKey = write.getKey();
            long writtenRecordId = write.getRecord().longValue();
            Action action = write.getType();
            if(writtenKey.toString().equals(key) && writtenRecordId == record) {
                snapshot = Sets.newLinkedHashSet(snapshot);
                Value newValue = write.getValue();
                if(action == Action.ADD) {
                    snapshot.add(newValue.getTObject());
                }
                else if(action == Action.REMOVE) {
                    snapshot.remove(newValue.getTObject());
                }
                if(timestamp >= start && !snapshot.isEmpty()) {
                    context.put(timestamp, snapshot);
                }
            }
        }
        return context;
    }

    @Override
    public boolean contains(long record) {
        return inventory.contains(record);
    }

    @Override
    public Set<String> describe(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        for (Iterator<Write> it = iterator(record, timestamp); it.hasNext();) {
            Write write = it.next();
            Set<TObject> values;
            values = context.get(write.getKey().toString());
            if(values == null) {
                values = Sets.newHashSet();
                context.put(write.getKey().toString(), values);
            }
            if(write.getType() == Action.ADD) {
                values.add(write.getValue().getTObject());
            }
            else {
                values.remove(write.getValue().getTObject());
            }
        }
        return newLinkedHashMap(Maps.filterValues(context, emptySetFilter))
                .keySet();
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
            long timestamp, String key, Operator operator, TObject... values) {
        for (Iterator<Write> it = iterator(key, timestamp); it.hasNext();) {
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
        return TMaps.asSortedMap(context);
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
    public int getDesiredTransportSleepTimeInMs() {
        return transportThreadSleepTimeInMs;
    }

    @Override
    public Inventory getInventory() {
        return inventory;
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
    public boolean insert(Write write, boolean sync) {
        structure.lock();
        try {
            boolean notify = pages.size() == 2 && currentPage.size == 0;
            currentPage.append(write, sync);
            if(notify) {
                synchronized (transportable) {
                    transportable.notify();
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

    @Override
    public Iterator<Write> iterator() {
        return new AllSeekingIterator(Time.NONE);
    }

    /**
     * Return an iterator over all writes in the buffer that occurred no later
     * than {@code timestamp}.
     * 
     * @param timestamp
     * @return the iterator
     */
    public Iterator<Write> iterator(long timestamp) {
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
    public Iterator<Write> iterator(long record, long timestamp) {
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
    public Iterator<Write> iterator(String key, long timestamp) {
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
    public Iterator<Write> iterator(String key, long record, long timestamp) {
        return new KeyInRecordSeekingIterator(key, record, timestamp);
    }

    /**
     * Return an iterator over all writes in the buffer that equal the input
     * {@code write} and that occurred no later than {@code timestamp}.
     * 
     * @param timestamp
     * @return the iterator
     */
    public Iterator<Write> iterator(Write write, long timestamp) {
        return new WriteSeekingIterator(write, timestamp);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        for (Iterator<Write> it = iterator(record, timestamp); it.hasNext();) {
            Write write = it.next();
            Set<TObject> values;
            values = context.get(write.getKey().toString());
            if(values == null) {
                values = Sets.newLinkedHashSet();
                context.put(write.getKey().toString(), values);
            }
            if(write.getType() == Action.ADD) {
                values.add(write.getValue().getTObject());
            }
            else {
                values.remove(write.getValue().getTObject());
            }
        }
        return Maps.newTreeMap((SortedMap<String, Set<TObject>>) Maps
                .filterValues(context, emptySetFilter));
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp,
            Set<TObject> context) {
        for (Iterator<Write> it = iterator(key, record, timestamp); it
                .hasNext();) {
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

    /**
     *
     * Called by the parent {@link Engine} to set the environment that the
     * Buffer
     * associated to
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
                addPage(false);
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

    @Override
    public void sync() {
        ConcourseExecutors.executeAndAwaitTermination(threadNamePrefix,
                pageSync, inventorySync);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method will transport at least one write from the buffer, in
     * chronological order.
     * </p>
     */
    @Override
    public void transport(PermanentStore destination, boolean sync) {
        // NOTE: The #sync parameter is ignored because the Database does not
        // support allowing the Buffer to control when syncs happen.
        if(pages.size() > 1) {
            Page page = pages.get(0);
            if(!page.transportLock.writeLock().isHeldByCurrentThread()
                    && page.transportLock.writeLock().tryLock()) {
                try {
                    for (int i = 0; i < transportRate; ++i) {
                        if(page.hasNext()) {
                            destination.accept(page.next());
                            page.remove();
                        }
                        else {
                            ((Database) destination).triggerSync();
                            removePage();
                            break;
                        }
                    }
                    timeOfLastTransport.set(Time.now());
                    transportRate = transportRate >= MAX_TRANSPORT_RATE
                            ? MAX_TRANSPORT_RATE
                            : (transportRate * transportRateMultiplier);
                    --transportThreadSleepTimeInMs;
                    if(transportThreadSleepTimeInMs < MIN_TRANSPORT_THREAD_SLEEP_TIME_IN_MS) {
                        transportThreadSleepTimeInMs = MIN_TRANSPORT_THREAD_SLEEP_TIME_IN_MS;
                    }
                }
                finally {
                    page.transportLock.writeLock().unlock();
                }
            }
        }
    }

    @Override
    public boolean verify(Write write, long timestamp, boolean exists) {
        numVerifyRequests.incrementAndGet();
        for (Iterator<Write> it = iterator(write, timestamp); it.hasNext();) {
            it.next();
            exists ^= true; // toggle boolean
        }
        return exists;
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
                try {
                    transportable.wait();
                }
                catch (InterruptedException e) {/* ignore */}
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
        Action action = null;
        while (it.hasNext()) {
            action = it.next().getType();
        }
        return action;
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
            currentPage = new Page(BUFFER_PAGE_SIZE);
            pages.add(currentPage);
            Logger.debug("Added page {} to Buffer", currentPage);
        }
        finally {
            structure.unlock();
        }
    }

    /**
     * Determines the percentage within range [0, 1] of verifies that scan
     * the buffer.
     * 
     * @return: decimal percentage of verifies initiated that scanned the
     *          buffer.
     */
    @SuppressWarnings("unused")
    private float getPercentVerifyScans() { // to be used for CON-236
        return ((float) numVerifyScans.get()) / numVerifyRequests.get();
    }

    /**
     * Remove the first page in the Buffer.
     */
    private void removePage() {
        structure.lock();
        try {
            pages.remove(0).delete();
        }
        finally {
            structure.unlock();
        }
    }

    /**
     * Scale back the number of items that are transported in a single cycle.
     */
    private void scaleBackTransportRate() {
        transportRate = 1;
        transportThreadSleepTimeInMs = MAX_TRANSPORT_THREAD_SLEEP_TIME_IN_MS;
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
        private final PrimaryKey record;

        /**
         * Construct a new instance.
         * 
         * @param timestamp
         */
        protected KeyInRecordSeekingIterator(String key, long record,
                long timestamp) {
            super(timestamp);
            this.key = Text.wrapCached(key);
            this.record = PrimaryKey.wrap(record);
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
     * An {@link Iterator} that can traverse Writes directly from disk for a
     * Buffer that uses {@code location} as a store. Call
     * {@link Buffer#onDiskIterator(String)} to instantiate one of these. This
     * should only be used in cases where it is necessary (and safe) to iterate
     * through a Buffer's writes while the Buffer is offline.
     * 
     * @author Jeff Nelson
     */
    private static class OnDiskIterator extends ReadOnlyIterator<Write> {

        /**
         * An {@link Iterator} over all the files in the input directory.
         */
        private final Iterator<String> fileIt;

        /**
         * An {@link Iterator} over the data chunks in the current file.
         */
        private Iterator<ByteBuffer> it = null;

        /**
         * Construct a new instance.
         * 
         * @param location
         */
        private OnDiskIterator(String location) {
            this.fileIt = FileSystem.fileOnlyIterator(location);
            flip();
        }

        @Override
        public boolean hasNext() {
            if(it == null) {
                return false;
            }
            else if(!it.hasNext() && fileIt.hasNext()) {
                flip();
                return hasNext();
            }
            else if(!it.hasNext()) {
                return false;
            }
            else {
                return true;
            }
        }

        @Override
        public Write next() {
            if(hasNext()) {
                return Byteables.readStatic(it.next(), Write.class);
            }
            else {
                return null;
            }
        }

        /**
         * Flip to the next page in the iterator.
         */
        private void flip() {
            if(fileIt.hasNext()) {
                ByteBuffer bytes = FileSystem.readBytes(fileIt.next());
                it = ByteableCollections.iterator(bytes);
            }
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
    private class Page implements Iterator<Write>, Iterable<Write> {

        // NOTE: This class does not define hashCode() and equals() because the
        // defaults are the desired behaviour.

        /**
         * The filename extension.
         */
        private static final String ext = ".buf";

        /**
         * The local lock for read/write access on the page. This is only used
         * when this page is equal to the {@link #currentPage}. In that case,
         * this lock is grabbed before any access is allowed on the page, so
         * subsequent structures that are used need not be thread safe.
         */
        private transient StampedLock accessLock = new StampedLock();

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
         * The transportLock makes it possible to append new Writes and
         * transport old writes concurrently while prohibiting reading the Page
         * and transporting writes at the same time.
         */
        private final transient ReentrantReadWriteLock transportLock = PriorityReadWriteLock
                .prioritizeReads();

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
            writeCache.disableThreadSafety();
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
            Preconditions.checkState(this == currentPage, "Illegal attempt to "
                    + "append a Write to an inactive Page");
            long stamp = accessLock.writeLock();
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
                accessLock.unlockWrite(stamp);
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
         * Returns {@code true} if {@link #head} is smaller than the largest
         * occupied index in {@link #writes}. This means that it is possible for
         * calls to this method to initially return {@code false} at t0, but
         * eventually return {@code true} at t1 if an element is added to the
         * Page between t0 and t1.
         */
        @Override
        public boolean hasNext() {
            long stamp = Locks.stampLockReadIfCondition(accessLock,
                    this == currentPage);
            try {
                return head < size;
            }
            finally {
                Locks.stampUnlockReadIfCondition(accessLock, stamp,
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
        public boolean mightContain(PrimaryKey record) {
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
        public boolean mightContain(Text key, PrimaryKey record) {
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
            long stamp = Locks.stampLockReadIfCondition(accessLock,
                    this == currentPage);
            try {
                return writes[head];
            }
            finally {
                Locks.stampUnlockReadIfCondition(accessLock, stamp,
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
            long stamp = Locks.stampLockWriteIfCondition(accessLock,
                    this == currentPage);
            try {
                ++head;
            }
            finally {
                Locks.stampUnlockWriteIfCondition(accessLock, stamp,
                        this == currentPage);
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
            long stamp = Locks.stampLockReadIfCondition(accessLock,
                    this == currentPage);
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
                Locks.stampUnlockReadIfCondition(accessLock, stamp,
                        this == currentPage);
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
                    BINARY_QUEUE.add(event);
                }
            });
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
        private final PrimaryKey record;

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
            this.record = PrimaryKey.wrap(record);
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
    private abstract class SeekingIterator implements Iterator<Write> {

        /**
         * A flag that indicates whether we should not perform a timestamp check
         * because we want all the writes up until the present state.
         */
        private boolean ignoreTimestamp = false;

        /**
         * The stamp returned from grabbing the read access lock from
         * {@link #myCurrentPage}.
         */
        private long myAccessStamp = 0L;

        /**
         * A reference to the page in which the iterator is currently
         * traversing.
         */
        private Page myCurrentPage;

        /**
         * The next write to return.
         */
        private Write next = null;

        /**
         * An iterator over all the pages in the Buffer.
         */
        private Iterator<Page> pageIterator = pages.iterator();

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
         * Construct a new instance.
         * 
         * @param timestamp
         */
        protected SeekingIterator(long timestamp) {
            this.timestamp = timestamp;
            if(timestamp >= getOldestWriteTimestamp()) {
                scaleBackTransportRate();
                this.ignoreTimestamp = timestamp == Long.MAX_VALUE;
                this.next = advance();
                this.useable = true;
            }
            else {
                this.useable = false;
            }

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
                        writeIterator = null;
                        pageIterator = null;
                        releaseLocks();
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
         * Flip to the next page in the Buffer with the option to temporarily
         * skip the timestamp check.
         * 
         * @param skipTsCheck
         */
        private void flip(boolean skipTsCheck) {
            writeIterator = null;
            releaseLocks();
            if(pageIterator.hasNext()) {
                while (pageIterator.hasNext()) {
                    Page next = pageIterator.next();
                    grabLocks(next);
                    if(!skipTsCheck && !ignoreTimestamp
                            && next.getOldestWriteTimestamp() > timestamp) {
                        writeIterator = null;
                        pageIterator = null;
                        releaseLocks();
                        break;
                    }
                    if(pageMightContainRelevantWrites(next)) {
                        writeIterator = next.iterator();
                        break;
                    }
                    else {
                        releaseLocks();
                    }
                }
            }
        }

        /**
         * Grab the necessary locks to protected {@code #page} while it is used
         * in the iterator.
         * 
         * @param page
         */
        private void grabLocks(Page page) {
            myCurrentPage = page;
            myAccessStamp = Locks.stampLockReadIfCondition(
                    myCurrentPage.accessLock, myCurrentPage == currentPage);
            myCurrentPage.transportLock.readLock().lock();
        }

        /**
         * Release the locks for {@link #myCurrentPage}.
         */
        private void releaseLocks() {
            if(myCurrentPage != null) {
                Locks.stampUnlockReadIfCondition(myCurrentPage.accessLock,
                        myAccessStamp, myCurrentPage == currentPage);
                myCurrentPage.transportLock.readLock().unlock();
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
         * A flag to check whether the buffer has already been scanned (to
         * mitigate multiple increments given multiple scans
         * to the same buffer).
         */
        private boolean scanned;

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
            boolean mightContain = page.mightContain(write);
            if(!scanned && mightContain) {
                numVerifyScans.incrementAndGet();
            }
            return mightContain;
        }
    }
}
