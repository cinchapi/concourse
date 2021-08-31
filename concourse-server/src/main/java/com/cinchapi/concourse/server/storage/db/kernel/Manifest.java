/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.storage.db.search.SearchIndexer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link Manifest} stores and provides the efficient lookup for the start and
 * end position for sequences of bytes that relate to a key that is described by
 * one or more {@link Byteable} objects.
 * <p>
 * A {@link Manifest} is associated with each {@link Chunk} to determine where
 * to look on disk for a particular {@code locator} or
 * {@code locator}/{@code key} pair.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class Manifest extends TransferableByteSequence {

    /**
     * Create a new {@link Manifest} that is expected to have
     * {@code expectedInsertions} entries.
     * 
     * @param expectedInsertions
     * @return the new {@link Manifest}
     */
    public static Manifest create(int expectedInsertions) {
        return new Manifest(expectedInsertions);
    }

    /**
     * Load an existing {@link Manifest} from {@code file}.
     * 
     * @param file
     * @param position
     * @param length
     * @return the loaded {@link Manifest}
     */
    public static Manifest load(Path file, long position, long length) {
        return new Manifest(file, position, length);
    }

    /**
     * If the (byte) length of a flushed {@link Manifest} exceeds, this value,
     * the entries will be streamed into memory one-by-one using
     * {@link StreamedEntries} instead of loading them all into memory.
     */
    @VisibleForTesting
    protected static int MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD = (int) Math
            .pow(2, 25); // ~33.5mb or 419,430 entries

    /**
     * Represents an entry that has not been recorded.
     */
    public static final int NO_ENTRY = -1;

    /**
     * The number of worker threads to reserve for the {@link SearchIndexer}.
     */
    private static int ASYNC_BACKGROUND_LOADER_NUM_THREADS = Math.max(3,
            (int) Math.round(0.5 * Runtime.getRuntime().availableProcessors()));

    /**
     * An {@link ExecutorService} that asynchronously loads manifest entries in
     * the background without blocking a search for a specific
     * {@link #entries(Composite) entry}.
     */
    private static ExecutorService ASYNC_BACKGROUND_LOADER = Executors
            .newFixedThreadPool(ASYNC_BACKGROUND_LOADER_NUM_THREADS,
                    new ThreadFactoryBuilder().setDaemon(true)
                            .setNameFormat("Manifest Loader" + " %d").build());

    /**
     * Returned from {@link #lookup(Composite)} when an associated entry does
     * not exist.
     */
    private static final Range NULL_RANGE = new Range() {

        @Override
        public long end() {
            return NO_ENTRY;
        }

        @Override
        public long start() {
            return NO_ENTRY;
        }

    };

    /**
     * A {@link SoftReference} to the entries contained in the {@link Manifest}
     * that is used to reduce memory overhead.
     * 
     * <p>
     * It is {@code null} until the
     * {@link Manifest} is {@link #freeze(Path, long) frozen} or the
     * {@link #entries()} are loaded
     * </p>
     */
    @Nullable
    private SoftReference<Map<Composite, Entry>> $entries;

    /**
     * The entries contained in the {@link Manifest}.
     * <p>
     * It is {@code null} if the
     * {@link Manifest} has been {@link #freeze(Path, long) frozen}.
     * </p>
     */
    @Nullable
    private Map<Composite, Entry> entries;

    /**
     * The running size of the {@link Manifest} in bytes.
     */
    private long length = 0;

    /**
     * Final reference to {@link #MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD}
     * (which is accessible for testing) in hopes of getting a little
     * performance gain...
     */
    private final transient int streamingThreshold = MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD;

    /**
     * Construct a new instance.
     * 
     * @param expectedInsertions
     */
    private Manifest(int expectedInsertions) {
        super();
        this.length = 0;
        this.entries = new HashMap<>(expectedInsertions);
        this.$entries = null;
    }

    /**
     * Load an existing instance.
     * 
     * @param file
     * @param position
     * @param length
     */
    private Manifest(Path file, long position, long length) {
        super(file, position, length);
        this.length = length;
        this.entries = null;
        this.$entries = null;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Manifest) {
            Manifest other = (Manifest) obj;
            return entries().equals(other.entries());
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return entries().hashCode();
    }

    @Override
    public long length() {
        return length;
    }

    /**
     * Return the {@link Entry} which contains the start and end positions of
     * data related to the {@code byteables}, if data was recorded using
     * {@link #putStart(long, Byteable...)} and
     * {@link #putEnd(long, Byteable...)}.
     * 
     * @param composite
     * @return the {@link Entry} containing the start and end positions
     */
    public Range lookup(Byteable... bytables) {
        return lookup(Composite.create(bytables));
    }

    /**
     * Return the {@link Entry} which contains the start and end positions of
     * data related to the {@code composite}, if data was recorded using
     * {@link #putStart(long, Byteable...)} and
     * {@link #putEnd(long, Byteable...)}.
     * 
     * @param composite
     * @return the {@link Entry} containing the start and end positions
     */
    public Range lookup(Composite composite) {
        Entry entry = entries(composite).get(composite);
        return entry != null ? new EntryRange(entry) : NULL_RANGE;
    }

    /**
     * Record the end position for the {@code byteables}.
     * 
     * @param end
     * @param byteables
     */
    public void putEnd(long end, Byteable... byteables) {
        putEnd(end, Composite.create(byteables));
    }

    /**
     * Record the end position for the {@code composite}.
     * 
     * @param end
     * @param composite
     */
    public void putEnd(long end, Composite composite) {
        Preconditions.checkArgument(end >= 0,
                "Cannot have negative index. Tried to put %s", end);
        Preconditions.checkState(isMutable());
        Entry entry = entries.get(composite);
        Preconditions.checkState(entry != null,
                "Cannot set the end position before setting "
                        + "the start position. Tried to put %s",
                end);
        entry.setEnd(end);
    }

    /**
     * Record the start position for the {@code byteables}.
     * 
     * @param start
     * @param byteables
     */
    public void putStart(long start, Byteable... byteables) {
        putStart(start, Composite.create(byteables));
    }

    /**
     * Record the start position for the {@code byteables}.
     * 
     * @param start
     * @param composite
     */
    public void putStart(long start, Composite composite) {
        Preconditions.checkArgument(start >= 0,
                "Cannot have negative index. Tried to put %s", start);
        Preconditions.checkState(isMutable());
        Entry entry = entries.get(composite);
        if(entry == null) {
            entry = new Entry(composite);
            entries.put(composite, entry);
            length += entry.size() + 4;
        }
        entry.setStart(start);
    }

    @Override
    protected void flush(ByteSink sink) {
        for (Entry entry : entries().values()) {
            sink.putInt(entry.size());
            entry.copyTo(sink);
        }
    }

    @Override
    protected void free() {
        this.$entries = new SoftReference<Map<Composite, Entry>>(entries);
        this.entries = null; // Make eligible for GC
    }

    /**
     * Return {@code true} if this {@link Manifest} is considered
     * <em>loaded</em> meaning all of its entries are available in memory.
     * 
     * @return {@code true} if the entries are loaded
     */
    protected boolean isLoaded() { // visible for testing
        return isMutable() || ($entries != null && $entries.get() != null);
    }

    /**
     * Return the entries in this index. This method will lazily load the
     * entries on demand if they do not currently exist in memory.
     * 
     * @return the entries
     */
    private synchronized Map<Composite, Entry> entries() {
        return entries(null);
    }

    /**
     * Return the entries in this index with a hint that {@code composite} will
     * be subsequently used in a {@link Map#get(Object)} call.
     * <p>
     * If necessary, this method will lazily load the entries on demand if they
     * do not currently exist in memory. If the entries are loaded on-demand and
     * {@code composite} is not null, the returned {@link Map} is only
     * guaranteed to have a mapping from {@code composite} if it exists in the
     * {@link Manifest} or be {@link Map#isEmpty() empty} if it does not.
     * Therefore, only use the value returned from this method to called
     * {@link Map#get(Object)} using {@code composite} or there will be
     * undefined behaviour. If the full set of entries is required, use
     * {@link #entries()}
     * 
     * @param the {@link Composite} that will be sought on a subsequent call to
     *            {@link Map#get(Object)}
     * @return the entries
     */
    private synchronized Map<Composite, Entry> entries(
            @Nullable Composite composite) {
        if(entries != null) {
            return entries;
        }
        else if($entries != null && $entries.get() != null) {
            return $entries.get();
        }
        else {
            Map<Composite, Entry> entries = new StreamedEntries();
            // If the Manifest is small enough to fit comfortably into memory,
            // eagerly load all of the entries instead of streaming them from
            // disk one-by-one (as is done in the StreamedEntries).
            if(length < streamingThreshold) {
                // If #composite != null, shortcut the loading process by
                // forking the job to a background thread and listening for the
                // sought #composite to be found and returned immediately
                BlockingQueue<Map<Composite, Entry>> queue = new ArrayBlockingQueue<>(
                        1);
                // @formatter:off
                Executor executor = composite != null 
                        ? ASYNC_BACKGROUND_LOADER
                        : MoreExecutors.directExecutor();
                // @formatter:on
                Map<Composite, Entry> heapEntries = new HashMap<>(
                        (int) length / Entry.CONSTANT_SIZE);
                executor.execute(() -> {
                    entries.forEach((key, value) -> {
                        heapEntries.put(key, value);
                        if(composite != null && key.equals(composite)) {
                            queue.add(ImmutableMap.of(key, value));
                        }
                    });
                    queue.offer(composite != null ? Collections.emptyMap()
                            : heapEntries);
                    $entries = new SoftReference<Map<Composite, Entry>>(
                            heapEntries);
                });
                try {
                    return queue.take();
                }
                catch (InterruptedException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
            }
            else {
                return entries;
            }
        }
    }

    /**
     * Contains the start and end positions for an entry in the
     * {@link Manifest}.
     *
     * @author Jeff Nelson
     */
    @Immutable
    public static abstract class Range {

        // This class is returned from the #lookup methods to provide a clean
        // interface to callers without exposing the totality of what is
        // encapsulated in each Entry.

        /**
         * Return the end position. If it has not been recorded, return
         * {@link Manifest#NO_ENTRY}.
         * 
         * @return the end position
         */
        public abstract long end();

        /**
         * Return the start position. If it has not been recorded, return
         * {@link Manifest#NO_ENTRY}.
         * 
         * @return the start position
         */
        public abstract long start();
    }

    /**
     * Represents a single entry in the {@link Manifest}.
     * 
     * @author Jeff Nelson
     */
    private final class Entry implements Byteable {

        private static final int CONSTANT_SIZE = 16; // start(8), end(8)

        private long end = NO_ENTRY;
        private final Composite key;
        private long start = NO_ENTRY;

        /**
         * Construct an instance that represents an existing Entry from a
         * ByteBuffer. This constructor is public so as to comply with the
         * {@link Byteable} interface. Calling this constructor directly is not
         * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
         * advantage of reference caching.
         * 
         * @param bytes
         */
        public Entry(ByteBuffer bytes) {
            this.start = bytes.getLong();
            this.end = bytes.getLong();
            this.key = Composite
                    .load(ByteBuffers.get(bytes, bytes.remaining()));
        }

        /**
         * Construct a new instance.
         * 
         * @param key
         */
        public Entry(Composite key) {
            this.key = key;
        }

        @Override
        public void copyTo(ByteSink sink) {
            sink.putLong(start);
            sink.putLong(end);
            key.copyTo(sink);
        }

        /**
         * Return the end position.
         * 
         * @return the end
         */
        public long end() {
            return end;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Entry) {
                Entry other = (Entry) obj;
                return start == other.start && end == other.end
                        && key.equals(other.key);
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, start, end);
        }

        /**
         * Return the entry key
         * 
         * @return the key
         */
        public Composite key() {
            return key;
        }

        /**
         * Set the end position.
         * 
         * @param end the end to set
         */
        public void setEnd(long end) {
            this.end = end;
        }

        /**
         * Set the start position.
         * 
         * @param start the start to set
         */
        public void setStart(long start) {
            this.start = start;
        }

        @Override
        public int size() {
            return CONSTANT_SIZE + key.size();
        }

        /**
         * Return the start position.
         * 
         * @return the start
         */
        public long start() {
            return start;
        }

    }

    /**
     * A {@link Range} backed by an {@link Entry}.
     */
    @Immutable
    private final class EntryRange extends Range {

        /**
         * The underlying {@link Entry}.
         */
        private final Entry entry;

        private EntryRange() {
            this.entry = null;
        };

        /**
         * Construct a new instance.
         * 
         * @param entry
         */
        private EntryRange(Entry entry) {
            this.entry = entry;
        }

        @Override
        public long end() {
            return entry.end();
        }

        @Override
        public long start() {
            return entry.start();
        }
    }

    /**
     * A {@link Map} that reads the {@link Entry entries} from disk one-by-one.
     * This should be used for an immutable {@link Manifest} that is larger than
     * {@link #MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD}.
     *
     * @author Jeff Nelson
     */
    private final class StreamedEntries extends AbstractMap<Composite, Entry> {

        @Override
        public Set<Entry<Composite, Manifest.Entry>> entrySet() {
            Set<Entry<Composite, Manifest.Entry>> entrySet = new HashSet<>();
            forEach((composite, entry) -> entrySet
                    .add(new SimpleEntry<>(composite, entry)));
            return entrySet;
        }

        @Override
        public void forEach(
                BiConsumer<? super Composite, ? super Manifest.Entry> action) {
            Iterator<ByteBuffer> it = length > 0
                    ? ByteableCollections.stream(file(), position(), length,
                            GlobalState.DISK_READ_BUFFER_SIZE)
                    : Collections.emptyIterator();

            while (it.hasNext()) {
                Manifest.Entry entry = new Manifest.Entry(it.next());
                action.accept(entry.key(), entry);
            }

        }

        @Override
        public Manifest.Entry get(Object o) {
            if(o instanceof Composite) {
                Composite key = (Composite) o;
                Iterator<ByteBuffer> it = length > 0
                        ? ByteableCollections.stream(file(), position(), length,
                                GlobalState.DISK_READ_BUFFER_SIZE)
                        : Collections.emptyIterator();
                while (it.hasNext()) {
                    ByteBuffer next = it.next();
                    if(key.size() + Manifest.Entry.CONSTANT_SIZE == next
                            .remaining()) {
                        // Shortcut by only considering ByteBuffers that
                        // match the expected size of an entry mapped from
                        // the #key
                        Manifest.Entry entry = new Manifest.Entry(next);
                        if(key.equals(entry.key())) {
                            return entry;
                        }
                    }
                }

            }
            return null;
        }

    }
}
