/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.storage.db.search.SearchIndexer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

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
     * Load an existing {@link Manifest} from {@code segment}.
     * 
     * @param segment
     * @param position
     * @param length
     * @return the loaded {@link Manifest}
     */
    public static Manifest load(Segment segment, long position, long length) {
        return new Manifest(segment.file(), segment.channel(), position,
                length);
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
    private static final Span NULL_RANGE = new Span() {

        @Override
        public long end() {
            return NO_ENTRY;
        }

        @Override
        public void setEnd(long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setStart(long start) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long start() {
            return NO_ENTRY;
        }

    };

    /**
     * The estimated number of bytes required to store an entry. This
     * calculation is based on the variable range of {@link Composite} lengths.
     */
    // @formatter:off
    private static final int ESTIMATED_ENTRY_SIZE_IN_BYTES = 
            ((Span.CONSTANT_SIZE + 1) + 
            (Span.CONSTANT_SIZE + Composite.MAX_SIZE)) 
            / 2;
    //@formatter:on

    /**
     * A {@link SoftReference} to the entries contained in the {@link Manifest}.
     * After the {@link Manifest}'s memory is {@link {@link #free() freed}, this
     * reference is populated to opportunistically keep the entries in memory if
     * there is room to do so.
     * 
     * <p>
     * It is {@code null} until the {@link Manifest} is {@link #flush(ByteSink)
     * flushed} or the {@link #entries()} are loaded.
     * </p>
     */
    @Nullable
    private SoftReference<Map<Composite, Span>> $entries;

    /**
     * The entries contained in the {@link Manifest}.
     * <p>
     * Entries are represented as a mapping from a lookup {@link Composite key}
     * to a {@link Span} that encapsulates a start and end position of a
     * specific data block in a {@link Chunk}.
     * </p>
     * <p>
     * It is {@code null} if the
     * {@link Manifest}'s memory has been {@link #free() freed} or the
     * {@link Manifest} has been loaded from disk. The intent is to only keep
     * the entries in memory for a {@link Manifest} that is being actively
     * updated.
     * </p>
     */
    @Nullable
    private Map<Composite, Span> entries;

    /**
     * The running size of the {@link Manifest} in bytes. Incremented as entries
     * are {@link #putStart(long, Composite) record}.
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
        this.entries = GlobalState.ENABLE_EFFICIENT_METADATA
                ? new BinaryHashMap(expectedInsertions)
                : new HashMap<>(expectedInsertions);
        this.$entries = null;
    }

    /**
     * Load an existing instance.
     * 
     * @param file
     * @param channel
     * @param position
     * @param length
     */
    private Manifest(Path file, FileChannel channel, long position,
            long length) {
        super(file, channel, position, length);
        this.length = length;
        this.entries = null;
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
        this(file, null, position, length);
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
     * Return the {@link Span} which contains the start and end
     * positions of
     * data related to the {@code byteables}, if data was recorded using
     * {@link #putStart(long, Byteable...)} and
     * {@link #putEnd(long, Byteable...)}.
     * 
     * @param composite
     * @return the {@link Span} containing the start and end positions
     */
    public Range lookup(Byteable... bytables) {
        return lookup(Composite.create(bytables));
    }

    /**
     * Return the {@link Span} which contains the start and end
     * positions of
     * data related to the {@code composite}, if data was recorded using
     * {@link #putStart(long, Byteable...)} and
     * {@link #putEnd(long, Byteable...)}.
     * 
     * @param composite
     * @return the {@link Span} containing the start and end positions
     */
    public Range lookup(Composite composite) {
        Span range = entries(composite).get(composite);
        return range != null ? range : NULL_RANGE;
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
        Span range = entries.get(composite);
        Preconditions.checkState(range != null,
                "Cannot set the end position before setting "
                        + "the start position. Tried to put %s",
                end);
        range.setEnd(end);
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
        Span range = entries.get(composite);
        if(range == null) {
            // @formatter:off
            range = GlobalState.ENABLE_EFFICIENT_METADATA 
                    ? new BinarySpan()
                    : new LongSpan();
            // @formatter:on
            entries.put(composite, range);
            // @formatter:off
            length += composite.size() + 
                    Span.CONSTANT_SIZE + 
                    4; // (each entry is preceded by 4 bytes that gives the overall length)
            // @formatter:on
        }
        range.setStart(start);
    }

    @Override
    protected void flush(ByteSink sink) {
        for (Entry<Composite, Span> entry : entries().entrySet()) {
            Composite key = entry.getKey();
            Span range = entry.getValue();
            int size = Span.CONSTANT_SIZE + key.size();
            sink.putInt(size);
            sink.put(range.bytes());
            key.copyTo(sink);
        }
    }

    @Override
    protected void free() {
        this.$entries = new SoftReference<Map<Composite, Span>>(entries);
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
    private synchronized Map<Composite, Span> entries() {
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
    private synchronized Map<Composite, Span> entries(
            @Nullable Composite composite) {
        if(entries != null) {
            return entries;
        }
        else if($entries != null && $entries.get() != null) {
            return $entries.get();
        }
        else {
            Map<Composite, Span> entries = new StreamedEntries();
            // If the Manifest is small enough to fit comfortably into memory,
            // eagerly load all of the entries instead of streaming them from
            // disk one-by-one (as is done in the StreamedEntries).
            if(length < streamingThreshold) {
                // If #composite != null, shortcut the loading process by
                // forking the job to a background thread which listens for the
                // sought #composite to be found and returned immediately while
                // the other entries continue to be read
                BlockingQueue<Map<Composite, Span>> queue = new ArrayBlockingQueue<>(
                        1);
                // @formatter:off
                Executor executor = composite != null 
                        ? ASYNC_BACKGROUND_LOADER
                        : MoreExecutors.directExecutor();
                // @formatter:on
                int capacity = (int) length
                        / (4 + ESTIMATED_ENTRY_SIZE_IN_BYTES);
                Map<Composite, Span> heapEntries = GlobalState.ENABLE_EFFICIENT_METADATA
                        ? new BinaryHashMap(capacity)
                        : new HashMap<>(capacity);
                executor.execute(() -> {
                    boolean found = false;
                    for (Entry<Composite, Span> entry : entries.entrySet()) {
                        Composite key = entry.getKey();
                        Span value = entry.getValue();
                        heapEntries.put(key, value);
                        if(composite != null && !found
                                && key.equals(composite)) {
                            queue.add(ImmutableMap.of(key, value));
                            found = true;
                        }
                    }
                    queue.offer(composite != null ? Collections.emptyMap()
                            : heapEntries);
                    $entries = new SoftReference<Map<Composite, Span>>(
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
     * The start and end markers for an entry in a {@link Manifest}.
     *
     * @author Jeff Nelson
     */
    public static interface Range {

        /**
         * Return the end position.
         * 
         * @return the end position
         */
        public long end();

        /**
         * Return the start position.
         * 
         * @return the start position
         */
        public long start();

    }

    /**
     * A {@link Map} that stores {@link Manifest} entries on heap in a
     * memory-efficient manner.
     * <p>
     * Used instead of a normal {@link HashMap} when
     * {@link GlobalState#ENABLE_EFFICIENT_METADATA} is {@code true}.
     * </p>
     *
     * @author Jeff Nelson
     */
    private final static class BinaryHashMap
            extends AbstractMap<Composite, Span> {

        /**
         * Strategy used to correctly determine hash codes and equality among
         * byte arrays.
         */
        private final static Hash.Strategy<byte[]> HASH_STRATEGY = new Hash.Strategy<byte[]>() {

            @Override
            public boolean equals(byte[] a, byte[] b) {
                return Arrays.equals(a, b);
            }

            @Override
            public int hashCode(byte[] o) {
                return Arrays.hashCode(o);
            }

        };

        /**
         * The internal on-heap data structure where the entries are maintained.
         * <p>
         * An entry is represented as the mapping between two byte arrays to
         * avoid memory overhead that would accompany the storage of
         * {@link Composite} and {@link Span} objects directly. This is
         * necessary because the storage overhead (especially in the case of a
         * {@link Span}) would equal or exceed the amount of memory
         * needed for
         * the essence of the data).
         * </p>
         * <p>
         * Ad hoc, {@link Composite} and {@link Span} objects are read
         * and
         * constructed on the fly, as necessary.
         * </p>
         */
        private final Map<byte[], byte[]> internal;

        /**
         * Construct a new instance.
         * 
         * @param initialCapacity
         */
        private BinaryHashMap(int initialCapacity) {
            this.internal = new Object2ObjectOpenCustomHashMap<>(
                    initialCapacity, HASH_STRATEGY);
        }

        @Override
        public Set<Entry<Composite, Span>> entrySet() {
            return new AbstractSet<Entry<Composite, Span>>() {

                @Override
                public Iterator<Entry<Composite, Span>> iterator() {
                    return Iterators.transform(internal.entrySet().iterator(),
                            entry -> {
                                Composite key = Composite
                                        .load(ByteBuffer.wrap(entry.getKey()));
                                Span value = new BinarySpan(entry.getValue());
                                return new SimpleImmutableEntry<>(key, value);
                            });
                }

                @Override
                public int size() {
                    return internal.size();
                }

            };
        }

        @Override
        public Span get(Object key) {
            if(key instanceof Composite) {
                byte[] value = internal.get(((Composite) key).bytes());
                if(value != null) {
                    return new BinarySpan(value);
                }
            }
            return null;
        }

        @Override
        public boolean isEmpty() {
            return internal.isEmpty();
        }

        @Override
        public Span put(Composite key, Span value) {
            byte[] k = key.bytes();
            byte[] v = value.bytes();
            byte[] prev = internal.put(k, v);
            return prev != null ? new BinarySpan(prev) : null;
        }

        @Override
        public int size() {
            return internal.size();
        }

    }

    /**
     * A {@link Span} that stores positions in a byte array.
     *
     * @author Jeff Nelson
     */
    private static class BinarySpan implements Span {

        /**
         * The bytes for each marker.
         */
        private byte[] bytes;

        /**
         * Construct a new instance.
         */
        BinarySpan() {
            this.bytes = new byte[CONSTANT_SIZE];
            long value = NO_ENTRY;
            for (int i = 7; i >= 0; i--) {
                bytes[i] = bytes[i + 8] = (byte) (value & 0xFF);
                value >>= 8;
            }
        }

        /**
         * Load an existing instance.
         * 
         * @param bytes
         */
        BinarySpan(ByteBuffer bytes) {
            this.bytes = new byte[CONSTANT_SIZE];
            bytes.get(this.bytes);
        }

        /**
         * Construct a new ad-hoc instance.
         * 
         * @param bytes
         */
        private BinarySpan(byte[] bytes) {
            // This constructor should only be used to construct ad ad-hoc Range
            // from a byte array that is already stored in a HeapEntries
            // instance.
            Preconditions.checkArgument(bytes.length == CONSTANT_SIZE);
            this.bytes = bytes;
        }

        @Override
        public byte[] bytes() {
            return bytes;
        }

        @Override
        public long end() {
            return read(8);
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj) {
                return true;
            }
            else if(obj instanceof Span) {
                Span other = (Span) obj;
                return Arrays.equals(bytes, other.bytes());
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public void setEnd(long value) {
            write(8, value);
        }

        @Override
        public void setStart(long value) {
            write(0, value);
        }

        @Override
        public long start() {
            return read(0);
        }

        /**
         * Read 8 of the {@link #bytes} starting at {@code index} and return
         * the corresponding long.
         * 
         * @param index
         * @return the read value
         */
        private long read(int index) {
            return ((long) bytes[index] << 56)
                    | ((long) (bytes[index + 1] & 0xff) << 48)
                    | ((long) (bytes[index + 2] & 0xff) << 40)
                    | ((long) (bytes[index + 3] & 0xff) << 32)
                    | ((long) (bytes[index + 4] & 0xff) << 24)
                    | ((long) (bytes[index + 5] & 0xff) << 16)
                    | ((long) (bytes[index + 6] & 0xff) << 8)
                    | ((long) (bytes[index + 7] & 0xff));
        }

        /**
         * Write {@code value} to {@link #bytes} starting at {@code index}.
         * 
         * @param index
         * @param value
         */
        private void write(int index, long value) {
            bytes[index] = (byte) (value >> 56);
            bytes[index + 1] = (byte) (value >> 48);
            bytes[index + 2] = (byte) (value >> 40);
            bytes[index + 3] = (byte) (value >> 32);
            bytes[index + 4] = (byte) (value >> 24);
            bytes[index + 5] = (byte) (value >> 16);
            bytes[index + 6] = (byte) (value >> 8);
            bytes[index + 7] = (byte) value;
        }
    }

    /**
     * A {@link Span} that stores positions as 64-bit long values.
     *
     * @author Jeff Nelson
     */
    private static class LongSpan implements Span {

        /**
         * The start position.
         */
        private long start;

        /**
         * The end position.
         */
        private long end;

        /**
         * Construct a new instance.
         */
        LongSpan() {
            this.start = NO_ENTRY;
            this.end = NO_ENTRY;
        }

        /**
         * Construct a new instance.
         * 
         * @param bytes
         */
        LongSpan(ByteBuffer bytes) {
            this.start = bytes.getLong();
            this.end = bytes.getLong();
        }

        @Override
        public long end() {
            return end;
        }

        @Override
        public void setEnd(long end) {
            this.end = end;
        }

        @Override
        public void setStart(long start) {
            this.start = start;
        }

        @Override
        public long start() {
            return start;
        }

    }

    /**
     * A {@link Map#Entry} where both the {@link #getKey()} and
     * {@link #getValue()} can be updated. An instance of this is returned from
     * the {@link Map#entrySet()} of the {@link StreamedEntries} in an effort to
     * prevent temporary object creation.
     *
     * @author Jeff Nelson
     */
    private static final class ReusableMapEntry<K, V> implements
            Map.Entry<K, V> {

        private K key;
        private V value;

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        public K setKey(K key) {
            K old = key;
            this.key = key;
            return old;
        }

        @Override
        public V setValue(V value) {
            V old = value;
            this.value = value;
            return old;
        }

    }

    /**
     * A {@link Range} that can be mutated.
     *
     * @author Jeff Nelson
     */
    private static interface Span extends Range {

        /**
         * The number of bytes required to record each {@link Span}.
         */
        static final int CONSTANT_SIZE = 16; // start(8), end(8)

        /**
         * Return the binary representation of this {@link Span}.
         * 
         * @return the {@link Span} bytes
         */
        public default byte[] bytes() {
            byte[] bytes = new byte[CONSTANT_SIZE];
            long start = start();
            long end = end();
            for (int i = 7; i >= 0; i--) {
                bytes[i] = (byte) (start & 0xFF);
                bytes[i + 8] = (byte) (end & 0xFF);
                start >>= 8;
                end >>= 8;
            }
            return bytes;
        }

        /**
         * Set the end position to {@code value}.
         * 
         * @param value
         */
        void setEnd(long end);

        /**
         * Set the start position to {@code value}.
         * 
         * @param value
         */
        void setStart(long start);

    }

    /**
     * A {@link Map} that reads the {@link Entry entries} from disk one-by-one.
     * This should be used for an immutable {@link Manifest} that is larger than
     * {@link #MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD}.
     *
     * @author Jeff Nelson
     */
    private final class StreamedEntries extends AbstractMap<Composite, Span> {

        @Override
        public Set<Entry<Composite, Span>> entrySet() {
            // It is assumed that the return #entrySet is only used to
            // facilitate streaming all the entries, so it is not appropriate to
            // perform query operations (e.g. get()) directly on it.
            return new AbstractSet<Entry<Composite, Span>>() {

                @Override
                public Iterator<Entry<Composite, Span>> iterator() {
                    return new Iterator<Entry<Composite, Span>>() {

                        Iterator<ByteBuffer> it = ByteableCollections.stream(
                                channel(), position(), length,
                                GlobalState.DISK_READ_BUFFER_SIZE);

                        /**
                         * A {@link ReusableMapEntry} that is updated and
                         * returned on each call to {@link #next()} so that we
                         * don't create unnecessary temporary objects.
                         */
                        ReusableMapEntry<Composite, Span> reusable = new ReusableMapEntry<>();

                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Entry<Composite, Span> next() {
                            ByteBuffer next = it.next();
                            Span range = GlobalState.ENABLE_EFFICIENT_METADATA
                                    ? new BinarySpan(next)
                                    : new LongSpan(next);
                            Composite key = Composite.load(next);
                            reusable.setKey(key);
                            reusable.setValue(range);
                            return reusable;
                        }

                    };
                }

                @Override
                public int size() {
                    throw new UnsupportedOperationException();
                }

            };
        }

        @Override
        public void forEach(
                BiConsumer<? super Composite, ? super Span> action) {
            for (Entry<Composite, Span> entry : entrySet()) {
                action.accept(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public Span get(Object o) {
            if(o instanceof Composite) {
                Composite key = (Composite) o;
                Iterator<ByteBuffer> it = ByteableCollections.stream(channel(),
                        position(), length, GlobalState.DISK_READ_BUFFER_SIZE);
                ByteBuffer keyBytes = key.getBytes();
                while (it.hasNext()) {
                    ByteBuffer next = it.next();
                    if(equals(keyBytes, next)) {
                        return GlobalState.ENABLE_EFFICIENT_METADATA
                                ? new BinarySpan(next)
                                : new LongSpan(next);
                    }
                }

            }
            return null;
        }

        /**
         * Assuming {@code key} is the {@link Composite#getBytes() byte buffer}
         * of a {@link Composite}, return {@code true} if the {@link Composite}
         * encoded in the {@code next} {@link ByteBuffer} is equal.
         * 
         * @param key
         * @param next
         * @return {@code true} if there is a match
         */
        private boolean equals(ByteBuffer key, ByteBuffer next) {
            if(key.remaining() + Span.CONSTANT_SIZE == next.remaining()) {
                next.mark();
                next.position(next.position() + Span.CONSTANT_SIZE);
                if(key.equals(next)) {
                    next.reset();
                    return true;
                }
            }
            return false;
        }

    }
}
