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
package com.cinchapi.concourse.server.storage.db.search;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.internal.announcer.InternalAnnouncer;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import org.jetbrains.annotations.NotNull;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.logging.Logging;
import com.cinchapi.concourse.annotate.Experimental;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Logger;
import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.LargeByteBuffer;
import com.github.davidmoten.bplustree.Serializer;
import com.google.common.collect.Iterators;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * A {@link Set} of {@link Text} that is stored off-heap.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
@Experimental
public abstract class OffHeapTextSet extends AbstractSet<Text> implements
        Closeable {

    /**
     * Return an {@link OffHeapTextSet}.
     * 
     * @return the {@link OffHeapTextSet}
     */
    public static OffHeapTextSet create() {
        return create(GlobalState.BUFFER_PAGE_SIZE);
    }

    /**
     * Return an {@link OffHeapTextSet} that is configured to accommodate
     * {@code expectedInsertions}.
     * 
     * @param expectedInsertions
     * @return the {@link OffHeapTextSet}
     */
    public static OffHeapTextSet create(int expectedInsertions) {
        try {
            return new ChronicleTextSet(expectedInsertions);
        }
        catch (OutOfMemoryError e) {
            Logger.warn("There isn't enough native memory to deduplicate "
                    + "up to {} search indexes, so the operation must be "
                    + "performed on disk, which is A LOT slower...",
                    expectedInsertions);
            // This *usually* means that there isn't enough off heap memory
            // needed for Chronicle, so fallback to storing the entries on disk
            // and using a bloom filter to "speed" things up. This will be slow
            // AF, but is a last resort to try to keep things running.
            return new FileTextSet(expectedInsertions);
        }
    }

    /**
     * Return an {@link OffHeapTextSet} for unit tests.
     * 
     * @return the {@link OffHeapTextSet}
     */
    static OffHeapTextSet test() {
        return new FileTextSet(GlobalState.BUFFER_PAGE_SIZE);
    }

    /**
     * {@link OffHeapTextSet} that is backed by a {@link ChronicleSet}.
     *
     * @author Jeff Nelson
     */
    private static class ChronicleTextSet extends OffHeapTextSet {

        static {
            Logging.disable(Jvm.class);
            Logging.disable(InternalAnnouncer.class);
            Logging.disable(VanillaChronicleMap.class);
        }

        /**
         * The backing store.
         */
        private final ChronicleSet<Text> chronicle;

        /**
         * Construct a new instance.
         */
        public ChronicleTextSet(int expectedInsertions) {
            this.chronicle = ChronicleSet.of(Text.class)
                    .averageKeySize(GlobalState.MAX_SEARCH_SUBSTRING_LENGTH > 0
                            ? GlobalState.MAX_SEARCH_SUBSTRING_LENGTH
                            : 100)
                    .keyMarshaller(TextBytesMarshaller.INSTANCE)
                    .entries(expectedInsertions).create();
        }

        @Override
        public boolean add(Text text) {
            return chronicle.add(text);
        }

        @Override
        public void close() throws IOException {
            chronicle.close();
        }

        @Override
        public boolean contains(Object o) {
            return chronicle.contains(o);
        }

        @Override
        public Iterator<Text> iterator() {
            return chronicle.iterator();
        }

        @Override
        public int size() {
            return chronicle.size();
        }

        /**
         * Used to serialize {@link Text} to/from a {@link ChronicleSet}.
         *
         * @author Jeff Nelson
         */
        private static final class TextBytesMarshaller implements
                BytesWriter<Text>,
                BytesReader<Text>,
                ReadResolvable<TextBytesMarshaller> {

            /**
             * Singleton.
             */
            static TextBytesMarshaller INSTANCE = new TextBytesMarshaller();

            @SuppressWarnings("rawtypes")
            @Override
            public Text read(Bytes in, Text using) {
                int size = in.readInt();
                ByteBuffer bytes = ByteBuffer.allocate(size);
                in.read(bytes);
                bytes.flip();
                return Text.fromByteBufferCached(bytes);
            }

            @Override
            public @NotNull TextBytesMarshaller readResolve() {
                return INSTANCE;
            }

            @SuppressWarnings("rawtypes")
            @Override
            public void write(Bytes out, Text toWrite) {
                byte[] bytes = ByteBuffers.getByteArray(toWrite.getBytes());
                out.writeInt(bytes.length);
                out.write(bytes);
            }

        }
    }

    /**
     * {@link OffHeapTextSet} that is backed by a {@link Path file}.
     *
     * @author Jeff Nelson
     */
    private static class FileTextSet extends OffHeapTextSet {

        /**
         * {@link Funnel} for {@link #filter}.
         */
        @SuppressWarnings("serial")
        private static Funnel<Text> FUNNEL = new Funnel<Text>() {

            @Override
            public void funnel(Text from, PrimitiveSink into) {
                into.putBytes(from.getBytes());
            }

        };

        /**
         * Known collisions that are stored in an effort to prevent a linear
         * probe of the underlying file. We only store known collisions here
         * (e.g. when a call to {@link #contains(byte[])} returns true) so this
         * does not have the same memory overhead as storing the entire
         * set of {@link Text}.
         */
        private final Set<Text> collisions = new HashSet<>();

        /**
         * A {@link BloomFilter} to speed up calls to {@link #contains(Object)}.
         */
        private final BloomFilter<Text> filter;

        /**
         * {@link Text} {@link Serializer} for {@link #tree}.
         */
        private final Serializer<Text> SERIALIZER = new Serializer<Text>() {

            @Override
            public int maxSize() {
                return 0;
            }

            @Override
            public Text read(LargeByteBuffer bb) {
                int size = bb.getVarint();
                byte[] bytes = new byte[size];
                bb.get(bytes);
                return Text.fromByteBuffer(ByteBuffer.wrap(bytes));
            }

            @Override
            public void write(LargeByteBuffer bb, Text t) {
                bb.putVarint(t.size());
                bb.put(ByteBuffers.getByteArray(t.getBytes()));
            }

        };

        /**
         * Disk based backing store.
         */
        private final BPlusTree<Integer, Text> tree;

        /**
         * Construct a new instance.
         */
        public FileTextSet(int expectedInsertions) {
            this.filter = BloomFilter.create(FUNNEL, expectedInsertions);
            this.tree = BPlusTree.file().directory(FileOps.tempDir(""))
                    .keySerializer(Serializer.INTEGER)
                    .valueSerializer(SERIALIZER).naturalOrder();
        }

        @Override
        public boolean add(Text text) {
            if(!contains(text)) {
                filter.put(text);
                tree.insert(text.hashCode(), text);
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            try {
                tree.close();
            }
            catch (IOException e) {
                throw e;
            }
            catch (Exception e1) {
                throw CheckedExceptions.wrapAsRuntimeException(e1);
            }
        }

        @Override
        public boolean contains(Object o) {
            if(o instanceof Text) {
                Text text = (Text) o;
                if(filter.mightContain(text)) {
                    if(collisions.contains(text)) {
                        return true;
                    }
                    else {
                        Iterator<Text> it = tree.find(text.hashCode())
                                .iterator();
                        while (it.hasNext()) {
                            if(text.equals(it.next())) {
                                try {
                                    collisions.add(text);
                                }
                                catch (OutOfMemoryError e) {
                                    // Ignore and keep on trucking. We were
                                    // using this data structure because memory
                                    // was in short supply, but we kept an
                                    // in-memory of known collisions to try to
                                    // speed things up. If we're truly at a
                                    // point where memory isn't available, just
                                    // suffer the reality of needing a linear
                                    // probe to be aware of any new collisions.
                                }
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public Iterator<Text> iterator() {
            return null;
        }

        @Override
        public int size() {
            return Iterators.size(iterator());
        }
    }

}
