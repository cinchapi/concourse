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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractSet;
import java.util.Arrays;
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
import com.cinchapi.concourse.collect.CloseableIterator;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Logger;
import com.google.common.collect.Iterators;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

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
                    + "performed on disk, which is A LOT slower...", e);
            System.out.println("Out of memory...");
            // This *usually* means that there isn't enough off heap memory
            // needed for Chronicle, so fallback to storing the entires on disk
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
        public Iterator<Text> iterator() {
            return chronicle.iterator();
        }

        @Override
        public int size() {
            return chronicle.size();
        }

        @Override
        public boolean contains(Object o) {
            return chronicle.contains(o);
        }

        @Override
        public boolean add(Text text) {
            return chronicle.add(text);
        }

        @Override
        public void close() throws IOException {
            chronicle.close();
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

            @Override
            public @NotNull TextBytesMarshaller readResolve() {
                return INSTANCE;
            }

            @SuppressWarnings("rawtypes")
            @Override
            public Text read(Bytes in, Text using) {
                int size = in.readInt();
                ByteBuffer bytes = ByteBuffer.allocate(size);
                in.read(bytes);
                bytes.flip();
                return Text.fromByteBufferCached(bytes);
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
         * A {@link BloomFilter} to speed up calls to {@link #contains(Object)}.
         */
        private final BloomFilter<byte[]> filter;

        /**
         * {@link FileChannel} used for reading and writing the {@link Text} to
         * disk.
         */
        private final FileChannel channel;

        /**
         * A {@link ByteSink} used for abstracting writes to the
         * {@link #channel}.
         */
        private final ByteSink sink;

        /**
         * The position for writing in {@link FileChannel}.
         */
        private long position = 0;

        /**
         * The backing file.
         */
        private final Path file;

        /**
         * Construct a new instance.
         */
        public FileTextSet(int expectedInsertions) {
            this.filter = BloomFilter.create(Funnels.byteArrayFunnel(),
                    expectedInsertions);
            this.file = Paths.get(FileOps.tempFile());
            this.channel = FileSystem.getFileChannel(file);
            this.sink = ByteSink.to(channel);
        }

        @Override
        public Iterator<Text> iterator() {
            return new CloseableIterator<Text>() {

                MappedByteBuffer bytes = FileSystem.map(file, MapMode.READ_ONLY,
                        0, position);
                Iterator<ByteBuffer> it = ByteableCollections.iterator(bytes);

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Text next() {
                    return Text.fromByteBuffer(it.next());
                }

                @Override
                public void close() throws IOException {
                    FileSystem.unmapAsync(bytes);
                }

            };
        }

        @Override
        public int size() {
            return Iterators.size(iterator());
        }

        @Override
        public boolean contains(Object o) {
            if(o instanceof Text) {
                Text text = (Text) o;
                return contains(ByteBuffers.getByteArray(text.getBytes()));
            }
            else {
                return false;
            }
        }

        private boolean contains(byte[] bytes) {
            if(filter.mightContain(bytes)) {
                MappedByteBuffer buffer = FileSystem.map(file,
                        MapMode.READ_ONLY, 0, position);
                Iterator<ByteBuffer> it = ByteableCollections.iterator(buffer);
                try {
                    while (it.hasNext()) {
                        if(Arrays.equals(bytes,
                                ByteBuffers.getByteArray(it.next()))) {
                            return true;
                        }
                    }
                }
                finally {
                    FileSystem.unmapAsync(buffer);
                }
            }
            return false;
        }

        @Override
        public boolean add(Text text) {
            byte[] bytes = ByteBuffers.getByteArray(text.getBytes());
            if(!contains(bytes)) {
                try {
                    filter.put(bytes);
                    channel.position(position);
                    sink.putInt(bytes.length);
                    sink.put(bytes);
                    sink.flush();
                    position += 4 + bytes.length;
                    return true;
                }
                catch (IOException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
            }
            else {
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

}
