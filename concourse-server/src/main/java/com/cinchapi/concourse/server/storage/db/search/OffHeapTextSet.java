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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractSet;
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
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.FileOps;
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
            // TODO: how to size chronicle if we don't know the key in advance?
            this.chronicle = ChronicleSet.of(Text.class)
                    .averageKeySize(expectedInsertions)
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
         * The {@link Funnel} to sink {@link Text} into the {@link #filter}.
         */
        @SuppressWarnings("serial")
        private static final Funnel<Text> FUNNEL = new Funnel<Text>() {

            @Override
            public void funnel(Text from, PrimitiveSink into) {
                into.putBytes(from.getBytes());
            }

        };

        /**
         * A {@link BloomFilter} to speed up calls to {@link #contains(Object)}.
         */
        private final BloomFilter<Text> filter;

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
            this.filter = BloomFilter.create(FUNNEL, expectedInsertions);
            this.file = Paths.get(FileOps.tempFile());
            channel = FileSystem.getFileChannel(file);
            sink = ByteSink.to(channel);
        }

        @Override
        public Iterator<Text> iterator() {
            return new Iterator<Text>() {

                @SuppressWarnings("deprecation")
                Iterator<ByteBuffer> it = ByteableCollections.streamingIterator(
                        file, 0, position, GlobalState.DISK_READ_BUFFER_SIZE);

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Text next() {
                    return Text.fromByteBuffer(it.next());
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
                if(filter.mightContain(text)) {
                    Iterator<Text> it = iterator();
                    while (it.hasNext()) {
                        Text next = it.next();
                        if(text.equals(next)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public boolean add(Text text) {
            if(!contains(text)) {
                try {
                    filter.put(text);
                    channel.position(position);
                    ByteBuffer bytes = text.getBytes();
                    int size = bytes.remaining();
                    sink.putInt(size);
                    sink.put(bytes);
                    sink.flush();
                    position += 4 + size;
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
