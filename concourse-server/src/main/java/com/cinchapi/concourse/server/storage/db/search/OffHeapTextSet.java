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

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.logging.Logging;
import com.cinchapi.concourse.annotate.Experimental;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.model.Text;

/**
 * A {@link Set} of {@link Text} that is stored off-heap.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
@Experimental
public class OffHeapTextSet extends AbstractSet<Text> implements Closeable {

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
    public OffHeapTextSet() {
        this(GlobalState.BUFFER_PAGE_SIZE);
    }

    /**
     * Construct a new instance.
     */
    public OffHeapTextSet(int expectedInsertions) {
        this.chronicle = ChronicleSet.of(Text.class).averageKeySize(100)
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
