/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.legacy;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.storage.db.Revision;

/**
 * Viewer for the legacy (pre-0.11) {@link Block} format.
 * <p>
 * This class can be used to stream the revisions from a {@link Block} file from
 * disk and return them in an {@link #iterator()}.
 * </p>
 *
 * @author Jeff Nelson
 */
public class Block<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        implements
        Iterable<Revision<L, K, V>> {

    private final Path file;
    private final Class<Revision<L, K, V>> revisionClass;

    /**
     * Construct a new instance.
     * 
     * @param file
     * @param revisionClass
     */
    public Block(Path file, Class<Revision<L, K, V>> revisionClass) {
        this.file = file;
        this.revisionClass = revisionClass;
    }

    @Override
    public Iterator<Revision<L, K, V>> iterator() {
        return new Iterator<Revision<L, K, V>>() {

            private final Iterator<ByteBuffer> it = ByteableCollections
                    .streamingIterator(file.toString(),
                            GlobalState.BUFFER_PAGE_SIZE);

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Revision<L, K, V> next() {
                ByteBuffer next = it.next();
                if(next != null) {
                    return Byteables.read(next, revisionClass);
                }
                else {
                    return null;
                }
            }

        };
    }

}
