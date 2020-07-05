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
package com.cinchapi.concourse.server.storage.db.disk;

import java.nio.file.Path;

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;

/**
 * A {@link Chunk} that is written serially.
 *
 * @author Jeff Nelson
 */
abstract class SerialChunk<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends Chunk<L, K, V> {

    /**
     * The running value returned from {@link #sizeImpl()}.
     */
    protected int _size = 0;

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param file
     * @param position
     * @param size
     * @param filter
     * @param manifest
     */
    protected SerialChunk(@Nullable Segment segment, Path file, long position,
            long size, BloomFilter filter, Manifest manifest) {
        super(segment, file, position, size, filter, manifest);
    }

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param filter
     */
    protected SerialChunk(@Nullable Segment segment, BloomFilter filter) {
        super(segment, filter);
    }

    @Override
    protected final void incrementSizeBy(int delta) {
        _size += delta;
    }

    @Override
    protected final int sizeImpl() {
        return _size;
    }

}
