/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.nio.channels.FileChannel;
import java.nio.file.Path;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.IndexRevision;
import com.cinchapi.concourse.server.storage.db.Revision;

/**
 * A {@link Chunk} that stores {@link IndexRevision IndexRevisions} for
 * various {@link IndexRecord IndexRecords}.
 *
 * @author Jeff Nelson
 */
public class IndexChunk extends SerialChunk<Text, Value, Identifier> {

    /**
     * Return a new {@link IndexChunk}.
     * 
     * @param filter
     * @return the created {@link Chunk}
     */
    public static IndexChunk create(BloomFilter filter) {
        return create(null, filter);
    }

    /**
     * Return a new {@link IndexChunk}.
     * 
     * @param segment
     * @param filter
     * @return the created {@link Chunk}
     */
    public static IndexChunk create(@Nullable Segment segment,
            BloomFilter filter) {
        return new IndexChunk(segment, filter);
    }

    /**
     * Load an existing {@link IndexChunk}.
     * 
     * @param file
     * @param position
     * @param size
     * @param filter
     * @param manifest
     * @return the loaded {@link Chunk}
     */
    public static IndexChunk load(Path file, long position, long size,
            BloomFilter filter, Manifest manifest) {
        return new IndexChunk(null, file, null, position, size, filter,
                manifest);
    }

    /**
     * Load an existing {@link TableChunk}.
     * 
     * @param segment
     * @param position
     * @param size
     * @param filter
     * @param manifest
     * @return the loaded {@link Chunk}
     */
    public static IndexChunk load(@Nonnull Segment segment, long position,
            long size, BloomFilter filter, Manifest manifest) {
        return new IndexChunk(segment, segment.file(), segment.channel(),
                position, size, filter, manifest);
    }

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param filter
     */
    private IndexChunk(@Nullable Segment segment, BloomFilter filter) {
        super(segment, filter);
    }

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param file
     * @param channel
     * @param position
     * @param size
     * @param filter
     * @param manifest
     */
    private IndexChunk(@Nullable Segment segment, Path file,
            FileChannel channel, long position, long size, BloomFilter filter,
            Manifest manifest) {
        super(segment, file, channel, position, size, filter, manifest);
    }

    @Override
    public IndexArtifact insert(Text locator, Value key, Identifier value,
            long version, Action type) throws IllegalStateException {
        return (IndexArtifact) super.insert(locator, key, value, version, type);
    }

    @Override
    protected IndexArtifact makeArtifact(
            Revision<Text, Value, Identifier> revision,
            Composite[] composites) {
        return new IndexArtifact((IndexRevision) revision, composites);
    }

    @Override
    protected IndexRevision makeRevision(Text locator, Value key,
            Identifier value, long version, Action type) {
        return Revision.createIndexRevision(locator, key, value, version, type);
    }

    @Override
    protected Class<IndexRevision> xRevisionClass() {
        return IndexRevision.class;
    }

}
