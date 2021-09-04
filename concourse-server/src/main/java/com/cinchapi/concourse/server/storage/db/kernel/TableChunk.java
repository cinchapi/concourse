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
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.TableRevision;

/**
 * A {@link Chunk} that stores {@link TableRevision TableRevisions} for
 * various {@link TableRecord TableRecords}.
 *
 * @author Jeff Nelson
 */
public class TableChunk extends SerialChunk<Identifier, Text, Value> {

    /**
     * Return a new {@link TableChunk}.
     * 
     * @param filter
     * @return the created {@link Chunk}
     */
    public static TableChunk create(BloomFilter filter) {
        return create(null, filter);
    }

    /**
     * Return a new {@link TableChunk}.
     * 
     * @param segment
     * @param filter
     * @return the created {@link Chunk}
     */
    public static TableChunk create(@Nullable Segment segment,
            BloomFilter filter) {
        return new TableChunk(segment, filter);
    }

    /**
     * Load an existing {@link TableChunk}.
     * 
     * @param file
     * @param position
     * @param size
     * @param filter
     * @param manifest
     * @return the loaded {@link Chunk}
     */
    public static TableChunk load(Path file, long position, long size,
            BloomFilter filter, Manifest manifest) {
        return new TableChunk(null, file, null, position, size, filter,
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
    public static TableChunk load(@Nonnull Segment segment, long position,
            long size, BloomFilter filter, Manifest manifest) {
        return new TableChunk(segment, segment.file(), segment.channel(),
                position, size, filter, manifest);
    }

    /**
     * Load an existing {@link TableChunk}.
     * 
     * @param segment
     * @param file
     * @param channel
     * @param position
     * @param size
     * @param filter
     * @param manifest
     * @return the loaded {@link Chunk}
     */
    public static TableChunk load(@Nullable Segment segment, Path file,
            FileChannel channel, long position, long size, BloomFilter filter,
            Manifest manifest) {
        return new TableChunk(segment, file, channel, position, size, filter,
                manifest);
    }

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param filter
     */
    protected TableChunk(@Nullable Segment segment, BloomFilter filter) {
        super(segment, filter);
    }

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
    private TableChunk(@Nullable Segment segment, Path file,
            FileChannel channel, long position, long size, BloomFilter filter,
            Manifest manifest) {
        super(segment, file, channel, position, size, filter, manifest);
    }

    @Override
    public TableArtifact insert(Identifier locator, Text key, Value value,
            long version, Action type) throws IllegalStateException {
        return (TableArtifact) super.insert(locator, key, value, version, type);
    }

    @Override
    protected Artifact<Identifier, Text, Value> makeArtifact(
            Revision<Identifier, Text, Value> revision,
            Composite[] composites) {
        return new TableArtifact((TableRevision) revision, composites);
    }

    @Override
    protected TableRevision makeRevision(Identifier locator, Text key,
            Value value, long version, Action type) {
        return Revision.createTableRevision(locator, key, value, version, type);
    }

    @Override
    protected Class<TableRevision> xRevisionClass() {
        return TableRevision.class;
    }

}
