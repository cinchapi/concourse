/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import java.nio.file.Path;

import javax.annotation.Nullable;

import com.cinchapi.bucket.Bucket;
import com.cinchapi.bucket.PersistentBucket;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.Syncable;
import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper for {@link Block} stats and metadata.
 *
 * @author Jeff Nelson
 */
public class BlockStats implements Syncable {

    /**
     * Return a persistent {@link Bucket} that stores data in the {@code file}.
     * 
     * @param file
     * @return a persistent {@link Bucket}
     */
    private static Bucket getPersistentBucket(Path file) {
        return Bucket.persistent(file, "stats");
    }

    /**
     * The underlying {@link Bucket store} where the stats are maintained.
     * <p>
     * <strong>NOTE:</strong> We don't explicitly cleanup this resource in
     * this class's {@link #finalize()} method because the bucket takes care
     * of that internally.
     * </p>
     * <p>
     * The bucket is held in memory until the stats are {@link #sync() synced}
     * to disk and which point the bucket is swaped out for a persistent one.
     * </p>
     */
    private Bucket bucket;

    /**
     * The location where the stats are persisted.
     */
    private final Path file;

    /**
     * Construct a new instance.
     * 
     * @param file
     */
    /* package */ BlockStats(Path file) {
        this.file = file;
        if(FileSystem.getFileSize(file.toAbsolutePath().toString()) == 0) {
            bucket = Bucket.memory();
        }
        else {
            bucket = getPersistentBucket(file);
        }
    }

    /**
     * Return the value associated with the {@code attribute} for this
     * {@link Block}.
     * 
     * @param attribute
     * @return the value
     */
    @Nullable
    public <T> T get(Attribute attribute) {
        return get(attribute.name());
    }

    /**
     * Associate the {@code value} with the {@code attribute} for this
     * {@link Block}, overwriting any previously set values.
     * 
     * @param attribute
     * @param value
     */
    public <T> void put(Attribute attribute, T value) {
        put(attribute.name(), value);
    }

    @Override
    public void sync() {
        if(!(bucket instanceof PersistentBucket)) {
            Bucket backup = bucket;
            bucket = getPersistentBucket(file);
            bucket.merge(backup);
        }
    }

    /**
     * Return the value associated with the {@code attribute} for this
     * {@link Block}.
     * 
     * @param attribute
     * @return the value
     */
    @VisibleForTesting
    protected <T> T get(String key) {
        return bucket.get(key);
    }

    /**
     * Associate the {@code value} with the {@code attribute} for this
     * {@link Block}, overwriting any previously set values.
     * 
     * @param attribute
     * @param value
     */
    @VisibleForTesting
    protected <T> void put(String key, T value) {
        bucket.put(key, value);
    }

    /**
     * The keys that can be used to read/write {@link BlockStats stats}.
     *
     * @author Jeff Nelson
     */
    public enum Attribute {
        SCHEMA_VERSION;
    }

}
