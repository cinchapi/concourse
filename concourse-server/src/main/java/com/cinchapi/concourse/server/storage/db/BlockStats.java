/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import java.nio.file.Files;
import java.nio.file.Path;

import javax.annotation.Nullable;

import com.cinchapi.bucket.Bucket;
import com.cinchapi.bucket.PersistentBucket;
import com.cinchapi.common.base.validate.BiCheck;
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
        if(!Files.exists(file) || FileSystem
                .getFileSize(file.toAbsolutePath().toString()) == 0) {
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

    /**
     * Atomically associate {@code value} with {@code attribute} if the
     * {@code currentValueCondition} passes.
     * 
     * @param attribute
     * @param value
     * @param currentValueCondition
     * @return {@code true} if the association from {@code attribute} to
     *         {@code value} is added
     */
    public <T1, T2> boolean putIf(Attribute attribute, T2 value,
            BiCheck<T1, T2> currentValueCondition) {
        return putIf(attribute.name(), value, currentValueCondition);
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
     * Atomically associate {@code value} with {@code attribute} if the
     * {@code currentValueCondition} passes.
     * 
     * @param attribute
     * @param value
     * @param currentValueCondition
     * @return {@code true} if the association from {@code attribute} to
     *         {@code value} is added
     */
    @VisibleForTesting
    protected <T1, T2> boolean putIf(String key, T2 value,
            BiCheck<T1, T2> currentValueCondition) {
        return bucket.putIf(key, value, currentValueCondition);
    }

    /**
     * The keys that can be used to read/write {@link BlockStats stats}.
     *
     * @author Jeff Nelson
     */
    public enum Attribute {

        /**
         * The data storage schema version used in the Block. This attribute is
         * used to determine whether a in-memory Block code version is able to
         * handle contents from a Block file on dick.
         */
        SCHEMA_VERSION,

        /**
         * The smallest revision version contained in the corresponding Block.
         * This attribute is nullable if no data has been inserted. But, if the
         * Block has at least one revision, this attribute is guaranteed to have
         * a value.
         */
        MIN_REVISION_VERSION,

        /**
         * The largest revision version contained in the corresponding Block.
         * This attribute is nullable if no data has been inserted. But, if the
         * Block has at least one revision, this attribute is guaranteed to have
         * a value.
         */
        MAX_REVISION_VERSION;
    }

}
