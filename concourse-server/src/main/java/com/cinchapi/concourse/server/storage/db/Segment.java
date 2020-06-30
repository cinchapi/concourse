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
package com.cinchapi.concourse.server.storage.db;

import java.nio.file.Path;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.Syncable;
import com.cinchapi.concourse.server.storage.db.BlockStats.Attribute;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * A {@link Segment} is an immutable and atomic group of {@link Block Blocks}
 * that all contain the same {@link Revision revisions} in different formats.
 * <p>
 * All data is physically stored in various {@link Block Blocks}, but a
 * {@link Segment} is used to group {@link Block Blocks} that contain the same
 * logical data groups for easier operations within the {@link Database}.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public class Segment implements Comparable<Segment>, Syncable {

    /**
     * Create a new {@link Segment} with {@code id} in {@code directory}.
     * 
     * @param id
     * @param directory
     * @return the new {@link Segment}
     */
    public static Segment create(String id, Path directory) {
        try {
            return new Segment(id, directory, false);
        }
        catch (MalformedBlockException | SegmentLoadingException e) {
            throw new IllegalStateException(
                    "Exception occurred trying to create blocks within a new segment");
        }
    }

    /**
     * Load an existing {@link Segment} with {@code id} from {@code directory}.
     * 
     * @param id
     * @param directory
     * @return the existing {@link Segment}
     */
    public static Segment load(String id, Path directory)
            throws MalformedBlockException, SegmentLoadingException,
            IllegalStateException {
        return new Segment(id, directory, true);
    }

    /*
     * BLOCK DIRECTORIES
     * -----------------
     * Each Block type is stored in its own directory so that we can reduce the
     * number of files in a single directory. It is important to note that the
     * filename extensions for files are the same across directories (i.e. 'blk'
     * for block, 'fltr' for bloom filter and 'indx' for index). Furthermore,
     * blocks that are synced at the same time all have the same block id.
     * Therefore, the only way to distinguish blocks of different types from one
     * another is by the directory in which they are stored.
     */
    // @formatter:off
    static final String PRIMARY_BLOCK_DIRECTORY = "cpb";

    static final String SEARCH_BLOCK_DIRECTORY = "ctb";

    static final String SECONDARY_BLOCK_DIRECTORY = "csb";
    // @formatter:on

    /**
     * The unique id for this {@link Segment}.
     */
    private final String id;

    /**
     * The {@link PrimaryBlock} that contains the logical view of the data.
     */
    private final PrimaryBlock primary;

    /**
     * The {@link SearchBlock} that contains a searchable view of the data.
     */
    @Nullable
    private final SearchBlock search;

    /**
     * The {@link SecondaryBlock} that contains an inverted view of the data.
     */
    private final SecondaryBlock secondary;

    /**
     * Construct a new instance.
     * 
     * @param id
     * @param directory the parent directory for database files; each block type
     *            will store files in a different sub directory under this
     * @param load
     */
    private Segment(String id, Path directory, boolean load)
            throws MalformedBlockException, SegmentLoadingException,
            IllegalStateException {
        this.id = id;
        String primaryDirectory = directory.resolve(PRIMARY_BLOCK_DIRECTORY)
                .toString();
        String secondaryDirectory = directory.resolve(SECONDARY_BLOCK_DIRECTORY)
                .toString();
        String searchDirectory = directory.resolve(SEARCH_BLOCK_DIRECTORY)
                .toString();
        for (String dir : Array.containing(primaryDirectory, secondaryDirectory,
                searchDirectory)) {
            FileSystem.mkdirs(dir);
        }
        if(load) {
            // Load PrimaryBlock
            try {
                this.primary = new PrimaryBlock(id, primaryDirectory, true);
            }
            catch (MalformedBlockException e) {
                throw e;
            }
            catch (Exception e) {
                throw new SegmentLoadingException(PrimaryBlock.class, e);
            }

            // Load SecondaryBlock
            try {
                this.secondary = new SecondaryBlock(id, secondaryDirectory,
                        true);
            }
            catch (MalformedBlockException e) {
                throw e;
            }
            catch (Exception e) {
                throw new SegmentLoadingException(SecondaryBlock.class, e);
            }

            // Load SearchBlock
            if(FileSystem.hasFile(directory.resolve(SEARCH_BLOCK_DIRECTORY)
                    .resolve(id + ".blk"))) {
                try {
                    this.search = new SearchBlock(id, searchDirectory, true);
                }
                catch (MalformedBlockException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new SegmentLoadingException(SearchBlock.class, e);
                }
            }
            else {
                this.search = null;
            }
            if(primary.size() == 0 || secondary.size() == 0) {
                // CON-83: Get rid of any Segments that aren't "balanced"
                // (e.g. has both non-empty primary and secondary blocks) under
                // the assumption that the server crashed and the corresponding
                // Buffer page still exists. Please note that since we do not
                // sync empty blocks, it is possible that there are some primary
                // and secondary blocks without a corresponding search one. But,
                // it is also possible that a legitimate search block is missing
                // because the server crashed before it was synced, in which
                // case the data that was in that block is lost because we can't
                // both legitimately avoid syncing empty (search) blocks and
                // rely on the fact that a search block is missing to assume
                // that the server crashed. :-/
                throw new IllegalStateException(AnyStrings
                        .format("Segment {} is unbalanced because it does not contain both a "
                                + "Primary and Secondary Block", id));
            }
            for (Block<?, ?, ?> block : Array.containing(primary, secondary,
                    search)) {
                if(block != null) {
                    Logger.info("Loaded {} metadata for {}",
                            block.getClass().getSimpleName(), id);
                }
            }

            // Validate that all the Blocks in this segment have the same
            // attributes
            for (Attribute attribute : Attribute.values()) {
                Verify.that(
                        primary.stats().get(attribute)
                                .equals(secondary.stats().get(attribute)),
                        "Inconsistent attribute {} between primary and secondary blocks in Segment {}",
                        attribute.name(), id);
                // TODO: how to handle search stats not being collected in 0.9.Z
                // if(search != null) {
                // Verify.that(
                // primary.stats().get(attribute)
                // .equals(search.stats().get(attribute)),
                // "Inconsistent attribute {} between primary and search blocks
                // in Segment {}",
                // attribute.name(), id);
                // }
            }

        }
        else {
            this.primary = PrimaryBlock.createPrimaryBlock(id,
                    primaryDirectory);
            this.secondary = SecondaryBlock.createSecondaryBlock(id,
                    secondaryDirectory);
            this.search = SearchBlock.createSearchBlock(id, searchDirectory);
        }
    }

    /**
     * Return the checksum of this {@link Segment}, which is the concatenation
     * of the {@link Block#checksum() checksum} for each of its {@link Block
     * Blocks}.
     * <p>
     * NOTE: The checksum for a {@link Segment} can only be returned if its
     * {@link Block Blocks} are {@code immutable}.
     * </p>
     * 
     * @return the Segment checksum
     */
    public String checksum() {
        return primary.checksum() + secondary.checksum();
    }

    @Override
    public int compareTo(Segment other) {
        if(primary.mutable && !other.primary.mutable) {
            return 1;
        }
        else if(other.primary.mutable && !primary.mutable) {
            return -1;
        }
        else if(primary.stats().<Long> getOrDefault(
                Attribute.MAX_REVISION_VERSION,
                Time.now()) < other.primary.stats().<Long> getOrDefault(
                        Attribute.MIN_REVISION_VERSION, Time.now())) {
            return -1;
        }
        else if(primary.stats().<Long> getOrDefault(
                Attribute.MIN_REVISION_VERSION,
                Time.now()) > other.primary.stats().<Long> getOrDefault(
                        Attribute.MAX_REVISION_VERSION, Time.now())) {
            return 1;
        }
        else if(id().equals(other.id())) {
            return 0;
        }
        else {
            throw new IllegalStateException(
                    "Cannot compare two Segments with overlapping data revision ranges");
        }
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof Segment) {
            return primary.equals(((Segment) other).primary())
                    && secondary.equals(((Segment) other).secondary());
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(primary, secondary);
    }

    /**
     * Return the {@code id} of this {@link Segment}.
     * 
     * @return the segment id
     */
    public String id() {
        return id;
    }

    /**
     * Return this {@link Segment Segment's} {@link PrimaryBlock}.
     * 
     * @return the {@link PrimaryBlock}
     */
    public PrimaryBlock primary() {
        return primary;
    }

    /**
     * Return this {@link Segment Segment's} {@link SearchBlock}, if it exists.
     * 
     * @return the {@link SearchhBlock} or {@code null} it it does not exist
     */
    public SearchBlock search() {
        return search;
    }

    /**
     * Return this {@link Segment Segment's} {@link SecondaryBlock}.
     * 
     * @return the {@link SecondaryBlock}
     */
    public SecondaryBlock secondary() {
        return secondary;
    }

    @Override
    public void sync() {
        try {
            sync(new AwaitableExecutorService(
                    MoreExecutors.newDirectExecutorService()));
        }
        catch (InterruptedException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Asynchronously {@link Block#sync() sync} each of this {@link Segment
     * Segment's}
     * {@link Block Blocks} using the {@code executor}.
     * 
     * @param executor
     * @throws InterruptedException
     */
    public void sync(AwaitableExecutorService executor)
            throws InterruptedException {
        // TODO we need a transactional file system to ensure that these
        // blocks are written atomically (all or nothing)
        ArrayBuilder<Runnable> _tasks = ArrayBuilder.builder();
        for (Block<?, ?, ?> block : Array.containing(primary, secondary,
                search)) {
            if(block != null) {
                _tasks.add(() -> {
                    block.sync();
                    Logger.debug("Completed sync of {}", block);

                });
            }
        }
        Runnable[] tasks = _tasks.build();
        executor.await((task, error) -> Logger.error(
                "The database is unable to sync all the Blocks with id {} because {}.",
                id, error.getMessage(), error), tasks);
    }

    /**
     * Transfer the {@code write} to this {@link Segment}.
     * <p>
     * This method is only suitable for testing.
     * </p>
     * 
     * @param write
     * @return a {@link Receipt} that contains the {@link Revision Revisions}
     *         that were created as a consequence of the transfer
     */
    public Receipt transfer(Write write) {
        try {
            return transfer(write, new AwaitableExecutorService(
                    MoreExecutors.newDirectExecutorService()));
        }
        catch (InterruptedException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Transfer the {@code write} to this {@link Segment} using the
     * {@code executor} to asynchronously write to all the contained
     * {@link Block blocks}.
     * 
     * @param write
     * @param executor
     * @return a {@link Receipt} that contains the {@link Revision Revisions}
     *         that were created as a consequence of the transfer
     * @throws InterruptedException
     */
    public Receipt transfer(Write write, AwaitableExecutorService executor)
            throws InterruptedException {
        Receipt.Builder receipt = Receipt.builder();
        Runnable[] tasks = Array.containing(() -> {
            PrimaryRevision revision = primary.insert(write.getRecord(),
                    write.getKey(), write.getValue(), write.getVersion(),
                    write.getType());
            receipt.itemize(revision);
        }, () -> {
            SecondaryRevision revision = secondary.insert(write.getKey(),
                    write.getValue(), write.getRecord(), write.getVersion(),
                    write.getType());
            receipt.itemize(revision);
        }, () -> {
            search.insert(write.getKey(), write.getValue(), write.getRecord(),
                    write.getVersion(), write.getType());
            // NOTE: We do not place a SearchRevision within the Entry because
            // the database does not cache SearchRecords since they have the
            // potential to be VERY large. Holding references to them in a
            // database's cache would prevent them from being garbage collected
            // resulting in more OOMs.
        });
        executor.await((task, error) -> Logger.error(
                "Unexpected error when trying to transfer the following Write to the Database: {}",
                write, error), tasks);
        return receipt.build();
    }

    /**
     * A {@link Receipt} is acknowledges the successful
     * {@link Segment#transfer(Write, AwaitableExecutorService) transfer} of a
     * {@link Write} to a {@link Segment} and includes the {@link Revision
     * revisions} that were created in the Segment's storage {@link Block
     * Blocks}.
     *
     * @author Jeff Nelson
     */
    @Immutable
    public static class Receipt {

        /**
         * Return an incremental {@link Receipt} {@link Builder}.
         * 
         * @return the {@link Builder}
         */
        static Builder builder() {
            return new Builder();
        }

        private final PrimaryRevision primaryRevision;
        private final SecondaryRevision secondaryRevision;

        /**
         * Construct a new instance.
         * 
         * @param primaryRevision
         * @param secondaryRevision
         */
        Receipt(PrimaryRevision primaryRevision,
                SecondaryRevision secondaryRevision) {
            this.primaryRevision = primaryRevision;
            this.secondaryRevision = secondaryRevision;
        }

        /**
         * Return the {@link PrimaryRevision} included with this
         * {@link Receipt}.
         * 
         * @return the {@link PrimaryRevision}
         */
        public PrimaryRevision primaryRevision() {
            return primaryRevision;
        }

        /**
         * Return the {@link SecondaryRevision} included with this
         * {@link Receipt}.
         * 
         * @return the {@link SecondaryRevision}
         */
        public SecondaryRevision secondaryRevision() {
            return secondaryRevision;
        }

        /**
         * {@link Receipt} builder.
         */
        static class Builder {

            PrimaryRevision primary;
            SecondaryRevision secondary;

            /**
             * Build and return the {@link Receipt}.
             * 
             * @return the built {@link Receipt}
             */
            Receipt build() {
                return new Receipt(primary, secondary);
            }

            /**
             * Add the {@code revision} to the {@link Receipt}.
             * 
             * @param revision
             * @return this
             */
            Builder itemize(PrimaryRevision revision) {
                primary = revision;
                return this;
            }

            /**
             * Add the {@code revision} to the {@link Receipt}.
             * 
             * @param revision
             * @return this
             */
            Builder itemize(SecondaryRevision revision) {
                secondary = revision;
                return this;
            }
        }

    }

}
