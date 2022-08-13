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
package com.cinchapi.concourse.server.storage.format;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.collect.CloseableIterator;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.io.Checksums;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.NaturalSorter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Limited ability to handle Storage Format Version 2 data files.
 *
 * @author Jeff Nelson
 */
public final class StorageFormatV2 {

    /**
     * Load the {@link Block Blocks} from {@code directory}.
     * 
     * @param directory
     * @return an {@link Iterable} containing all the loaded {@link Block
     *         Blocks}
     */
    public static <L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> Iterable<Block<L, K, V>> load(
            Path directory, Class<? extends Revision<L, K, V>> revisionClass) {
        List<Block<L, K, V>> blocks = Lists.newArrayList();
        SortedMap<File, Block<L, K, V>> sorted = Maps
                .newTreeMap(NaturalSorter.INSTANCE);
        Stream<Path> files = FileSystem.ls(directory)
                .filter(file -> file.toString()
                        .endsWith(Block.BLOCK_NAME_EXTENSION))
                .map(Path::toFile).filter(file -> file.length() > 0)
                .map(File::getAbsolutePath).map(Paths::get);
        files.forEach(file -> {
            String id = Block.getId(file.toString());
            try {
                Block<L, K, V> block = new Block<>(file, revisionClass);
                sorted.put(file.toFile(), block);
                Logger.info("Loaded v2 data file {}", file);
            }
            catch (MalformedBlockException e) {
                Logger.warn(
                        "{}. As a result the Block was NOT loaded. A malformed block is usually an indication that the Block was only partially synced to disk before Concourse Server shutdown. In this case, it is safe to delete any Block files that were written for id {}",
                        e.getMessage(), id);
            }
            catch (Exception e) {
                Logger.error("An error occured while loading v2 data file {}",
                        file);
                Logger.error("", e);
            }
        });
        files.close();
        blocks.addAll(sorted.values());

        // Remove duplicate Blocks. Block duplication can occur when the server
        // crashes and a Block group is only partially synced. When the server
        // restarts, it will try to sync the Block group again, generating
        // duplicate Blocks on disk for the Blocks that succeeded in syncing
        // before the crash
        Set<String> checksums = Sets.newHashSetWithExpectedSize(blocks.size());
        Iterator<? extends Block<?, ?, ?>> it = blocks.iterator();
        while (it.hasNext()) {
            Block<?, ?, ?> block = it.next();
            if(!checksums.add(block.checksum())) {
                it.remove();
                Logger.warn(
                        "v2 data file {} contains duplicate data, so it was not loaded. You can safely delete this file.",
                        block.getId());
            }
        }
        return blocks;
    }

    /**
     * Viewer for the Storage Format v2 Block format.
     * <p>
     * This class can be used to stream the {@link Revision Revisions} from a
     * {@link Block} file from disk and return them in an {@link #iterator()}.
     * </p>
     *
     * @author Jeff Nelson
     */
    public static class Block<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
            implements
            Iterable<Revision<L, K, V>> {

        /**
         * Return the block id from the name of the block file.
         * 
         * @param filename
         * @return the block id
         */
        public static String getId(String filename) {
            return FileSystem.getSimpleName(filename);
        }

        /**
         * The extension for the block file.
         */
        private static final String BLOCK_NAME_EXTENSION = ".blk";

        /**
         * The extension for the {@link BloomFilter} file.
         */
        private static final String FILTER_NAME_EXTENSION = ".fltr";

        /**
         * The extension for the {@link BlockIndex} file.
         */
        private static final String INDEX_NAME_EXTENSION = ".indx";

        /**
         * The extension for the {@link Blockstats} file
         */
        private static final String STATS_NAME_EXTENSION = ".stts";

        /**
         * The data file.
         */
        private final Path file;

        /**
         * The class that represent's the Block's revisions, in memory.
         */
        private final Class<? extends Revision<L, K, V>> revisionClass;

        /**
         * Construct a new instance.
         * 
         * @param file
         * @param revisionClass
         */
        public Block(Path file,
                Class<? extends Revision<L, K, V>> revisionClass) {
            this.file = file;
            this.revisionClass = revisionClass;
            Path directory = file.getParent();
            String id = getId(file.toString());
            String[] missing = ImmutableList
                    .of(directory.resolve(id + STATS_NAME_EXTENSION),
                            directory.resolve(id + FILTER_NAME_EXTENSION),
                            directory.resolve(id + INDEX_NAME_EXTENSION))
                    .stream().filter(path -> !path.toFile().exists())
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList()).toArray(Array.containing());
            if(missing.length > 0) {
                throw new MalformedBlockException(id, directory.toString(),
                        missing);
            }
        }

        @Override
        public Iterator<Revision<L, K, V>> iterator() {
            return new Iterator<Revision<L, K, V>>() {

                private final CloseableIterator<ByteBuffer> it = ByteableCollections
                        .stream(file, GlobalState.DISK_READ_BUFFER_SIZE);

                @Override
                public boolean hasNext() {
                    if(it.hasNext()) {
                        return true;
                    }
                    else {
                        it.closeQuietly();
                        return false;
                    }
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

        /**
         * Return a checksum of the {@link Block Block's} content.
         * 
         * @return a checksum of the Block's content
         * @throws IllegalStateException if the Block is mutable
         */
        public String checksum() {
            return Checksums.generate(file);
        }

        /**
         * Return the block id.
         * 
         * @return the id
         */
        public String getId() {
            return getId(file.toString());
        }
    }

    /**
     * A {@link MalformedBlockException} is thrown when a Block does not contain
     * all of the components (e.g. stats, index, filter, etc) necessary to
     * function properly.
     * <p>
     * A "malformed" block is usually an indicator that the Database exited in
     * the middle of a syncing operation. Malformed blocks can usually be safely
     * discarded.
     * </p>
     *
     * @author Jeff Nelson
     */
    public static class MalformedBlockException extends RuntimeException {

        private static final long serialVersionUID = -1721757690680045080L;

        /**
         * Construct a new instance.
         * 
         * @param id
         * @param directory
         * @param missing
         */
        public MalformedBlockException(String id, String directory,
                String... missing) {
            super(AnyStrings.format("Block {} in {} is missing {}", id,
                    directory, String.join(", ", missing)));
        }

    }

}
