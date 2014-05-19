/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.db;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.concurrent.ConcourseExecutors;
import org.cinchapi.concourse.server.io.Composite;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.jmx.ManagedOperation;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Functions;
import org.cinchapi.concourse.server.storage.PermanentStore;
import org.cinchapi.concourse.server.storage.VersionGetter;
import org.cinchapi.concourse.server.storage.temp.Buffer;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Logger;
import org.cinchapi.concourse.util.NaturalSorter;
import org.cinchapi.concourse.util.TStrings;
import org.cinchapi.concourse.util.Transformers;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static org.cinchapi.concourse.server.GlobalState.*;

/**
 * The {@code Database} is the {@link PermanentStore} for data. The
 * Database accepts {@link Write} objects that are initially stored in a
 * {@link Buffer} and converts them to {@link Revision} objects that are stored
 * in various {@link Block} objects, which provide indexed views for optimized
 * reads.
 * 
 * @author jnelson
 */
@ThreadSafe
public final class Database implements PermanentStore, VersionGetter {

    /**
     * Return a cache for records of type {@code T}.
     * 
     * @return the cache
     */
    private static <T> Cache<Composite, T> buildCache() {
        return CacheBuilder.newBuilder().maximumSize(100000).build();
    }

    /**
     * Return the Block identified by {@code id} if it exists in {@code list},
     * otherwise {@code null}.
     * 
     * @param list
     * @param id
     * @return the Block identified by {@code id} or {@code null}
     */
    @Nullable
    private static <T extends Block<?, ?, ?>> T findBlock(List<T> list,
            String id) {
        // TODO: use binary search, since the ids of the list are sorted...this
        // may require making Blocks comparable by id
        for (T block : list) {
            if(block.getId().equals(id)) {
                return block;
            }
        }
        return null;
    }

    private static final String threadNamePrefix = "database-write-thread";

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
    private static final String PRIMARY_BLOCK_DIRECTORY = "cpb";
    private static final String SECONDARY_BLOCK_DIRECTORY = "csb";
    private static final String SEARCH_BLOCK_DIRECTORY = "ctb";

    /*
     * RECORD CACHES
     * -------------
     * Records are cached in memory to reduce the number of seeks required. When
     * writing new revisions, we check the appropriate caches for relevant
     * records and append the new revision so that the cached data doesn't grow
     * stale.
     */
    private final Cache<Composite, PrimaryRecord> cpc = buildCache();
    private final Cache<Composite, PrimaryRecord> cppc = buildCache();
    private final Cache<Composite, SecondaryRecord> csc = buildCache();

    /*
     * CURRENT BLOCK POINTERS
     * ----------------------
     * We hold direct references to the current blocks. These pointers change
     * whenever the database triggers a sync operation.
     */
    private transient PrimaryBlock cpb0;
    private transient SecondaryBlock csb0;
    private transient SearchBlock ctb0;

    /*
     * BLOCK COLLECTIONS
     * -----------------
     * We maintain a collection to all the blocks, in chronological order, so
     * that we can seek for the necessary revisions to populate a requested
     * record.
     */
    private final transient List<PrimaryBlock> cpb = Lists.newArrayList();
    private final transient List<SecondaryBlock> csb = Lists.newArrayList();
    private final transient List<SearchBlock> ctb = Lists.newArrayList();

    /**
     * Lock used to ensure the object is ThreadSafe. This lock provides access
     * to a masterLock.readLock()() and masterLock.writeLock()().
     */
    private final transient ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    /**
     * The location where the Database stores data.
     */
    private final transient String backingStore;

    /**
     * A flag to indicate if the Buffer is running or not.
     */
    private transient boolean running = false;

    /**
     * Construct a Database that is backed by the default location which is in
     * {@link GlobalState#DATABASE_DIRECTORY}.
     * 
     */
    public Database() {
        this(DATABASE_DIRECTORY);
    }

    /**
     * Construct a Database that is backed by {@link backingStore} directory.
     * The {@link backingStore} is passed to each {@link Record} as the
     * {@code parentStore}.
     * 
     * @param backingStore
     */
    public Database(String backingStore) {
        this.backingStore = backingStore;
    }

    @Override
    public void accept(Write write) {
        // NOTE: Write locking happens in each individual Block, and furthermore
        // this method is only called from the Buffer, which transports data
        // serially.
        ConcourseExecutors.executeAndAwaitTermination(threadNamePrefix,
                new BlockWriter(cpb0, write), new BlockWriter(csb0, write),
                new BlockWriter(ctb0, write));
    }

    @Override
    public Map<Long, String> audit(long record) {
        return getPrimaryRecord(PrimaryKey.wrap(record)).audit();
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        Text key0 = Text.wrap(key);
        return getPrimaryRecord(PrimaryKey.wrap(record), key0).audit(key0);
    }

    @Override
    public Set<String> describe(long record) {
        return Transformers.transformSet(
                getPrimaryRecord(PrimaryKey.wrap(record)).describe(),
                Functions.TEXT_TO_STRING);
    }

    @Override
    public Set<String> describe(long record, long timestamp) {
        return Transformers.transformSet(
                getPrimaryRecord(PrimaryKey.wrap(record)).describe(timestamp),
                Functions.TEXT_TO_STRING);
    }

    /**
     * Return dumps for all the blocks identified by {@code id}. This method IS
     * NOT necessarily optimized for performance, so it should be used with
     * caution. Its only really necessary to use this method for debugging.
     * 
     * @param id
     * @return the block dumps.
     */
    public String dump(String id) {
        PrimaryBlock _cpb = findBlock(cpb, id);
        SecondaryBlock _csb = findBlock(csb, id);
        SearchBlock _ctb = findBlock(ctb, id);
        Preconditions.checkArgument(_cpb != null && _csb != null,
                "Insufficient number of blocks identified by %s", id);
        StringBuilder sb = new StringBuilder();
        sb.append(_cpb.dump());
        sb.append(_csb.dump());
        if(_ctb != null) {
            sb.append(_ctb.dump());
        }
        return sb.toString();
    }

    @Override
    public Set<TObject> fetch(String key, long record) {
        Text key0 = Text.wrap(key);
        return Transformers.transformSet(
                getPrimaryRecord(PrimaryKey.wrap(record), key0).fetch(key0),
                Functions.VALUE_TO_TOBJECT);
    }

    @Override
    public Set<TObject> fetch(String key, long record, long timestamp) {
        Text key0 = Text.wrap(key);
        return Transformers.transformSet(
                getPrimaryRecord(PrimaryKey.wrap(record), key0).fetch(key0,
                        timestamp), Functions.VALUE_TO_TOBJECT);
    }

    @Override
    public Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values) {
        return Transformers.transformSet(
                getSecondaryRecord(Text.wrap(key)).find(
                        timestamp,
                        operator,
                        Transformers.transformArray(values,
                                Functions.TOBJECT_TO_VALUE, Value.class)),
                Functions.PRIMARY_KEY_TO_LONG);
    }

    @Override
    public Set<Long> find(String key, Operator operator, TObject... values) {
        return Transformers.transformSet(
                getSecondaryRecord(Text.wrap(key)).find(
                        operator,
                        Transformers.transformArray(values,
                                Functions.TOBJECT_TO_VALUE, Value.class)),
                Functions.PRIMARY_KEY_TO_LONG);
    }

    /**
     * Return a the list of ids for all the blocks that are currently in scope.
     * 
     * @return the block dump list
     */
    @ManagedOperation
    public List<String> getDumpList() {
        List<String> ids = Lists.newArrayList();
        for (PrimaryBlock block : cpb) {
            ids.add(block.getId());
        }
        return ids;
    }

    @Override
    public long getVersion(long record) {
        return getPrimaryRecord(PrimaryKey.wrap(record)).getVersion();
    }

    @Override
    public long getVersion(String key) {
        // NOTE: We must consult the SecondaryRecord over the SearchRecord
        // because ALL writes for a key are secondary indexed whereas only text
        // writes are search indexed.
        return getSecondaryRecord(Text.wrap(key)).getVersion();
    }

    @Override
    public long getVersion(String key, long record) {
        return getPrimaryRecord(PrimaryKey.wrap(record), Text.wrap(key))
                .getVersion();
    }

    @Override
    public Set<Long> search(String key, String query) {
        return Transformers.transformSet(
                getSearchRecord(Text.wrap(key), Text.wrap(query)).search(
                        Text.wrap(query)), Functions.PRIMARY_KEY_TO_LONG);
    }

    @Override
    public void start() {
        if(!running) {
            running = true;
            Logger.info("Database configured to store data in {}", backingStore);
            ConcourseExecutors.executeAndAwaitTermination("Database",
                    new BlockLoader<PrimaryBlock>(PrimaryBlock.class,
                            PRIMARY_BLOCK_DIRECTORY, cpb),
                    new BlockLoader<SecondaryBlock>(SecondaryBlock.class,
                            SECONDARY_BLOCK_DIRECTORY, csb),
                    new BlockLoader<SearchBlock>(SearchBlock.class,
                            SEARCH_BLOCK_DIRECTORY, ctb));
            triggerSync(false);
        }
    }

    @Override
    public void stop() {
        if(running) {
            running = false;
        }
    }

    /**
     * Create new blocks and sync the current blocks to disk.
     */
    @GuardedBy("triggerSync(boolean)")
    public void triggerSync() {
        triggerSync(true);
    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        Text key0 = Text.wrap(key);
        return getPrimaryRecord(PrimaryKey.wrap(record), key0).verify(key0,
                Value.wrap(value));
    }

    @Override
    public boolean verify(String key, TObject value, long record, long timestamp) {
        Text key0 = Text.wrap(key);
        return getPrimaryRecord(PrimaryKey.wrap(record), key0).verify(key0,
                Value.wrap(value), timestamp);
    }

    /**
     * Return the PrimaryRecord identifier by {@code primaryKey}.
     * 
     * @param pkey
     * @return the PrimaryRecord
     */
    private PrimaryRecord getPrimaryRecord(PrimaryKey pkey) {
        masterLock.readLock().lock();
        try {
            PrimaryRecord record = cpc.getIfPresent(Composite.create(pkey));
            if(record == null) {
                record = Record.createPrimaryRecord(pkey);
                for (PrimaryBlock block : cpb) {
                    block.seek(pkey, record);
                }
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the partial PrimaryRecord identifier by {@code key} in
     * {@code primaryKey}
     * 
     * @param pkey
     * @param key
     * @return the PrimaryRecord
     */
    private PrimaryRecord getPrimaryRecord(PrimaryKey pkey, Text key) {
        masterLock.readLock().lock();
        try {
            PrimaryRecord record = cppc.getIfPresent(Composite
                    .create(pkey, key));
            if(record == null) {
                record = Record.createPrimaryRecordPartial(pkey, key);
                for (PrimaryBlock block : cpb) {
                    block.seek(pkey, key, record);
                }
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the SearchRecord identified by {@code key}.
     * 
     * @param key
     * @param query
     * @return the SearchRecord
     */
    private SearchRecord getSearchRecord(Text key, Text query) {
        // NOTE: We do not cache SearchRecords because they have the potential
        // to be VERY large. Holding references to them in a cache would prevent
        // them from being garbage collected resulting in more OOMs.
        masterLock.readLock().lock();
        try {
            SearchRecord record = Record.createSearchRecordPartial(key, query);
            for (SearchBlock block : ctb) {
                // Seek each word in the query to make sure that multi word
                // search works.
                String[] toks = query
                        .toString()
                        .toLowerCase()
                        .split(TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
                for (String tok : toks) {
                    block.seek(key, Text.wrap(tok), record);
                }
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the SecondaryRecord identified by {@code key}.
     * 
     * @param key
     * @return the SecondaryRecord
     */
    private SecondaryRecord getSecondaryRecord(Text key) {
        masterLock.readLock().lock();
        try {
            SecondaryRecord record = csc.getIfPresent(Composite.create(key));
            if(record == null) {
                record = Record.createSecondaryRecord(key);
                for (SecondaryBlock block : csb) {
                    block.seek(key, record);
                }
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Create new mutable blocks and sync the current blocks to disk if
     * {@code doSync} is {@code true}.
     * 
     * @param doSync
     */
    private void triggerSync(boolean doSync) {
        masterLock.writeLock().lock();
        try {
            if(doSync) {
                // TODO we need a transactional file system to ensure that these
                // blocks are written atomically (all or nothing)
                ConcourseExecutors.executeAndAwaitTermination(threadNamePrefix,
                        new BlockSyncer(cpb0), new BlockSyncer(csb0),
                        new BlockSyncer(ctb0));
            }
            String id = Long.toString(Time.now());
            cpb.add((cpb0 = Block.createPrimaryBlock(id, backingStore
                    + File.separator + PRIMARY_BLOCK_DIRECTORY)));
            csb.add((csb0 = Block.createSecondaryBlock(id, backingStore
                    + File.separator + SECONDARY_BLOCK_DIRECTORY)));
            ctb.add((ctb0 = Block.createSearchBlock(id, backingStore
                    + File.separator + SEARCH_BLOCK_DIRECTORY)));
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    /**
     * A runnable that traverses the appropriate directory for a block type
     * under {@link #backingStore} and loads the block metadata into memory.
     * 
     * @author jnelson
     * @param <T> - the Block type
     */
    private final class BlockLoader<T extends Block<?, ?, ?>> implements
            Runnable {

        private final Class<T> clazz;
        private final String directory;
        private final List<T> blocks;

        /**
         * Construct a new instance.
         * 
         * @param clazz
         * @param directory
         * @param blocks
         */
        public BlockLoader(Class<T> clazz, String directory, List<T> blocks) {
            this.clazz = clazz;
            this.directory = directory;
            this.blocks = blocks;
        }

        @Override
        public void run() {
            File _file = null;
            try {
                final String path = backingStore + File.separator + directory;
                FileSystem.mkdirs(path);
                SortedMap<File, T> blockSorter = Maps
                        .newTreeMap(NaturalSorter.INSTANCE);
                for (File file : new File(path).listFiles(new FilenameFilter() {

                    @Override
                    public boolean accept(File dir, String name) {
                        return dir.getAbsolutePath().equals(path)
                                && name.endsWith(Block.BLOCK_NAME_EXTENSION);
                    }

                })) {
                    _file = file;
                    String id = Block.getId(file.getName());
                    Constructor<T> constructor = clazz.getDeclaredConstructor(
                            String.class, String.class, Boolean.TYPE);
                    constructor.setAccessible(true);
                    blockSorter.put(file,
                            constructor.newInstance(id, path.toString(), true));
                    Logger.info("Loaded {} metadata for {}",
                            clazz.getSimpleName(), file.getName());
                }
                blocks.addAll(blockSorter.values());
            }
            catch (ReflectiveOperationException e) {
                Logger.error(
                        "An error occured while loading {} metadata for {}",
                        clazz.getSimpleName(), _file.getName());
                Logger.error("", e);
            }

        }

    }

    /**
     * A runnable that will sync a block to disk.
     * 
     * @author jnelson
     */
    private final class BlockSyncer implements Runnable {

        private final Block<?, ?, ?> block;

        /**
         * Construct a new instance.
         * 
         * @param block
         */
        public BlockSyncer(Block<?, ?, ?> block) {
            this.block = block;
        }

        @Override
        public void run() {
            block.sync();
            Logger.debug("Completed sync of {}", block);
        }

    }

    /**
     * A runnable that will insert a Writer into a block.
     * 
     * @author jnelson
     */
    private final class BlockWriter implements Runnable {

        private final Block<?, ?, ?> block;
        private final Write write;

        /**
         * Construct a new instance.
         * 
         * @param block
         * @param write
         */
        public BlockWriter(Block<?, ?, ?> block, Write write) {
            this.block = block;
            this.write = write;
        }

        @Override
        public void run() {
            Logger.debug("Writing {} to {}", write, block);
            if(block instanceof PrimaryBlock) {
                PrimaryRevision revision = (PrimaryRevision) ((PrimaryBlock) block)
                        .insert(write.getRecord(), write.getKey(),
                                write.getValue(), write.getVersion(),
                                write.getType());
                PrimaryRecord record = cpc.getIfPresent(Composite.create(write
                        .getRecord()));
                PrimaryRecord partialRecord = cppc.getIfPresent(Composite
                        .create(write.getRecord(), write.getKey()));
                if(record != null) {
                    record.append(revision);
                }
                if(partialRecord != null) {
                    partialRecord.append(revision);
                }
            }
            else if(block instanceof SecondaryBlock) {
                SecondaryRevision revision = (SecondaryRevision) ((SecondaryBlock) block)
                        .insert(write.getKey(), write.getValue(),
                                write.getRecord(), write.getVersion(),
                                write.getType());
                SecondaryRecord record = csc.getIfPresent(Composite
                        .create(write.getKey()));
                if(record != null) {
                    record.append(revision);
                }
            }
            else if(block instanceof SearchBlock) {
                ((SearchBlock) block).insert(write.getKey(), write.getValue(),
                        write.getRecord(), write.getVersion(), write.getType());
                // NOTE: We do not cache SearchRecords because they have the
                // potential to be VERY large. Holding references to them in a
                // cache would prevent them from being garbage collected
                // resulting in more OOMs.
            }
            else {
                throw new IllegalArgumentException();
            }
        }
    }
}
