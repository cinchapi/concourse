/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.concurrent.ConcourseExecutors;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Loggers;
import org.cinchapi.concourse.util.Transformers;
import org.slf4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import static org.cinchapi.concourse.server.GlobalState.*;

/**
 * The {@code Database} is the permanent {@link PermanentStore} for
 * {@link Write} objects that are initially stored in a {@link Buffer}.
 * <p>
 * When the Database accepts a write, it creates relevant indexes for efficient
 * retrieval, query and search reads.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
public final class Database implements PermanentStore {

	private static final String PRIMARY_BLOCK_DIRECTORY = "cpb";
	private static final String SECONDARY_BLOCK_DIRECTORY = "csb";
	private static final String SEARCH_BLOCK_DIRECTORY = "ctb";

	/**
	 * Lock used to ensure the object is ThreadSafe. This lock provides access
	 * to a masterLock.readLock()() and masterLock.writeLock()().
	 */
	private final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

	private final transient List<PrimaryBlock> cpb = Lists.newArrayList();
	private final transient List<SecondaryBlock> csb = Lists.newArrayList();
	private final transient List<SearchBlock> ctb = Lists.newArrayList();

	private transient PrimaryBlock primaryBlock;
	private transient SecondaryBlock secondaryBlock;
	private transient SearchBlock searchBlock;

	/**
	 * The location where the Database stores data.
	 */
	private final String backingStore;

	/**
	 * A flag to indicate if the Buffer is running or not.
	 */
	private boolean running = false;

	private static final String threadNamePrefix = "database-write-thread";
	private static final Logger log = Loggers.getLogger();

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
		ConcourseExecutors.executeAndAwaitTermination(threadNamePrefix,
				new BlockWriter(primaryBlock, write), new BlockWriter(
						secondaryBlock, write), new BlockWriter(searchBlock,
						write));
	}

	@Override
	public Map<Long, String> audit(long record) {
		return getPrimaryRecord(PrimaryKey.wrap(record)).audit();
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		return getPrimaryRecord(PrimaryKey.wrap(record), Text.wrap(key)).audit(
				Text.wrap(key));
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

	@Override
	public Set<TObject> fetch(String key, long record) {
		return Transformers.transformSet(
				getPrimaryRecord(PrimaryKey.wrap(record), Text.wrap(key))
						.fetch(Text.wrap(key)), Functions.VALUE_TO_TOBJECT);
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		return Transformers.transformSet(
				getPrimaryRecord(PrimaryKey.wrap(record), Text.wrap(key))
						.fetch(Text.wrap(key), timestamp),
				Functions.VALUE_TO_TOBJECT);
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

	@Override
	public boolean ping(long record) {
		return getPrimaryRecord(PrimaryKey.wrap(record)).ping();
	}

	@Override
	public Set<Long> search(String key, String query) {
		return Transformers.transformSet(getSearchRecord(Text.wrap(key))
				.search(Text.wrap(query)), Functions.PRIMARY_KEY_TO_LONG);
	}

	@Override
	public void start() {
		if(!running) {
			running = true;
			log.info("Database configured to store data in {}", backingStore);
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
		Value value0 = Value.wrap(value);
		PrimaryKey record0 = PrimaryKey.wrap(record);
		return getPrimaryRecord(record0, key0).verify(key0, value0);
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		return getPrimaryRecord(PrimaryKey.wrap(record), Text.wrap(key))
				.verify(Text.wrap(key), Value.wrap(value), timestamp);
	}

	private PrimaryRecord getPrimaryRecord(PrimaryKey primaryKey) {
		masterLock.readLock().lock();
		try {
			PrimaryRecord record = Record.createPrimaryRecord(primaryKey);
			for (PrimaryBlock block : cpb) {
				block.seek(primaryKey, record);
			}
			return record;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	private PrimaryRecord getPrimaryRecord(PrimaryKey primaryKey, Text key) {
		masterLock.readLock().lock();
		try {
			PrimaryRecord record = Record.createPrimaryRecordPartial(
					primaryKey, key);
			for (PrimaryBlock block : cpb) {
				block.seek(primaryKey, key, record);
			}
			return record;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	private SearchRecord getSearchRecord(Text key) {
		masterLock.readLock().lock();
		try {
			SearchRecord record = Record.createSearchRecord(key);
			for (SearchBlock block : ctb) {
				block.seek(key, record);
			}
			return record;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	private SecondaryRecord getSecondaryRecord(Text key) {
		masterLock.readLock().lock();
		try {
			SecondaryRecord record = Record.createSecondaryRecord(key);
			for (SecondaryBlock block : csb) {
				block.seek(key, record);
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
				ConcourseExecutors.executeAndAwaitTermination(threadNamePrefix,
						new BlockSyncer(primaryBlock), new BlockSyncer(
								secondaryBlock), new BlockSyncer(searchBlock));
			}
			cpb.add((primaryBlock = Block.createPrimaryBlock(backingStore
					+ File.separator + PRIMARY_BLOCK_DIRECTORY)));
			csb.add((secondaryBlock = Block.createSecondaryBlock(backingStore
					+ File.separator + SECONDARY_BLOCK_DIRECTORY)));
			ctb.add((searchBlock = Block.createSearchBlock(backingStore
					+ File.separator + SEARCH_BLOCK_DIRECTORY)));
		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	/**
	 * A runnable that traverses the appropriate directory for a block type
	 * under {@code backingStore} and loads the blocks into memory.
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
			try {
				Path path = Paths.get(backingStore, directory);
				FileSystem.mkdirs(path.toString());
				DirectoryStream<Path> paths = Files.newDirectoryStream(path);
				for (Path p : paths) {
					if(p.toString().endsWith(Block.BLOCK_NAME_EXTENSION)) {
						String id = Block.getId(p.toString());
						Constructor<T> constructor = clazz
								.getDeclaredConstructor(String.class,
										String.class);
						constructor.setAccessible(true);
						blocks.add(constructor.newInstance(path.toString(), id));
						log.info("Loaded {} at {}", clazz.getSimpleName(),
								p.toString());
					}
				}
				paths.close();
			}
			catch (IOException e) {
				throw Throwables.propagate(e);
			}
			catch (ReflectiveOperationException e) {
				throw Throwables.propagate(e);
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
			log.debug("Writing {} to {}", write, block);
			if(block instanceof PrimaryBlock) {
				((PrimaryBlock) block).insert(write.getRecord(),
						write.getKey(), write.getValue(), write.getVersion());
			}
			else if(block instanceof SecondaryBlock) {
				((SecondaryBlock) block)
						.insert(write.getKey(), write.getValue(),
								write.getRecord(), write.getVersion());
			}
			else if(block instanceof SearchBlock) {
				((SearchBlock) block).insert(write.getKey(), write.getValue(),
						write.getRecord(), write.getVersion());
			}
			else {
				throw new IllegalArgumentException();
			}
		}

	}
}
