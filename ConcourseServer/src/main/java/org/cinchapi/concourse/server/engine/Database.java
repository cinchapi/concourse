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
package org.cinchapi.concourse.server.engine;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.cinchapi.common.tools.Transformers;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.cinchapi.concourse.server.engine.PrimaryRecord.loadPrimaryRecord;
import static org.cinchapi.concourse.server.engine.SecondaryIndex.loadSecondaryIndex;
import static org.cinchapi.concourse.server.engine.SearchIndex.loadSearchIndex;
import static org.cinchapi.concourse.server.engine.DatabaseTools.invokeWriteRunnable;

/**
 * The {@code Database} is the permanent {@link Destination} for {@link Write}
 * objects that are initially stored in a {@link Buffer}.
 * <p>
 * When the Database accepts a write, it creates relevant indexes for efficient
 * retrieval, query and search reads.
 * </p>
 * 
 * @author jnelson
 */
public class Database implements Readable, Destination {

	private static final Logger log = LoggerFactory.getLogger(Database.class);

	/**
	 * Catches exceptions thrown from threads in {@link #executor}. Exceptions
	 * will occur in the event that an attempt is made to write a duplicate
	 * non-offset write when the system shuts down in the middle of a buffer
	 * flush. Those exceptions can be ignored, so we catch them here and print
	 * log statements.
	 */
	private static final UncaughtExceptionHandler uncaughtExceptionHandler;
	static {
		uncaughtExceptionHandler = new UncaughtExceptionHandler() {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				log.warn("Uncaught exception in thread '{}'. This possibly "
						+ "indicates that the system shutdown prematurely "
						+ "during a buffer flushing operation.", t);
				log.warn("", e);

			}

		};
	}

	/**
	 * Responsible for creating threads to asynchronously write to all the
	 * necessary record views in the database.
	 */
	private final ExecutorService executor = Executors
			.newCachedThreadPool(new ThreadFactoryBuilder()
					.setNameFormat("database-write-thread-%d")
					.setUncaughtExceptionHandler(uncaughtExceptionHandler)
					.build());

	@Override
	public void accept(Write write) {
		executor.execute(invokeWriteRunnable(
				loadPrimaryRecord(write.getRecord()), write));
		executor.execute(invokeWriteRunnable(
				loadSecondaryIndex(write.getKey()), write));
		executor.execute(invokeWriteRunnable(loadSearchIndex(write.getKey()),
				write));
	}

	@Override
	public Map<Long, String> audit(long record) {
		return loadPrimaryRecord(PrimaryKey.notForStorage(record)).audit();
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		return loadPrimaryRecord(PrimaryKey.notForStorage(record)).audit(
				Text.fromString(key));
	}

	@Override
	public Set<String> describe(long record) {
		return Transformers.transformSet(
				loadPrimaryRecord(PrimaryKey.notForStorage(record)).describe(),
				Functions.TEXT_TO_STRING);
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		return Transformers.transformSet(
				loadPrimaryRecord(PrimaryKey.notForStorage(record)).describe(
						timestamp), Functions.TEXT_TO_STRING);
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		return Transformers.transformSet(
				loadPrimaryRecord(PrimaryKey.notForStorage(record)).fetch(
						Text.fromString(key)), Functions.VALUE_TO_TOBJECT);
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		return Transformers.transformSet(
				loadPrimaryRecord(PrimaryKey.notForStorage(record)).fetch(
						Text.fromString(key), timestamp),
				Functions.VALUE_TO_TOBJECT);
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		return Transformers.transformSet(
				loadSecondaryIndex(Text.fromString(key)).find(
						timestamp,
						operator,
						Transformers.transformArray(values,
								Functions.TOBJECT_TO_VALUE, Value.class)),
				Functions.PRIMARY_KEY_TO_LONG);
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		return Transformers.transformSet(
				loadSecondaryIndex(Text.fromString(key)).find(
						operator,
						Transformers.transformArray(values,
								Functions.TOBJECT_TO_VALUE, Value.class)),
				Functions.PRIMARY_KEY_TO_LONG);
	}

	@Override
	public boolean ping(long record) {
		return loadPrimaryRecord(PrimaryKey.notForStorage(record)).ping();
	}

	@Override
	public Set<Long> search(String key, String query) {
		return Transformers.transformSet(loadSearchIndex(Text.fromString(key))
				.search(Text.fromString(query)), Functions.PRIMARY_KEY_TO_LONG);
	}

	/**
	 * Shutdown the database gracefully. Make sure any blocked tasks
	 * have a chance to complete before being dropped.
	 */
	public synchronized void shutdown() {
		executor.shutdown();
		try {
			if(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				List<Runnable> tasks = executor.shutdownNow();
				log.error("The Database could not properly shutdown. "
						+ "The following tasks were dropped: {}", tasks);
			}
		}
		catch (InterruptedException e) {
			log.error("An error occured while shutting down the Database: {}",
					e);
		}
		log.info("The Database has shutdown");
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		return loadPrimaryRecord(PrimaryKey.notForStorage(record)).verify(
				Text.fromString(key), Value.notForStorage(value));
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		return loadPrimaryRecord(PrimaryKey.notForStorage(record)).verify(
				Text.fromString(key), Value.notForStorage(value), timestamp);
	}
}
