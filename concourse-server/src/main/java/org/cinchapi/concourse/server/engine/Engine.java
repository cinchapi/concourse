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

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.model.Write;
import org.cinchapi.concourse.thrift.TObject;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.*;
import static org.cinchapi.concourse.server.GlobalState.*;
import static org.cinchapi.concourse.util.Loggers.getLogger;

/**
 * The {@code Engine} schedules concurrent CRUD operations, manages ACID
 * transactions, versions writes and indexes data.
 * <p>
 * The Engine is a {@link BufferedStore}. Writing to the {@link Database} is
 * expensive because multiple index records must be deserialized, updated and
 * flushed back to disk for each revision. By using a {@link Buffer}, the Engine
 * can handle writes in a more efficient manner which minimal impact on Read
 * performance. The buffering system provides full CD guarantees.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
public final class Engine extends BufferedStore implements
		Transactional,
		PermanentStore {

	private static final Logger log = getLogger();

	/**
	 * The location that the engine uses as the base store for its components.
	 */
	@PackagePrivate
	final String bufferStore; // visible for Transaction backups

	/**
	 * The thread that is responsible for transporting buffer content in the
	 * background.
	 */
	private final Thread bufferTransportThread;

	/**
	 * A flag to indicate if the Engine is running or not.
	 */
	private boolean running = false;

	/**
	 * Construct an Engine that is made up of a {@link Buffer} and
	 * {@link Database} in the default locations.
	 * 
	 */
	public Engine() {
		this(new Buffer(), new Database(), BUFFER_STORE);
	}

	/**
	 * Construct an Engine that is made up of a {@link Buffer} and
	 * {@link Database} that are both backed by {@code bufferStore} and
	 * {@code dbStore} respectively.
	 * 
	 * @param bufferStore
	 * @param dbStore
	 */
	public Engine(String bufferStore, String dbStore) {
		this(new Buffer(bufferStore), new Database(dbStore), bufferStore);
	}

	/**
	 * Construct an Engine that is made up of {@code buffer} and
	 * {@code database}.
	 * 
	 * @param buffer
	 * @param database
	 * @param bufferStore
	 */
	private Engine(Buffer buffer, Database database, String bufferStore) {
		super(buffer, database);
		this.bufferStore = bufferStore;
		this.bufferTransportThread = new BufferTransportThread();
	}

	/*
	 * (non-Javadoc)
	 * The Engine is a Destination for Transaction commits. The accept method
	 * here will accept a write from a Transaction and create a new Write
	 * within the underlying BufferingService (i.e. it will create a Write in
	 * the buffer that will eventually be flushed to the database). Creating a
	 * new Write does associate a new timestamp with the data, but this is the
	 * desired behaviour because the timestamp associated with transactional
	 * data should be the timestamp of the data post commit.
	 */
	@Override
	@DoNotInvoke
	public void accept(Write write) {
		checkArgument(write.getType() != Write.Type.NOT_FOR_STORAGE);
		Lock lock = writeLock();
		try {
			String key = write.getKey().toString();
			TObject value = write.getValue().getQuantity();
			long record = write.getRecord().longValue();
			boolean accepted = write.getType() == Write.Type.ADD ? add(key,
					value, record) : remove(key, value, record);
			if(!accepted) {
				log.warn("Write {} was rejected by the Engine"
						+ "because it was previously accepted "
						+ "but not offset. This implies that a "
						+ "premature shutdown occured and the parent"
						+ "Transaction is attempting to restore"
						+ "itself from backup and finish committing.", write);
			}
			else {
				log.debug("'{}' was accepted by the Engine", write);
			}
		}
		finally {
			lock.release();
		}

	}

	@Override
	public boolean add(String key, TObject value, long record) {
		if(super.add(key, value, record)) {
			BLOOM_FILTERS.add(key, value, record);
			return true;
		}
		return false;
	}

	@Override
	public void start() {
		if(!running) {
			log.info("Starting the Engine...");
			running = true;
			buffer.start();
			destination.start();
			bufferTransportThread.start();
		}
	}

	@Override
	public Transaction startTransaction() {
		return Transaction.start(this);
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		return BLOOM_FILTERS.verify(key, value, record) ? super.verify(key,
				value, record) : false;
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		return BLOOM_FILTERS.verify(key, value, record) ? super.verify(key,
				value, record, timestamp) : false;
	}

	/**
	 * A thread that is responsible for transporting content from
	 * {@link #buffer} to {@link #destination}.
	 * 
	 * @author jnelson
	 */
	private class BufferTransportThread extends Thread {

		@Override
		public void run() {
			while (running) {
				buffer.transport(destination);
				try {
					Thread.sleep(5);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
					Thread.currentThread().interrupt();
				}
			}
		}

	}
}
