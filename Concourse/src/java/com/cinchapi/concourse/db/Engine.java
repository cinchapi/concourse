package com.cinchapi.concourse.db;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.config.ConcourseConfiguration.PrefsKey;

/**
 * <p>
 * The Concourse storage engine handles concurrent CRUD operations, provides
 * ACID-compliant transactions, and automatically indexes and versions data. The
 * engine is a {@link StaggeredWriteService} that first writes data to the
 * {@link Buffer} and eventually flushes to {@link DurableStorage}.
 * </p>
 * <h2>ACID Transactions</h2>
 * <p>
 * By default, Concourse writes data immediately, but does provides the option
 * to dynamically defer writes to a transaction that provides full ACID
 * guarantees.
 * <ul>
 * <li><strong>Atomicity</strong>: The writes in a transaction are all or
 * nothing.</li>
 * <li><strong>Consistency</strong>: Each successful transaction write is at a
 * minimum <em>instantaneously consistent</em> such that a write is
 * <em><strong>guaranteed</strong></em> to be consistent at the instant when it
 * is written and also consistent up until the instant when another write occurs
 * in the parent service. Once a transaction is committed, the parent service is
 * locked and all the operations that occurred in the transaction are replayed
 * in a new transaction against the current state of the parent at the time of
 * locking to ensure that each operation is consistent with the most recent
 * data. If the replay succeeds, all the operations are permanently written to
 * the parent and the lock is released, otherwise the commit fails and the
 * transaction is discarded.</li>
 * <li><strong>Isolation</strong>: Each concurrent transaction exists in a
 * sandbox, and the operations of an uncommitted transaction are invisible to
 * others. Each write in a transaction is conducted against the current snapshot
 * of the parent service. At the time of commit, the transaction grabs a lock on
 * the parent service, so commits also happen in isolation.</li>
 * <li><strong>Durability</strong>: Once a transaction is committed, it is
 * permanently written to the parent service. Before attempting to commit a
 * transaction to the parent service, the transaction is logged to a file on
 * disk so that, in the event of a power loss, crash, shutdown, etc, the
 * transaction can be recovered and resumed. Once the transaction is fully
 * committed, the file on disk is deleted.</li>
 * </ul>
 * </p>
 * 
 * 
 * @author jnelson
 */
public final class Engine extends StaggeredWriteService implements
		TransactionService {

	/**
	 * Return an {@link Engine} tuned with the parameters defined in the
	 * {@code prefs}.
	 * 
	 * @param prefs
	 * @return Engine
	 */
	public static Engine start(ConcourseConfiguration prefs) {
		final String home = prefs.getString(Prefs.CONCOURSE_HOME,
				DEFAULT_CONCOURSE_HOME);

		// Setup the Buffer
		Buffer buffer;
		File bufferFile = new File(home + File.separator + "buf");
		boolean bufferIsFlushed;
		if(bufferFile.exists()) {
			buffer = Buffer.fromFile(bufferFile.getAbsolutePath(), prefs);
			bufferIsFlushed = false;
		}
		else {
			buffer = Buffer.newInstance(bufferFile.getAbsolutePath(), prefs);
			bufferIsFlushed = true;
		}

		// Setup the DurableStorage
		DurableStorage durable;
		File databaseDir = new File(home + File.separator + "ds");
		databaseDir.mkdirs();
		durable = DurableStorage.in(databaseDir.getAbsolutePath());

		// Setup for Transactions
		String transactionFile = home + File.separator + TRANSACTION_FILE_NAME;

		// Return the Engine
		return new Engine(buffer, durable, bufferIsFlushed, transactionFile);
	}

	/**
	 * By default, the Engine stores data in the 'concourse' directory under the
	 * user's home directory.
	 */
	public static final String DEFAULT_CONCOURSE_HOME = System
			.getProperty("user.home") + File.separator + "concourse";
	private static final String TRANSACTION_FILE_NAME = "transaction";
	private static final Logger log = LoggerFactory.getLogger(Engine.class);

	private boolean flushed;
	private final String transactionFile;

	/**
	 * Construct a new instance.
	 * 
	 * @param buffer
	 * @param durable
	 * @param flushed
	 * @param transactionFile
	 */
	private Engine(Buffer buffer, DurableStorage durable, boolean flushed,
			String transactionFile) {
		super(buffer, durable);
		this.flushed = flushed;
		this.transactionFile = transactionFile;
		log.info("The engine has started.");
		recover();
	}

	/**
	 * Force an immediate flush of the write buffer to primary storage. This
	 * method will block other reads and writes and is <em>likely</em> to cause
	 * a GC.
	 */
	public synchronized void flush() {
		((DurableStorage) primary).flush((Buffer) initial);
		flushed = true;
		System.gc();
		log.info("The buffer has been flushed.");
	}

	@Override
	public String getTransactionFileName() {
		return this.transactionFile;
	}

	@Override
	public synchronized void shutdown() {
		synchronized (this) {
			if(!flushed) {
				log.warn("The buffer was not flushed prior to shutdown.");
			}
			initial.shutdown();
			primary.shutdown();
			log.info("The engine has shutdown gracefully.");
			super.shutdown();
		}
	}

	@Override
	public Transaction startTransaction() {
		return Transaction.initFrom(this);
	}

	@Override
	protected boolean addSpi(String column, Object value, long row) {
		checkFlush(column, value, row);
		flushed = false;
		return super.addSpi(column, value, row);
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		checkFlush(column, value, row);
		flushed = false;
		return super.removeSpi(column, value, row);
	}

	/**
	 * Flush the {@code writeBuffer} to the {@code database} iff it is too full
	 * for the specified write.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 */
	private void checkFlush(String column, Object value, long row) {
		if(((Buffer) initial).isFull(column, value, row)) {
			flush();
		}
	}

	/**
	 * Take the necessary steps on startup to recover in the event that Engine
	 * was not properly shutdown.
	 */
	private void recover() {
		// Recover dropped write
		DroppedWrite write = ((Buffer) initial).checkForDroppedWrite();
		if(write != null) {
			log.info("It appears that the engine was last shutdown while "
					+ "flushing the buffer and a write was dropped. "
					+ "The engine will attempt to flush that write "
					+ "now: {}", write);
			((DurableStorage) primary).flush(write);
			write.discard();
		}

		// Recover transaction
		File tf = new File(transactionFile);
		if(tf.exists()) {
			log.info("It appears that the engine was last shutdown while"
					+ " committing a transaction. The engine will attempt to "
					+ "finish committing the transaction now");
			Transaction transaction = Transaction.recoverFrom(transactionFile,
					this);
			tf.delete();
			transaction.doCommit();
		}
	}

	/**
	 * The configurable preferences used in this class.
	 */
	private enum Prefs implements PrefsKey {
		CONCOURSE_HOME
	}
}
