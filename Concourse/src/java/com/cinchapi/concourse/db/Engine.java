package com.cinchapi.concourse.db;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.config.ConcourseConfiguration.PrefsKey;

/**
 * <p>
 * The Concourse storage engine handles concurrent CRUD operations,
 * automatically indexes data and provides ACID-compliant transactions. The
 * engine is a {@link StaggeredWriteService} that first writes data to the
 * {@link Buffer} and eventually flushes to {@link DurableStorage}.
 * </p>
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
		String bufferBackupFile = home + File.separator
				+ WRITE_BUFFER_BACKUP_FILE_NAME;

		// Setup the DurableStorage
		DurableStorage durable;
		File databaseDir = new File(home + File.separator + "ds");
		databaseDir.mkdirs();
		durable = DurableStorage.in(databaseDir.getAbsolutePath());

		// Setup for Transactions
		String transactionFile = home + File.separator + TRANSACTION_FILE_NAME;

		// Return the Engine
		return new Engine(buffer, durable, bufferIsFlushed, transactionFile,
				bufferBackupFile);
	}

	/**
	 * By default, the Engine stores data in the 'concourse' directory under the
	 * user's home directory.
	 */
	public static final String DEFAULT_CONCOURSE_HOME = System
			.getProperty("user.home") + File.separator + "concourse";
	private static final String TRANSACTION_FILE_NAME = "transaction";
	private static final String WRITE_BUFFER_BACKUP_FILE_NAME = "buffer.bak";
	private static final Logger log = LoggerFactory.getLogger(Engine.class);

	private boolean flushed;
	private final String transactionFile;
	private final String bufferBackupFile;

	/**
	 * Construct a new instance.
	 * 
	 * @param buffer
	 * @param durable
	 * @param flushed
	 * @param transactionFile
	 * @param bufferBackupFile
	 */
	private Engine(Buffer buffer, DurableStorage durable, boolean flushed,
			String transactionFile, String bufferBackupFile) {
		super(buffer, durable);
		this.flushed = flushed;
		this.transactionFile = transactionFile;
		this.bufferBackupFile = bufferBackupFile;
		recover();
		log.info("The engine has started.");
	}

	/**
	 * Force an immediate flush of the write buffer to primary storage. This
	 * method will block other reads and writes and is <em>likely</em> to cause
	 * a GC.
	 */
	public synchronized void flush() {
		// TODO make a backup of the write log
		((DurableStorage) primary).flush((Buffer) initial);
		flushed = true;
		System.gc();
		// TODO delete the backup of the write log
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
		File tf = new File(transactionFile);
		if(tf.exists()) {
			log.info("It appears that the engine was last shutdown while"
					+ " commiting a transaction. The Engine will attempt to "
					+ "finish committing the transaction now");
			Transaction transaction = Transaction.recoverFrom(transactionFile,
					this);
			tf.delete();
			transaction.doCommit();
		}
		File wbf = new File(bufferBackupFile);
		if(wbf.exists()) {
			log.info("It appears that the engine was last shutdown while"
					+ " flushing the buffer. The Engine will attempt to "
					+ "finish flushing the buffer now.");
			// TODO copy the backup to the main file and delete the backup, then
			// flush the commitLog
		}
	}

	/**
	 * The configurable preferences used in this class.
	 */
	private enum Prefs implements PrefsKey {
		CONCOURSE_HOME
	}
}
