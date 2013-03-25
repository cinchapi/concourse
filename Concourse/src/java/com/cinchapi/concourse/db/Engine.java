package com.cinchapi.concourse.db;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.config.ConcourseConfiguration.PrefsKey;

/**
 * <p>
 * The storage engine that is used by {@code Concourse}. The engine provides
 * ACID-compliant transactions and automatic indexing.
 * </p>
 * <p>
 * The engine is a {@link StaggeredWriteService} that first writes data to the
 * {@link WriteBuffer} and eventually flushes to {@link PrimaryStorage}.
 * </p>
 * 
 * @author jnelson
 */
public final class Engine extends StaggeredWriteService implements
		TransactionService {

	/**
	 * Return an {@link Engine} tuned with the parameters in the {@code prefs}.
	 * 
	 * @param prefs
	 * @return Engine
	 */
	public static Engine start(ConcourseConfiguration prefs) {
		final String home = prefs.getString(Prefs.CONCOURSE_HOME,
				DEFAULT_CONCOURSE_HOME);

		// Setup the WriteBuffer
		WriteBuffer writeBuffer;
		File writeBufferFile = new File(home + File.separator + "buffer");
		boolean writeBufferIsFlushed;
		if(writeBufferFile.exists()) {
			writeBuffer = WriteBuffer.fromFile(
					writeBufferFile.getAbsolutePath(), prefs);
			writeBufferIsFlushed = false;
		}
		else {
			writeBuffer = WriteBuffer.newInstance(
					writeBufferFile.getAbsolutePath(), prefs);
			writeBufferIsFlushed = true;
		}
		String writeBufferBackupFile = home + File.separator
				+ WRITE_BUFFER_BACKUP_FILE_NAME;

		// Setup the PrimaryStorage
		PrimaryStorage primary;
		File databaseDir = new File(home + File.separator + "db");
		databaseDir.mkdirs();
		primary = PrimaryStorage.inDir(databaseDir.getAbsolutePath());

		// Setup for Transactions
		String transactionFile = home + File.separator + TRANSACTION_FILE_NAME;

		// Return the Engine
		return new Engine(writeBuffer, primary, writeBufferIsFlushed,
				transactionFile, writeBufferBackupFile);
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
	private final String writeBufferBackupFile;

	/**
	 * Construct a new instance.
	 * 
	 * @param writeBuffer
	 * @param database
	 * @param flushed
	 * @param home
	 */
	private Engine(WriteBuffer writeBuffer, PrimaryStorage database,
			boolean flushed, String transactionFile,
			String writeBufferBackupFile) {
		super(writeBuffer, database);
		this.flushed = flushed;
		this.transactionFile = transactionFile;
		this.writeBufferBackupFile = writeBufferBackupFile;
		recover();
		log.info("The Engine has started.");
	}

	/**
	 * <p>
	 * Force an immediate flush of the {@code writeBuffer} to the permanent
	 * {@code database}.
	 * </p>
	 * <p>
	 * <p>
	 * <strong>Note</strong>: This method is synchronized and will block so that
	 * reads/writes do not occur during the flush.
	 * </p>
	 * <p>
	 * <strong>Note</strong>: This method is <em>likely</em> cause a GC.
	 * </p>
	 */
	public synchronized void flush() {
		// TODO make a backup of the write log
		((PrimaryStorage) primary).flush((WriteBuffer) initial);
		flushed = true;
		System.gc();
		// TODO delete the backup of the write log
		log.info("The WriteBuffer has been flushed.");
	}

	@Override
	public synchronized void shutdown() {
		synchronized (this) {
			if(!flushed) {
				log.warn("The WriteBuffer was not flushed prior to shutdown.");
			}
			initial.shutdown();
			primary.shutdown();
			log.info("The Engine has shutdown gracefully.");
			super.shutdown();
		}
	}

	@Override
	public Transaction startTransaction() {
		return Transaction.initFrom(this);
	}

	@Override
	public String z_() {
		return this.transactionFile;
	}

	@Override
	protected boolean addSpi(String column, Object value, long row) {
		flush(column, value, row);
		flushed = false;
		return super.addSpi(column, value, row);
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		flush(column, value, row);
		flushed = false;
		return super.removeSpi(column, value, row);
	}

	/**
	 * Flush the {@code writeBuffer} to the {@code database} if it is too full
	 * to
	 * write the specified revision.
	 * 
	 * @param force
	 */
	private void flush(String column, Object value, long row) {
		if(((WriteBuffer) initial).isFull(column, value, row)) {
			flush();
		}
	}

	/**
	 * Take the necessary steps to recover in the event that Engine was not
	 * properly shutdown before.
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
		File wbf = new File(writeBufferBackupFile);
		if(wbf.exists()) {
			log.info("It appears that the engine was last shutdown while"
					+ " flushing the WriteBuffer. The Engine will attempt to "
					+ "finish flushing the WriteBuffer.");
			// TODO copy the backup to the main file and delete the backup, then
			// flush the commitLog
		}
	}

	/**
	 * The configurable preferences used in this class.
	 */
	enum Prefs implements PrefsKey {
		CONCOURSE_HOME, COMMIT_LOG_SIZE_IN_BYTES
	}
}
