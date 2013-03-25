package com.cinchapi.concourse.internal;

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
 * The engine is a {@link StaggeredWriteService} in which data is initially
 * written to a {@link CommitLog} and eventually flushed to a {@link Database}.
 * </p>
 * 
 * @author jnelson
 */
public final class Engine extends StaggeredWriteService implements
		TransactionService {

	/**
	 * Return an {@link Engine} tuned with the parameters in {@code config}.
	 * 
	 * @param prefs
	 * @return Concourse
	 */
	public static Engine start(ConcourseConfiguration prefs) {
		final String home = prefs.getString(Prefs.CONCOURSE_HOME,
				DEFAULT_CONCOURSE_HOME);

		// Setup the CommitLog
		CommitLog commitLog;
		File commitLogFile = new File(home + File.separator + "commitlog");
		boolean commitLogIsFlushed;
		if(commitLogFile.exists()) {
			commitLog = CommitLog.fromFile(commitLogFile.getAbsolutePath(),
					prefs);
			commitLogIsFlushed = false;
		}
		else {
			commitLog = CommitLog.newInstance(commitLogFile.getAbsolutePath(),
					prefs);
			commitLogIsFlushed = true;
		}

		// Setup the Database
		Database database;
		File databaseDir = new File(home + File.separator + "db");
		databaseDir.mkdirs();
		database = Database.inDir(databaseDir.getAbsolutePath());

		// Setup for Transactions
		String transactionFile = home + File.separator + TRANSACTION_FILE_NAME;

		// Return the Engine
		return new Engine(commitLog, database, commitLogIsFlushed,
				transactionFile);
	}

	/**
	 * By default, Concourse is based in the 'concourse' directory under the
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
	 * @param commitLog
	 * @param database
	 * @param flushed
	 * @param home
	 */
	private Engine(CommitLog commitLog, Database database, boolean flushed,
			String transactionFile) {
		super(database, commitLog);
		this.flushed = flushed;
		this.transactionFile = transactionFile;
		recover();
	}

	/**
	 * <p>
	 * Force an immediate flush of the {@code commitLog} to the permanent
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
		// TODO make a backup of the commit log
		((Database) primary).flush((CommitLog) secondary);
		flushed = true;
		System.gc();
		// TODO delete the backup of the commit log
		log.info("The CommitLog has been flushed.");
	}

	@Override
	public synchronized void shutdown() {
		synchronized (this) {
			if(!flushed) {
				log.warn("The commitlog was not flushed prior to shutdown");
			}
			secondary.shutdown();
			primary.shutdown();
			log.info("Successfully shutdown the engine.");
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
	 * Flush the {@code commitLog} to the {@code database} if it is too full to
	 * commit the specified revision.
	 * 
	 * @param force
	 */
	private void flush(String column, Object value, long row) {
		if(((CommitLog) secondary).isFull(column, value, row)) {
			flush();
		}
	}

	/**
	 * Take the necessary steps to recover in the event that Engine was not
	 * properly shutdown before.
	 */
	private void recover() {
		File file = new File(transactionFile);
		if(file.exists()) {
			log.info("It appears that the engine was last shutdown while"
					+ " commiting a transaction. The Engine will attempt to "
					+ "finish committing the transaction now");
			Transaction transaction = Transaction.recoverFrom(transactionFile,
					this);
			file.delete();
			transaction.doCommit();
		}
	}

	/**
	 * The configurable preferences used in this class.
	 */
	enum Prefs implements PrefsKey {
		CONCOURSE_HOME, COMMIT_LOG_SIZE_IN_BYTES
	}
}
