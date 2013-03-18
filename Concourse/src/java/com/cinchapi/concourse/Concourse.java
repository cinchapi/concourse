package com.cinchapi.concourse;

import java.io.File;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.service.StaggeredWriteService;
import com.cinchapi.concourse.service.TransactionService;
import com.cinchapi.concourse.store.CommitLog;
import com.cinchapi.concourse.store.Database;
import com.cinchapi.concourse.store.Transaction;
import com.google.common.collect.Sets;

/**
 * <p>
 * Concourse is a schemaless database that is designed for applications that
 * have large amounts of sparse data in read and write heavy environments.
 * Concourse comes with automatic indexing, data versioning and support for ACID
 * transactions.
 * 
 * <h2>Intent</h2>
 * Concourse aims to be a service that is easy for developers to deploy, access
 * and scale with minimal tuning, while also being highly optimized for fast
 * read/write operations. With Concourse there is no need to declare any
 * structure up front--no schema, no tables, no keyspaces, no indexes, no column
 * families, etc. Instead, you simply write any value <sup>1</sup> to any column
 * in any row at any time and the necessary structure is created for you on
 * demand. <br>
 * <br>
 * <sup>1</sup> - You cannot write a duplicate value to a cell.
 * <h2>Data Model</h2>
 * Concourse is a big matrix where each row represents a single, canonical
 * object record and each column represents an attribute in the data universe.
 * The intersection of a row and column--a cell--specifies the
 * <strong>values</strong><sup>2</sup> for the relevant attribute on the
 * relevant object.
 * <ul>
 * <li>Each value is versioned by timestamp.<sup>3</sup></li>
 * <li>Each cell sorts its values by timestamp in descending order and also
 * maintains a historical log of revisions.</li>
 * <li>An index of rows sorted by id, an index of columns sorted logically, and
 * a full text index of values are all maintained for optimal reads.</li>
 * </ul>
 * <sup>2</sup> - A cell can simultaneously hold many distinct values and
 * multiple types.<br>
 * <sup>3</sup> - Each value is guaranteed to have a unique timestamp.
 * 
 * <h2>Graph Model</h2>
 * As a matrix, Concourse naturally represents which nodes on a graph are
 * connected to which other nodes: each row and each column corresponds to a
 * node and each value in the cell formed at the intersection of the row and
 * column corresponds to an edge between the corresponding row node and column
 * node on the graph--an edge whose weight is equal to the value.
 * 
 * <h2>Data Processing</h2>
 * Concourse is designed for highly efficient reads and writes.
 * <h4>Writes</h4>
 * Initially all data is written to an append only commit log. The commit log
 * exists in memory and is flushed to disk periodically.
 * <h4>Reads</h4>
 * For reads, Concourse queries its internal database and commit log for the
 * appropriate result sets according to their respective views of the data. The
 * two results sets are resolved by taking their XOR (see
 * {@link Sets#symmetricDifference(Set, Set)} before being returned.
 * 
 * <h2>Additional Notes</h2>
 * <ul>
 * <li>
 * In its present implementation, Concourse can only increase in size (even if
 * data is removed) because every single revision is tracked. In the future,
 * functionality to purge history and therefore reduce the size of the database
 * <em>should</em> be added.</li>
 * </ul>
 * </p>
 * 
 * 
 * @author jnelson
 */
public final class Concourse extends StaggeredWriteService implements
		TransactionService {

	/**
	 * Close Concourse.
	 * 
	 * @param concourse
	 */
	public static void close(Concourse concourse) {
		concourse.close();
	}

	/**
	 * Return Concourse based out of the {@link DEFAULT_HOME} directory.
	 * 
	 * @return Concourse
	 */
	public static Concourse withDefaultHome() {
		return Concourse.withHomeAt(DEFAULT_HOME);
	}

	/**
	 * Return Concourse based out of the directory at {@code location}.
	 * 
	 * @param home
	 * @return Concourse
	 */
	public static Concourse withHomeAt(String home) {
		CommitLog commitLog;
		Database database;

		File commitLogFile = new File(home + File.separator + "commitlog");
		boolean commitLogIsFlushed;
		if(commitLogFile.exists()) {
			commitLog = CommitLog.fromFile(commitLogFile.getAbsolutePath());
			commitLogIsFlushed = false;
		}
		else {
			commitLog = CommitLog.newInstance(commitLogFile.getAbsolutePath(),
					CommitLog.DEFAULT_SIZE_IN_BYTES);
			commitLogIsFlushed = true;
		}

		File databaseDir = new File(home + File.separator + "db");
		databaseDir.mkdirs();
		database = Database.inDir(databaseDir.getAbsolutePath());

		return new Concourse(commitLog, database, commitLogIsFlushed);
	}

	/**
	 * The default HOME directory for Concourse.
	 */
	public static final String DEFAULT_HOME = System.getProperty("user.home")
			+ File.separator + "concourse";
	private static final Logger log = LoggerFactory.getLogger(Concourse.class);

	private boolean flushed;

	private Concourse(CommitLog commitLog, Database database, boolean flushed) {
		super(database, commitLog);
		this.flushed = flushed;
	}

	/**
	 * Close Concourse.
	 */
	public synchronized void close() {
		if(!flushed) {
			log.warn("The commitlog has not been flushed");
		}
	}

	@Override
	public boolean commitTransaction(Transaction transaction) {
		
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * <p>
	 * Force an immediate flush of the {@code commitLog} to the permanent
	 * {@code database}.
	 * </p>
	 * <p>
	 * <strong>Note:</strong> This method is synchronized and will block so that
	 * reads/writes do not occur simultaneously.
	 * </p>
	 */
	public synchronized void flush() {
		((Database) primary).flush((CommitLog) secondary);
		flushed = true;
	}

	@Override
	public Transaction startTransaction() {
		return Transaction.startTransaction(this);
	}

	@Override
	protected boolean addSpi(long row, String column, Object value) {
		flush(false);
		flushed = false;
		return super.addSpi(row, column, value);
	}

	/**
	 * Flush the {@code commitLog} to the {@code database} iff it is full or
	 * {@code force} is {@code true}.
	 * 
	 * @param force
	 */
	protected void flush(boolean force) {
		if(force || ((CommitLog) secondary).isFull()) {
			flush();
		}
	}

	@Override
	protected boolean removeSpi(long row, String column, Object value) {
		flush(false);
		flushed = false;
		return super.removeSpi(row, column, value);
	}
}
