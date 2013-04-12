package com.cinchapi.concourse.engine.old;

import java.io.File;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.config.ConcourseConfiguration.PrefsKey;
import com.google.common.collect.Sets;

/**
 * <p>
 * The Concourse storage engine handles concurrent CRUD operations, provides
 * ACID-compliant transactions, and automatically indexes and versions data. The
 * engine is a {@link BufferedWriteService} that first writes data to a
 * {@link Buffer} and eventually flushes to {@link Storage}.
 * </p>
 * <h2>Write Buffering</h2>
 * <p>
 * Because Concourse is schemaless and indexes everything<sup>1</sup>,
 * committing a single write to storage requires deserializing the entire row to
 * modify the cell and deserializing the entire column to modify the index.
 * Those changes must then flushed back to disk before the write is considered
 * committed.
 * </p>
 * <p>
 * The overhead of committing individual writes to storage is avoided by using a
 * durable buffer. The buffer is append-only and maintained entirely in memory
 * with append-only indexing. All writes are first committed to the buffer and
 * eventually flushed to storage.
 * </p>
 * <p>
 * The buffered write system provides CD guarantees.
 * <ol>
 * <li><strong>Consistency</strong>: Each buffered write individually
 * transitions the database from one consistent state to another. Thus the flush
 * operation is guaranteed to maintain the consistency of the database.</li>
 * <li><strong>Durability</strong>: The buffer itself is durable, so writes are
 * not lost in the event of irregular shutdown. During a flush, writes are
 * dropped from the buffer individually and placed in storage. A backup of the
 * write currently being flushed is saved to disk before it is dropped and the
 * backup is removed once the write is in storage. Saving the dropped write
 * allows recovery in the case that a shutdown occurs after the write was
 * dropped, but before it was placed in storage. This guarantees durability in
 * the flush from buffer to storage.</li>
 * </ol>
 * </p>
 * <p>
 * <sup>1</sup> - Databases typically take advantage of predefined and regular
 * structure to map only the necessary parts of files into memory during
 * read/write operations.
 * </p>
 * <h2>Consistent Reading</h2>
 * <p>
 * The buffer keeps a running count of write occurrences<sup>2</sup> to easily
 * determine if a write currently exists or not. Before committing a write to
 * the buffer, the engine checks both the buffer and storage to see if the write
 * exists. The same protocol&mdash;consulting both the buffer and
 * storage&mdash;is used for all reads. The results from the buffer and storage
 * are resolved by taking their XOR (see
 * {@link Sets#symmetricDifference(Set, Set)} or <a
 * href="https://en.wikipedia.org/wiki/Exclusive_or#Truth_table">XOR truth</a>
 * before being returned.
 * <p>
 * <p>
 * <sup>2</sup> - Write equality is based only on row, column and value (not
 * write type).
 * </p>
 * </p> <h2>Transactions</h2>
 * <p>
 * By default, Concourse commits writes immediately, but does provides the
 * option to dynamically defer a group of writes to a transaction that provides
 * full ACID semantics.
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
 * <strong>NOTE</strong>: ACID compliance is not guaranteed for transaction
 * reads because a failure is not detected if a read value changes before the
 * transaction is committed unless the value was also involved in a write. This
 * may change in the future.
 * </p>
 * 
 * 
 * @author jnelson
 */
public final class Engine extends BufferedWriteService implements
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

		// Setup the Storage
		Storage storage;
		File databaseDir = new File(home + File.separator + "ds");
		databaseDir.mkdirs();
		storage = Storage.in(databaseDir.getAbsolutePath());

		// Setup for Transactions
		String transactionFile = home + File.separator + TRANSACTION_FILE_NAME;

		// Return the Engine
		return new Engine(buffer, storage, bufferIsFlushed, transactionFile);
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
	 * @param storage
	 * @param flushed
	 * @param transactionFile
	 */
	private Engine(Buffer buffer, Storage storage, boolean flushed,
			String transactionFile) {
		super(buffer, storage);
		this.flushed = flushed;
		this.transactionFile = transactionFile;
		log.info("The engine has started.");
		recover();
	}

	/**
	 * Force an immediate flush of the write buffer to primary storage.
	 */
	public synchronized void flush() {
		((Storage) primary).flush((Buffer) buffer);
		flushed = true;
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
			buffer.shutdown();
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
	 * Flush the {@code buffer} to {@code storage} iff it is too full
	 * for the specified write.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 */
	private void checkFlush(String column, Object value, long row) {
		if(((Buffer) buffer).doesNotHaveCapacityForWrite(column, value, row)) {
			flush();
		}
	}

	/**
	 * Take the necessary steps on startup to recover in the event that Engine
	 * was not properly shutdown.
	 */
	private void recover() {
		// Recover dropped write
		DroppedWrite write = ((Buffer) buffer).checkForDroppedWrite();
		if(write != null) {
			log.info("It appears that the engine was last shutdown while "
					+ "flushing the buffer and a write was dropped. "
					+ "The engine will attempt to flush that write "
					+ "now: {}", write);
			((Storage) primary).flush(write);
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
