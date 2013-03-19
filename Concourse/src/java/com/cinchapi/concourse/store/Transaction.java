/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.store;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.cinchapi.common.Strings;
import com.cinchapi.concourse.service.ConcourseService;
import com.cinchapi.concourse.service.StaggeredWriteService;
import com.cinchapi.concourse.service.TransactionService;
import com.cinchapi.concourse.structure.Commit;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * A {@link Transaction} is initiated from a {@link TransactionService} for the
 * purpose of conducting an ACID-compliant operation.
 * </p>
 * <p>
 * 
 * </p>
 * 
 * 
 * @author jnelson
 */
public final class Transaction extends StaggeredWriteService  implements Serializable{
	// NOTE: This class does not define hashCode() or equals() because the
	// defaults are the desired behaviour.

	/**
	 * Start a {@code transaction} for the {@code parent} service.
	 * 
	 * @param parent
	 * @return the transaction
	 */
	public static Transaction start(TransactionService parent) {
		return new Transaction(parent);
	}

	/**
	 * Replay the {@code transaction} against the most current view of the
	 * parent service and see if the commits succeed.
	 * 
	 * @param transaction
	 * @return a new {@code transaction} if the replay succeeds, and
	 *         {@code null} if the replay fails
	 */
	@Nullable
	protected static Transaction replay(Transaction transaction) {
		Transaction t = Transaction
				.start((TransactionService) transaction.primary);
		Preconditions.checkState(transaction.isClosed(),
				"Cannot replay a transaction that is not closed");
		Preconditions.checkState(transaction.primary.equals(t.primary),
				"Cannot replay a transaction with a different parent service");
		synchronized (transaction.primary) {
			for (int i = 0; i < transaction.operations.size(); i++) {
				Operation operation = transaction.operations.get(i);
				Commit commit = ((VolatileDatabase) (transaction.secondary)).ordered
						.get(i);
				if(operation == Operation.ADD
						&& t.add(commit.getRow().asLong(), commit.getColumn(),
								commit.getValue().getQuantity())) {
					continue;
				}
				else if(operation == Operation.REMOVE
						&& t.remove(commit.getRow().asLong(), commit
								.getColumn(), commit.getValue().getQuantity())) {
					continue;
				}
				else { // detected a merge conflict that would cause the
						// transaction to fail
					t.rollback(); // ensure that the transaction is not used in
									// other write contexts because its in a
									// weird state
					return null;
				}
			}
		}
		t.rollback(); // ensure that the transaction is not used in other write
						// contexts because its in a weird state
		return t;
	}

	private static final int INITIAL_CAPACITY = 10;
	private static final long serialVersionUID = 1L;

	/**
	 * Contains a list of write operators in the same order as the internal
	 * volatile database's list of commits. When the transaction is being
	 * committed, the iterator should access operations
	 * in the same order it would the commits in the database to get the correct
	 * mapping.
	 */
	private final List<Operation> operations = Lists
			.newArrayListWithExpectedSize(INITIAL_CAPACITY);
	private boolean closed = false;

	/**
	 * Construct a new instance for replaying.
	 * 
	 * @param transaction
	 */
	private Transaction(Transaction transaction) {
		super(transaction.primary, VolatileDatabase
				.newInstancewithExpectedCapacity(transaction.operations.size()));
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param parent
	 *            - must be an instance of ConcourseService
	 */
	private Transaction(TransactionService parent) {
		super((ConcourseService) parent, VolatileDatabase
				.newInstancewithExpectedCapacity(INITIAL_CAPACITY));
	} 

	/**
	 * Return {@code true} if the transaction is closed.
	 * 
	 * @return {@code true} if the transaction is closed.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Rollback and cancel all the changes in the transaction.
	 */
	public void rollback() {
		closed = true;
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

	@Override
	protected boolean addSpi(long row, String column, Object value) {
		if(super.addSpi(row, column, value) && !isClosed()) {
			operations.add(Operation.ADD);
			assertSize();
			return true;
		}
		return false;
	}

	/**
	 * Return an iterator of mappings from {@link Commit} to
	 * {@link WriterOperation} for the purpose of flushing this transaction to
	 * its parent service.
	 * 
	 * @return the flusher
	 */
	protected Iterator<Entry<Commit, Operation>> flusher() {
		Preconditions.checkState(isClosed(),
				"Cannot flush a transaction that is not closed"); // Ensuring
																	// that the
																	// transaction
																	// is closed
																	// protects
																	// against
																	// concurrent
																	// modification
		return new Iterator<Entry<Commit, Operation>>() {

			int index = 0;

			@Override
			public boolean hasNext() {
				return index < operations.size();
			}

			@Override
			public Entry<Commit, Operation> next() {
				Entry<Commit, Operation> entry = new AbstractMap.SimpleEntry<Commit, Operation>(
						((VolatileDatabase) secondary).ordered.get(index),
						operations.get(index));
				index++;
				return entry;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException(
						"This operation is not supported");

			}

		};
	}

	/**
	 * Prepare the the transaction for commit. This will close the transaction
	 * for further writes.
	 */
	protected void prepare() {
		closed = true;
	}

	@Override
	protected boolean removeSpi(long row, String column, Object value) {
		if(super.removeSpi(row, column, value) && !isClosed()) {
			operations.add(Operation.REMOVE);
			assertSize();
			return true;
		}
		return false;
	}

	/**
	 * Assert that the number of operations is equal to the number of commits.
	 */
	private void assertSize() {
		assert operations.size() == ((VolatileDatabase) secondary).ordered
				.size() : "There is a discrepency between the number of operations and the number of commits";
	}

	/**
	 * The write operations that can occur within a transaction.
	 */
	public enum Operation {
		ADD, REMOVE
	}

}
