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
package com.cinchapi.concourse.service;

import com.cinchapi.concourse.store.Transaction;

/**
 * A service that can start and commit a {@link Transaction}.
 * 
 * @author jnelson
 */
public interface TransactionService {

	/**
	 * Start and return a {@link Transaction}.
	 * 
	 * @return the transaction
	 */
	public Transaction startTransaction();

	/**
	 * Commit a {@link Transaction}.
	 * 
	 * @param transaction
	 * @return {@code true} if the transaction is fully committed
	 */
	public boolean commitTransaction(Transaction transaction);

}
