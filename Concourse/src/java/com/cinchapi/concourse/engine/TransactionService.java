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
package com.cinchapi.concourse.engine;

/**
 * A service that can start a {@link Transaction}.
 * 
 * @author jnelson
 */
public interface TransactionService {

	/**
	 * Start and return a {@link Transaction} which can be used for
	 * performing ACID operations.
	 * 
	 * @return the transaction
	 */
	public Transaction startTransaction();

	/**
	 * Return the name of the transaction file used by the service.
	 * 
	 * @return the transaction filename
	 */
	public String getTransactionFileName(); // This method should NOT be called
											// publicly, but Java does not allow
											// non-public methods in an
											// interface...furthermore, Java
											// does not allow an interface to
											// specify variables that a class
											// should define :-/

}
