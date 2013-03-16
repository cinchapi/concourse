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
package com.cinchapi.concourse.server;

import com.cinchapi.concourse.exception.ConcourseException;

/**
 * A Concourse Database Server that is responsible for handling secure
 * interactions with clients trying to interact with Concourse data.
 * 
 * @author jnelson
 */
public interface Server {

	/**
	 * Execute a CAL statement.
	 * 
	 * @param statement
	 * @return the CAL statement result
	 * @throws ConcourseException
	 */
	public String cal(String statement) throws ConcourseException;

	/**
	 * Authenticate the credentials for access to the server.
	 * 
	 * @param username
	 * @param password
	 * @return {@code true} if the credentials are valid
	 * @throws ConcourseException
	 */
	public boolean login(String username, String password)
			throws ConcourseException;

	/**
	 * Start the server and accept client connections.
	 * 
	 * @throws ConcourseException
	 */
	public void start() throws ConcourseException;

	/**
	 * Stop the server and drop all client connections.
	 * 
	 * @throws ConcourseException
	 */
	public void stop() throws ConcourseException;

}
