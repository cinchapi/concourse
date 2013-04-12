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
package com.cinchapi.concourse.exception;

/**
 * Base class for all unchecked exceptions in Concourse.
 * 
 * @author jnelson
 */
public class ConcourseRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Construct a new instance.
	 * 
	 * @param message
	 */
	public ConcourseRuntimeException(String message) {
		super(message);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param cause
	 */
	public ConcourseRuntimeException(Throwable cause) {
		super(cause);
	}

}
