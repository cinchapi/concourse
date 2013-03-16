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
package com.cinchapi.concourse.cal.result;

import javax.annotation.concurrent.Immutable;

/**
 * A result that contains a single boolean which indicates if the action
 * succeeded or failed.
 * 
 * @author jnelson
 */
@Immutable
public class BooleanResult implements Result{

	/**
	 * Return a new {@link BooleanResult} that wraps the {@code result}.
	 * 
	 * @param result
	 * @return the result wrapper
	 */
	public static BooleanResult forBoolean(boolean result) {
		return new BooleanResult(result);
	}

	private final boolean result;

	/**
	 * Construct a new instance.
	 * 
	 * @param result
	 */
	private BooleanResult(boolean result) {
		this.result = result;
	}

	/**
	 * Return {@code true} if the result is {@code true}.
	 * 
	 * @return {@code true} if the result is {@code true}.
	 */
	public boolean isTrue() {
		return result;
	}

}
