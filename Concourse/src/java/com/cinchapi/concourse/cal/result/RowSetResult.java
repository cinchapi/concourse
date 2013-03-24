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

import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.internal.Key;

/**
 * A result that contains a set of {@link Key} objects, each of which identifies
 * a {@link Row}.
 * 
 * @author jnelson
 */
@Immutable
public class RowSetResult extends AbstractSetResult<Key> {

	/**
	 * Return a new {@link RowSetResult} that wraps the {@code results}.
	 * 
	 * @param results
	 * @return the wrapped results
	 */
	public static RowSetResult forSet(Set<Key> results) {
		return new RowSetResult(results);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param results
	 */
	private RowSetResult(Set<Key> results) {
		super(results);
	}

}
