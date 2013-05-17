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
package com.cinchapi.concourse.internal;

import java.util.Set;

import com.cinchapi.concourse.Operator;

/**
 * The {@code Kernel} is the lowest-level abstraction layer that is both
 * exposed to external applications and implemented by core components.
 * The Kernel is a highly available service that manages disk I/O, memory
 * caching, etc
 * 
 * @author jnelson
 */
public interface Kernel {

	/**
	 * Add {@code key} as {@code value} in {@code collection}.
	 * currently exists.
	 * 
	 * @param key
	 * @param value
	 * @param collection
	 * @return {@code true} if the mapping is added
	 */
	public boolean add(String key, Object value, long collection);

	public void audit(long collection); // TODO figure out return type...return
										// a map
	// timestamp to ????

	public void audit(String key, long collection);

	public Set<String> describe(long collection);

	public Set<String> describe(long collection, long timestamp);

	public Set<Object> fetch(String key, long collection);

	public Set<Object> fetch(String key, long collection, long timestamp);

	public Set<Long> query(long timestamp, String key, Operator operator,
			Object... values);

	public Set<Long> query(String key, Operator operator, Object... values);

	/**
	 * Remove {@code key} as {@code value} from {@code collection} if the
	 * mapping
	 * currently exists.
	 * 
	 * @param key
	 * @param value
	 * @param collection
	 * @return {@code true} if the mapping is removed
	 */
	public boolean remove(String key, Object value, long collection);

	/**
	 * Revert {@code key} in {@code collection} to {@code timestamp}.
	 * 
	 * @param key
	 * @param collection
	 * @param timestamp
	 */
	public void revert(String key, long collection, long timestamp);

	/**
	 * Set {@code key} as {@code value} in {@code collection} by removing all
	 * the
	 * mappings that currently exist and adding a new mapping from key to value.
	 * 
	 * @param key
	 * @param value
	 * @param collection
	 * @return {@code true} if the other mappings are removed and the new
	 *         mapping is added
	 */
	public boolean set(String key, Object value, long collection);

	/**
	 * Verify {@code key} equals {@code value} in {@code collection}.
	 * 
	 * @param key
	 * @param value
	 * @param collection
	 * @return {@code true} if there is a mapping from {@code key} to
	 *         {@code value} in {@code collection}
	 */
	public boolean verify(String key, Object value, long collection);

	/**
	 * Verify {@code key} equals {@code value} in {@code collection} at
	 * {@code timestamp}.
	 * 
	 * @param key
	 * @param value
	 * @param collection
	 * @param timestamp
	 * @return {@code true} if there is a mapping from {@code key} to
	 *         {@code value} in {@code collection} at {@code timestamp}
	 */
	public boolean verify(String key, Object value, long collection,
			long timestamp);

}
