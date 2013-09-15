/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.engine;

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.concourse.thrift.TObject;

/**
 * A {@link Store} that can process writes directly.
 * 
 * @author jnelson
 */
@PackagePrivate
interface WritableStore extends Store {

	/**
	 * Add {@code key} as {@code value} to {@code record}.
	 * <p>
	 * This method maps {@code key} to {@code value} in {@code record}, if and
	 * only if that mapping does not <em>currently</em> exist (i.e.
	 * {@link #verify(String, Object, long)} is {@code false}). Adding
	 * {@code value} to {@code key} does not replace any existing mappings from
	 * {@code key} in {@code record} because a field may contain multiple
	 * distinct values.
	 * </p>
	 * <p>
	 * To overwrite existing mappings from {@code key} in {@code record}, use
	 * {@link #set(String, Object, long)} instead.
	 * </p>
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is added
	 */
	public boolean add(String key, TObject value, long record);

	/**
	 * Remove {@code key} as {@code value} from {@code record}.
	 * <p>
	 * This method deletes the mapping from {@code key} to {@code value} in
	 * {@code record}, if that mapping <em>currently</em> exists (i.e.
	 * {@link #verify(String, Object, long)} is {@code true}. No other mappings
	 * from {@code key} in {@code record} are affected.
	 * </p>
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is removed
	 */
	public boolean remove(String key, TObject value, long record);

}
