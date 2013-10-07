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
package org.cinchapi.concourse.util;

import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;

/**
 * A utility class for getting test data.
 * 
 * @author jnelson
 */
public final class TestData extends Random {

	/**
	 * Return a random forStorage {@link PrimaryKey}.
	 * 
	 * @return a PrimaryKey
	 */
	public static PrimaryKey getPrimaryKeyForStorage() {
		return PrimaryKey.forStorage(getLong());
	}

	/**
	 * Return a random notForStorage {@link PrimaryKey}.
	 * 
	 * @return a PrimaryKey
	 */
	public static PrimaryKey getPrimaryKeyNotForStorage() {
		return PrimaryKey.notForStorage(getLong());
	}

	/**
	 * Return a random {@link Position}.
	 * 
	 * @return a Position
	 */
	public static Position getPosition() {
		return Position.fromPrimaryKeyAndIndex(getPrimaryKeyForStorage(),
				Math.abs(getInt()));
	}

	private TestData() {/* Utility class */}

}
