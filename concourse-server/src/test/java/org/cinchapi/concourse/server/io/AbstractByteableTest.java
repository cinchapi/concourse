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
package org.cinchapi.concourse.server.io;

import org.junit.Assert;
import org.junit.Test;

/**
 * Abstract implementation of the {@link ByteableTest} interface.
 * 
 * @author jnelson
 */
public abstract class AbstractByteableTest implements ByteableTest {

	/**
	 * Return a random instance of the test class.
	 * 
	 * @return a random instance
	 */
	protected abstract Byteable getRandomTestInstance();

	/**
	 * Return the test class
	 * 
	 * @return the test class
	 */
	protected abstract Class<?> getTestClass();

	@Override
	@Test
	public void testSerialization() {
		Byteable object = getRandomTestInstance();
		Assert.assertTrue(Byteables.read(object.getBytes(), getTestClass())
				.equals(object));
	}

}
