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

import java.lang.reflect.Method;

import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Throwables;

/**
 * Tests for {@link Byteable} objects.
 * 
 * @author jnelson
 */
public abstract class ByteableTest {

	/**
	 * Return a random instance of the test class defined in
	 * {@link #getTestClass()}. This method assumes that there is a method in
	 * {@link TestData} that takes no arguments and returns the appropriate
	 * type.
	 * 
	 * @return a random instance
	 */
	protected Byteable getRandomTestInstance() {
		try {
			for (Method method : TestData.class.getMethods()) {
				if(method.getReturnType() == getTestClass()
						&& method.getParameterTypes().length == 0) {
					return (Byteable) method.invoke(null);
				}
			}
			throw new IllegalStateException(
					"There is no method in TestData that takes no parameters and returns a "
							+ getTestClass());
		}
		catch (Exception e) {
			throw Throwables.propagate(e);
		}

	}

	/**
	 * Return the test class
	 * 
	 * @return the test class
	 */
	protected abstract Class<? extends Byteable> getTestClass();

	@Test
	public void testSerialization() {
		Byteable object = getRandomTestInstance();
		Assert.assertTrue(Byteables.read(object.getBytes(), getTestClass())
				.equals(object));
	}

}
