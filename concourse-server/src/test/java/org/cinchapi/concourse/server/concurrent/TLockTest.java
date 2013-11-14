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
package org.cinchapi.concourse.server.concurrent;

import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TLock}.
 * 
 * @author jnelson
 */
public class TLockTest {

	@Test
	public void testLocksAreTheSame() {
		Object[] components = new Byteable[Math.abs(TestData.getScaleCount())];
		for (int i = 0; i < components.length; i++) {
			components[i] = TestData.getValue();
		}
		TLock a = TLock.grab(components);
		TLock b = TLock.grabWithToken(Token.wrap(components));
		Assert.assertSame(a, b);
	}

	@Test
	public void testGetAliveTime() throws InterruptedException{
		TLock a = TLock.grab(TestData.getObject());
		int sleep = TestData.getScaleCount();
		Thread.sleep(sleep);
		Assert.assertTrue(a.getTimeSinceLastGrab(TimeUnit.MILLISECONDS) >= sleep);
	}
	

}
