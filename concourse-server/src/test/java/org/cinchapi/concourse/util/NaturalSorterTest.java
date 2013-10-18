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

import java.io.File;

import org.cinchapi.concourse.time.Time;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Unit tests for {@link NaturalSorter}.
 * 
 * @author jnelson
 */
public class NaturalSorterTest {

	private File f1;
	private File f2;

	@Rule
	public TestWatcher watcher = new TestWatcher() {

		@Override
		protected void finished(Description description) {
			f1.delete();
			f2.delete();
		}

	};

	@Test
	public void testDiffTimestamp() {
		f1 = getFile("a");
		f2 = getFile("a");
		Assert.assertTrue(NaturalSorter.INSTANCE.compare(f1, f2) < 0);
	}
	
	@Test
	public void testSameTimestampSameExt(){
		String ts = getTimeString();
		f1 = new File(ts + ".a");
		f2 = new File(ts + ".a");
		Assert.assertTrue(NaturalSorter.INSTANCE.compare(f1, f2) == 0);
	}
	
	@Test
	public void testSameTimestampDiffExt(){
		String ts = getTimeString();
		f1 = new File(ts + ".b");
		f2 = new File(ts + ".a");
		Assert.assertTrue(NaturalSorter.INSTANCE.compare(f1, f2) > 0);
	}

	/**
	 * Return a File that is named after the current time string with extension
	 * {@code ext}.
	 * 
	 * @param ext
	 * @return a new File
	 */
	private File getFile(String ext) {
		return new File(getTimeString() + "." + ext);
	}

	/**
	 * Return a string version of the current timestamp.
	 * 
	 * @return the time string
	 */
	private String getTimeString() {
		return Long.toString(Time.now());
	}

}
