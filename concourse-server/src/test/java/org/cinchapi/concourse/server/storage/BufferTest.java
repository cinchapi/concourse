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
package org.cinchapi.concourse.server.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;

import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Test;

import com.google.common.base.Stopwatch;

/**
 * Unit tests for {@link Buffer}.
 * 
 * @author jnelson
 */
public class BufferTest extends LimboTest {

	private String current;

	@Override
	protected Buffer getStore() {
		current = TestData.DATA_DIR + File.separator + Time.now();
		return new Buffer(current);
	}

	@Override
	protected void cleanup(Store store) {
		FileSystem.deleteDirectory(current);
	}

	public class Fake {

		ByteBuffer bytes;
		long number;

		public Fake(ByteBuffer bytes, long number) {
			this.bytes = bytes;
			this.number = number;
		}
	}

	@Test
	public void testBufferInsertStringBenchmark() throws IOException {
//		Buffer buffer = (Buffer) store;
//		URL url = this.getClass().getResource("/words.txt");
//		File file = new File(url.getFile());
//		BufferedReader reader = new BufferedReader(new FileReader(file));
//		String line;
//		long record = 0;
//		Stopwatch watch = new Stopwatch();
//		watch.start();
//		while ((line = reader.readLine()) != null) {
//			String key = "strings";
//			buffer.insert(Write.add(key, Convert.javaToThrift(line), record));
//			record++;
//		}
//		watch.stop();
//		log.info("String Benchmark: {} ms", watch.elapsedMillis());
//		reader.close();
	}

	@Test
	public void testBufferInsertLongBenchmark() {
		Buffer buffer = (Buffer) store;
		String key = "count";
		long i = 0;
		Stopwatch watch = new Stopwatch();
		watch.start();
		while (i < 1000) {
			buffer.insert(Write.add(key, Convert.javaToThrift(i), i));
			i++;
		}
		watch.stop();
		log.info("Long Benchmark: {} ms", watch.elapsedMillis());
	}

}
