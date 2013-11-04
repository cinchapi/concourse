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
package org.cinchapi.concourse.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.transport.TTransportException;
import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

/**
 * 
 * 
 * @author jnelson
 */
public class ConcourseServerTest {

	private static final String SERVER_HOST = "localhost";
	private static final int SERVER_PORT = 1718;
	private static final String SERVER_DATA_HOME = System
			.getProperty("user.home")
			+ File.separator
			+ "concourse_"
			+ Long.toString(Time.now());
	private static final String SERVER_DATABASE_DIRECTORY = SERVER_DATA_HOME
			+ File.separator + "db";
	private static final String SERVER_BUFFER_DIRECTORY = SERVER_DATA_HOME
			+ File.separator + "buffer";

	private ConcourseServer server;
	private Concourse client;
	private Table<Long, String, Set<Object>> data;

	@Rule
	public TestWatcher watcher = new TestWatcher() {

		@Override
		protected void starting(Description description) {
			try {
				server = new ConcourseServer(SERVER_PORT,
						SERVER_BUFFER_DIRECTORY, SERVER_DATABASE_DIRECTORY);
			}
			catch (TTransportException e1) {
				throw Throwables.propagate(e1);
			}
			Thread t = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						server.start();
					}
					catch (TTransportException e) {
						throw Throwables.propagate(e);
					}

				}

			});
			t.start();
			client = new Concourse.Client(SERVER_HOST, SERVER_PORT, "admin",
					"admin");
			data = HashBasedTable.<Long, String, Set<Object>> create();

		}

		@Override
		protected void finished(Description description) {
			client.exit();
			server.stop();
			FileSystem.deleteDirectory(SERVER_DATA_HOME);
		}

	};

	@Test
	public void testAddAndFetch() {
		setup(data, client);
		for (long record : data.rowKeySet()) {
			Map<String, Set<Object>> map = data.row(record);
			for (String key : map.keySet()) {
				Assert.assertEquals(client.fetch(key, record), map.get(key));
			}
		}
	}

	@Test
	public void testAddAndDescribe() {
		setup(data, client);
		for (long record : data.rowKeySet()) {
			Assert.assertEquals(client.describe(record), data.row(record)
					.keySet());
		}
	}

	@Test
	public void testReproCON_3() throws IOException, InterruptedException {
		BufferedReader reader = new BufferedReader(new FileReader(this
				.getClass().getResource("/words.txt").getFile()));
		long record = 0;
		String line;
		String key = "strings";
		while ((line = reader.readLine()) != null) {
			client.add(key, line, record);
			record++;
		}
		reader.close();
		Thread.sleep(10000); //let some of the data transport
		record = 0;
		Set<Long> expected = Sets.newLinkedHashSet();
		while (record < 1000) {
			client.add("count", record, record);
			if(record > 0) {
				expected.add(record);
			}
			record++;
		}
		Thread.sleep(5000); //let some of the data transport
		Set<Long> actual = client.find("count", Operator.GREATER_THAN, 0);
		Assert.assertEquals(expected, actual);
	}

	/**
	 * Add random data to {@code data} and {@code concourse}.
	 * 
	 * @param data
	 * @param concourse
	 */
	private void setup(Table<Long, String, Set<Object>> data,
			Concourse concourse) {
		int count = TestData.getScaleCount();
		for (int i = 0; i < count; i++) {
			long row = TestData.getLong();
			String key = TestData.getString();
			Object value = TestData.getObject();
			Set<Object> values = data.get(row, key);
			values = values == null ? Sets.<Object> newLinkedHashSet() : values;
			values.add(value);
			data.put(row, key, values);
			concourse.add(key, value, row);
		}
	}

}
