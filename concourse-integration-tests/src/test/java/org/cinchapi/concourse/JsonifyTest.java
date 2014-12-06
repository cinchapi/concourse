/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.junit.Assert;
import org.junit.Test;

import com.google.gson.Gson;


/**
 * Unit tests for the {@code jsonify()} API methods.
 * Jsonify takes a list of records and represents each record's data
 * into a JSON formatted string.
 * 
 * @author hyin
 */
public class JsonifyTest extends ConcourseIntegrationTest {

	@Test
	//Boundary cases
	public void testEmptyJsonify() {
		String expected = "{}";
		List<Long> empty = new ArrayList<>();
		String actual = client.jsonify(empty);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testToJson() {
		Map<String, List<Integer>> myMap = new HashMap<>();
		myMap.put("integers", Arrays.asList(1,2,3,4,5));
		myMap.put("123", Arrays.asList(1,2,3));
		String expected = "{\"integers\":[1,2,3,4,5],\"123\":[1,2,3]}";
		Gson gson = new Gson();
		String actual = gson.toJson(myMap);
		Assert.assertEquals(expected, actual);
	}
	
	//Typical cases flag set as true
	@Test
	public void testJsonify() {
		long record1 = 1;
		long record2 = 2;
		long record3 = 3;
		List<Long> recordsList = new ArrayList<Long>();
		recordsList.add(record1);
		recordsList.add(record2);
		recordsList.add(record3);
		client.add("a", 1, record1);
		client.add("a", 2, record1);
		client.add("a", 3, record1);
		client.add("b", 1, record1);
		client.add("b", 2, record1);
		client.add("b", 3, record1);
		client.add("c", 1, record2);
		client.add("c", 2, record2);
		client.add("c", 3, record2);
		client.add("d", 1, record3);
		client.add("d", 2, record3);
		client.add("d", 3, record3);
		String expected = "{\"1\":{\"b\":[2,1,3],\"a\":[2,1,3]}," + 
						  "\"2\":{\"c\":[2,1,3]}," + 
						  "\"3\":{\"d\":[2,1,3]}}";
		String actual = client.jsonify(recordsList);
		Assert.assertEquals(expected, actual);
	}
	
	// Without primary key included
	@Test
	public void testJsonifyFalse() {
		long record1 = 1;
		long record2 = 2;
		long record3 = 3;
		List<Long> recordsList = new ArrayList<Long>();
		recordsList.add(record1);
		recordsList.add(record2);
		recordsList.add(record3);
		client.add("a", 1, record1);
		client.add("a", 2, record1);
		client.add("a", 3, record1);
		client.add("b", 1, record1);
		client.add("b", 2, record1);
		client.add("b", 3, record1);
		client.add("c", 1, record2);
		client.add("c", 2, record2);
		client.add("c", 3, record2);
		client.add("d", 4, record3);
		client.add("d", 5, record3);
		client.add("d", 6, record3);
		String expected = "{\"d\":[6,4,5],\"b\":[2,1,3],"
				+ "\"c\":[2,1,3],"
				+ "\"a\":[2,1,3]}";
		String actual = client.jsonify(recordsList, false);
		Assert.assertEquals(expected, actual);
	}
}
