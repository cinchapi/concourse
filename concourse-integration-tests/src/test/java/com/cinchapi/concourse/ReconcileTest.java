/*
 * Licensed to Cinchapi Inc, under one or more contributor license 
 * agreements. See the NOTICE file distributed with this work for additional 
 * information regarding copyright ownership. Cinchapi Inc. licenses this 
 * file to you under the Apache License, Version 2.0 (the "License"); you may 
 * not use this file except in compliance with the License. You may obtain a 
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Sets;

/*
 * Unit test for API method that sets the values of 
 * the key in record exactly same as the input values
 */
public class ReconcileTest extends ConcourseIntegrationTest {
	
	@Test
	public void testReconcileEmptyValues() {
		//client.add("foo", 2000, 17);
		//client.add("foo", -2000, 17);
		client.reconcile("foo", 17, Sets.newHashSet());
		Assert.assertTrue(client.select("foo", 17).isEmpty());
	}
	
	@Test
	public void testReconcile() {
		String field = "testKey"; // key name
		long r = 1; 			  // record
		client.add(field, 'A', r);
		client.add(field, 'C', r);
		client.add(field, 'D', r);
		client.add(field, 'E', r);
		client.add(field, 'F', r);
		
		char[] chars = {'A', 'B', 'D', 'G'};
		Set<Character> values = Sets.newHashSet();
		for (char c: chars) {
			values.add(c);
		}
		client.reconcile(field, r, values);
		Assert.assertEquals(client.select(field, r), values);
	}
	
	@Test (expected = IllegalArgumentException.class)
	public void testReconcileDuplicates() {
		Set<Double> values = Sets.newHashSet();
		values.add(1.5);
		values.add(1.5);
		client.reconcile("testKey", 5, values);
	}
}
