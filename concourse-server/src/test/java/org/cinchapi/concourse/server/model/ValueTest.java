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
package org.cinchapi.concourse.server.model;

import org.cinchapi.concourse.server.io.StorableTest;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * Unit tests for {@link Value}
 * 
 * @author jnelson
 */
@RunWith(Theories.class)
public class ValueTest extends StorableTest {

	public static @DataPoints
	Object[] theories = { false, TestData.getDouble(), TestData.getFloat(),
			TestData.getInt(), TestData.getLong(), TestData.getString() };
	
	@Test
	public void testCompareToForStorageAndForStorage(){
		Value v1 = TestData.getValueForStorage();
		Value v2 = TestData.getValueForStorage();
		Assert.assertTrue(v1.compareTo(v2) > 0);
	}
	
	@Test
	public void testCompareToForStorageAndNotForStorage(){
		Value v1 = TestData.getValueForStorage();
		Value v2 = TestData.getValueNotForStorage();
		Assert.assertTrue(v1.compareTo(v2) < 0);
	}

	@Test
	@Theory
	public void testCompareToLogically(Object q1) {
		Object q2 = increase(q1);
		Value v1 = Value.forStorage(Convert.javaToThrift(q1));
		Value v2 = Value.forStorage(Convert.javaToThrift(q2));
		Assert.assertTrue(v1.compareToLogically(v2) < 0);
	}

	@Test
	public void testCompareToLogicallyNumbersDiffType() {
		Number o1 = TestData.getNumber();
		Number o2 = null;
		while (o2 == null || o2.getClass() == o1.getClass()
				|| Numbers.isEqualTo(o1, o2)) {
			o2 = TestData.getNumber();
		}
		Assert.assertEquals(
				Numbers.isGreaterThan(o1, o2),
				Value.forStorage(Convert.javaToThrift(o1)).compareToLogically(
						Value.forStorage(Convert.javaToThrift(o2))) > 0);
	}

	@Override
	protected Value[] getForStorageAndNotForStorageVersionOfObject() {
		TObject qty = TestData.getTObject();
		return new Value[] { Value.forStorage(qty), Value.notForStorage(qty) };
	}

	@Override
	protected Value getForStorageInstance() {
		return TestData.getValueForStorage();
	}

	@Override
	protected Value getNotForStorageInstance() {
		return TestData.getValueNotForStorage();
	}

	@Override
	protected Class<Value> getTestClass() {
		return Value.class;
	}

	/**
	 * Return a random number of type {@code clazz}.
	 * 
	 * @param clazz
	 * @return a random Number
	 */
	private Number getRandomNumber(Class<? extends Number> clazz) {
		if(clazz == Integer.class) {
			return TestData.getInt();
		}
		else if(clazz == Long.class) {
			return TestData.getLong();
		}
		else if(clazz == Double.class) {
			return TestData.getDouble();
		}
		else {
			return TestData.getFloat();
		}
	}

	/**
	 * Return an object that is logically greater than {code obj}.
	 * 
	 * @param obj
	 * @return the increased object
	 */
	@SuppressWarnings("unchecked")
	private Object increase(Object obj) {
		if(obj instanceof Boolean) {
			return !(boolean) obj;
		}
		else if(obj instanceof String) {
			StringBuilder sb = new StringBuilder();
			for (char c : ((String) obj).toCharArray()) {
				sb.append(++c);
			}
			return sb.toString();
		}
		else {
			Number n = null;
			while (n == null || Numbers.isGreaterThan((Number) obj, n)) {
				n = getRandomNumber((Class<? extends Number>) obj.getClass());
			}
			return n;
		}
	}

}
