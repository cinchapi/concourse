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
package org.cinchapi.concourse.server.engine;

import junit.framework.Assert;

import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.tools.Numbers;
import org.cinchapi.common.util.Random;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;
import org.junit.Test;

/**
 * Unit tests for {@link Value}.
 * 
 * @author jnelson
 */
public class ValueTest extends StorableTest {

	@Test
	public void testForStorageAndNotForStorageLogicalCompareTo() {
		int num = Math.abs(Random.getInt());
		Value a = Value.forStorage(Convert.javaToThrift(num));
		Value b = Value.forStorage(Convert.javaToThrift(num - 1));
		Assert.assertTrue(a.compareToLogically(b) > 0);
	}

	@Test
	public void testGetBooleanQuantity() {
		assertQuantity(Convert.javaToThrift(Random.getBoolean()));
	}

	@Test
	public void testGetBooleanType() {
		assertType(Value.forStorage(Convert.javaToThrift(Random.getBoolean())),
				Type.BOOLEAN);
	}

	@Test
	public void testGetDoubleQuantity() {
		assertQuantity(Convert.javaToThrift(Random.getDouble()));
	}

	@Test
	public void testGetDoubleType() {
		assertType(Value.forStorage(Convert.javaToThrift(Random.getDouble())),
				Type.DOUBLE);
	}

	@Test
	public void testGetFloatQuantity() {
		assertQuantity(Convert.javaToThrift(Random.getFloat()));
	}

	@Test
	public void testGetFloatType() {
		assertType(Value.forStorage(Convert.javaToThrift(Random.getFloat())),
				Type.FLOAT);
	}

	@Test
	public void testGetIntQuantity() {
		assertQuantity(Convert.javaToThrift(Random.getInt()));
	}

	@Test
	public void testGetIntType() {
		assertType(Value.forStorage(Convert.javaToThrift(Random.getInt())),
				Type.INTEGER);
	}

	@Test
	public void testGetLinkQuantity() {
		assertQuantity(Convert.javaToThrift(Link.to(Random.getLong())));
	}

	@Test
	public void testGetLinkType() {
		assertType(Value.forStorage(Convert.javaToThrift(Link.to(Random
				.getLong()))), Type.LINK);
	}

	@Test
	public void testGetLongQuantity() {
		assertQuantity(Convert.javaToThrift(Random.getLong()));
	}

	@Test
	public void testGetLongType() {
		assertType(Value.forStorage(Convert.javaToThrift(Random.getLong())),
				Type.LONG);
	}

	@Test
	public void testGetStringQuantity() {
		assertQuantity(Convert.javaToThrift(Random.getString()));
	}

	@Test
	public void testGetStringType() {
		assertType(Value.forStorage(Convert.javaToThrift(Random.getString())),
				Type.STRING);
	}

	@Test
	public void testLogicalCompareTo() {
		long num = Math.abs(Random.getInt());
		Value a = Value.forStorage(Convert.javaToThrift(num));
		Value b = Value.forStorage(Convert.javaToThrift(num - 1));
		Assert.assertTrue(a.compareToLogically(b) > 0);
	}

	@Test
	public void testNumberCompareToInteroperability() {
		Value a = Value.forStorage(Convert.javaToThrift(Random
				.getNegativeNumber()));
		Value b = Value.forStorage(Convert.javaToThrift(Random
				.getPositiveNumber()));
		while (Numbers.isEqualTo(
				(Number) Convert.thriftToJava(a.getQuantity()),
				(Number) Convert.thriftToJava(b.getQuantity()))) {
			b = Value
					.forStorage(Convert.javaToThrift(Random.getPositiveNumber()));
		}
		Assert.assertTrue(a.compareToLogically(b) < 0);
	}

	@Test
	public void testTemporalComapreTo() {
		Value a = getForStorage();
		Value b = getForStorage();
		Assert.assertTrue(a.compareTo(b) > 0);
	}

	@Override
	protected Storable copy(Byteable object) {
		Value value = (Value) object;
		return value.isForStorage() ? Value.forStorage(value.getQuantity())
				: Value.notForStorage(value.getQuantity());
	}

	@Override
	protected Value getForStorage() {
		return Value.forStorage(Convert.javaToThrift(Random.getObject()));
	}

	@Override
	protected Value getNotForStorage() {
		return Value.notForStorage(Convert.javaToThrift(Random.getObject()));
	}

	/**
	 * Assert that an Value created from {@code object} has a quantity that is
	 * equal to {@code object}.
	 * 
	 * @param object
	 */
	private void assertQuantity(TObject object) {
		log.info("Using quantity '{}'", object);
		Value value = Value.forStorage(object);
		Assert.assertEquals(object, value.getQuantity());
	}

	/**
	 * Assert that the type of {@code value} is equal to {@code type}.
	 * 
	 * @param value
	 * @param type
	 */
	private void assertType(Value value, Type type) {
		log.info("Using value '{}' and type '{}'", value, type);
		Assert.assertEquals(type, value.getType());
	}

}
