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
import org.cinchapi.common.util.Tests;
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
		int num = Math.abs(Tests.getInt());
		Value a = Value.forStorage(Convert.javaToThrift(num));
		Value b = Value.forStorage(Convert.javaToThrift(num - 1));
		Assert.assertTrue(a.compareToLogically(b) > 0);
	}

	@Test
	public void testGetBooleanQuantity() {
		assertQuantity(Convert.javaToThrift(Tests.getBoolean()));
	}

	@Test
	public void testGetBooleanType() {
		assertType(Value.forStorage(Convert.javaToThrift(Tests.getBoolean())),
				Type.BOOLEAN);
	}

	@Test
	public void testGetDoubleQuantity() {
		assertQuantity(Convert.javaToThrift(Tests.getDouble()));
	}

	@Test
	public void testGetDoubleType() {
		assertType(Value.forStorage(Convert.javaToThrift(Tests.getDouble())),
				Type.DOUBLE);
	}

	@Test
	public void testGetFloatQuantity() {
		assertQuantity(Convert.javaToThrift(Tests.getFloat()));
	}

	@Test
	public void testGetFloatType() {
		assertType(Value.forStorage(Convert.javaToThrift(Tests.getFloat())),
				Type.FLOAT);
	}

	@Test
	public void testGetIntQuantity() {
		assertQuantity(Convert.javaToThrift(Tests.getInt()));
	}

	@Test
	public void testGetIntType() {
		assertType(Value.forStorage(Convert.javaToThrift(Tests.getInt())),
				Type.INTEGER);
	}

	@Test
	public void testGetLinkQuantity() {
		assertQuantity(Convert.javaToThrift(Link.to(Tests.getLong())));
	}

	@Test
	public void testGetLinkType() {
		assertType(Value.forStorage(Convert.javaToThrift(Link.to(Tests
				.getLong()))), Type.LINK);
	}

	@Test
	public void testGetLongQuantity() {
		assertQuantity(Convert.javaToThrift(Tests.getLong()));
	}

	@Test
	public void testGetLongType() {
		assertType(Value.forStorage(Convert.javaToThrift(Tests.getLong())),
				Type.LONG);
	}

	@Test
	public void testGetStringQuantity() {
		assertQuantity(Convert.javaToThrift(Tests.getString()));
	}

	@Test
	public void testGetStringType() {
		assertType(Value.forStorage(Convert.javaToThrift(Tests.getString())),
				Type.STRING);
	}

	@Test
	public void testLogicalCompareTo() {
		long num = Math.abs(Tests.getInt());
		Value a = Value.forStorage(Convert.javaToThrift(num));
		Value b = Value.forStorage(Convert.javaToThrift(num - 1));
		Assert.assertTrue(a.compareToLogically(b) > 0);
	}

	@Test
	public void testNumberCompareToInteroperability() {
		Value a = Value.forStorage(Convert.javaToThrift(Tests
				.getNegativeNumber()));
		Value b = Value.forStorage(Convert.javaToThrift(Tests
				.getPositiveNumber()));
		while (Numbers.isEqualTo(
				(Number) Convert.thriftToJava(a.getQuantity()),
				(Number) Convert.thriftToJava(b.getQuantity()))) {
			b = Value
					.forStorage(Convert.javaToThrift(Tests.getPositiveNumber()));
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
		return Value.forStorage(Convert.javaToThrift(Tests.getObject()));
	}

	@Override
	protected Value getNotForStorage() {
		return Value.notForStorage(Convert.javaToThrift(Tests.getObject()));
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
