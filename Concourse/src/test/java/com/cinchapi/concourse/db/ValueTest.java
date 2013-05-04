/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.db;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

/**
 * Unit tests for {@link Value}.
 * 
 * @author jnelson
 */
public class ValueTest extends DatabaseTest {

	@Test
	public void testCompare() {
		// default compare should compare time in descending order
		Value a = randomValueForStorage();
		Value b = randomValueForStorage();
		while (a.getQuantity().equals(b.getQuantity())) {
			b = randomValueForStorage();
		}
		Value c = randomValueForStorage();
		while (c.getQuantity().equals(b.getQuantity())
				|| c.getQuantity().equals(a.getQuantity())) {
			c = randomValueForStorage();
		}

		assertTrue(a.compareTo(b) > 0);
		assertTrue(a.compareTo(c) > 0);
		assertTrue(b.compareTo(a) < 0);
		assertTrue(b.compareTo(c) > 0);
		assertTrue(c.compareTo(a) < 0);
		assertTrue(c.compareTo(b) < 0);

		// a notForStorage value should always be equal to an forStorage one
		// with the same quantity using default comparison
		Value d = Value.notForStorage(a.getQuantity());
		assertTrue(d.compareTo(a) == 0);

		// a notForStorage value should always be "greater" than an forStorage
		// value with a different raw value using default comparison
		assertTrue(d.compareTo(b) > 0);
		assertTrue(d.compareTo(c) > 0);

		// logical comparison of numbers, regardless of type and timestamp`
		Number p = randomPositiveLong();
		Value[] pos = new Value[4];
		pos[0] = Value.forStorage(p.longValue());
		int intValP = p.intValue() < 0 ? -1 * p.intValue() : p.intValue(); // sometimes
																			// a
																			// pos
																			// long
																			// as
																			// an
																			// int
																			// is
																			// cast
																			// negative
		pos[1] = Value.forStorage(intValP);
		pos[2] = Value.forStorage(p.doubleValue());
		pos[3] = Value.forStorage(p.floatValue());

		Number n = randomNegativeLong();
		Value[] neg = new Value[4];
		neg[0] = Value.forStorage(n.longValue());
		int intValN = n.intValue() > 0 ? -1 * n.intValue() : n.intValue(); // sometimes
																			// a
																			// neg
																			// long
																			// as
																			// an
																			// int
																			// is
																			// cast
																			// positive
		neg[1] = Value.forStorage(intValN);
		neg[2] = Value.forStorage(n.doubleValue());
		neg[3] = Value.forStorage(n.floatValue());

		for (int i = 0; i < pos.length; i++) { // all positives should be
												// greater than the negatives
			for (int j = 0; j < neg.length; j++) {
				assertTrue(pos[i].compareToLogically(neg[j]) > 0);
				assertTrue(neg[j].compareToLogically(pos[i]) < 0);
			}
		}

		// logical comparison of strings that look like numbers
		assertTrue(Value.forStorage(p.toString()).compareToLogically(
				Value.forStorage(n.toString())) > 0);

		// logical comparison of strings that look like booleans
		assertTrue(Value.forStorage("true").compareToLogically(
				Value.forStorage(true)) == 0);
		assertTrue(Value.forStorage("false").compareToLogically(
				Value.forStorage(false)) == 0);

		// logical comparison of strings
		String s1 = randomString();
		String s2 = randomString();
		assertEquals(s1.compareTo(s2),
				Value.forStorage(s1).compareToLogically(Value.forStorage(s2)));

		// logical comparison regardless of storage type
		Value v1 = randomValueForStorage();
		Value v2 = randomValueForStorage();
		Value v2nfs = Value.notForStorage(v2.getQuantity());
		assertEquals(v1.compareToLogically(v2), v1.compareToLogically(v2nfs));
	}

	@Test
	public void testGetBytes() {
		Value v1 = randomValueForStorage();
		Value v2 = Value.fromByteSequence(ByteBuffer.wrap(v1.getBytes()));
		assertEquals(v2, v1);
		assertEquals(v2.getTimestamp(), v1.getTimestamp()); // equality is only
															// based on quantity
															// and type, so i'm
															// checking the
															// timestamp
															// explicitly
	}

	@Test
	public void testEquals() {
		Value v1 = randomValueForStorage();
		Value v2 = randomValueForStorage();
		while (v2.getQuantity().equals(v1.getQuantity())) {
			v2 = randomValueForStorage();
		}
		Value v3 = Value.forStorage(v1.getQuantity());
		assertTrue(v1.equals(v3));
		assertFalse(v1.equals(v2));
		assertFalse(v2.equals(v3));
	}

	@Test
	public void testGetQuantity() { // this is an important test because the
									// quantity is represented back and forth as
									// a byte buffer
		Object quantity = randomObject();
		Value v = Value.forStorage(quantity);
		assertEquals(v.getQuantity(), quantity);
	}

	@Test
	public void testGetTimestamp() {
		// testing for correctness here is impractical because we can't
		// anticipate time latency through method calls, but we can ensure that
		// there are no duplicate timestamps with forStorage values
		int size = randomScaleFrequency();
		List<Value> values = Lists.newArrayList();
		for (int i = 0; i < size; i++) {
			values.add(randomValueForStorage());
		}
		Iterator<Value> it1 = values.iterator();
		Iterator<Value> it2 = values.iterator();
		while (it1.hasNext()) {
			Value v1 = it1.next();
			while (it2.hasNext()) {
				Value v2 = it2.next();
				if(!v1.equals(v2)) {
					assertFalse(Longs.compare(v1.getTimestamp(),
							v2.getTimestamp()) == 0);
				}
			}
		}

		// test that notForStorage values all have the same timestamp
		size = randomScaleFrequency();
		values = Lists.newArrayList();
		for (int i = 0; i < size; i++) {
			values.add(randomValueNotForStorage());
		}
		it1 = values.iterator();
		it2 = values.iterator();
		while (it1.hasNext()) {
			Value v1 = it1.next();
			while (it2.hasNext()) {
				Value v2 = it2.next();
				assertTrue(Longs.compare(v1.getTimestamp(), v2.getTimestamp()) == 0);
			}
		}
	}

	@Test
	public void testGetType() {
		Value intVal = Value.forStorage(randomInt());
		Value longVal = Value.forStorage(randomLong());
		Value doubleVal = Value.forStorage(randomDouble());
		Value floatVal = Value.forStorage(randomFloat());
		Value stringVal = Value.forStorage(randomString());
		Value boolVal = Value.forStorage(randomBoolean());
		Value keyVal = Value.forStorage(randomKeyNotForStorage());
		assertEquals(Type.INTEGER.toString(), intVal.getType());
		assertEquals(Type.LONG.toString(), longVal.getType());
		assertEquals(Type.DOUBLE.toString(), doubleVal.getType());
		assertEquals(Type.FLOAT.toString(), floatVal.getType());
		assertEquals(Type.STRING.toString(), stringVal.getType());
		assertEquals(Type.BOOLEAN.toString(), boolVal.getType());
		assertEquals(Type.RELATION.toString(), keyVal.getType());
	}

	@Test
	public void testForStorageStatus() {
		Value forStorage = randomValueForStorage();
		Value notForStorage = randomValueNotForStorage();
		assertTrue(forStorage.isForStorage());
		assertFalse(forStorage.isNotForStorage());
		assertTrue(notForStorage.isNotForStorage());
		assertFalse(notForStorage.isForStorage());
	}

	@Test
	public void testSize() {
		Value v = randomValueForStorage();
		assertEquals(v.size(), v.getBytes().length);
	}

}
