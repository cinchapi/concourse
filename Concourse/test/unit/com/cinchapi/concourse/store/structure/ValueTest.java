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
package com.cinchapi.concourse.store.structure;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.cinchapi.concourse.BaseTest;
import com.cinchapi.concourse.structure.Value;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

/**
 * Unit tests for {@link Value}.
 * 
 * @author jnelson
 */
public class ValueTest extends BaseTest {

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
		Value d = new ValueBuilder().forStorage(false)
				.quantity(a.getQuantity()).build();
		assertTrue(d.compareTo(a) == 0);

		// a notForStorage value should always be "greater" than an forStorage
		// value with a different raw value using default comparison
		assertTrue(d.compareTo(b) > 0);
		assertTrue(d.compareTo(c) > 0);

		// logical comparison of numbers, regardless of type and timestamp`
		Number p = randomPositiveLong();
		Value[] pos = new Value[4];
		pos[0] = new ValueBuilder().quantity(p.longValue()).build();
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
		pos[1] = new ValueBuilder().quantity(intValP).build();
		pos[2] = new ValueBuilder().quantity(p.doubleValue()).build();
		pos[3] = new ValueBuilder().quantity(p.floatValue()).build();

		Number n = randomNegativeLong();
		Value[] neg = new Value[4];
		neg[0] = new ValueBuilder().quantity(n.longValue()).build();
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
		neg[1] = new ValueBuilder().quantity(intValN).build();
		neg[2] = new ValueBuilder().quantity(n.doubleValue()).build();
		neg[3] = new ValueBuilder().quantity(n.floatValue()).build();

		for (int i = 0; i < pos.length; i++) { // all positives should be
												// greater than the negatives
			for (int j = 0; j < neg.length; j++) {
				assertTrue(pos[i].compareToLogically(neg[j]) > 0);
				assertTrue(neg[j].compareToLogically(pos[i]) < 0);
			}
		}

		// logical comparison of strings that look like numbers
		assertTrue(new ValueBuilder()
				.quantity(p.toString())
				.build()
				.compareToLogically(
						new ValueBuilder().quantity(n.toString()).build()) > 0);

		// logical comparison of strings that look like booleans
		assertTrue(new ValueBuilder().quantity("true").build()
				.compareToLogically(new ValueBuilder().quantity(true).build()) == 0);
		assertTrue(new ValueBuilder().quantity("false").build()
				.compareToLogically(new ValueBuilder().quantity(false).build()) == 0);

		// logical comparison of strings
		String s1 = randomString();
		String s2 = randomString();
		assertEquals(s1.compareTo(s2), new ValueBuilder().quantity(s1).build()
				.compareToLogically(new ValueBuilder().quantity(s2).build()));

		// logical comparison regardless of storage type
		Value v1 = randomValueForStorage();
		Value v2 = randomValueForStorage();
		Value v2nfs = new ValueBuilder().quantity(v2.getQuantity())
				.forStorage(false).build();
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
		Value v3 = new ValueBuilder().quantity(v1.getQuantity()).build();
		assertTrue(v1.equals(v3));
		assertFalse(v1.equals(v2));
		assertFalse(v2.equals(v3));
	}

	@Test
	public void testGetQuantity() { // this is an important test because the
									// quantity is represented back and forth as
									// a byte buffer
		Object quantity = randomObject();
		Value v = new ValueBuilder().quantity(quantity).build();
		assertEquals(v.getQuantity(), quantity);
	}

	@Test
	public void testGetTimestamp() {
		// testing for correctness here is impractical because we can't
		// anticipate time latency through method calls, but we can ensure that
		// there are no duplicate timestamps with forStorage values
		int size = getScaleFrequency();
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
		size = getScaleFrequency();
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
		Value intVal = new ValueBuilder().quantity(randomInt()).build();
		Value longVal = new ValueBuilder().quantity(randomLong()).build();
		Value doubleVal = new ValueBuilder().quantity(randomDouble()).build();
		Value floatVal = new ValueBuilder().quantity(randomFloat()).build();
		Value stringVal = new ValueBuilder().quantity(randomString()).build();
		Value boolVal = new ValueBuilder().quantity(randomBoolean()).build();
		Value keyVal = new ValueBuilder().quantity(randomKey()).build();
		assertEquals(Value.Type.INTEGER.toString(), intVal.getType());
		assertEquals(Value.Type.LONG.toString(), longVal.getType());
		assertEquals(Value.Type.DOUBLE.toString(), doubleVal.getType());
		assertEquals(Value.Type.FLOAT.toString(), floatVal.getType());
		assertEquals(Value.Type.STRING.toString(), stringVal.getType());
		assertEquals(Value.Type.BOOLEAN.toString(), boolVal.getType());
		assertEquals(Value.Type.RELATION.toString(), keyVal.getType());
	}

	@Test
	public void testForStorageStatus() {
		Value forStorage = new ValueBuilder().forStorage(true).build();
		Value notForStorage = new ValueBuilder().forStorage(false).build();
		assertTrue(forStorage.isForStorage());
		assertFalse(forStorage.isNotForStorage());
		assertTrue(notForStorage.isNotForStorage());
		assertFalse(notForStorage.isForStorage());
	}
	
	@Test
	public void testSize(){
		Value v = randomValueForStorage();
		assertEquals(v.size(), v.getBytes().length);
	}

}
