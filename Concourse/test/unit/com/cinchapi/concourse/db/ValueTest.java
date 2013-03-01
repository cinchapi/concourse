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
import org.junit.Test;

/**
 * Unit tests for {@link Value}.
 * 
 * @author jnelson
 */
public class ValueTest extends DbBaseTest {

	@Test
	public void testCompare() {
		// default compare should compare time in descending order
		Value a = randomValueForStorage();
		Value b = randomValueForStorage();
		Value c = randomValueForStorage();

		assertTrue(a.compareTo(b) > 0);
		assertTrue(a.compareTo(c) > 0);
		assertTrue(b.compareTo(a) < 0);
		assertTrue(b.compareTo(c) > 0);
		assertTrue(c.compareTo(a) < 0);
		assertTrue(c.compareTo(b) < 0);

		// a notForStorage value should always be equal to an forStorage one
		// with the same quantity
		Value d = new ValueBuilder().setForStorage(false)
				.setQuantity(a.getQuantity()).build();
		assertTrue(d.compareTo(a) == 0);

		// a notForStorage value should always be "greater" than an forStorage
		// value with a different raw value
		assertTrue(d.compareTo(b) > 0);
		assertTrue(d.compareTo(c) > 0);

		// TODO logical number comparison should work regardless of type
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

	// @Test
	// public void testBenchmark() throws IOException {
	// log.info("Running testBenchmark");
	// NumberFormat format = NumberFormat.getNumberInstance();
	// format.setGroupingUsed(true);
	//
	// //Test write to disk time
	// int size = 100000;
	// TimeUnit unit = TimeUnit.MILLISECONDS;
	//
	// Value[] values = new Value[size];
	// long numBytes = 0;
	// log.info("Creating {} Values...", format.format(size));
	// for (int i = 0; i < size; i++) {
	// Value value = Value.forStorage(getRandomValue());
	// numBytes += value.size();
	// values[i] = value;
	// }
	//
	// String filePath = "test/value_test_benchmark.tst";
	// RandomAccessFile file = new RandomAccessFile(filePath, "rw");
	// Timer t = new Timer();
	// t.start();
	// log.info("Writing {} total BYTES to {}...", format.format(numBytes),
	// filePath);
	// for (int i = 0; i < size; i++) {
	// Value value = values[i];
	// value.writeTo(file.getChannel());
	// }
	// long elapsed = t.stop(unit);
	// long bytesPerUnit = numBytes / elapsed;
	//
	// log.info("Total write time was {} {} with {} bytes written per {}",
	// format.format(elapsed), unit, format.format(bytesPerUnit),
	// unit.toString().substring(0, unit.toString().length() - 1));
	// }

}
