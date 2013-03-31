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
package com.cinchapi.concourse.engine;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.cinchapi.concourse.engine.EngineBaseTest;
import com.cinchapi.concourse.engine.Write;
import com.cinchapi.concourse.engine.WriteType;

/**
 * Unit tests for {@link Write}.
 * 
 * @author jnelson
 */
public class WriteTest extends EngineBaseTest {

	@Test
	public void testIsIdenticalTo() {
		String column = randomColumnName();
		Object value = randomObject();
		long row = randomLong();
		WriteType type1 = WriteType.ADD;
		WriteType type2 = WriteType.REMOVE;
		Write write1a = Write.forStorage(column, value, row, type1);
		Write write1b = Write.forStorage(column, value, row, type1);
		Write write1c = Write.notForStorage(column, value, row);
		Write write2 = Write.forStorage(column, value, row, type2);
		assertTrue(write1a.isIdenticalTo(write1b));
		assertFalse(write1a.isIdenticalTo(write1c));
		assertFalse(write1a.isIdenticalTo(write2));
	}

	@Test
	public void testEquals() {
		// storage status should not affect equality
		String column = randomColumnName();
		Object value = randomObject();
		long row = randomLong();
		Write wfs = Write
				.forStorage(column, value, row, WriteType.values()[rand
						.nextInt(WriteType.values().length - 1)]); // using -1
																	// so that
																	// WriteType.NOT_FOR_STORAGE
																	// isn't
																	// picked
		Write wnfs = Write.notForStorage(column, value, row);
		assertEquals(wfs, wnfs);

		// write type should not affect equality
		Write add = Write.forStorage(column, value, row, WriteType.ADD);
		Write remove = Write.forStorage(column, value, row, WriteType.REMOVE);
		assertEquals(add, remove);
	}

	@Test
	public void testGetBytes() {
		Write w1 = randomWriteForStorage();
		ByteBuffer bytes = ByteBuffer.wrap(w1.getBytes());
		Write w2 = Write.fromByteSequence(bytes);
		assertEquals(w1, w2);
		assertTrue(w1.isIdenticalTo(w2));
	}

}
