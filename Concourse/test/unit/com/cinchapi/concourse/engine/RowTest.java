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

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.engine.Cell;
import com.cinchapi.concourse.engine.EngineBaseTest;
import com.cinchapi.concourse.engine.Row;
import com.cinchapi.concourse.engine.Value;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link Row}
 * 
 * @author jnelson
 */
public class RowTest extends EngineBaseTest {

	@Test
	public void testAdd() {
		Row row = randomNewRow();
		String column = randomString();
		Value value = randomValueForStorage();

		row.add(column, value);
		assertTrue(row.fetch(column).contains(value));

		// add multiple values to a column
		int scale = randomScaleFrequency();
		List<Value> values = Lists.newArrayListWithExpectedSize(scale);
		for (int i = 0; i < scale; i++) {
			Value v = randomValueForStorage();
			while (values.contains(v) || v.equals(value)) {
				v = randomValueForStorage();
			}
			values.add(v);
			row.add(column, v);
		}
		ListIterator<Value> it = values.listIterator();
		while (it.hasNext()) {
			assertTrue(row.fetch(column).contains(it.next()));
		}

		// add multiple columns
		scale = randomScaleFrequency();
		List<String> columns = Lists.newArrayListWithExpectedSize(scale);
		for (int i = 0; i < scale; i++) {
			String c = randomString();
			while (columns.contains(c) || c.equalsIgnoreCase(column)) {
				c = randomString();
			}
			Value v = randomValueForStorage();
			columns.add(c);
			row.add(c, v);
		}
		ListIterator<String> it2 = columns.listIterator();
		while (it2.hasNext()) {
			assertTrue(row.describe().contains(it2.next()));
		}

		// TODO concurrent modification
		String column2 = randomString();
		Value value2 = randomValueForStorage();

	}

	@Test
	public void testColumnSet() throws Exception {
		Row row = randomNewRow();

		// present column set
		int scale = randomScaleFrequency();
		Set<String> columns = Sets.newTreeSet();
		for (int i = 0; i < scale; i++) {
			String column = randomString();
			while (columns.contains(column)) {
				column = randomString();
			}
			columns.add(column);
			row.add(column, randomValueForStorage());
		}
		assertTrue(Sets.symmetricDifference(row.describe(), columns).isEmpty());

		// column set from the past
		long at = Time.now();
		Number rawMaxWaitInMs = 10000000;
		Number scaledMaxWaitInMs = 1000;
		long wait = Math.round(Numbers.scale(
				rand.nextInt(rawMaxWaitInMs.intValue()) + 1, 1, rawMaxWaitInMs,
				1, scaledMaxWaitInMs).doubleValue());
		Thread.sleep(wait);
		Iterator<String> it = columns.iterator();
		Set<String> removed = Sets.newTreeSet();
		while (it.hasNext()) {
			String c = it.next();
			if(randomBoolean()) {
				List<Value> values = row.fetch(c).getValues();
				for (Value v : values) {
					row.remove(c, v);
				}
				it.remove();
				removed.add(c);
			}
		}
		assertEquals(Sets.symmetricDifference(row.describe(at), columns),
				removed);

	}

	@Test
	public void testFetch() {
		Row row = randomNewRow();

		// get nonexistent cell
		String column = randomString();
		assertNull(row.fetch(column));

		// get() returns a cell with all the added values
		List<Value> values = Lists.newArrayList();
		int scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Value value = randomValueForStorage();
			while (values.contains(value)) {
				value = randomValueForStorage();
			}
			values.add(value);
			row.add(column, value);
		}
		Cell cell = row.fetch(column);
		for (Value value : values) {
			assertTrue(cell.contains(value));
		}

		// removing a value is reflected in the returned cell
		Value randomValueFromCell = values.get(rand.nextInt(values.size()));
		row.remove(column, randomValueFromCell);
		assertFalse(row.fetch(column).contains(randomValueFromCell));

	}

}
