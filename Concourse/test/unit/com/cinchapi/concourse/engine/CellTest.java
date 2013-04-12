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
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.db.old.Value;
import com.cinchapi.concourse.engine.EngineBaseTest;
import com.cinchapi.concourse.engine.old.Cell;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link Cell}
 * 
 * @author jnelson
 */
public class CellTest extends EngineBaseTest {

	@Test
	public void testAdd() throws Exception {
		Cell cell = randomNewCell();
		Value forStorage = randomValueForStorage();
		Value notForStorage = randomValueNotForStorage();

		// Can add a forStorage value if it does not exist and i get no
		// exception
		cell.add(forStorage);

		// Should get exception for trying to add a value that already exists
		try {
			cell.add(forStorage);
			fail("Was expecting an IllegalStateException");
		}
		catch (IllegalStateException e) {}

		// Cannot add a notForStorage value
		try {
			cell.add(notForStorage);
			fail("Was expecting an IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {}

	}

	@Test
	public void testContains() {
		Value v = randomValueForStorage();
		Cell cell = randomNewCell();
		assertFalse(cell.contains(v));
		cell.add(v);
		assertTrue(cell.contains(v));
	}

	@Test
	public void testCount() {
		Cell cell = randomNewCell();
		int count = randomScaleFrequency();
		for (int i = 0; i < count; i++) {
			int currentCount = cell.count();
			Value v = randomValueForStorage();
			while (cell.contains(v)) {
				v = randomValueForStorage();
			}
			cell.add(v);
			assertEquals(currentCount + 1, cell.count());
		}

		Iterator<Value> it = cell.getValues().iterator();
		while (it.hasNext()) {
			int currentCount = cell.count();
			cell.remove(it.next());
			assertEquals(currentCount - 1, cell.count());
		}
	}

	@Test
	public void testRemove() {
		Cell cell = randomNewCell();
		Value value = randomValueForStorage();
		assertFalse(cell.contains(value));
		cell.add(value);
		assertTrue(cell.contains(value));
		cell.remove(value);
		assertFalse(cell.contains(value));

		// cannot remove value that does not exist
		try {
			cell.remove(value);
			fail("Was expecting an IllegalStateException");
		}
		catch (IllegalStateException e) {}

	}
	
	@Test
	public void testIsEmpty(){
		Cell cell = randomNewCell();
		int scale = randomScaleFrequency();
		assertTrue(cell.isEmpty());
		Value[] values = new Value[scale];
		for(int i = 0; i < scale; i++){
			Value value = null;
			while(value == null || cell.contains(value)){
				value = randomValueForStorage();
			}
			cell.add(value);
			values[i] = value;
			assertFalse(cell.isEmpty());
		}
		
		for(Value value : values){
			assertFalse(cell.isEmpty());
			cell.remove(value);
		}
		
		assertTrue(cell.isEmpty());
	}
	
	@Test
	public void testGetColumn(){
		String column = randomColumnName();
		Cell cell = Cell.newInstance(column);
		assertEquals(column, cell.getColumn());
	}
	
	@Test
	public void testGetValues(){
		Cell cell = randomNewCell();
		int scale = randomScaleFrequency();
		List<Value> values = Lists.newArrayListWithCapacity(scale);
		for(int i = 0; i < scale; i++){
			Value value = null;
			while(value == null || cell.contains(value)){
				value = randomValueForStorage();
			}
			cell.add(value);
			values.add(value);
			assertEquals(values, cell.getValues());
		}
		
		Iterator<Value> it = values.iterator();
		while(it.hasNext()){
			Value value = it.next();
			if(Numbers.isEven(rand.nextInt())){
				cell.remove(value);
				it.remove();
				assertEquals(values, cell.getValues());
			}
		}
		assertEquals(values, cell.getValues());
		
		//get values at specified timestamp
		cell = randomNewCell();
		scale = randomScaleFrequency();
		values = Lists.newArrayListWithCapacity(scale);
		for(int i = 0; i < scale; i++){
			Value value = null;
			while(value == null || cell.contains(value)){
				value = randomValueForStorage();
			}
			cell.add(value);
			values.add(value);
			if(rand.nextInt() % 3 == 0){
				cell.remove(value);
				cell.add(value);
			}
		}
		
		long at = Time.now();
		sleep();
		
		it = values.iterator();
		while(it.hasNext()){
			Value value = it.next();
			if(Numbers.isOdd(rand.nextInt())){
				cell.remove(value);
			}
		}
		int scale2 = randomScaleFrequency();
		for(int i = 0; i < scale2; i++){
			Value value = null;
			while(value == null || cell.contains(value)){
				value = randomValueForStorage();
			}
			cell.add(value);
		}		
		assertEquals(values, cell.getValues(at));
	}
	
	@Test
	public void testGetBytes(){
		Cell c1 = randomPopulatedCell();
		ByteBuffer bytes = ByteBuffer.wrap(c1.getBytes());
		Cell c2 = Cell.fromByteSequence(bytes);
		
		//NOTE: Cell does not define equals() or hashCode()
		assertEquals(c1.getColumn(), c2.getColumn());
		assertEquals(c1.getValues(), c2.getValues());
	}

}
