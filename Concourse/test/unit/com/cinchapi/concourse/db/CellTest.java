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

import java.util.Iterator;

import org.junit.Test;

import com.cinahpi.concourse.BaseTest;
import com.cinchapi.concourse.store.perm.Cell;
import com.cinchapi.concourse.store.perm.Value;

/**
 * Unit tests for {@link Cell}
 * 
 * @author jnelson
 */
public class CellTest extends BaseTest {

	@Test
	public void testAdd() throws Exception {
		Cell cell = randomNewCell();
		Value forStorage = randomValueForStorage();
		Value notForStorage = randomValueNotForStorage();

		// Can add a forStorage value if it does not exist and i get no exception 
		cell.add(forStorage);

		// Should get exception for trying to add a value that already exists
		try {
			cell.add(forStorage);
			fail("Was expecting an IllegalStateException");
		}
		catch (IllegalStateException e) {}
		
		//Cannot add a notForStorage value
		try {
			cell.add(notForStorage);
			fail("Was expecting an IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {}
		
		//Scale tests
		int count = getScaleFrequency();
		for(int i = 0; i < count; i++){
			Value v2 = randomValueForStorage();
			while(cell.contains(v2)){
				v2 = randomValueForStorage();
			}
			cell.add(v2);
		}
	}
	
	@Test
	public void testContains(){
		Value v = randomValueForStorage();
		Cell cell = randomNewCell();
		assertFalse(cell.contains(v));
		cell.add(v);
		assertTrue(cell.contains(v));
	}
	
	@Test
	public void testCount(){
		Cell cell = randomNewCell();
		int count = getScaleFrequency();
		for(int i = 0; i < count; i++){
			int currentCount = cell.count();
			Value v = randomValueForStorage();
			while(cell.contains(v)){
				v = randomValueForStorage();
			}
			cell.add(v);
			assertEquals(currentCount+1, cell.count());
		}
		
		Iterator<Value> it = cell.getValues().iterator();
		while(it.hasNext()){
			int currentCount = cell.count();
			cell.remove(it.next());
			assertEquals(currentCount-1, cell.count());
		}
	}

}
