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

import org.junit.Test;

import com.cinchapi.concourse.util.Mocks;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Tests;

import junit.framework.TestCase;

/**
 * Unit tests for {@link CellHistory}
 * 
 * @author jnelson
 */
public class CellHistoryTest extends TestCase {

	private CellHistory getInstance() {
		return CellHistory.createEmpty();
	}

	private Value getRandomValueForStorage() {
		return Mocks.getValueForStorage();
	}

	private Value getRandomValueNotForStorage() {
		return Mocks.getValueNotForStorage();
	}

	private Value getValueForStorage(Object value) {
		return Mocks.getValue(value, Tests.currentTime());
	}

	private Value getValueNotForStorage(Object value) {
		return Mocks.getValue(value, 0);
	}

	@Test
	public void testCount() {
		CellHistory history = getInstance();
		Object value = Tests.randomValue();

		// count is 0 before any history for value is recorded
		assertEquals(0, history.count(getValueForStorage(value)));

		// count is incremented as history for value is record
		int freq = Tests.randomScaleFreq();
		for (int i = 0; i < freq; i++) {
			Value v = Numbers.isEven(i) ? getValueForStorage(value)
					: getValueNotForStorage(value);
			history.log(v);
		}
		assertEquals(freq, history.count(getValueForStorage(value)));

		// count before a timestamp works as expected
		long timestamp = Tests.currentTime();
		int freq2 = Tests.randomScaleFreq();
		for (int i = freq; i < freq + freq2; i++) {
			Value v = Numbers.isEven(i) ? getValueForStorage(value)
					: getValueNotForStorage(value);
			history.log(v);
			int more = Tests.randomInt(Tests.randomScaleFreq());
			for (int j = 0; j < more; j++) {
				Object value2 = Tests.randomValue();
				while(value2.equals(value)){
					value2 = Tests.randomValue();
				}
				if(history.exists(getValueForStorage(value2))){
					history.log(getValueNotForStorage(value2));
				}
				else{
					history.log(getValueForStorage(value2));
				}
			}
		}
		assertEquals(freq + freq2, history.count(getValueForStorage(value)));
		assertEquals(freq, history.count(getValueForStorage(value)), timestamp);
	}
	
	public void testExists(){
		CellHistory history = getInstance();
		Object value = Tests.randomValue();
		
		//value does not exist if never logged
		assertFalse(history.exists(getValueForStorage(value)));
		
		//value exists if count is odd and does not exist if count is even
		int freq = Tests.randomScaleFreq();
		for(int i = 0; i < freq; i++){
			Value v = Numbers.isEven(i) ? getValueForStorage(value)
					: getValueNotForStorage(value);
			history.log(v);
			if(Numbers.isEven(history.count(v))){
				assertFalse(history.exists(v));
			}
			else{
				assertTrue(history.exists(v));
			}
		}
	}

}
