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
package com.cinchapi.concourse.commitlog;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import com.cinahpi.concourse.ConcourseServiceTest;
import com.cinchapi.concourse.store.temp.CommitLog;
/**
 * Unit tests for {@link CommitLog}.
 * 
 * @author jnelson
 */
public class CommitLogTest extends ConcourseServiceTest {

	private static final String location = "test/commitlog";
	private static final int size = 1024 * 1024;

	@Override
	protected CommitLog getService() {
		try {
			return CommitLog.newInstance(location, size);
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Test
	public void testIsFull(){
		CommitLog service = getService();
		while(!service.isFull()){
			try{
				service.add(randomLong(), randomStringNoSpaces(), randomObject());
			}
			catch(IllegalStateException e){
				log("{}", e);
				break;
			}
		}
		assertTrue(service.isFull());
		assertTrue(service.size() <= size);
	}

}
