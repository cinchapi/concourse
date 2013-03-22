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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import com.cinchapi.concourse.service.ConcourseService;
import com.cinchapi.concourse.service.QueryService.Operator;
import com.cinchapi.concourse.store.CommitLog;
import com.cinchapi.concourse.store.Concourse;
import com.cinchapi.concourse.store.Transaction;
import com.cinchapi.concourse.structure.Commit;
import com.cinchapi.concourse.structure.Key;


/**
 * 
 * 
 * @author jnelson
 */
public class Test {
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		Concourse service = Concourse.start("concourse.prefs");
		long row = service.add("name", "Johnson");
		Transaction t = service.startTransaction();
		t.add("friend", Key.fromLong(230));
		t.remove("name", "Johnson", row);
//		service.remove("name", "Bobby", row);
		t.commit();
		System.out.println(service.query("name", Operator.EQUALS, "Johnson")); 
		service.shutdown();

		
	}

}
