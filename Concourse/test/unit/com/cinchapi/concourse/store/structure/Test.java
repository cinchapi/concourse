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
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.cinchapi.common.time.StopWatch;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.internal.Commit;
import com.cinchapi.concourse.internal.CommitLog;
import com.cinchapi.concourse.internal.Concourse;
import com.cinchapi.concourse.internal.ConcourseService;
import com.cinchapi.concourse.internal.Key;
import com.cinchapi.concourse.internal.Row;
import com.cinchapi.concourse.internal.Transaction;
import com.cinchapi.concourse.internal.Value;
import com.cinchapi.concourse.internal.QueryService.Operator;


/**
 * 
 * 
 * @author jnelson
 */
public class Test {
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
//		Concourse service = Concourse.start("concourse.prefs");
//		service.add("name", "Jeff Nelson", 1);
//		service.add("name", "Jeff Nelson", 2);
//		service.add("age", 25, 1);
//		service.add("age", 30, 2);
//		service.remove("name", "Jeff Nelson", 2);
//		service.remove("age", 25, 1);
//		System.out.println(service.query("name", Operator.EQUALS, "Jeff Nelson"));
//		System.out.println(service.query("age", Operator.GREATER_THAN_OR_EQUALS, 25));
//		service.shutdown();
		
		StopWatch timer = new StopWatch();
//		timer.start();
//		Row row = Row.load(Key.fromLong(Time.now()), "/Users/jnelson/concourse2/db");
//		long elapsed = timer.stop(TimeUnit.MILLISECONDS);
//		System.out.println(elapsed +" ms");
//		row.add("time", Value.forStorage(Time.now()));
//		row.fsync();
		
		timer.start();
		Row row = Row.identifiedBy(Key.fromLong(Time.now()), "/Users/jnelson/concourse2/db/rows");
		long elapsed = timer.stop(TimeUnit.MILLISECONDS);
		System.out.println(elapsed +" ms");
		row.fsync();
		
//		row.add("name", Value.forStorage("Jeff Nelson"));
//		row.add("name", Value.forStorage(23));
//		row.fsync();
//		List<Value> values = row.fetch("name").getValues();
//		for(Value v : values){
//			System.out.println(v.getQuantity());
//		}
		
	}

}
