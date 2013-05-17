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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 
 * 
 * @author jnelson
 */
class Repository {
	
	private final ExecutorService executor = Executors.newCachedThreadPool();
	
	private boolean add(final PrimaryKey key, final SuperString field, final Value value){
		Future<?> task1 = executor.submit(Tasks.addToRecord(key, field, value));
		
		Future<?> task2 = executor.submit(new Runnable(){
			
		});
		Collection.fromKey(key).add(field, value);
		SecondaryIndex.fromName(field).add(value, key);
		return true;
	}

}
