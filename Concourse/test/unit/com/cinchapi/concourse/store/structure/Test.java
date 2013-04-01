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
import java.util.concurrent.TimeUnit;

import com.cinchapi.common.time.StopWatch;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConcourseController;
import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.engine.Engine;
import com.cinchapi.concourse.engine.Key;
import com.cinchapi.concourse.engine.Transaction;
import com.cinchapi.concourse.engine.QueryService.Operator;



/**
 * 
 * 
 * @author jnelson
 */
public class Test {
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		Concourse concourse = Concourse.useEmbeddedServer();
		
	}

}
