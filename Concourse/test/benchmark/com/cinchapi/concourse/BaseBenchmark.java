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
package com.cinchapi.concourse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.text.NumberFormat;

import org.junit.BeforeClass;

import com.cinahpi.concourse.BaseTest;
import com.cinchapi.common.time.StopWatch;

/**
 * Base class for all benchmark tests
 * 
 * @author jnelson
 */
public class BaseBenchmark extends BaseTest {

	protected final String outputBaseDir = "test/benchmarks/";
	private final StopWatch timer = new StopWatch();
	protected final NumberFormat format = NumberFormat.getNumberInstance();
	
	/**
	 * Construct a new instance.
	 */
	public BaseBenchmark(){
		new File(outputBaseDir).mkdir();
		format.setGroupingUsed(true);
	}
	
	@BeforeClass
	public void setUp(){
		Runtime rt = Runtime.getRuntime();
		log("The HEAP SIZE is {} bytes", format.format(rt.maxMemory()));
	}

	/**
	 * Return the {@link StopWatch}.
	 * 
	 * @return the timer.
	 */
	protected final StopWatch timer() {
		return timer;
	}

	/**
	 * Get a {@link RandomAccessFile} called {@code name} in read/write
	 * {@code mode}.
	 * 
	 * @param name
	 * @return the random access file
	 */
	protected RandomAccessFile file(String name) {
		return file(name, "rw");
	}

	/**
	 * Get a {@link RandomAccessFile} called {@name}.
	 * 
	 * @param name
	 *            - only specify the file name, the path will be taken care of
	 *            automatically
	 * @param mode
	 * @return the random access file
	 */
	protected RandomAccessFile file(String name, String mode) {
		try {
			return new RandomAccessFile(outputBaseDir + name, mode);
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

}
