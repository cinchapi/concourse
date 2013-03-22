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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.time.StopWatch;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.BaseBenchmark;
import com.cinchapi.concourse.store.structure.Benchmark;
import com.cinchapi.concourse.structure.Commit;
import com.cinchapi.concourse.structure.Value;

/**
 * Benchmarking for {@link Value}.
 * 
 * @author jnelson
 */
public class ValueBenchmark extends BaseBenchmark {
	

	@Test
	@Benchmark
	public void testBenchmarkWriteToDisk() throws IOException {
		int size = 500000;
		TimeUnit unit = TimeUnit.MILLISECONDS;
		String filename = "value_write_benchmark_"+Time.now();
		RandomAccessFile file = file(filename);
		
		Value[] values = new Value[size];
		int numBytes = 0;
		log("Creating {} Values...", format.format(size));
		for (int i = 0; i < size; i++) {
			Value value = randomValueForStorage();
			numBytes += value.size();
			values[i] = value;
		}
		ByteBuffer buffer = ByteBuffer.allocate(numBytes);

		
		log("Writing {} total BYTES to {}...", format.format(numBytes),
				filename);
		for (int i = 0; i < size; i++) {
			Value value = values[i];
			buffer.put(value.getBytes());
		}
		buffer.rewind();
		timer().start();
		file.getChannel().write(buffer); //i want to batch writes in memory before flushing to disk
		long elapsed = timer().stop(unit);
		long bytesPerUnit = numBytes / elapsed;

		log("Total write time was {} {} with {} bytes written per {}",
				format.format(elapsed), unit, format.format(bytesPerUnit), unit
						.toString().substring(0, unit.toString().length() - 1));

	}
	
	@Test
	public void testValueCreationTime(){
		int target = 100000;
		Object[] quantities = new Object[target];

		for (int count = 0; count < target; count++) {
			quantities[count] = randomObject();
		}
		StopWatch timer = timer();
		timer.start();
		for(int i = 0; i < target; i++){
			Value.forStorage(quantities[i]);
		}
		TimeUnit unit = TimeUnit.MILLISECONDS;
		long elapsed = timer.stop(unit);
		log("Created {} values in {} {}", target, elapsed, unit);
	}

}
