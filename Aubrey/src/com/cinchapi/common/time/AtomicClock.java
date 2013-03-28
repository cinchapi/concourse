package com.cinchapi.common.time;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock that gives as close to the current time as possible without
 * duplicates.
 * 
 * @author jnelson
 * 
 */
public class AtomicClock {

	private final AtomicLong atom = new AtomicLong();

	/**
	 * Get the current timestamp (in microseconds), which is guaranteed to be
	 * unique.
	 * 
	 * @return the timestamp.
	 * @see http 
	 *      ://stackoverflow.com/questions/9191288/creating-a-unique-timestamp
	 *      -in-java
	 */
	public synchronized Long time() {
		long now = System.currentTimeMillis() * 1000;
		while (true) {
			long lastTime = atom.get();
			if(lastTime >= now) {
				now = lastTime + 1;
			}
			if(atom.compareAndSet(lastTime, now)) {
				return now;
			}
		}
	}
}
