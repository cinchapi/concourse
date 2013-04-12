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

	private final AtomicLong time = new AtomicLong();

	/**
	 * Get the current timestamp (in microseconds), which is guaranteed to be
	 * unique.
	 * 
	 * @return the timestamp.
	 * @see http
	 *      ://stackoverflow.com/questions/9191288/creating-a-unique-timestamp
	 *      -in-java
	 */
	public long time() {
		long now = System.currentTimeMillis() * 1000;
		while (true) {
			long lastTime = time.get();
			if(lastTime >= now) {
				now = lastTime + 1;
			}
			if(time.compareAndSet(lastTime, now)) {
				return now;
			}
		}
	}
}
