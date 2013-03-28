package com.cinchapi.common.math;

import java.io.Serializable;

/**
 * A simple counter that sequentially increments itself by a {@code step} from
 * its starting place. The largest reachable number is
 * 9,223,372,036,854,775,807.
 */
public class Counter implements Serializable {

	private static final long serialVersionUID = 1L;

	private int step;
	private long next;

	/**
	 * Create a {@link Counter} that starts at 1 and is incremented by 1.
	 */
	public Counter() {
		this(1, 1);
	}

	/**
	 * Create a {@link Counter}.
	 * 
	 * @param start
	 * @param step
	 */
	public Counter(long start, int step) {
		this.next = start;
		this.step = step;
	}

	/**
	 * Get the {@code next} number and increment by the {@code step}.
	 * 
	 * @return the {@code next} number.
	 */
	public long next() {
		long count = this.next;
		this.next += this.step;
		return count;
	}

	/**
	 * Peek at the {@code next} number, but DO NOT increment the {@link Counter}
	 * .
	 * 
	 * @return the {@code next} number.
	 */
	public long peek() {
		return this.next;
	}
}
