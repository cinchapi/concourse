/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.cinchapi.concourse.annotate.UtilityClass;
import org.cinchapi.concourse.util.Loggers;
import org.slf4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A collection of thread and concurrency related utility methods.
 * 
 * @author jnelson
 */
@UtilityClass
public final class ConcourseExecutors {

	private static final Logger log = Loggers.getLogger();

	/**
	 * Catches exceptions thrown from pooled threads. For the Database,
	 * exceptions will occur in the event that an attempt is made to write a
	 * duplicate non-offset write when the system shuts down in the middle of a
	 * buffer flush. Those exceptions can be ignored, so we catch them here and
	 * print log statements.
	 */
	private static final UncaughtExceptionHandler uncaughtExceptionHandler;
	static {
		uncaughtExceptionHandler = new UncaughtExceptionHandler() {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				log.warn("Uncaught exception in thread '{}'. This possibly "
						+ "indicates that the system shutdown prematurely "
						+ "during a buffer transport operation.", t);
				log.warn("", e);

			}

		};
	}

	/**
	 * Return a {@link ExecutorService} thread pool with {@code num} threads,
	 * each whose name is prefixed with {@code threadNamePrefix}.
	 * 
	 * @param num
	 * @param threadNamePrefix
	 * @return a new thread pool
	 */
	public static ExecutorService newThreadPool(int num, String threadNamePrefix) {
		return Executors.newFixedThreadPool(num,
				getThreadFactory(threadNamePrefix));
	}

	public static ExecutorService newCachedThreadPool(String threadNamePrefix) {
		return Executors
				.newCachedThreadPool(getThreadFactory(threadNamePrefix));
	}

	private static ThreadFactory getThreadFactory(String threadNamePrefix) {
		return new ThreadFactoryBuilder()
				.setNameFormat(threadNamePrefix + "-%d")
				.setUncaughtExceptionHandler(uncaughtExceptionHandler).build();
	}

	/**
	 * Create an {@link ExecutorService} thread pool with enough threads to
	 * execute {@code commands} and block until all the tasks have completed.
	 * 
	 * @param threadNamePrefix
	 * @param commands
	 */
	public static void executeAndAwaitTermination(String threadNamePrefix,
			Runnable... commands) {
		executeAndAwaitTermination(
				newThreadPool(commands.length, threadNamePrefix), commands);
	}

	/**
	 * Execute {@code commands} using {@code executor} and block until all
	 * tasks have completed.
	 * 
	 * @param executor
	 * @param commands
	 */
	public static void executeAndAwaitTermination(ExecutorService executor,
			Runnable... commands) {
		for (Runnable command : commands) {
			executor.execute(command);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
			continue; // block until all tasks have completed
		}
	}

	private ConcourseExecutors() {/* utility-class */}

}
