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
package org.cinchapi.concourse.server.util;

import java.util.Map;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;

/**
 * Contains functions for dealing with logging programmatically.
 * 
 * @author jnelson
 */
public final class Loggers {

	/**
	 * Return a Logger that is initialized for standard logging.
	 * 
	 * @return the standard Logger
	 */
	public static Logger getLogger() {
		return getLogger("concourse", "log/concourse.log", Level.INFO);
	}
	/**
	 * Return a logger for {@code name} that writes to {@code file} at
	 * {@code level}. This method will reuse existing cached instances located
	 * in {@code #cache} if possible. Otherwise, a new Logger is created and
	 * cached.
	 * 
	 * @param name
	 * @param file
	 * @param level
	 * @return the Logger
	 */
	private static Logger getLogger(String name, String file, Level level) {
		Logger logger = CACHE.get(name);
		if(logger == null) {
			LoggerContext context = (LoggerContext) LoggerFactory
					.getILoggerFactory();
			// Configure Pattern
			PatternLayoutEncoder encoder = new PatternLayoutEncoder();
			encoder.setPattern("%date [%thread] %level %class{36} - %msg%n");
			encoder.setContext(context);
			encoder.start();

			// Configure Appender
			FileAppender<ILoggingEvent> appender = new FileAppender<ILoggingEvent>();
			appender.setFile(file);
			appender.setEncoder(encoder);
			appender.setContext(context);
			appender.start();

			// Get Logger
			logger = (Logger) LoggerFactory.getLogger(name);
			logger.addAppender(appender);
			logger.setLevel(level);
			logger.setAdditive(true);
			CACHE.put(name, logger);
		}
		return logger;
	}

	/**
	 * A cache to prevent duplicate loggers from being created.
	 */
	private static Map<String, Logger> CACHE = Maps.newHashMap();

	static {
		// Performance Logger
		String perf = "org.cinchapi.concourse.server.engine.PerformanceLogger";
		CACHE.put(perf, getLogger(perf, "log/performance.log", Level.INFO));
		
		// Standard Logger
		String standard = "concourse";
		CACHE.put(standard, getLogger(standard, "log/concourse.log", Level.INFO));
	}

	private Loggers() {} /* utility class */

}
