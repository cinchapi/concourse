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
package org.cinchapi.concourse.util;

import java.io.File;

import org.cinchapi.concourse.server.GlobalState;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;

/**
 * Contains methods to print log messages in the appropriate files.
 * 
 * @author jnelson
 */
public final class Logger {

	/**
	 * Print {@code message} with {@code params} to the INFO log.
	 * 
	 * @param message
	 * @param params
	 */
	public static void info(String message, Object... params) {
		INFO.info(message, params);
	}

	/**
	 * Print {@code message} with {@code params} to the ERROR log.
	 * 
	 * @param message
	 * @param params
	 */
	public static void error(String message, Object... params) {
		ERROR.error(message, params);
	}

	/**
	 * Print {@code message} with {@code params} to the WARN log.
	 * 
	 * @param message
	 * @param params
	 */
	public static void warn(String message, Object... params) {
		WARN.warn(message, params);
	}

	/**
	 * Print {@code message} with {@code params} to the DEBUG log.
	 * 
	 * @param message
	 * @param params
	 */
	public static void debug(String message, Object... params) {
		DEBUG.debug(message, params);
	}

	private static final String LOG_DIRECTORY = "log";
	private static final ch.qos.logback.classic.Logger ERROR = setup(
			"org.cinchapi.concourse.server.ErrorLogger", "error.log");
	private static final ch.qos.logback.classic.Logger WARN = setup(
			"org.cinchapi.concourse.server.WarnLogger", "warn.log");
	private static final ch.qos.logback.classic.Logger INFO = setup(
			"org.cinchapi.concourse.server.InfoLogger", "info.log");
	private static final ch.qos.logback.classic.Logger DEBUG = setup(
			"org.cinchapi.concourse.server.DebugLogger", "debug.log");

	/**
	 * Setup logger {@code name} that prints to {@code file}.
	 * 
	 * @param name
	 * @param file
	 * @return the logger
	 */
	private static ch.qos.logback.classic.Logger setup(String name, String file) {
		if(!GlobalState.ENABLE_CONSOLE_LOGGING) {
			ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
					.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
			root.detachAndStopAllAppenders();
		}
		LoggerContext context = (LoggerContext) LoggerFactory
				.getILoggerFactory();
		// Configure Pattern
		PatternLayoutEncoder encoder = new PatternLayoutEncoder();
		encoder.setPattern("%date [%thread] %level %class{36} - %msg%n");
		encoder.setContext(context);
		encoder.start();

		// Configure File Appender
		FileAppender<ILoggingEvent> appender = new FileAppender<ILoggingEvent>();
		appender.setFile(LOG_DIRECTORY + File.separator + file);
		appender.setEncoder(encoder);
		appender.setContext(context);
		appender.start();

		// Get Logger
		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory
				.getLogger(name);
		logger.addAppender(appender);
		logger.setLevel(GlobalState.LOG_LEVEL);
		logger.setAdditive(true);
		return logger;
	}

	/**
	 * Update the configuration of {@code logger} based on changes in the
	 * underlying prefs file.
	 * 
	 * @param logger
	 */
	@SuppressWarnings("unused")
	private static void update(ch.qos.logback.classic.Logger logger) {
		// TODO I need to actually reload the file from disk and check for
		// changes
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
				.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		if(!GlobalState.ENABLE_CONSOLE_LOGGING) {
			root.detachAndStopAllAppenders();
		}
		else {
			root.addAppender(new ConsoleAppender<ILoggingEvent>());
		}
		logger.setLevel(GlobalState.LOG_LEVEL);
	}

	private Logger() {} /* utility class */

}
